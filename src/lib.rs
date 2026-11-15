//! Redis's driver for SpelaionDB: Redis and the servers that speak its protocol (Valkey),
//! served as a plugin (SpelaionDB's ADR-041, ADR-056).
//!
//! It is sent **requests**, never statements (protocol 1.1): SpelaionDB's core reads each line
//! the user typed and sends its argument list, `{"args": ["HGETALL", "user:1"]}`, which this
//! driver sends to the server as it is. Replies are laid out as rows and columns
//! (`replies.rs`).
//!
//! The core opens every route to the server (tunnels, proxies, Kubernetes): this driver
//! connects only to the address it is given, never to a cluster's other nodes or a Sentinel.
//!
//! A cancel closes the connection the command runs on and opens another in the same database:
//! the server stops waiting for a command that blocks, and nothing else ran on that connection
//! that could be lost (the core refuses transactions and subscriptions).

mod replies;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream;
use redis::aio::MultiplexedConnection;
use redis::{
    AsyncConnectionConfig, Client, ClientTlsConfig, ConnectionAddr, ConnectionInfo,
    IntoConnectionInfo, ProtocolVersion, RedisConnectionInfo, RedisError, TlsCertificates,
    Value as Reply,
};
use serde_json::Value;
use spelaion_driver_api::{
    CancelHandle, Capabilities, ConnectParams, Connection, Driver, DriverError, EventStream,
    ExecEvent, ExecOptions, KeepAlive, Notice, ObjectInfo, ObjectKind, Result, SchemaColumn,
    SchemaInfo, TableInfo, TlsMode,
};
use tokio::sync::{Notify, mpsc};

pub use replies::{Table, escaped};

/// The most keys the explorer lists in a database: it reads them with `SCAN`, a page at a
/// time, and stops here. Nothing the app runs on its own walks a whole keyspace.
pub const MAX_KEYS: usize = 1000;

/// How many keys one `SCAN` asks for, and how many pages it reads at most.
const SCAN_COUNT: usize = 500;
const SCAN_PAGES: usize = 50;

/// The `redis` engine.
#[derive(Debug, Default, Clone)]
pub struct RedisDriver;

#[async_trait]
impl Driver for RedisDriver {
    /// The name SpelaionDB's configuration uses: `engine = "redis"`.
    fn engine(&self) -> &str {
        "redis"
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        if params.read_only {
            // SpelaionDB's core does not open such a connection (ADR-056, point 6); if it ever
            // did, nothing here could keep it read-only.
            return Err(DriverError::Connection(
                "Redis has no read-only mode in SpelaionDB yet".into(),
            ));
        }
        if let Some(key) = params.options.keys().next() {
            return Err(DriverError::Connection(format!(
                "Redis has no connection setting `{key}` here"
            )));
        }
        let database = database_number(&params.database)?;
        let client = client(params)?;
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(params.connect_timeout()))
            // A command runs as long as it takes: the core's statement timeout and cancel stop
            // it, not the client.
            .set_response_timeout(None);
        let connection = open(&client, &config, database, params.connect_timeout()).await?;
        Ok(Box::new(RedisConnection {
            shared: Arc::new(Shared {
                client,
                config,
                connect_timeout: params.connect_timeout(),
                database: Mutex::new(database),
                connection: Mutex::new(connection),
            }),
            capabilities: Capabilities {
                cancel: true,
                schemas: true,
                ..Capabilities::default()
            },
            running: Arc::new(Mutex::new(None)),
        }))
    }
}

/// The database a connection names: a number, `0` when it names none.
fn database_number(database: &str) -> Result<i64> {
    if database.trim().is_empty() {
        return Ok(0);
    }
    database
        .trim()
        .parse::<i64>()
        .ok()
        .filter(|n| *n >= 0)
        .ok_or_else(|| {
            DriverError::Connection(format!(
                "a Redis database is a number (0, 1, 2…), not `{database}`"
            ))
        })
}

/// The client for the address, login and TLS `connect` gave.
fn client(params: &ConnectParams) -> Result<Client> {
    let (host, port) = match params.connect_addr {
        Some(addr) => (addr.ip().to_string(), addr.port()),
        None => (params.host.clone(), params.port),
    };
    let tunnelled = params.connect_addr.is_some();
    let mut redis = RedisConnectionInfo::default()
        .set_protocol(ProtocolVersion::RESP3)
        .set_lib_name("spelaiondb", env!("CARGO_PKG_VERSION"));
    if !params.user.is_empty() {
        redis = redis.set_username(&params.user);
    }
    if let Some(password) = &params.password {
        redis = redis.set_password(password.expose());
    }
    let (addr, certificates) = match params.tls {
        // Redis has no "encrypt if the server can": `prefer` connects without TLS.
        TlsMode::Disable | TlsMode::Prefer => (ConnectionAddr::Tcp(host, port), None),
        TlsMode::Require => (
            ConnectionAddr::TcpTls {
                host,
                port,
                insecure: true,
                tls_params: None,
            },
            None,
        ),
        // rustls checks the name of the address it connects to: `verify-ca` checks it too
        // (stricter, never looser), and through a tunnel, where that address is this
        // machine's, neither can check the server.
        TlsMode::VerifyCa | TlsMode::VerifyFull => {
            if tunnelled {
                return Err(DriverError::Connection(
                    "Redis's client checks the certificate against the address it connects to, \
                     which through a tunnel is this machine's: use `require` (encrypted, the \
                     certificate not checked) or connect without a tunnel"
                        .into(),
                ));
            }
            let read = |path: &std::path::Path, what: &str| {
                std::fs::read(path).map_err(|e| {
                    DriverError::Connection(format!(
                        "the {what} {} cannot be read: {e}",
                        path.display()
                    ))
                })
            };
            let root_cert = match &params.ca_certificate {
                Some(path) => Some(read(path, "CA certificate")?),
                None => None,
            };
            let client_tls = match (&params.client_certificate, &params.client_key) {
                (Some(cert), Some(key)) => Some(ClientTlsConfig {
                    client_cert: read(cert, "client certificate")?,
                    client_key: read(key, "client key")?,
                }),
                (None, None) => None,
                _ => {
                    return Err(DriverError::Connection(
                        "a client certificate needs its key, and a key its certificate".into(),
                    ));
                }
            };
            let certificates =
                (root_cert.is_some() || client_tls.is_some()).then_some(TlsCertificates {
                    client_tls,
                    root_cert,
                });
            (
                ConnectionAddr::TcpTls {
                    host,
                    port,
                    insecure: false,
                    tls_params: None,
                },
                certificates,
            )
        }
    };
    let info: ConnectionInfo = addr
        .into_connection_info()
        .map_err(connection_error)?
        .set_redis_settings(redis);
    match certificates {
        Some(certificates) => Client::build_with_tls(info, certificates),
        None => Client::open(info),
    }
    .map_err(connection_error)
}

/// A connection in `database`, or why there is none within `timeout`.
async fn open(
    client: &Client,
    config: &AsyncConnectionConfig,
    database: i64,
    timeout: Duration,
) -> Result<MultiplexedConnection> {
    let attempt = async {
        let mut connection = client
            .get_multiplexed_async_connection_with_config(config)
            .await
            .map_err(connection_error)?;
        if database != 0 {
            redis::cmd("SELECT")
                .arg(database)
                .query_async::<()>(&mut connection)
                .await
                .map_err(connection_error)?;
        }
        Ok(connection)
    };
    match tokio::time::timeout(timeout, attempt).await {
        Ok(outcome) => outcome,
        Err(_) => Err(DriverError::Connection(format!(
            "the server did not answer within {} s",
            timeout.as_secs()
        ))),
    }
}

/// What a session's requests and its cancel share.
struct Shared {
    client: Client,
    config: AsyncConnectionConfig,
    connect_timeout: Duration,
    database: Mutex<i64>,
    connection: Mutex<MultiplexedConnection>,
}

impl Shared {
    fn connection(&self) -> MultiplexedConnection {
        self.connection.lock().expect("lock poisoned").clone()
    }

    fn database(&self) -> i64 {
        *self.database.lock().expect("lock poisoned")
    }

    /// A new connection in the session's database, in place of the one a cancelled command
    /// ran on: dropping the last handle of that one closes it.
    async fn reopen(&self) -> Result<()> {
        let fresh = open(
            &self.client,
            &self.config,
            self.database(),
            self.connect_timeout,
        )
        .await?;
        *self.connection.lock().expect("lock poisoned") = fresh;
        Ok(())
    }

    /// A short-lived connection in another database, so the session's own never changes.
    async fn in_database(&self, database: i64) -> Result<MultiplexedConnection> {
        open(&self.client, &self.config, database, self.connect_timeout).await
    }
}

struct RedisConnection {
    shared: Arc<Shared>,
    capabilities: Capabilities,
    /// Told when the request that is running must stop.
    running: Arc<Mutex<Option<Arc<Notify>>>>,
}

/// The argument list of a request, checked.
fn arguments(request: Value) -> Result<Vec<String>> {
    let Value::Object(mut fields) = request else {
        return Err(bad_request("it is not an object"));
    };
    let args = match fields.remove("args") {
        Some(Value::Array(args)) if !args.is_empty() => args,
        _ => return Err(bad_request("it has no arguments")),
    };
    if let Some(other) = fields.keys().next() {
        return Err(bad_request(&format!("it has a field `{other}`")));
    }
    args.into_iter()
        .map(|arg| match arg {
            Value::String(text) => Ok(text),
            _ => Err(bad_request("an argument is not text")),
        })
        .collect()
}

fn bad_request(why: &str) -> DriverError {
    DriverError::Other(format!(
        "the request SpelaionDB sent is not a Redis command: {why}"
    ))
}

#[async_trait]
impl Connection for RedisConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn execute(&self, _sql: &str, _opts: &ExecOptions) -> Result<EventStream> {
        Err(DriverError::Other(
            "Redis runs requests, not statements: SpelaionDB's core sends each line as its \
             arguments"
                .into(),
        ))
    }

    async fn execute_request(&self, request: Value, opts: &ExecOptions) -> Result<EventStream> {
        let args = arguments(request)?;
        let cancelled = Arc::new(Notify::new());
        *self.running.lock().expect("lock poisoned") = Some(cancelled.clone());
        let shared = self.shared.clone();
        let batch_size = opts.batch_size.max(1);
        // One event at a time: the core's credit decides when the next is read.
        let (sender, receiver) = mpsc::channel::<Result<ExecEvent>>(1);
        tokio::spawn(async move {
            let mut connection = shared.connection();
            let mut command = redis::cmd(&args[0]);
            for arg in &args[1..] {
                command.arg(arg);
            }
            let reply = tokio::select! {
                biased;
                () = cancelled.notified() => {
                    // The command may still be waiting on the server: close its connection.
                    drop(connection);
                    let error = match shared.reopen().await {
                        Ok(()) => DriverError::Cancelled,
                        Err(error) => error,
                    };
                    let _ = sender.send(Err(error)).await;
                    return;
                }
                reply = command.query_async::<Reply>(&mut connection) => reply,
            };
            let reply = match reply {
                Ok(Reply::ServerError(error)) => Err(server_error(&error)),
                Ok(reply) => Ok(reply),
                Err(error) => Err(command_error(error)),
            };
            let table = match reply {
                Ok(reply) => Table::of(&args[0], reply),
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            };
            let schema = table.schema();
            if sender
                .send(Ok(ExecEvent::Columns(schema.clone())))
                .await
                .is_err()
            {
                return;
            }
            let mut from = 0;
            while from < table.len() {
                let to = (from + batch_size).min(table.len());
                let batch = table.batch(&schema, from, to);
                let failed = batch.is_err();
                if sender.send(batch.map(ExecEvent::Batch)).await.is_err() || failed {
                    return;
                }
                from = to;
            }
            if let Some(message) = &table.notice {
                let notice = Notice {
                    severity: "INFO".into(),
                    message: message.clone(),
                };
                if sender.send(Ok(ExecEvent::Notice(notice))).await.is_err() {
                    return;
                }
            }
            let _ = sender
                .send(Ok(ExecEvent::Done {
                    rows_affected: None,
                }))
                .await;
        });
        Ok(Box::pin(stream::unfold(
            receiver,
            |mut receiver| async move { receiver.recv().await.map(|event| (event, receiver)) },
        )))
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(Cancel(self.running.clone()))
    }

    async fn server_version(&self) -> Result<String> {
        let info: String = redis::cmd("INFO")
            .arg("server")
            .query_async(&mut self.shared.connection())
            .await
            .map_err(command_error)?;
        Ok(server_version(&info))
    }

    async fn keep_alive(&self) -> Result<KeepAlive> {
        redis::cmd("PING")
            .query_async::<()>(&mut self.shared.connection())
            .await
            .map_err(connection_error)?;
        Ok(KeepAlive::Alive)
    }

    async fn current_database(&self) -> Result<Option<String>> {
        Ok(Some(self.shared.database().to_string()))
    }

    async fn database_context(&self) -> Result<Option<String>> {
        self.current_database().await
    }

    async fn select_database(&self, database: &str) -> Result<()> {
        let number = database_number(database)?;
        redis::cmd("SELECT")
            .arg(number)
            .query_async::<()>(&mut self.shared.connection())
            .await
            .map_err(command_error)?;
        *self.shared.database.lock().expect("lock poisoned") = number;
        Ok(())
    }

    /// Every database the server has (`CONFIG GET databases`), or, where that is not allowed
    /// (a managed server, an ACL), the ones that hold keys and this session's.
    async fn list_databases(&self) -> Result<Vec<String>> {
        let mut connection = self.shared.connection();
        // A map in RESP3 (`{databases: 16}`), a pair in RESP2.
        let configured: std::result::Result<Reply, RedisError> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("databases")
            .query_async(&mut connection)
            .await;
        let count = match configured {
            Ok(Reply::Map(pairs)) => pairs.into_iter().next().map(|(_, v)| v),
            Ok(Reply::Array(mut pair)) if pair.len() == 2 => pair.pop(),
            _ => None,
        }
        .and_then(|v| redis::from_redis_value::<String>(v).ok())
        .and_then(|n| n.parse::<i64>().ok());
        if let Some(count) = count
            && (1..=1024).contains(&count)
        {
            return Ok((0..count).map(|n| n.to_string()).collect());
        }
        Ok(self
            .used_databases()
            .await?
            .into_iter()
            .map(|n| n.to_string())
            .collect())
    }

    /// A database is what the explorer shows as a schema: the ones that hold keys, and this
    /// session's.
    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        Ok(self
            .used_databases()
            .await?
            .into_iter()
            .map(|n| SchemaInfo {
                name: n.to_string(),
                system: false,
            })
            .collect())
    }

    /// The database's keys, at most [`MAX_KEYS`], each with its type as its comment, read a
    /// `SCAN` page at a time on a connection of their own.
    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        let database = database_number(schema)?;
        let mut connection = self.shared.in_database(database).await?;
        let keys = scan_keys(&mut connection).await?;
        let mut types = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(100) {
            let mut pipe = redis::pipe();
            for key in chunk {
                pipe.cmd("TYPE").arg(key);
            }
            let found: Vec<String> = pipe
                .query_async(&mut connection)
                .await
                .map_err(command_error)?;
            types.extend(found);
        }
        let mut objects: Vec<ObjectInfo> = keys
            .into_iter()
            .zip(types)
            // A key that expired between the two is gone.
            .filter(|(_, t)| t != "none")
            .map(|(key, key_type)| ObjectInfo {
                schema: schema.to_string(),
                name: key,
                kind: ObjectKind::Collection,
                estimated_rows: None,
                comment: Some(key_type),
            })
            .collect();
        objects.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(objects)
    }

    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        let database = database_number(schema)?;
        let mut connection = self.shared.in_database(database).await?;
        let key_type: String = redis::cmd("TYPE")
            .arg(name)
            .query_async(&mut connection)
            .await
            .map_err(command_error)?;
        if key_type == "none" {
            return Err(DriverError::Other(format!(
                "there is no key {name} in database {schema}"
            )));
        }
        Ok(TableInfo {
            object: ObjectInfo {
                schema: schema.to_string(),
                name: name.to_string(),
                kind: ObjectKind::Collection,
                estimated_rows: None,
                comment: Some(key_type),
            },
            columns: Vec::new(),
            primary_key: Vec::new(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            constraints: Vec::new(),
        })
    }

    async fn list_columns(&self, _schema: &str) -> Result<Vec<SchemaColumn>> {
        // A key has no columns.
        Ok(Vec::new())
    }
}

impl RedisConnection {
    /// The databases `INFO keyspace` names (those that hold keys) and this session's, in order.
    async fn used_databases(&self) -> Result<Vec<i64>> {
        let info: String = redis::cmd("INFO")
            .arg("keyspace")
            .query_async(&mut self.shared.connection())
            .await
            .map_err(command_error)?;
        let mut databases = keyspace_databases(&info);
        databases.push(self.shared.database());
        databases.sort_unstable();
        databases.dedup();
        Ok(databases)
    }
}

/// The keys of a connection's database, a `SCAN` page at a time, at most [`MAX_KEYS`].
async fn scan_keys(connection: &mut MultiplexedConnection) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut cursor = "0".to_string();
    for _ in 0..SCAN_PAGES {
        let (next, page): (String, Vec<Vec<u8>>) = redis::cmd("SCAN")
            .arg(&cursor)
            .arg("COUNT")
            .arg(SCAN_COUNT)
            .query_async(connection)
            .await
            .map_err(command_error)?;
        // A key that is not text is not listed: the explorer names objects with text.
        keys.extend(page.into_iter().filter_map(|k| String::from_utf8(k).ok()));
        if next == "0" || keys.len() >= MAX_KEYS {
            break;
        }
        cursor = next;
    }
    keys.sort();
    keys.dedup();
    keys.truncate(MAX_KEYS);
    Ok(keys)
}

/// `db0:keys=1,expires=0` lines of `INFO keyspace`, as numbers.
fn keyspace_databases(info: &str) -> Vec<i64> {
    info.lines()
        .filter_map(|line| line.strip_prefix("db")?.split_once(':')?.0.parse().ok())
        .collect()
}

/// The version `INFO server` reports: Valkey's own when it is Valkey.
fn server_version(info: &str) -> String {
    let field = |name: &str| {
        info.lines()
            .find_map(|line| line.strip_prefix(name))
            .map(|v| v.trim().to_string())
    };
    match field("valkey_version:") {
        Some(version) => format!("Valkey {version}"),
        None => field("redis_version:").unwrap_or_default(),
    }
}

fn server_error(error: &redis::ServerError) -> DriverError {
    DriverError::Database {
        code: Some(error.code().to_string()),
        message: error.details().unwrap_or(error.code()).to_string(),
        detail: None,
        hint: None,
        position: None,
    }
}

/// An error of a command the server refused, with its code; one reaching it as a connection's.
fn command_error(error: RedisError) -> DriverError {
    if error.is_io_error() || error.is_connection_dropped() || error.is_timeout() {
        return connection_error(error);
    }
    match error.code() {
        Some(code) => DriverError::Database {
            code: Some(code.to_string()),
            message: error.detail().unwrap_or(code).to_string(),
            detail: None,
            hint: None,
            position: None,
        },
        None => DriverError::Other(error.to_string()),
    }
}

/// An error reaching or logging in to the server.
fn connection_error(error: RedisError) -> DriverError {
    match error.code() {
        Some(code @ ("WRONGPASS" | "NOAUTH" | "NOPERM")) => DriverError::Connection(format!(
            "the server refused the login: {}",
            error.detail().unwrap_or(code)
        )),
        _ => DriverError::Connection(error.to_string()),
    }
}

struct Cancel(Arc<Mutex<Option<Arc<Notify>>>>);

#[async_trait]
impl CancelHandle for Cancel {
    async fn cancel(&self) -> Result<()> {
        // `notify_one` keeps the wake-up if the request has not started waiting yet.
        if let Some(running) = self.0.lock().expect("lock poisoned").as_ref() {
            running.notify_one();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_database_is_a_number() {
        assert_eq!(database_number("").unwrap(), 0);
        assert_eq!(database_number(" 3 ").unwrap(), 3);
        assert!(database_number("-1").is_err());
        assert!(database_number("shop").is_err());
    }

    #[test]
    fn a_request_is_an_argument_list_and_nothing_else() {
        assert_eq!(
            arguments(json!({"args": ["GET", "k"]})).unwrap(),
            ["GET", "k"]
        );
        assert!(arguments(json!({"args": []})).is_err());
        assert!(arguments(json!({"args": ["GET", 1]})).is_err());
        assert!(arguments(json!({"args": ["GET"], "database": "1"})).is_err());
        assert!(arguments(json!(["GET"])).is_err());
    }

    #[test]
    fn the_keyspace_and_the_version_are_read_from_info() {
        let info =
            "# Keyspace\r\ndb0:keys=3,expires=0,avg_ttl=0\r\ndb5:keys=1,expires=1,avg_ttl=9\r\n";
        assert_eq!(keyspace_databases(info), [0, 5]);
        assert_eq!(
            server_version("# Server\r\nredis_version:7.4.1\r\n"),
            "7.4.1"
        );
        assert_eq!(
            server_version("redis_version:7.2.4\r\nvalkey_version:8.0.1\r\n"),
            "Valkey 8.0.1"
        );
    }
}
