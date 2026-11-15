//! Redis through this plugin's own program, as SpelaionDB's core reaches it: each kind of reply
//! as rows (SpelaionDB's ADR-056, point 3), databases and keys in the explorer, errors, cancel,
//! and what connecting refuses.
//!
//! It needs the test servers (`ci/services.sh`): `SPELAION_TEST_REDIS=127.0.0.1:16380`, and
//! `SPELAION_TEST_VALKEY=127.0.0.1:16381` for the tests that also run on Valkey. Without them
//! it is skipped, which does not count as tested.

use std::time::{Duration, Instant};

use arrow::array::{Array, BinaryArray, Float64Array, Int64Array, StringArray};
use futures_util::StreamExt;
use serde_json::{Value, json};
use spelaion_driver_api::{
    ColumnMeta, ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions,
    LogicalType, ObjectKind, Secret,
};
use spelaion_plugin_host::Plugin;

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-redis");

fn server(variable: &str) -> Option<String> {
    match std::env::var(variable) {
        Ok(addr) => Some(addr),
        Err(_) => {
            eprintln!("{variable} not set: test skipped");
            None
        }
    }
}

/// Both servers, where each is set.
fn servers() -> Vec<(&'static str, String)> {
    [
        ("Redis", "SPELAION_TEST_REDIS"),
        ("Valkey", "SPELAION_TEST_VALKEY"),
    ]
    .into_iter()
    .filter_map(|(name, variable)| server(variable).map(|addr| (name, addr)))
    .collect()
}

fn params(addr: &str, database: &str) -> ConnectParams {
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    ConnectParams {
        host: host.into(),
        port: port.parse().expect("a port"),
        password: Some(Secret::new("spelaion")),
        database: database.into(),
        connect_timeout: Some(Duration::from_secs(10)),
        ..Default::default()
    }
}

async fn connect(plugin: &Plugin, addr: &str, database: &str) -> Box<dyn Connection> {
    tokio::time::timeout(
        Duration::from_secs(30),
        plugin.driver("redis").connect(&params(addr, database)),
    )
    .await
    .expect("connecting in time")
    .expect("connecting")
}

fn request(args: &[&str]) -> Value {
    json!({ "args": args })
}

/// Everything a request returned, bounded.
async fn run(connection: &dyn Connection, args: &[&str]) -> Vec<Result<ExecEvent, DriverError>> {
    let options = ExecOptions {
        batch_size: 1000,
        ..Default::default()
    };
    match connection.execute_request(request(args), &options).await {
        Ok(events) => tokio::time::timeout(Duration::from_secs(60), events.collect())
            .await
            .expect("the request ended in time"),
        Err(error) => vec![Err(error)],
    }
}

struct Outcome {
    columns: Vec<ColumnMeta>,
    batches: Vec<arrow::array::RecordBatch>,
    notices: Vec<String>,
}

async fn ok(connection: &dyn Connection, args: &[&str]) -> Outcome {
    let mut columns = None;
    let mut batches = Vec::new();
    let mut notices = Vec::new();
    let mut done = false;
    for event in run(connection, args).await {
        match event {
            Ok(ExecEvent::Columns(schema)) => columns = Some(schema.columns.clone()),
            Ok(ExecEvent::Batch(batch)) => batches.push(batch),
            Ok(ExecEvent::Notice(notice)) => notices.push(notice.message),
            Ok(ExecEvent::Done { .. }) => done = true,
            Err(error) => panic!("{args:?} failed: {error}"),
        }
    }
    assert!(done, "{args:?} never ended");
    Outcome {
        columns: columns.expect("columns"),
        batches,
        notices,
    }
}

async fn failure(connection: &dyn Connection, args: &[&str]) -> DriverError {
    run(connection, args)
        .await
        .into_iter()
        .find_map(Result::err)
        .unwrap_or_else(|| panic!("{args:?} did not fail"))
}

impl Outcome {
    fn names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    fn rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Every cell of a column, as text.
    fn texts(&self, column: usize) -> Vec<Option<String>> {
        let meta = &self.columns[column];
        let mut out = Vec::new();
        for batch in &self.batches {
            let array = batch.column(column);
            for row in 0..array.len() {
                if array.is_null(row) {
                    out.push(None);
                    continue;
                }
                out.push(Some(match meta.logical {
                    LogicalType::Int => array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row)
                        .to_string(),
                    LogicalType::Float => array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(row)
                        .to_string(),
                    LogicalType::Bytes => format!(
                        "{:?}",
                        array
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .unwrap()
                            .value(row)
                    ),
                    _ => array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row)
                        .to_string(),
                }));
            }
        }
        out
    }
}

fn some(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some(v.to_string())).collect()
}

#[tokio::test]
async fn each_kind_of_reply_reads_as_rows() {
    for (name, addr) in servers() {
        let plugin = Plugin::new(PROGRAM);
        let c = connect(&plugin, &addr, "1").await;
        let prefix = "spelaion:replies";
        for key in ["s", "h", "l", "z", "x", "b", "set"] {
            ok(&*c, &["DEL", &format!("{prefix}:{key}")]).await;
        }
        // A string, nil, OK and an integer.
        let set = ok(&*c, &["SET", &format!("{prefix}:s"), "ñandú"]).await;
        assert_eq!(set.texts(0), some(&["OK"]), "{name}");
        let get = ok(&*c, &["GET", &format!("{prefix}:s")]).await;
        assert_eq!(
            (get.names(), get.texts(0)),
            (vec!["value"], some(&["ñandú"])),
            "{name}"
        );
        assert_eq!(
            ok(&*c, &["GET", "spelaion:replies:none"]).await.texts(0),
            [None],
            "{name}"
        );
        let incr = ok(&*c, &["INCRBY", &format!("{prefix}:n"), "5"]).await;
        assert_eq!(incr.columns[0].logical, LogicalType::Int, "{name}");
        ok(&*c, &["DEL", &format!("{prefix}:n")]).await;
        // A hash: fields and values.
        ok(
            &*c,
            &["HSET", &format!("{prefix}:h"), "name", "Ana", "age", "7"],
        )
        .await;
        let hash = ok(&*c, &["HGETALL", &format!("{prefix}:h")]).await;
        assert_eq!(hash.names(), ["field", "value"], "{name}");
        let mut pairs: Vec<_> = hash.texts(0).into_iter().zip(hash.texts(1)).collect();
        pairs.sort();
        assert_eq!(
            pairs,
            [
                (Some("age".into()), Some("7".into())),
                (Some("name".into()), Some("Ana".into()))
            ],
            "{name}"
        );
        // A list: one row per element.
        ok(&*c, &["RPUSH", &format!("{prefix}:l"), "a", "b", "c"]).await;
        let list = ok(&*c, &["LRANGE", &format!("{prefix}:l"), "0", "-1"]).await;
        assert_eq!(list.texts(0), some(&["a", "b", "c"]), "{name}");
        // A sorted set with its scores.
        ok(&*c, &["ZADD", &format!("{prefix}:z"), "1.5", "a", "2", "b"]).await;
        let z = ok(
            &*c,
            &["ZRANGE", &format!("{prefix}:z"), "0", "-1", "WITHSCORES"],
        )
        .await;
        assert_eq!(z.names(), ["member", "score"], "{name}");
        assert_eq!(z.texts(0), some(&["a", "b"]), "{name}");
        assert_eq!(z.texts(1), some(&["1.5", "2"]), "{name}");
        // A stream's entries: their id and fields.
        ok(&*c, &["XADD", &format!("{prefix}:x"), "1-1", "f", "v"]).await;
        let x = ok(&*c, &["XRANGE", &format!("{prefix}:x"), "-", "+"]).await;
        assert_eq!(x.names(), ["id", "fields"], "{name}");
        assert_eq!(x.texts(0), some(&["1-1"]), "{name}");
        assert_eq!(x.texts(1), some(&[r#"["f","v"]"#]), "{name}");
        // A set.
        ok(&*c, &["SADD", &format!("{prefix}:set"), "only"]).await;
        assert_eq!(
            ok(&*c, &["SMEMBERS", &format!("{prefix}:set")])
                .await
                .texts(0),
            some(&["only"]),
            "{name}"
        );
        // A scan's next cursor is a notice: every field of a big hash is not read at once.
        // (A hash of up to 512 fields is kept compact, and the server answers it whole.)
        let big = format!("{prefix}:h");
        let mut hset = vec!["HSET".to_string(), big.clone()];
        for i in 0..2000 {
            hset.push(format!("f{i}"));
            hset.push("v".into());
        }
        ok(&*c, &hset.iter().map(String::as_str).collect::<Vec<_>>()).await;
        let scan = ok(&*c, &["HSCAN", &big, "0", "COUNT", "10"]).await;
        assert_eq!(scan.names(), ["field", "value"], "{name}");
        assert!(scan.rows() < 2002, "{name}: {}", scan.rows());
        assert!(
            scan.notices.iter().any(|n| n.contains("cursor")),
            "{name}: {:?}",
            scan.notices
        );
        // Bytes that are not text stay whole.
        let raw = ok(&*c, &["EVAL", "return '\\255\\0'", "0"]).await;
        assert_eq!(raw.columns[0].logical, LogicalType::Bytes, "{name}");
        assert_eq!(raw.texts(0), some(&["[255, 0]"]), "{name}");
        // A server's error, with its code.
        let wrong = failure(&*c, &["LPUSH", &format!("{prefix}:s"), "x"]).await;
        match wrong {
            DriverError::Database { code, message, .. } => {
                assert_eq!(code.as_deref(), Some("WRONGTYPE"), "{name}");
                assert!(message.contains("wrong kind"), "{name}: {message}");
            }
            other => panic!("{name}: {other:?}"),
        }
        // The session still works after an error.
        assert_eq!(ok(&*c, &["PING"]).await.texts(0), some(&["PONG"]), "{name}");
    }
}

#[tokio::test]
async fn databases_and_keys_are_what_the_explorer_shows() {
    for (name, addr) in servers() {
        let plugin = Plugin::new(PROGRAM);
        let c = connect(&plugin, &addr, "2").await;
        ok(&*c, &["FLUSHDB"]).await;
        ok(&*c, &["SET", "user:1", "x"]).await;
        ok(&*c, &["HSET", "user:2", "f", "v"]).await;
        ok(&*c, &["RPUSH", "queue", "a"]).await;
        assert_eq!(
            c.current_database().await.unwrap().as_deref(),
            Some("2"),
            "{name}"
        );
        let schemas: Vec<String> = c
            .list_schemas()
            .await
            .unwrap()
            .into_iter()
            .map(|s| s.name)
            .collect();
        assert!(schemas.contains(&"2".to_string()), "{name}: {schemas:?}");
        let databases = c.list_databases().await.unwrap();
        assert_eq!(databases.len(), 16, "{name}: {databases:?}");
        let keys = c.list_objects("2").await.unwrap();
        let listed: Vec<(String, Option<String>)> = keys
            .iter()
            .map(|k| (k.name.clone(), k.comment.clone()))
            .collect();
        assert_eq!(
            listed,
            [
                ("queue".into(), Some("list".into())),
                ("user:1".into(), Some("string".into())),
                ("user:2".into(), Some("hash".into())),
            ],
            "{name}"
        );
        assert!(
            keys.iter()
                .all(|k| k.kind == ObjectKind::Collection && k.schema == "2")
        );
        let described = c.describe_table("2", "user:2").await.unwrap();
        assert_eq!(described.object.comment.as_deref(), Some("hash"), "{name}");
        assert!(c.describe_table("2", "missing").await.is_err(), "{name}");
        // Listing another database does not move the session.
        let other = connect(&plugin, &addr, "3").await;
        ok(&*other, &["FLUSHDB"]).await;
        ok(&*other, &["SET", "elsewhere", "1"]).await;
        assert_eq!(c.list_objects("3").await.unwrap().len(), 1, "{name}");
        assert_eq!(
            c.current_database().await.unwrap().as_deref(),
            Some("2"),
            "{name}"
        );
        assert_eq!(ok(&*c, &["DBSIZE"]).await.texts(0), some(&["3"]), "{name}");
        // Choosing a database runs what follows there.
        c.select_database("3").await.unwrap();
        assert_eq!(
            ok(&*c, &["GET", "elsewhere"]).await.texts(0),
            some(&["1"]),
            "{name}"
        );
        assert!(c.select_database("shop").await.is_err(), "{name}");
    }
}

#[tokio::test]
async fn the_explorer_never_lists_more_than_its_bound() {
    let Some(addr) = server("SPELAION_TEST_REDIS") else {
        return;
    };
    let plugin = Plugin::new(PROGRAM);
    let c = connect(&plugin, &addr, "9").await;
    ok(&*c, &["FLUSHDB"]).await;
    // 1,500 keys in one round trip.
    let mut args = vec!["MSET".to_string()];
    for i in 0..1500 {
        args.push(format!("k:{i}"));
        args.push("v".into());
    }
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    ok(&*c, &args).await;
    let keys = c.list_objects("9").await.unwrap();
    assert_eq!(keys.len(), spelaion_driver_redis::MAX_KEYS);
    ok(&*c, &["FLUSHDB"]).await;
}

#[tokio::test]
async fn a_cancel_stops_a_command_that_waits_and_the_session_goes_on_in_its_database() {
    for (name, addr) in servers() {
        let plugin = Plugin::new(PROGRAM);
        let c = connect(&plugin, &addr, "4").await;
        ok(&*c, &["SET", "spelaion:cancel", "still here"]).await;
        let options = ExecOptions {
            batch_size: 100,
            ..Default::default()
        };
        let events = c
            .execute_request(
                request(&["BLPOP", "spelaion:cancel:nothing", "60"]),
                &options,
            )
            .await
            .unwrap();
        let cancel = c.cancel_handle();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let started = Instant::now();
        cancel.cancel().await.unwrap();
        let outcome: Vec<_> = tokio::time::timeout(Duration::from_secs(10), events.collect())
            .await
            .expect("the cancel was obeyed in time");
        assert!(
            outcome
                .iter()
                .any(|e| matches!(e, Err(DriverError::Cancelled))),
            "{name}: {outcome:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(5), "{name}");
        // The next command answers at once, in the same database.
        let next =
            tokio::time::timeout(Duration::from_secs(5), ok(&*c, &["GET", "spelaion:cancel"]))
                .await
                .expect("the session answers after a cancel");
        assert_eq!(next.texts(0), some(&["still here"]), "{name}");
    }
}

#[tokio::test]
async fn the_server_and_its_version_are_named() {
    if let Some(addr) = server("SPELAION_TEST_REDIS") {
        let plugin = Plugin::new(PROGRAM);
        let version = connect(&plugin, &addr, "0")
            .await
            .server_version()
            .await
            .unwrap();
        assert!(version.starts_with("7.4"), "{version}");
    }
    if let Some(addr) = server("SPELAION_TEST_VALKEY") {
        let plugin = Plugin::new(PROGRAM);
        let version = connect(&plugin, &addr, "0")
            .await
            .server_version()
            .await
            .unwrap();
        assert!(version.starts_with("Valkey 8"), "{version}");
    }
}

#[tokio::test]
async fn connecting_refuses_what_it_cannot_honour() {
    let Some(addr) = server("SPELAION_TEST_REDIS") else {
        return;
    };
    let plugin = Plugin::new(PROGRAM);
    let driver = plugin.driver("redis");
    // A wrong password, said as such and without the password.
    let mut wrong = params(&addr, "0");
    wrong.password = Some(Secret::new("not-the-password"));
    let error = driver.connect(&wrong).await.err().expect("refused");
    let text = error.to_string();
    assert!(text.contains("refused the login"), "{text}");
    assert!(!text.contains("not-the-password"), "{text}");
    // A database that is not a number.
    let error = driver
        .connect(&params(&addr, "shop"))
        .await
        .err()
        .expect("refused");
    assert!(error.to_string().contains("is a number"), "{error}");
    // Read-only: nothing here could keep it so.
    let mut read_only = params(&addr, "0");
    read_only.read_only = true;
    assert!(driver.connect(&read_only).await.is_err());
    // A setting nobody checked.
    let mut options = params(&addr, "0");
    options.options.insert("cluster".into(), "yes".into());
    assert!(driver.connect(&options).await.is_err());
}
