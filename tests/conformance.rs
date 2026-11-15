//! The conformance set of SpelaionDB's plugin protocol, run against this plugin's own program
//! (ADR-042), through requests (protocol 1.1, ADR-056): it starts, speaks the protocol's version,
//! opens sessions, returns results in the order the contract says, fails without hanging, obeys
//! a cancel while a request runs and ends when its input closes. What replies look like is
//! `tests/redis.rs`.
//!
//! It needs the test servers (`ci/services.sh`, `SPELAION_TEST_REDIS=127.0.0.1:16380` and
//! `SPELAION_TEST_VALKEY=127.0.0.1:16381`). Without the variables it is skipped, which does
//! not count as tested.

use serde_json::json;
use spelaion_driver_api::{ConnectParams, Secret};
use spelaion_plugin_host::conformance::{RequestTarget, assert_conforms_requests};

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-redis");

fn params(addr: &str) -> ConnectParams {
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    ConnectParams {
        host: host.into(),
        port: port.parse().expect("a port"),
        password: Some(Secret::new("spelaion")),
        database: "0".into(),
        ..Default::default()
    }
}

fn target(addr: &str) -> RequestTarget {
    RequestTarget {
        program: PROGRAM.into(),
        engine: "redis".into(),
        params: params(addr),
        one_row: json!({"args": ["PING"]}),
        refused: json!({"args": ["NOSUCHCOMMAND"]}),
        // A minute, unless it is cancelled: it waits for a list nobody pushes to.
        slow: Some(json!({"args": ["BLPOP", "spelaion:conformance:nothing", "60"]})),
    }
}

#[tokio::test]
async fn the_plugin_conforms_on_redis() {
    let Ok(addr) = std::env::var("SPELAION_TEST_REDIS") else {
        eprintln!("SPELAION_TEST_REDIS not set: test skipped");
        return;
    };
    assert_conforms_requests(&target(&addr)).await;
}

#[tokio::test]
async fn the_plugin_conforms_on_valkey() {
    let Ok(addr) = std::env::var("SPELAION_TEST_VALKEY") else {
        eprintln!("SPELAION_TEST_VALKEY not set: test skipped");
        return;
    };
    assert_conforms_requests(&target(&addr)).await;
}

/// `plugin.toml` is what SpelaionDB's pin says of this plugin: it must be what the program
/// says of itself. The program says it without a server.
#[tokio::test]
async fn the_manifest_is_what_the_program_says() {
    let manifest = include_str!("../plugin.toml");
    let value = |key: &str| -> String {
        manifest
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key} = ")))
            .unwrap_or_else(|| panic!("plugin.toml has no `{key}`"))
            .trim()
            .to_string()
    };
    let plugin = spelaion_plugin_host::Plugin::new(PROGRAM);
    let info = plugin.describe().await.expect("the program starts");

    assert_eq!(value("protocol"), format!("\"{}\"", info.protocol));
    let engines: Vec<String> = info.engines.iter().map(|e| format!("\"{e}\"")).collect();
    assert_eq!(value("engines"), format!("[{}]", engines.join(", ")));
    assert_eq!(info.plugin.name, env!("CARGO_PKG_NAME"));
    assert_eq!(info.plugin.version, env!("CARGO_PKG_VERSION"));
    // Three numbers, as SpelaionDB's `plugins.toml` wants.
    let min_app = value("min_app");
    let numbers = min_app.trim_matches('"').split('.').count();
    assert_eq!(numbers, 3, "min_app = {min_app}");
}
