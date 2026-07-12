use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Mutex;
use std::time::Duration;
use tempfile::TempDir;

const BINARY: &str = env!("CARGO_BIN_EXE_vigil");
const SOCKET: &str = "/tmp/vigil.sock";

// Integration tests share the daemon socket, so they must run sequentially.
static LOCK: Mutex<()> = Mutex::new(());

struct Daemon {
    log_file: PathBuf,
    // Held for its Drop side effect (directory cleanup); each test gets its
    // own isolated --data-dir so daemon restarts across tests never recover
    // a previous test's persisted state.
    _data_dir: TempDir,
}

impl Daemon {
    fn start(events: &[&str]) -> Self {
        Self::start_with_time_field(events, None)
    }

    fn start_with_time_field(events: &[&str], time_field: Option<&str>) -> Self {
        // Stop any daemon left over from a previous run.
        let _ = Command::new(BINARY).arg("--stop").output();
        std::thread::sleep(Duration::from_millis(100));

        let log_path = std::env::temp_dir().join("vigil_integration_test.log");
        let mut f = std::fs::File::create(&log_path).unwrap();
        for event in events {
            writeln!(f, "{}", event).unwrap();
        }

        let data_dir = TempDir::new().unwrap();

        let mut cmd = Command::new(BINARY);
        cmd.arg("--watch")
            .arg(&log_path)
            .arg("--data-dir")
            .arg(data_dir.path());
        if let Some(tf) = time_field {
            cmd.arg("--time-field").arg(tf);
        }
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        for _ in 0..50 {
            if Path::new(SOCKET).exists() {
                return Daemon {
                    log_file: log_path,
                    _data_dir: data_dir,
                };
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("Daemon did not start within 5 seconds");
    }

    fn query(&self, q: &str) -> Output {
        Command::new(BINARY).arg(q).output().unwrap()
    }

    fn append(&self, event: &str) {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&self.log_file)
            .unwrap();
        writeln!(f, "{}", event).unwrap();
        // Give the tailer (100ms poll interval) time to pick up the new line.
        std::thread::sleep(Duration::from_millis(300));
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = Command::new(BINARY).arg("--stop").output();
        std::fs::remove_file(&self.log_file).ok();
    }
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).trim().to_string()
}

// --- Query ---

#[test]
fn test_field_query_returns_matching_events() {
    let _lock = LOCK.lock().unwrap();
    let daemon = Daemon::start(&[
        r#"{"level":"ERROR","service":"auth"}"#,
        r#"{"level":"INFO","service":"api"}"#,
    ]);

    let out = daemon.query("level = ERROR");

    assert!(out.status.success());
    assert!(stdout(&out).contains(r#""service":"auth""#));
    assert!(!stdout(&out).contains(r#""level":"INFO""#));
}

#[test]
fn test_field_query_no_matches_returns_empty() {
    let _lock = LOCK.lock().unwrap();
    let daemon = Daemon::start(&[r#"{"level":"INFO"}"#]);

    let out = daemon.query("level = ERROR");

    assert!(out.status.success());
    assert!(stdout(&out).is_empty());
}

#[test]
fn test_and_query() {
    let _lock = LOCK.lock().unwrap();
    let daemon = Daemon::start(&[
        r#"{"level":"ERROR","service":"auth"}"#,
        r#"{"level":"ERROR","service":"api"}"#,
        r#"{"level":"INFO","service":"auth"}"#,
    ]);

    let out = daemon.query("level = ERROR AND service = auth");

    assert!(out.status.success());
    let body = stdout(&out);
    assert_eq!(body.lines().count(), 1);
    assert!(body.contains(r#""service":"auth""#));
}

#[test]
fn test_or_query() {
    let _lock = LOCK.lock().unwrap();
    let daemon = Daemon::start(&[
        r#"{"level":"ERROR"}"#,
        r#"{"level":"WARN"}"#,
        r#"{"level":"INFO"}"#,
    ]);

    let out = daemon.query("level = ERROR OR level = WARN");

    assert!(out.status.success());
    assert_eq!(stdout(&out).lines().count(), 2);
}

#[test]
fn test_not_and_paren_query() {
    let _lock = LOCK.lock().unwrap();
    let daemon = Daemon::start(&[
        r#"{"level":"ERROR","status":500}"#,
        r#"{"level":"WARN","status":503}"#,
        r#"{"level":"ERROR","status":200}"#,
        r#"{"level":"INFO","status":500}"#,
    ]);

    let out = daemon.query("(level = ERROR OR level = WARN) AND status >= 500");
    assert!(out.status.success());
    assert_eq!(stdout(&out).lines().count(), 2);

    let out = daemon.query("NOT status = 500 | count");
    assert!(out.status.success());
    assert_eq!(stdout(&out), "2");
}

// --- Aggregations ---

#[test]
fn test_count_aggregation() {
    let _lock = LOCK.lock().unwrap();
    let daemon = Daemon::start(&[
        r#"{"status":500}"#,
        r#"{"status":500}"#,
        r#"{"status":200}"#,
    ]);

    let out = daemon.query("status = 500 | count");

    assert!(out.status.success());
    assert_eq!(stdout(&out), "2");
}

#[test]
fn test_count_all_events() {
    let _lock = LOCK.lock().unwrap();
    let daemon = Daemon::start(&[
        r#"{"level":"ERROR"}"#,
        r#"{"level":"INFO"}"#,
        r#"{"level":"WARN"}"#,
    ]);

    let out = daemon.query("| count");

    assert!(out.status.success());
    assert_eq!(stdout(&out), "3");
}

#[test]
fn test_avg_aggregation() {
    let _lock = LOCK.lock().unwrap();
    let daemon = Daemon::start(&[
        r#"{"status":500,"latency":100}"#,
        r#"{"status":500,"latency":300}"#,
    ]);

    let out = daemon.query("status = 500 | avg latency");

    assert!(out.status.success());
    assert_eq!(stdout(&out), "200");
}

// --- Time range ---

#[test]
fn test_time_range_query() {
    let _lock = LOCK.lock().unwrap();
    let daemon = Daemon::start_with_time_field(
        &[
            r#"{"ts":"2026-01-01T00:00:00Z","level":"ERROR"}"#,
            r#"{"ts":"2026-06-01T00:00:00Z","level":"ERROR"}"#,
            r#"{"ts":"2026-12-31T00:00:00Z","level":"ERROR"}"#,
        ],
        Some("ts"),
    );

    let out = daemon.query("ts > 2026-03-01T00:00:00Z");
    assert!(out.status.success());
    assert_eq!(stdout(&out).lines().count(), 2);
}

#[test]
fn test_time_range_requires_time_field_flag() {
    let _lock = LOCK.lock().unwrap();
    // Daemon started without --time-field: "ts" is just a regular string field,
    // so the time range query returns no results.
    let daemon = Daemon::start(&[
        r#"{"ts":"2026-01-01T00:00:00Z","level":"ERROR"}"#,
    ]);

    let out = daemon.query("ts > 2026-03-01T00:00:00Z | count");
    assert!(out.status.success());
    assert_eq!(stdout(&out), "0");
}

// --- Incremental writes ---

#[test]
fn test_new_events_are_picked_up() {
    let _lock = LOCK.lock().unwrap();
    let daemon = Daemon::start(&[r#"{"level":"INFO"}"#]);

    assert!(stdout(&daemon.query("level = ERROR")).is_empty());

    daemon.append(r#"{"level":"ERROR","id":42}"#);

    let out = daemon.query("level = ERROR");
    assert!(out.status.success());
    assert!(stdout(&out).contains(r#""id":42"#));
}

// --- Errors ---

#[test]
fn test_invalid_query_exits_nonzero_with_message() {
    let _lock = LOCK.lock().unwrap();
    let daemon = Daemon::start(&[]);

    let out = daemon.query("level ???");

    assert!(!out.status.success());
    assert!(!stderr(&out).is_empty());
}

#[test]
fn test_aggregation_on_missing_field_exits_nonzero() {
    let _lock = LOCK.lock().unwrap();
    let daemon = Daemon::start(&[r#"{"status":500}"#]);

    let out = daemon.query("status = 500 | avg latency");

    assert!(!out.status.success());
    assert!(!stderr(&out).is_empty());
}

// --- Shutdown ---

#[test]
fn test_stop_shuts_down_daemon_and_removes_socket() {
    let _lock = LOCK.lock().unwrap();

    let log_path = std::env::temp_dir().join("vigil_integration_test.log");
    std::fs::File::create(&log_path).unwrap();
    let data_dir = TempDir::new().unwrap();

    let _ = Command::new(BINARY).arg("--stop").output();
    std::thread::sleep(Duration::from_millis(100));

    Command::new(BINARY)
        .arg("--watch")
        .arg(&log_path)
        .arg("--data-dir")
        .arg(data_dir.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    for _ in 0..50 {
        if Path::new(SOCKET).exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(Path::new(SOCKET).exists(), "Daemon did not start");

    let out = Command::new(BINARY).arg("--stop").output().unwrap();
    assert!(out.status.success());
    assert_eq!(stdout(&out), "Daemon stopped.");

    std::thread::sleep(Duration::from_millis(200));
    assert!(!Path::new(SOCKET).exists(), "Socket file should be removed after stop");

    std::fs::remove_file(&log_path).ok();
}
