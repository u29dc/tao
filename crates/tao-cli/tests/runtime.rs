#![cfg(unix)]

use std::fs;
use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

struct Fixture {
    _directory: tempfile::TempDir,
    root: PathBuf,
    vault: PathBuf,
    socket: PathBuf,
    config: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let output = repository.join("target/runtime-tests");
        fs::create_dir_all(&output).expect("create repository-local test directory");
        let directory = tempfile::Builder::new()
            .prefix("rt-")
            .tempdir_in(output)
            .expect("create fixture");
        let root = fs::canonicalize(directory.path()).expect("canonical fixture");
        let vault = root.join("vault");
        fs::create_dir(&vault).expect("create vault");
        fs::write(
            vault.join("a.md"),
            "---\ntitle: Alpha\n---\n# Alpha\nUnique runtime fixture token.\n",
        )
        .expect("write source");
        let config = root.join("empty.toml");
        fs::write(&config, "").expect("write isolated config");
        Self {
            _directory: directory,
            socket: root.join("daemon.sock"),
            root,
            vault,
            config,
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_tao"));
        command
            .current_dir(&self.root)
            .args(["--daemon-socket", self.socket.to_str().expect("socket")]);
        for name in [
            "TAO_VAULT_ROOT",
            "TAO_DATA_DIR",
            "TAO_DB_PATH",
            "TAO_CASE_POLICY",
            "TAO_READ_ONLY",
            "TAO_FEATURE_FLAGS",
        ] {
            command.env_remove(name);
        }
        command.env("TAO_CONFIG_PATH", &self.config);
        let backend = self.root.join("backend");
        if backend.is_dir() {
            let mut paths = vec![backend];
            paths.extend(std::env::split_paths(
                &std::env::var_os("PATH").unwrap_or_default(),
            ));
            command.env(
                "PATH",
                std::env::join_paths(paths).expect("fixture backend PATH"),
            );
        }
        command
    }

    fn run(&self, args: &[&str]) -> (i32, Value) {
        let output = self
            .command()
            .args(args)
            .output()
            .expect("run real executable");
        let value = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "invalid JSON: {error}; stdout={}; stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        });
        (output.status.code().unwrap_or(-1), value)
    }

    fn index(&self) {
        let (code, result) = self.run(&[
            "--execution-mode",
            "direct",
            "vault",
            "reindex",
            "--vault-root",
            self.vault.to_str().expect("vault"),
        ]);
        assert_eq!(code, 0, "{result}");
        assert!(
            !self.socket.exists(),
            "direct execution must never start a daemon"
        );
    }

    fn start(&self) -> Daemon {
        let child = self
            .command()
            .args([
                "vault",
                "daemon",
                "start",
                "--foreground",
                "--socket",
                self.socket.to_str().expect("socket"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start daemon");
        let mut daemon = Daemon {
            child,
            socket: self.socket.clone(),
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(mut stream) = UnixStream::connect(&self.socket) {
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("read timeout");
                stream
                    .write_all(b"{\"kind\":\"status\"}")
                    .expect("status write");
                stream.shutdown(Shutdown::Write).expect("finish status");
                let mut response = String::new();
                stream.read_to_string(&mut response).expect("status read");
                if serde_json::from_str::<Value>(&response).is_ok_and(|value| value["ok"] == true) {
                    return daemon;
                }
            }
            assert!(
                daemon.child.try_wait().expect("daemon status").is_none(),
                "daemon exited at startup"
            );
            assert!(Instant::now() < deadline, "daemon startup timeout");
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn request(&self, bytes: &[u8]) -> Value {
        let mut stream = UnixStream::connect(&self.socket).expect("connect request");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        stream.write_all(bytes).expect("write request");
        stream.shutdown(Shutdown::Write).expect("finish request");
        let mut response = Vec::new();
        stream.read_to_end(&mut response).expect("read response");
        serde_json::from_slice(&response).expect("daemon JSON")
    }
}

#[test]
fn extraction_and_reindex_both_progress_while_snapshot_reads_stay_responsive() {
    let fixture = Fixture::new();
    let backend = fixture.root.join("backend");
    fs::create_dir(&backend).unwrap();
    // Deterministic local process doubles keep this scheduler test independent
    // of optional PDF packages while retaining real child-process latency.
    for (name, script) in [
        (
            "pdfinfo",
            "#!/bin/sh\n/bin/sleep 1\nprintf 'Pages: 1\\nEncrypted: no\\n'\n",
        ),
        (
            "pdfimages",
            "#!/bin/sh\nprintf 'page num type width height\\n'\n",
        ),
        (
            "pdftotext",
            "#!/bin/sh\nfor last do :; done\nprintf 'Native scheduler fixture page content.\\n' > \"$last\"\n",
        ),
    ] {
        let path = backend.join(name);
        fs::write(&path, script).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    for index in 0..3 {
        fs::write(
            fixture.vault.join(format!("queued-{index}.pdf")),
            format!("%PDF-1.4\nfixture {index}\n%%EOF\n"),
        )
        .unwrap();
    }
    for index in 0..30 {
        fs::write(
            fixture.vault.join(format!("note-{index}.md")),
            format!("# Note {index}\n[[a]]\n"),
        )
        .unwrap();
    }
    fixture.index();
    let _daemon = fixture.start();
    let vault = fixture.vault.to_str().unwrap();
    let (code, response) = fixture.run(&[
        "--execution-mode",
        "required-daemon",
        "doc",
        "list",
        "--vault-root",
        vault,
    ]);
    assert_eq!(code, 0, "{response}");
    let database = rusqlite::Connection::open(fixture.vault.join(".tao/index.sqlite")).unwrap();
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let running: u64 = database
            .query_row(
                "SELECT COUNT(*) FROM extraction_jobs WHERE state='running'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        if running > 0 {
            break;
        }
        assert!(Instant::now() < deadline, "extraction never started");
        thread::sleep(Duration::from_millis(20));
    }
    let until = Instant::now() + Duration::from_secs(4);
    thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut revisions = 0;
            while Instant::now() < until {
                revisions += 1;
                let temporary = fixture.vault.join("a.tmp");
                fs::write(
                    &temporary,
                    format!("# Alpha\nScheduler revision {revisions}.\n"),
                )
                .unwrap();
                fs::rename(temporary, fixture.vault.join("a.md")).unwrap();
                let (code, response) = fixture.run(&[
                    "--execution-mode",
                    "required-daemon",
                    "--timeout-ms",
                    "1500",
                    "vault",
                    "reindex",
                    "--vault-root",
                    vault,
                ]);
                assert_eq!(
                    code, 0,
                    "explicit reindex starved during extraction: {response}"
                );
                thread::sleep(Duration::from_millis(40));
            }
            revisions
        });
        let mut reads = 0;
        while Instant::now() < until {
            let started = Instant::now();
            let (code, response) = fixture.run(&[
                "--execution-mode",
                "required-daemon",
                "--timeout-ms",
                "1000",
                "--no-result-cache",
                "doc",
                "list",
                "--vault-root",
                vault,
            ]);
            assert_eq!(
                code, 0,
                "snapshot read failed under maintenance: {response}"
            );
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "read waited for maintenance: {response}"
            );
            assert_eq!(response["data"]["total"], 31);
            reads += 1;
            thread::sleep(Duration::from_millis(30));
        }
        assert!(
            writer.join().unwrap() >= 3,
            "foreground maintenance did not progress"
        );
        assert!(reads >= 10, "insufficient concurrent reads");
    });
    let completed: u64 = database
        .query_row(
            "SELECT COUNT(*) FROM extraction_jobs WHERE state='done'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        completed > 0,
        "extraction made no progress during recurring reindex and reads"
    );
}

struct Daemon {
    child: Child,
    socket: PathBuf,
}
impl Drop for Daemon {
    fn drop(&mut self) {
        if let Ok(mut stream) = UnixStream::connect(&self.socket) {
            let _ = stream.write_all(b"{\"kind\":\"shutdown\"}");
            let _ = stream.shutdown(Shutdown::Write);
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn executable_modes_typed_failures_cache_and_context_are_consistent() {
    let fixture = Fixture::new();
    fixture.index();
    let args = [
        "doc",
        "list",
        "--vault-root",
        fixture.vault.to_str().expect("vault"),
    ];
    let (code, missing) =
        fixture.run(&[&["--execution-mode", "required-daemon"][..], &args].concat());
    assert_ne!(code, 0);
    assert_eq!(missing["error"]["code"], "daemon_unavailable");
    let _daemon = fixture.start();
    for (extra, cache) in [
        (&[][..], "miss"),
        (&[][..], "hit"),
        (&["--no-result-cache"][..], "bypass"),
    ] {
        let (code, response) =
            fixture.run(&[&["--execution-mode", "required-daemon"][..], extra, &args].concat());
        assert_eq!(code, 0, "{response}");
        assert_eq!(response["meta"]["runtime"]["backend"], "daemon");
        assert_eq!(response["meta"]["runtime"]["resultCache"], cache);
    }
    let invalid = [
        "doc",
        "read",
        "--vault-root",
        fixture.vault.to_str().expect("vault"),
        "--path",
        "../outside.md",
    ];
    let (direct_code, direct) =
        fixture.run(&[&["--execution-mode", "direct"][..], &invalid].concat());
    let (daemon_code, daemon) =
        fixture.run(&[&["--execution-mode", "required-daemon"][..], &invalid].concat());
    assert_ne!(direct_code, 0);
    assert_eq!(direct_code, daemon_code);
    assert_eq!(direct["error"], daemon["error"]);
    assert_eq!(fixture.request(b"{\"kind\":\"status\"}")["ok"], true);

    // Client configuration is resolved before forwarding, including environment-only database paths.
    let custom = fixture.root.join("custom.sqlite");
    let output = fixture
        .command()
        .env("TAO_DB_PATH", &custom)
        .args([
            "--execution-mode",
            "required-daemon",
            "vault",
            "reindex",
            "--vault-root",
            fixture.vault.to_str().expect("vault"),
        ])
        .output()
        .expect("client environment request");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        custom.exists(),
        "daemon must use the client's resolved database"
    );
}

#[test]
fn malformed_slow_oversized_and_incompatible_requests_do_not_stop_daemon() {
    let fixture = Fixture::new();
    fixture.index();
    let _daemon = fixture.start();
    assert_eq!(
        fixture.request(b"{broken")["failure"]["code"],
        "invalid_request"
    );
    assert_eq!(
        fixture.request(&vec![b'x'; 1024 * 1024 + 1])["failure"]["code"],
        "request_too_large"
    );
    let payload = json!({"kind":"execute","payload":{"command":{"Tools":{"name":null}},"json":true,"json_stream":false,"protocol":0,"build":"obsolete","request_id":"old","timeout_ms":1000}});
    assert_eq!(
        fixture.request(payload.to_string().as_bytes())["failure"]["code"],
        "daemon_incompatible"
    );
    let mut slow = Vec::new();
    for _ in 0..32 {
        let mut stream = UnixStream::connect(&fixture.socket).expect("slow client");
        stream.write_all(b"{").expect("partial frame");
        slow.push(stream);
    }
    let started = Instant::now();
    assert_eq!(fixture.request(b"{\"kind\":\"status\"}")["ok"], true);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "control requests must not queue behind slow clients"
    );
    drop(slow);
    let (code, result) = fixture.run(&[
        "--execution-mode",
        "required-daemon",
        "doc",
        "list",
        "--vault-root",
        fixture.vault.to_str().expect("vault"),
    ]);
    assert_eq!(code, 0, "{result}");
}

#[test]
fn observational_commands_never_initialize_an_absent_database() {
    let fixture = Fixture::new();
    let vault = fixture.vault.to_str().expect("vault");
    for args in [
        vec!["health", "--vault-root", vault],
        vec!["vault", "preflight", "--vault-root", vault],
        vec!["vault", "reindex", "--dry-run", "--vault-root", vault],
        vec![
            "query",
            "--from",
            "docs",
            "--explain",
            "--vault-root",
            vault,
        ],
    ] {
        let _ = fixture.run(&[&["--execution-mode", "direct"][..], &args].concat());
        assert!(
            !fixture.vault.join(".tao").exists(),
            "observational command created internal state: {args:?}"
        );
    }
}

#[test]
fn socket_ownership_preserves_files_permissions_and_concurrent_owner() {
    let fixture = Fixture::new();
    fs::write(&fixture.socket, "preserve me").expect("regular socket-named file");
    let output = fixture
        .command()
        .args([
            "vault",
            "daemon",
            "start",
            "--foreground",
            "--socket",
            fixture.socket.to_str().expect("socket"),
        ])
        .output()
        .expect("reject non-socket");
    assert!(!output.status.success());
    assert_eq!(
        fs::read_to_string(&fixture.socket).expect("preserved source"),
        "preserve me"
    );
    fs::remove_file(&fixture.socket).expect("remove test-owned regular file");
    fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o755))
        .expect("set fixture parent mode");
    let daemon = fixture.start();
    let second = fixture
        .command()
        .args([
            "vault",
            "daemon",
            "start",
            "--foreground",
            "--socket",
            fixture.socket.to_str().expect("socket"),
        ])
        .output()
        .expect("simultaneous owner check");
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stdout)
    );
    assert_eq!(fixture.request(b"{\"kind\":\"status\"}")["ok"], true);
    assert_eq!(
        fs::metadata(&fixture.root)
            .expect("parent metadata")
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    assert_eq!(
        fs::metadata(&fixture.socket)
            .expect("socket metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    drop(daemon);
    assert!(
        !fixture.socket.exists(),
        "owned socket must be cleaned up on shutdown"
    );
}

#[test]
fn deadlines_and_queued_cancellation_leave_control_and_following_reads_healthy() {
    let fixture = Fixture::new();
    fixture.index();
    let _daemon = fixture.start();
    let vault = fixture.vault.to_str().expect("vault");
    let (code, warm) = fixture.run(&[
        "--execution-mode",
        "required-daemon",
        "doc",
        "list",
        "--vault-root",
        vault,
    ]);
    assert_eq!(code, 0, "{warm}");
    let database = fixture.vault.join(".tao/index.sqlite");
    let blocker = rusqlite::Connection::open(&database).expect("fixture writer");
    blocker
        .execute_batch("BEGIN IMMEDIATE;")
        .expect("hold writer transaction");
    fs::write(
        fixture.vault.join("a.md"),
        "# Changed under a held writer lock\n",
    )
    .expect("fixture source change");
    let started = Instant::now();
    let writer = fixture
        .command()
        .args([
            "--execution-mode",
            "required-daemon",
            "--timeout-ms",
            "500",
            "vault",
            "reindex",
            "--vault-root",
            vault,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start timed writer");
    let status = loop {
        let status = fixture.request(b"{\"kind\":\"status\"}");
        if status["status"]["writer_busy"] == true {
            break status;
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "writer did not start"
        );
        thread::sleep(Duration::from_millis(5));
    };
    let mut queued = UnixStream::connect(&fixture.socket).expect("queued request");
    queued
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("queued read timeout");
    let payload = json!({"kind":"execute","payload":{
        "command":{"Vault":{"command":{"Reindex":{"vault_root":vault,"db_path":null,"dry_run":false,"wait_content_ms":0}}}},
        "json":true,"json_stream":false,"protocol":status["protocol"],"build":status["build"],"request_id":"cancel-queued","timeout_ms":2000,
        "context":{"vault_root":vault,"data_dir":fixture.vault.join(".tao"),"db_path":database,"case_insensitive":false}
    }});
    queued
        .write_all(payload.to_string().as_bytes())
        .expect("queue writer");
    queued
        .shutdown(Shutdown::Write)
        .expect("finish queued frame");
    thread::sleep(Duration::from_millis(30));
    assert_eq!(
        fixture.request(b"{\"kind\":\"cancel\",\"request_id\":\"cancel-queued\"}")["ok"],
        true
    );
    let mut response = String::new();
    queued
        .read_to_string(&mut response)
        .expect("cancel response");
    let cancelled: Value = serde_json::from_str(&response).expect("cancel JSON");
    assert_eq!(
        cancelled["failure"]["code"], "request_cancelled",
        "{cancelled}"
    );
    let health_started = Instant::now();
    let (_, health) = fixture.run(&[
        "--execution-mode",
        "required-daemon",
        "health",
        "--vault-root",
        vault,
    ]);
    assert_eq!(health["meta"]["runtime"]["backend"], "daemon", "{health}");
    assert!(
        health_started.elapsed() < Duration::from_secs(1),
        "health queued behind writer"
    );
    let output = writer.wait_with_output().expect("timed writer response");
    let timeout: Value = serde_json::from_slice(&output.stdout).expect("timeout JSON");
    assert_eq!(timeout["error"]["code"], "request_timeout", "{timeout}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "writer deadline was not bounded"
    );
    blocker.execute_batch("ROLLBACK;").expect("release writer");
    let (code, result) = fixture.run(&[
        "--execution-mode",
        "required-daemon",
        "doc",
        "list",
        "--vault-root",
        vault,
    ]);
    assert_eq!(code, 0, "{result}");
}

#[test]
fn automatic_routing_starts_and_reuses_only_the_fixture_daemon() {
    let fixture = Fixture::new();
    let vault = fixture.vault.to_str().expect("vault");
    let (first_code, first) = fixture.run(&["doc", "list", "--vault-root", vault]);
    let (second_code, second) = fixture.run(&["doc", "list", "--vault-root", vault]);
    let (stop_code, stopped) = fixture.run(&[
        "vault",
        "daemon",
        "stop",
        "--socket",
        fixture.socket.to_str().expect("socket"),
    ]);
    assert_eq!(first_code, 0, "{first}");
    assert_eq!(second_code, 0, "{second}");
    assert_eq!(stop_code, 0, "{stopped}");
    assert_eq!(first["meta"]["runtime"]["backend"], "daemon");
    assert_eq!(second["meta"]["runtime"]["resultCache"], "hit");
    let deadline = Instant::now() + Duration::from_secs(2);
    while fixture.socket.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(!fixture.socket.exists());
}

#[test]
fn continuation_binds_query_policy_and_generation_across_processes() {
    let fixture = Fixture::new();
    fs::write(fixture.vault.join("b.md"), "# Beta\n").expect("second source");
    fs::write(fixture.vault.join("c.md"), "# Gamma\n").expect("third source");
    fixture.index();
    let _daemon = fixture.start();
    let vault = fixture.vault.to_str().expect("vault");
    let (code, first) = fixture.run(&[
        "--execution-mode",
        "required-daemon",
        "doc",
        "list",
        "--vault-root",
        vault,
        "--limit",
        "1",
    ]);
    assert_eq!(code, 0, "{first}");
    let token = first["meta"]["continuation"]["token"]
        .as_str()
        .expect("page token");
    let (code, next) = fixture.run(&[
        "--execution-mode",
        "direct",
        "--continuation",
        token,
        "doc",
        "list",
        "--vault-root",
        vault,
        "--offset",
        "1",
        "--limit",
        "2",
    ]);
    assert_eq!(code, 0, "{next}");
    assert_eq!(next["meta"]["continuation"]["token"], token);
    assert_eq!(
        next["data"]["items"].as_array().expect("next page").len(),
        2
    );
    let (code, wrong_query) = fixture.run(&[
        "--execution-mode",
        "required-daemon",
        "--continuation",
        token,
        "task",
        "list",
        "--vault-root",
        vault,
    ]);
    assert_eq!(code, 2, "{wrong_query}");
    assert_eq!(wrong_query["error"]["code"], "continuation_mismatch");
    fs::write(fixture.vault.join("a.md"), "# Alpha changed\n").expect("change publication");
    let (code, indexed) = fixture.run(&[
        "--execution-mode",
        "direct",
        "vault",
        "reindex",
        "--vault-root",
        vault,
    ]);
    assert_eq!(code, 0, "{indexed}");
    let (code, stale) = fixture.run(&[
        "--execution-mode",
        "required-daemon",
        "--continuation",
        token,
        "doc",
        "list",
        "--vault-root",
        vault,
        "--offset",
        "1",
    ]);
    assert_eq!(code, 2, "{stale}");
    assert_eq!(stale["error"]["code"], "continuation_mismatch");
    assert_eq!(stale["error"]["details"]["restart_required"], true);
    let (code, restarted) = fixture.run(&[
        "--execution-mode",
        "required-daemon",
        "doc",
        "list",
        "--vault-root",
        vault,
        "--limit",
        "1",
    ]);
    assert_eq!(code, 0, "{restarted}");
    assert_ne!(restarted["meta"]["continuation"]["token"], token);
}

#[test]
fn slow_response_delivery_has_an_absolute_deadline() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    let fixture = Fixture::new();
    let line = "bounded delivery payload ".repeat(48);
    fs::write(fixture.vault.join("large.txt"), (line + "\n").repeat(1000))
        .expect("large text fixture");
    fixture.index();
    let _daemon = fixture.start();
    let status = fixture.request(b"{\"kind\":\"status\"}");
    let mut stream = UnixStream::connect(&fixture.socket).expect("slow response client");
    let payload = json!({"kind":"execute","payload":{
        "command":{"Doc":{"command":{"Read":{"vault_root":fixture.vault,"db_path":null,"path":"large.txt","offset":0,"limit":1000,"revision":null}}}},
        "json":true,"json_stream":false,"protocol":status["protocol"],"build":status["build"],"request_id":"slow-delivery","timeout_ms":1000,
        "context":{"vault_root":fixture.vault,"data_dir":fixture.vault.join(".tao"),"db_path":fixture.vault.join(".tao/index.sqlite"),"case_insensitive":false}
    }});
    stream
        .write_all(payload.to_string().as_bytes())
        .expect("slow request");
    stream
        .shutdown(Shutdown::Write)
        .expect("finish slow request");
    stream
        .set_nonblocking(true)
        .expect("nonblocking slow client");
    let stop = Arc::new(AtomicBool::new(false));
    let reader_stop = Arc::clone(&stop);
    let reader = thread::spawn(move || {
        let mut prefix = Vec::new();
        let mut received = 0;
        let mut buffer = [0_u8; 8192];
        while !reader_stop.load(Ordering::Acquire) {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    received += count;
                    if prefix.len() < 128 {
                        prefix.extend_from_slice(&buffer[..count.min(128 - prefix.len())]);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => break,
            }
            // Each partial drain previously restarted SO_SNDTIMEO + write_all.
            thread::sleep(Duration::from_millis(100));
        }
        (prefix, received)
    });
    thread::sleep(Duration::from_millis(3200));
    let status = fixture.request(b"{\"kind\":\"status\"}");
    stop.store(true, Ordering::Release);
    let (prefix, received) = reader.join().expect("slow reader");
    assert!(
        String::from_utf8_lossy(&prefix).starts_with("{\"ok\":true"),
        "large read failed: {}",
        String::from_utf8_lossy(&prefix)
    );
    assert!(
        received > 0 && received < 900_000,
        "reader should have consumed only part of the large response"
    );
    assert_eq!(
        status["status"]["active_requests"], 1,
        "delivery must release its request slot despite continuing partial drains: {status}"
    );
}

#[test]
fn restarting_with_only_an_expired_running_job_recovers_the_queue() {
    let fixture = Fixture::new();
    // No PDF backend is invoked: this exhausted lease must transition directly to failure.
    fs::write(
        fixture.vault.join("leased.pdf"),
        b"%PDF-1.4\nfixture awaiting extraction\n%%EOF\n",
    )
    .expect("PDF inventory fixture");
    fixture.index();
    let database = rusqlite::Connection::open(fixture.vault.join(".tao/index.sqlite"))
        .expect("inspect persistent queue");
    assert_eq!(database.execute("UPDATE extraction_jobs SET state='running',lease_token='crashed-worker',lease_until_ms=1,attempts=3", []).expect("simulate stopped worker"), 1);
    database.execute("INSERT INTO extraction_staged_pages(job_id,ordinal,text,method,coverage) SELECT job_id,1,'abandoned staged page','native','complete' FROM extraction_jobs", []).expect("simulate abandoned staged output");
    let _daemon = fixture.start();
    let (code, response) = fixture.run(&[
        "--execution-mode",
        "required-daemon",
        "doc",
        "list",
        "--vault-root",
        fixture.vault.to_str().expect("vault"),
    ]);
    assert_eq!(code, 0, "{response}");
    let deadline = Instant::now() + Duration::from_secs(4);
    let state = loop {
        let state: String = database
            .query_row("SELECT state FROM extraction_jobs", [], |row| row.get(0))
            .expect("job state");
        if state != "running" {
            break state;
        }
        assert!(
            Instant::now() < deadline,
            "sole running lease was never scheduled for recovery"
        );
        thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(state, "failed");
    let (coverage, staged): (String,u64) = database.query_row("SELECT coverage,(SELECT COUNT(*) FROM extraction_staged_pages) FROM content_documents WHERE format='pdf'", [], |row| Ok((row.get(0)?,row.get(1)?))).expect("recovery coverage");
    assert_eq!(coverage, "failed");
    assert_eq!(staged, 0);
}
