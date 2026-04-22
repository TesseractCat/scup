use std::fs;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_syncup")
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
    fs::create_dir_all(&dir).expect("failed to create temporary directory");
    dir
}

fn free_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("failed to bind to an ephemeral port");
    let port = listener
        .local_addr()
        .expect("failed to read local addr")
        .port();
    drop(listener);
    port
}

fn run_ok(mut cmd: Command, what: &str) -> std::process::Output {
    let output = cmd.output().unwrap_or_else(|e| panic!("failed to run {what}: {e}"));
    assert!(
        output.status.success(),
        "{what} failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn wait_for_tcp(addr: (&str, u16), timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("server did not start listening on {}:{} within {:?}", addr.0, addr.1, timeout);
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match self.0.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
                _ => {
                    let _ = self.0.kill();
                    let _ = self.0.wait();
                    break;
                }
            }
        }
    }
}

fn scan_until_found(expected_fullname: &str, expected_port: u16) {
    let deadline = Instant::now() + Duration::from_secs(15);

    while Instant::now() < deadline {
        let output = run_ok(
            {
                let mut cmd = Command::new(bin());
                cmd.arg("scan").arg("--timeout").arg("2");
                cmd
            },
            "syncup scan",
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        let needle = format!("- {expected_fullname} at ");
        let has_host = stdout.lines().any(|line| line.contains(&needle));
        let has_port = stdout
            .lines()
            .any(|line| line.contains(&format!(":{expected_port}")));

        if has_host && has_port {
            return;
        }

        thread::sleep(Duration::from_millis(250));
    }

    panic!(
        "did not scan expected host `{expected_fullname}` on port {expected_port} in time"
    );
}

#[test]
fn init_serve_scan_and_get_status() {
    let repo_dir = unique_temp_dir("syncup-e2e-repo");

    // Create tiny content so init's first snapshot has at least one regular file.
    fs::write(repo_dir.join("hello.txt"), b"hello syncup\n").expect("failed to write seed file");

    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.arg("init").current_dir(&repo_dir);
            cmd
        },
        "syncup init",
    );

    assert!(Path::new(&repo_dir.join(".syncup/repository")).exists());

    let port = free_port();
    let host_tag = format!("syncup-test-{}", std::process::id());
    let expected_fullname = format!("syncup-{host_tag}._syncup._tcp.local.");

    let child = {
        let mut cmd = Command::new(bin());
        cmd.arg("serve")
            .arg("--port")
            .arg(port.to_string())
            .env("HOSTNAME", &host_tag)
            .current_dir(&repo_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd.spawn().expect("failed to spawn syncup serve")
    };
    let child = ChildGuard(child);

    wait_for_tcp(("127.0.0.1", port), Duration::from_secs(10));

    scan_until_found(&expected_fullname, port);

    let status_out = run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.arg("debug").arg("status").arg(&expected_fullname);
            cmd
        },
        "syncup debug status",
    );

    let stdout = String::from_utf8_lossy(&status_out.stdout);
    assert!(
        stdout.contains(&format!("- {expected_fullname} status: head=")),
        "unexpected status output:\n{stdout}"
    );

    // Explicitly drop to kill server before temp cleanup.
    drop(child);

    let _ = fs::remove_dir_all(repo_dir);
}
