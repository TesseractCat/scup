use std::fs;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Output};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_syncup")
}

pub fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
    fs::create_dir_all(&dir).expect("failed to create temporary directory");
    dir
}

pub fn free_port() -> u16 {
    let listener =
        TcpListener::bind(("127.0.0.1", 0)).expect("failed to bind to an ephemeral port");
    let port = listener
        .local_addr()
        .expect("failed to read local addr")
        .port();
    drop(listener);
    port
}

pub fn run_ok(mut cmd: Command, what: &str) -> Output {
    let output = cmd
        .output()
        .unwrap_or_else(|e| panic!("failed to run {what}: {e}"));
    assert!(
        output.status.success(),
        "{what} failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

pub fn wait_for_tcp(addr: (&str, u16), timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "server did not start listening on {}:{} within {:?}",
        addr.0, addr.1, timeout
    );
}

pub struct ChildGuard(pub Child);

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
