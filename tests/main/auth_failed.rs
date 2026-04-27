use crate::common::{ChildGuard, bin, free_port, run_ok, unique_temp_dir, wait_for_tcp};
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn wait_for_scan_host(local_dir: &Path, expected_fullname: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        let output = run_ok(
            {
                let mut cmd = Command::new(bin());
                cmd.current_dir(local_dir)
                    .arg("scan")
                    .arg("--timeout")
                    .arg("2");
                cmd
            },
            "syncup scan",
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.lines().any(|line| line.contains(expected_fullname)) {
            return;
        }

        thread::sleep(Duration::from_millis(250));
    }

    panic!("did not discover host `{expected_fullname}` in time");
}

fn generate_key(path: &Path) {
    run_ok(
        {
            let mut cmd = Command::new("ssh-keygen");
            cmd.arg("-t")
                .arg("ed25519")
                .arg("-N")
                .arg("")
                .arg("-f")
                .arg(path)
                .arg("-q");
            cmd
        },
        "ssh-keygen",
    );
}

#[test]
fn authentication_fails_with_invalid_key() {
    let root = unique_temp_dir("syncup-auth-fail");
    let repo_dir = root.join("repo");
    let client_dir = root.join("client");
    fs::create_dir_all(&repo_dir).expect("failed to create repo dir");
    fs::create_dir_all(&client_dir).expect("failed to create client dir");

    fs::write(repo_dir.join("hello.txt"), b"hello\n").expect("failed to write seed file");
    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&repo_dir).arg("init");
            cmd
        },
        "syncup init",
    );

    let valid_key = root.join("valid_key");
    let invalid_key = root.join("invalid_key");
    generate_key(&valid_key);
    generate_key(&invalid_key);

    let port = free_port();
    let host_tag = format!("syncup-test-auth-{}", std::process::id());
    let expected_fullname = format!("syncup-{host_tag}._syncup._tcp.local.");

    let server = ChildGuard({
        let mut cmd = Command::new(bin());
        cmd.current_dir(&repo_dir)
            .env("HOSTNAME", &host_tag)
            .arg("--key")
            .arg(&valid_key)
            .arg("serve")
            .arg("--port")
            .arg(port.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd.spawn().expect("failed to start syncup serve")
    });

    wait_for_tcp(("127.0.0.1", port), Duration::from_secs(10));
    wait_for_scan_host(&client_dir, &expected_fullname, Duration::from_secs(20));

    let out = {
        let mut cmd = Command::new(bin());
        cmd.current_dir(&client_dir)
            .arg("--key")
            .arg(&invalid_key)
            .arg("debug")
            .arg("status")
            .arg(&expected_fullname);
        cmd.output().expect("failed to run syncup debug status")
    };

    assert!(
        !out.status.success(),
        "expected auth to fail, but command succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
    .to_ascii_lowercase();
    assert!(
        combined.contains("authentication failed"),
        "expected authentication failure message, got:\n{}",
        combined
    );

    drop(server);
    let _ = fs::remove_dir_all(root);
}
