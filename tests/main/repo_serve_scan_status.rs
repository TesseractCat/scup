use crate::common::{ChildGuard, bin, free_port, run_ok, unique_temp_dir, wait_for_tcp};
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn scan_until_found(expected_host_id: &str, expected_port: u16) {
    let deadline = Instant::now() + Duration::from_secs(15);

    while Instant::now() < deadline {
        let output = run_ok(
            {
                let mut cmd = Command::new(bin());
                cmd.arg("scan").arg("--timeout").arg("2");
                cmd
            },
            "scup scan",
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        let has_host = stdout.lines().any(|line| line.contains(expected_host_id));
        let has_port = stdout
            .lines()
            .any(|line| line.contains(&format!(":{expected_port}")));

        if has_host && has_port {
            return;
        }

        thread::sleep(Duration::from_millis(250));
    }

    panic!("did not scan expected host `{expected_host_id}` on port {expected_port} in time");
}

#[test]
fn init_serve_scan_and_get_status() {
    let repo_dir = unique_temp_dir("scup-e2e-repo");

    // Create tiny content so init's first snapshot has at least one regular file.
    fs::write(repo_dir.join("hello.txt"), b"hello scup\n").expect("failed to write seed file");

    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.arg("init").current_dir(&repo_dir);
            cmd
        },
        "scup init",
    );

    assert!(Path::new(&repo_dir.join(".scup/repository")).exists());

    let port = free_port();
    let host_tag = format!("scup-test-{}", std::process::id());
    let expected_fullname = format!("scup-{host_tag}._scup._tcp.local.");

    let child = {
        let mut cmd = Command::new(bin());
        cmd.arg("serve")
            .arg("--port")
            .arg(port.to_string())
            .env("HOSTNAME", &host_tag)
            .current_dir(&repo_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd.spawn().expect("failed to spawn scup serve")
    };
    let child = ChildGuard(child);

    wait_for_tcp(("127.0.0.1", port), Duration::from_secs(10));

    scan_until_found(&host_tag, port);

    let status_out = run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.arg("debug").arg("status").arg(&host_tag);
            cmd
        },
        "scup debug status",
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
