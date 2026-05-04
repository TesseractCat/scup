use crate::common::{ChildGuard, bin, free_port, run_ok, unique_temp_dir, wait_for_tcp};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn wait_for_scan_host(local_dir: &Path, expected_host_id: &str, timeout: Duration) {
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
            "scup scan",
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.lines().any(|line| line.contains(expected_host_id)) {
            return;
        }

        thread::sleep(Duration::from_millis(250));
    }

    panic!("did not discover host `{expected_host_id}` in time");
}

fn sha256_file(path: &Path) -> String {
    let mut file =
        fs::File::open(path).unwrap_or_else(|e| panic!("failed to open {}: {e}", path.display()));
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024];

    loop {
        let n = file
            .read(&mut buf)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn modified_time(path: &Path) -> std::time::SystemTime {
    fs::metadata(path)
        .unwrap_or_else(|e| panic!("failed to stat {}: {e}", path.display()))
        .modified()
        .unwrap_or_else(|e| panic!("failed to read mtime {}: {e}", path.display()))
}

#[test]
fn clone_repo_from_host_by_root_name() {
    let root = unique_temp_dir("scup-clone");
    let origin = root.join("origin-repo");
    let client = root.join("client");

    fs::create_dir_all(&origin).expect("failed to create origin dir");
    fs::create_dir_all(&client).expect("failed to create client dir");

    fs::write(origin.join("hello.txt"), b"hello clone\n").expect("failed to write seed file");
    run_ok(
        {
            let mut cmd = Command::new("dd");
            cmd.arg("if=/dev/random")
                .arg(format!("of={}", origin.join("random.bin").display()))
                .arg("bs=1M")
                .arg("count=50")
                .arg("status=none");
            cmd
        },
        "dd random.bin",
    );
    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&origin).arg("init");
            cmd
        },
        "scup init (origin)",
    );

    let port = free_port();
    let host_tag = format!("scup-test-clone-{}", std::process::id());

    let server = ChildGuard({
        let mut cmd = Command::new(bin());
        cmd.current_dir(&origin)
            .env("HOSTNAME", &host_tag)
            .arg("serve")
            .arg("--port")
            .arg(port.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd.spawn().expect("failed to start scup serve")
    });

    wait_for_tcp(("127.0.0.1", port), Duration::from_secs(10));
    wait_for_scan_host(&client, &host_tag, Duration::from_secs(20));

    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&client)
                .arg("clone")
                .arg(&host_tag)
                .arg("origin-repo");
            cmd
        },
        "scup clone",
    );

    let cloned_repo_dir = client.join("origin-repo");
    assert!(
        cloned_repo_dir.join(".scup/repository").exists(),
        "cloned repository file missing"
    );
    assert!(
        cloned_repo_dir.join("hello.txt").exists(),
        "cloned hello.txt missing"
    );
    assert!(
        cloned_repo_dir.join("random.bin").exists(),
        "cloned random.bin missing"
    );

    let src_repo = scup::RepositorySession::load(&origin)
        .expect("failed to load origin repository session")
        .repository;
    let cloned_repo = scup::RepositorySession::load(&cloned_repo_dir)
        .expect("failed to load cloned repository session")
        .repository;

    assert_eq!(cloned_repo.repo_uuid, src_repo.repo_uuid);
    assert_eq!(cloned_repo.head, src_repo.head);
    assert_eq!(cloned_repo.objects.len(), src_repo.objects.len());

    assert_eq!(
        fs::read(cloned_repo_dir.join("hello.txt")).expect("failed to read cloned hello.txt"),
        fs::read(origin.join("hello.txt")).expect("failed to read origin hello.txt")
    );

    assert_eq!(
        fs::metadata(cloned_repo_dir.join("random.bin"))
            .expect("failed to stat cloned random.bin")
            .len(),
        50 * 1024 * 1024
    );
    assert_eq!(
        sha256_file(&cloned_repo_dir.join("random.bin")),
        sha256_file(&origin.join("random.bin"))
    );

    assert_eq!(
        modified_time(&cloned_repo_dir.join("hello.txt")),
        modified_time(&origin.join("hello.txt")),
        "hello.txt mtime differs after clone"
    );
    assert_eq!(
        modified_time(&cloned_repo_dir.join("random.bin")),
        modified_time(&origin.join("random.bin")),
        "random.bin mtime differs after clone"
    );

    drop(server);
    let _ = fs::remove_dir_all(root);
}
