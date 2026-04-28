use crate::common::{ChildGuard, bin, free_port, run_ok, unique_temp_dir, wait_for_tcp};
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use syncup::{Blob, List, Map, Object, ObjectId, Repository, to_hex};

fn copy_dir_recursive(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).expect("failed to create destination directory");
    for entry in fs::read_dir(src).expect("failed to read source directory") {
        let entry = entry.expect("failed to read directory entry");
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let ft = entry.file_type().expect("failed to read file type");
        if ft.is_dir() {
            copy_dir_recursive(&src_path, &dst_path);
        } else if ft.is_file() {
            fs::copy(&src_path, &dst_path).unwrap_or_else(|e| {
                panic!(
                    "failed to copy {} -> {}: {e}",
                    src_path.display(),
                    dst_path.display()
                )
            });
        }
    }
}

fn wait_for_scan_hosts(local_dir: &Path, expected_fullnames: &[String], timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let out = run_ok(
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
        let stdout = String::from_utf8_lossy(&out.stdout);
        let ok = expected_fullnames
            .iter()
            .all(|name| stdout.lines().any(|line| line.contains(name)));
        if ok {
            return;
        }
        thread::sleep(Duration::from_millis(250));
    }

    panic!("did not discover all expected hosts in time");
}

fn read_repo(dir: &Path) -> Repository {
    let bytes = fs::read(dir.join(".syncup/repository")).expect("failed to read repository file");
    postcard::from_bytes(&bytes).expect("failed to deserialize repository")
}

fn head_tree(repo: &Repository) -> &Map {
    let snap = match repo.objects.get(&repo.head) {
        Some(Object::Snapshot(s)) => s,
        _ => panic!("head is not a snapshot"),
    };
    match repo.objects.get(&snap.tree) {
        Some(Object::Map(t)) => t,
        _ => panic!("snapshot map missing"),
    }
}

fn blob_for_file<'a>(repo: &'a Repository, filename: &str) -> &'a Blob {
    let tree = head_tree(repo);
    let (_, blob_id) = tree
        .entries
        .iter()
        .find(|(path, _)| {
            path.as_str() == filename
                || path.ends_with(&format!("/{filename}"))
                || path.as_str() == format!("./{filename}")
        })
        .unwrap_or_else(|| panic!("file not found in head tree: {filename}"));

    match repo.objects.get(blob_id) {
        Some(Object::Blob(b)) => b,
        _ => panic!("blob object missing for file: {filename}"),
    }
}

fn collect_chunk_ids(repo: &Repository, list_id: ObjectId, out: &mut Vec<ObjectId>) {
    let list = match repo.objects.get(&list_id) {
        Some(Object::List(List { entries })) => entries,
        _ => panic!("missing list object: {}", to_hex(&list_id.0)),
    };

    for entry in list {
        match repo.objects.get(entry) {
            Some(Object::Chunk(_)) => out.push(*entry),
            Some(Object::List(_)) => collect_chunk_ids(repo, *entry, out),
            _ => panic!("list entry is neither chunk nor list"),
        }
    }
}

fn file_content_from_repo(dir: &Path, repo: &Repository, filename: &str) -> Vec<u8> {
    let blob = blob_for_file(repo, filename);
    let mut chunk_ids = Vec::new();
    collect_chunk_ids(repo, blob.chunks, &mut chunk_ids);

    let mut out = Vec::new();
    for id in chunk_ids {
        let path = dir.join(format!(".syncup/chunks/{}", to_hex(&id.0)));
        let bytes = fs::read(path).expect("failed to read chunk");
        out.extend_from_slice(&bytes);
    }
    out
}

#[test]
fn push_pull_multiple_remotes_then_conflict() {
    let root = unique_temp_dir("syncup-multi-remote");
    let seed = root.join("seed");
    let local = root.join("local");
    let remote1 = root.join("remote1");
    let remote2 = root.join("remote2");

    fs::create_dir_all(&seed).expect("failed to create seed dir");
    fs::write(seed.join("base.txt"), b"base\n").expect("failed to write base file");

    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&seed).arg("init");
            cmd
        },
        "syncup init (seed)",
    );

    copy_dir_recursive(&seed, &local);
    copy_dir_recursive(&seed, &remote1);
    copy_dir_recursive(&seed, &remote2);

    let port1 = free_port();
    let port2 = free_port();
    let host1 = format!("syncup-test-r1-{}", std::process::id());
    let host2 = format!("syncup-test-r2-{}", std::process::id());
    let full1 = format!("syncup-{host1}._syncup._tcp.local.");
    let full2 = format!("syncup-{host2}._syncup._tcp.local.");

    let server1 = ChildGuard({
        let mut cmd = Command::new(bin());
        cmd.current_dir(&remote1)
            .env("HOSTNAME", &host1)
            .arg("serve")
            .arg("--port")
            .arg(port1.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd.spawn().expect("failed to start server1")
    });

    let server2 = ChildGuard({
        let mut cmd = Command::new(bin());
        cmd.current_dir(&remote2)
            .env("HOSTNAME", &host2)
            .arg("serve")
            .arg("--port")
            .arg(port2.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd.spawn().expect("failed to start server2")
    });

    wait_for_tcp(("127.0.0.1", port1), Duration::from_secs(10));
    wait_for_tcp(("127.0.0.1", port2), Duration::from_secs(10));
    wait_for_scan_hosts(
        &local,
        &[full1.clone(), full2.clone()],
        Duration::from_secs(20),
    );

    // 1) Standard push to multiple remotes.
    fs::write(local.join("from_local.txt"), b"hello from local\n")
        .expect("failed to write local file");
    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&local)
                .arg("snapshot")
                .arg("-m")
                .arg("local change before push");
            cmd
        },
        "local snapshot before push",
    );

    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&local).arg("push");
            cmd
        },
        "local push",
    );

    let r1_repo = read_repo(&remote1);
    let r2_repo = read_repo(&remote2);
    let r1_tree = head_tree(&r1_repo);
    let r2_tree = head_tree(&r2_repo);
    assert!(r1_tree.entries.keys().any(|p| p.ends_with("from_local.txt")
        || p == "from_local.txt"
        || p == "./from_local.txt"));
    assert!(r2_tree.entries.keys().any(|p| p.ends_with("from_local.txt")
        || p == "from_local.txt"
        || p == "./from_local.txt"));
    assert_eq!(
        fs::read(remote1.join("from_local.txt")).expect("remote1 missing from_local.txt"),
        b"hello from local\n"
    );
    assert_eq!(
        fs::read(remote2.join("from_local.txt")).expect("remote2 missing from_local.txt"),
        b"hello from local\n"
    );

    // 2) Standard pull from one remote (still through multi-remote pull command).
    fs::write(remote1.join("from_remote1.txt"), b"hello from remote1\n")
        .expect("failed to write remote1 file");
    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&remote1)
                .arg("snapshot")
                .arg("-m")
                .arg("remote1 change before pull");
            cmd
        },
        "remote1 snapshot",
    );

    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&local).arg("pull");
            cmd
        },
        "local pull",
    );

    let local_repo_after_pull = read_repo(&local);
    let local_tree_after_pull = head_tree(&local_repo_after_pull);
    assert!(
        local_tree_after_pull
            .entries
            .keys()
            .any(|p| p.ends_with("from_remote1.txt")
                || p == "from_remote1.txt"
                || p == "./from_remote1.txt"),
        "local repo missing file from remote1 after pull"
    );
    assert_eq!(
        fs::read(local.join("from_remote1.txt"))
            .expect("local missing from_remote1.txt after pull"),
        b"hello from remote1\n"
    );

    // 3) Conflict: remote1 older, remote2 newer; pull should keep newer mtime content.
    fs::write(remote1.join("conflict.txt"), b"older\n")
        .expect("failed to write conflict on remote1");
    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&remote1)
                .arg("snapshot")
                .arg("-m")
                .arg("remote1 older conflict");
            cmd
        },
        "remote1 conflict snapshot",
    );

    thread::sleep(Duration::from_millis(1200));

    fs::write(remote2.join("conflict.txt"), b"newer\n")
        .expect("failed to write conflict on remote2");
    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&remote2)
                .arg("snapshot")
                .arg("-m")
                .arg("remote2 newer conflict");
            cmd
        },
        "remote2 conflict snapshot",
    );

    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&local).arg("pull");
            cmd
        },
        "local pull after conflict",
    );

    let local_repo_final = read_repo(&local);
    let conflict_content = file_content_from_repo(&local, &local_repo_final, "conflict.txt");
    assert_eq!(conflict_content, b"newer\n");
    assert_eq!(
        fs::read(local.join("conflict.txt")).expect("local missing conflict.txt"),
        b"newer\n"
    );

    drop(server1);
    drop(server2);

    let _ = fs::remove_dir_all(root);
}
