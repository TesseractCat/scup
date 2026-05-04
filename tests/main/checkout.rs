use crate::common::{bin, run_ok, unique_temp_dir};
use std::fs;
use std::process::Command;

#[test]
fn checkout_moves_head_and_removes_files_not_in_target_snapshot() {
    let repo_dir = unique_temp_dir("scup-checkout");

    fs::write(repo_dir.join("note.txt"), b"v1\n").expect("failed to write initial file");

    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&repo_dir).arg("init");
            cmd
        },
        "scup init",
    );

    let repo_after_init = scup::RepositorySession::load(&repo_dir)
        .expect("failed to load repo after init")
        .repository;
    let first_head = repo_after_init.head;

    fs::write(repo_dir.join("note.txt"), b"v2\n").expect("failed to update file");
    fs::write(repo_dir.join("extra.txt"), b"extra\n").expect("failed to write extra file");
    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&repo_dir)
                .arg("snapshot")
                .arg("-m")
                .arg("second snapshot");
            cmd
        },
        "scup snapshot",
    );

    let repo_after_second = scup::RepositorySession::load(&repo_dir)
        .expect("failed to load repo after second snapshot")
        .repository;
    let second_head = repo_after_second.head;
    assert_ne!(
        first_head, second_head,
        "head should move after second snapshot"
    );
    assert_eq!(
        fs::read(repo_dir.join("note.txt")).expect("failed to read working tree file"),
        b"v2\n"
    );
    assert!(
        repo_dir.join("extra.txt").exists(),
        "extra file should exist in second snapshot"
    );

    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&repo_dir)
                .arg("checkout")
                .arg(first_head.to_hex());
            cmd
        },
        "scup checkout <first_head>",
    );

    assert_eq!(
        fs::read(repo_dir.join("note.txt")).expect("failed to read checked out file"),
        b"v1\n"
    );
    assert!(
        !repo_dir.join("extra.txt").exists(),
        "checkout should remove files not present in target snapshot"
    );

    let repo_after_checkout = scup::RepositorySession::load(&repo_dir)
        .expect("failed to load repo after checkout")
        .repository;
    assert_eq!(
        repo_after_checkout.head, first_head,
        "checkout should move repository head to the checked-out snapshot"
    );

    let _ = fs::remove_dir_all(repo_dir);
}
