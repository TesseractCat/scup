use crate::common::{bin, run_ok, unique_temp_dir};
use std::fs;
use std::process::Command;

#[test]
fn checkout_restores_selected_snapshot_without_moving_head() {
    let repo_dir = unique_temp_dir("syncup-checkout");

    fs::write(repo_dir.join("note.txt"), b"v1\n").expect("failed to write initial file");

    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&repo_dir).arg("init");
            cmd
        },
        "syncup init",
    );

    let repo_after_init = syncup::Repository::load(&repo_dir);
    let first_head = repo_after_init.head;

    fs::write(repo_dir.join("note.txt"), b"v2\n").expect("failed to update file");
    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&repo_dir)
                .arg("snapshot")
                .arg("-m")
                .arg("second snapshot");
            cmd
        },
        "syncup snapshot",
    );

    let repo_after_second = syncup::Repository::load(&repo_dir);
    let second_head = repo_after_second.head;
    assert_ne!(
        first_head, second_head,
        "head should move after second snapshot"
    );
    assert_eq!(
        fs::read(repo_dir.join("note.txt")).expect("failed to read working tree file"),
        b"v2\n"
    );

    run_ok(
        {
            let mut cmd = Command::new(bin());
            cmd.current_dir(&repo_dir)
                .arg("checkout")
                .arg(syncup::to_hex(&first_head.0));
            cmd
        },
        "syncup checkout <first_head>",
    );

    assert_eq!(
        fs::read(repo_dir.join("note.txt")).expect("failed to read checked out file"),
        b"v1\n"
    );

    let repo_after_checkout = syncup::Repository::load(&repo_dir);
    assert_eq!(
        repo_after_checkout.head, second_head,
        "checkout should not modify repository head"
    );

    let _ = fs::remove_dir_all(repo_dir);
}
