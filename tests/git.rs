use std::fs;
use std::process::Command;

use hekate::adapters::git::LocalGit;

fn temporary_directory() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("hekate-git-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn git(root: &std::path::Path, args: &[&str]) {
    let status = Command::new("/usr/bin/git")
        .args(args)
        .current_dir(root)
        .status()
        .unwrap();
    assert!(status.success());
}

#[tokio::test]
async fn reads_status_diff_and_log_from_its_repository_root() {
    let root = temporary_directory();
    git(&root, &["init", "--quiet"]);
    git(&root, &["config", "user.name", "HEKATE test"]);
    git(&root, &["config", "user.email", "test@example.invalid"]);
    fs::write(root.join("file.txt"), "one\n").unwrap();
    git(&root, &["add", "file.txt"]);
    git(&root, &["commit", "--quiet", "-m", "initial"]);
    fs::write(root.join("file.txt"), "two\n").unwrap();

    let repository = LocalGit::new(&root).await.unwrap();
    assert!(repository
        .status()
        .await
        .unwrap()
        .output
        .contains("file.txt"));
    assert!(repository.diff().await.unwrap().output.contains("-one"));
    assert!(repository.log(1).await.unwrap().output.contains("initial"));

    fs::remove_dir_all(root).unwrap();
}
