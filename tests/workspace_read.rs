use std::fs;

use hekate::adapters::local_workspace::{LocalWorkspace, WorkspaceError};

fn temporary_directory(name: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("hekate-{name}-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&path).unwrap();
    path
}

#[cfg(unix)]
#[tokio::test]
async fn workspace_paths_stay_inside_the_configured_root() {
    let root = temporary_directory("workspace");
    let outside = temporary_directory("outside");
    fs::write(root.join("allowed.txt"), "allowed").unwrap();
    fs::write(outside.join("secret.txt"), "secret").unwrap();
    std::os::unix::fs::symlink(&outside, root.join("outside-link")).unwrap();

    let workspace = LocalWorkspace::new(&root).unwrap();
    assert_eq!(
        workspace.read_text("allowed.txt").await.unwrap().content,
        "allowed"
    );
    assert!(matches!(
        workspace
            .read_text(&format!(
                "../{}/secret.txt",
                outside.file_name().unwrap().to_string_lossy()
            ))
            .await,
        Err(WorkspaceError::Escape)
    ));
    assert!(matches!(
        workspace.read_text("outside-link/secret.txt").await,
        Err(WorkspaceError::Escape)
    ));
    assert!(matches!(
        workspace.write_text("../escaped.txt", "nope").await,
        Err(WorkspaceError::Escape)
    ));

    fs::remove_dir_all(root).unwrap();
    fs::remove_dir_all(outside).unwrap();
}
