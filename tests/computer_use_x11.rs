#![cfg(target_os = "linux")]

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use hekate::adapters::computer_use::ComputerUseAdapter;

struct Fixture {
    children: Vec<Child>,
    workspace: std::path::PathBuf,
    previous_display: Option<String>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for child in self.children.iter_mut().rev() {
            let _ = child.kill();
            let _ = child.wait();
        }
        match self.previous_display.take() {
            Some(value) => std::env::set_var("DISPLAY", value),
            None => std::env::remove_var("DISPLAY"),
        }
        let _ = std::fs::remove_dir_all(&self.workspace);
    }
}

#[test]
fn xvfb_observes_and_changes_a_local_gui() {
    if !commands_exist(&["Xvfb", "openbox", "python3"]) {
        eprintln!("skipping X11 fixture: Xvfb, openbox, or Python/Tk is unavailable");
        return;
    }

    let workspace = std::env::temp_dir().join(format!("hekate-computer-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&workspace).expect("create test workspace");
    let previous_display = std::env::var("DISPLAY").ok();
    let mut xvfb = Command::new("Xvfb")
        .args([
            "-displayfd",
            "1",
            "-screen",
            "0",
            "800x600x24",
            "-nolisten",
            "tcp",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("start Xvfb");
    let mut display = String::new();
    BufReader::new(xvfb.stdout.take().expect("Xvfb display pipe"))
        .read_line(&mut display)
        .expect("read Xvfb display");
    std::env::set_var("DISPLAY", format!(":{}", display.trim()));

    let openbox = Command::new("openbox")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start openbox");
    let dialog = Command::new("python3")
        .args([
            "-c",
            "import tkinter as t; r=t.Tk(); r.title('HEKATE fixture'); r.geometry('300x160'); e=t.Entry(r); e.pack(fill='x',padx=10,pady=10); t.Button(r,text='OK',command=r.destroy).pack(fill='both',expand=True,padx=10,pady=10); r.after(100,e.focus_force); r.mainloop()",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start local GUI fixture");
    let _fixture = Fixture {
        children: vec![xvfb, openbox, dialog],
        workspace: workspace.clone(),
        previous_display,
    };
    let adapter = ComputerUseAdapter::new(workspace);
    let deadline = Instant::now() + Duration::from_secs(5);
    let window = loop {
        if let Ok(Some(window)) = adapter.list_windows().map(|windows| {
            windows
                .into_iter()
                .find(|window| window.title == "HEKATE fixture")
        }) {
            break window;
        }
        assert!(Instant::now() < deadline, "fixture window did not appear");
        std::thread::sleep(Duration::from_millis(50));
    };

    let screenshot = adapter.screenshot().expect("capture Xvfb screenshot");
    assert!(std::path::Path::new(&screenshot.path).is_file());
    adapter
        .focus_window(window.id)
        .expect("focus fixture window");
    let typed = adapter.type_text("HEKATE").expect("type into fixture");
    assert!(typed.state_changed, "typing should change the observed UI");

    let x = window.bounds.x + i32::try_from(window.bounds.width / 2).unwrap_or(0);
    let y = window.bounds.y + i32::try_from(window.bounds.height * 3 / 4).unwrap_or(0);
    let clicked = adapter.click(x, y, false).expect("click fixture button");
    assert!(clicked.state_changed, "click should change the observed UI");
}

fn commands_exist(commands: &[&str]) -> bool {
    commands.iter().all(|command| {
        Command::new("sh")
            .args(["-c", "command -v \"$1\" >/dev/null", "sh", command])
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    })
}
