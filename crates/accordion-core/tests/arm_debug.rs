//! Temporary: finds which sandbox rule makes ffmpeg stop with SIGILL on
//! the ARM runner.

use std::process::Command;

const BOX: &str = r#"
import ctypes, os, resource, sys
libc = ctypes.CDLL(None, use_errno=True)
mode = sys.argv[1]
if "mdwe" in mode:
    if libc.prctl(65, 1, 0, 0, 0) != 0:
        print("prctl failed", ctypes.get_errno(), file=sys.stderr)
if "mem" in mode:
    resource.setrlimit(resource.RLIMIT_DATA, (1 << 30, 1 << 30))
os.execvp(sys.argv[2], sys.argv[2:])
"#;

fn sh(script: &str) -> String {
    let out = Command::new("bash").args(["-c", script]).output().unwrap();
    format!(
        "$ {script}\n{}{}[{}]\n",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
        out.status
    )
}

#[test]
fn find_the_sigill() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().display();
    std::fs::write(dir.path().join("box.py"), BOX).unwrap();
    let mut log = String::new();
    log += &sh("uname -a; grep -m1 Features /proc/cpuinfo; ffmpeg -version | head -1");
    log += &sh(&format!(
        "ffmpeg -nostdin -v error -y -f lavfi -i testsrc2=size=640x360:rate=30:duration=2 \
         -f lavfi -i sine=duration=2 -c:v libvpx-vp9 -deadline realtime -c:a libopus {d}/talk.mkv"
    ));
    let font = "$(fc-list : file | grep -m1 -i 'liberation.*sans.*regular' | cut -d: -f1)";
    let copy =
        format!("ffmpeg -nostdin -v error -i {d}/talk.mkv -map 0 -c copy -f webm -y {d}/out.webm");
    let draw = format!(
        "ffmpeg -nostdin -v error -threads 2 -f lavfi -i color=c=white:s=480x360:d=1 \
         -vf drawtext=fontfile={font}:expansion=none:text=Aa:fontsize=160:x=10:y=10 \
         -frames:v 1 -c:v png -f image2pipe - > {d}/out.png"
    );
    let png = format!(
        "ffmpeg -nostdin -v error -f lavfi -i color=c=white:s=480x360:d=1 \
         -frames:v 1 -c:v png -f image2pipe - > {d}/plain.png"
    );
    let mkv = format!(
        "ffmpeg -nostdin -v error -i {d}/talk.mkv -map 0 -c copy -f matroska -y {d}/out.mkv"
    );
    for mode in ["none", "mem", "mdwe", "mem+mdwe"] {
        for (name, command) in [
            ("copy", &copy),
            ("draw", &draw),
            ("png", &png),
            ("mkv", &mkv),
        ] {
            log += &format!("== {mode} {name}\n");
            log += &sh(&format!("python3 {d}/box.py {mode} {command}"));
        }
    }
    log += &sh("sudo apt-get install -y -q gdb >/dev/null 2>&1; gdb --version | head -1");
    for command in [&copy, &png] {
        let gdb_command = command
            .replace(&format!(" > {d}/plain.png"), &format!(" -y {d}/g.png"))
            .replace("image2pipe -", "image2");
        log += &sh(&format!(
            "gdb -batch -ex run -ex bt -ex 'x/6i $pc-12' -ex 'info sharedlibrary' --args \
             python3 {d}/box.py mdwe {gdb_command} 2>&1 | grep -v '^\\[' | tail -80"
        ));
    }
    panic!("\n{log}");
}
