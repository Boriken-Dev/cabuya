//! The `cabuya` program, run as a person would run it: two homes, a pairing by PIN, `serve`, `connect` over standard input and output,
//! `forward`, and a grant taken away from a stream that is running.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

mod common;

use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Output, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::thread;
use std::time::Duration;

const PROGRAM: &str = env!("CARGO_BIN_EXE_cabuya");
const WAIT: Duration = Duration::from_secs(30);

/// A folder removed when the test ends.
struct Dir(PathBuf);

impl Dir {
    fn new(test: &str, name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("cabuya-cli-{test}-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A running program whose standard output is read a line at a time; it is killed when dropped.
struct Running {
    child: Child,
    lines: Receiver<String>,
}

impl Running {
    fn start(home: &Path, args: &[&str]) -> Self {
        let mut child = Command::new(PROGRAM)
            .arg("--home")
            .arg(home)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let out: ChildStdout = child.stdout.take().unwrap();
        let (send, lines) = channel();
        thread::spawn(move || {
            for line in BufReader::new(out).lines().map_while(Result::ok) {
                if send.send(line).is_err() {
                    return;
                }
            }
        });
        Self { child, lines }
    }

    /// The value of the next line that starts with `key `.
    fn line(&self, key: &str) -> String {
        loop {
            match self.lines.recv_timeout(WAIT) {
                Ok(line) => {
                    if let Some(value) = line.strip_prefix(&format!("{key} ")) {
                        return value.to_owned();
                    }
                }
                Err(RecvTimeoutError::Timeout) => panic!("no `{key}` line within {WAIT:?}"),
                Err(RecvTimeoutError::Disconnected) => panic!("the program ended before `{key}`"),
            }
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn cabuya(home: &Path, args: &[&str]) -> Output {
    Command::new(PROGRAM)
        .arg("--home")
        .arg(home)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .unwrap()
}

/// A TCP server that answers each connection with what it sent, until the sender stops.
fn echo_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        for connection in listener.incoming() {
            let Ok(mut connection) = connection else {
                return;
            };
            thread::spawn(move || {
                let mut reader = connection.try_clone().unwrap();
                let mut buffer = [0_u8; 4096];
                while let Ok(read) = reader.read(&mut buffer) {
                    if read == 0 || connection.write_all(&buffer[..read]).is_err() {
                        break;
                    }
                }
                let _ = connection.shutdown(Shutdown::Write);
            });
        }
    });
    port
}

/// A remote that serves and takes a pairing, and a client paired with it as `box`, each in its own home.
struct Pair {
    /// Kept so the remote stays up for the test.
    _remote: Running,
    remote_home: Dir,
    client_home: Dir,
    client_id: String,
}

fn paired(test: &str, grants: &[&str]) -> Pair {
    let remote_home = Dir::new(test, "remote");
    let client_home = Dir::new(test, "client");
    let mut args = vec!["serve", "--listen", "127.0.0.1:0", "--pair"];
    for grant in grants {
        args.extend(["--grant", grant]);
    }
    let remote = Running::start(remote_home.path(), &args);
    let address = remote.line("listening");
    let pin = remote.line("pin");
    let joined = cabuya(
        client_home.path(),
        &["pair", "--join", &address, "--name", "box", "--pin", &pin],
    );
    assert!(
        joined.status.success(),
        "{}",
        String::from_utf8_lossy(&joined.stderr)
    );
    let client_id = String::from_utf8(joined.stdout)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("this-peer ").map(str::to_owned))
        .unwrap();
    assert_eq!(remote.line("paired"), client_id);
    Pair {
        _remote: remote,
        remote_home,
        client_home,
        client_id,
    }
}

/// Runs `connect` with `input` on its standard input, ending it, and returns the program's result.
fn connect_with(home: &Path, args: &[&str], input: &[u8]) -> Output {
    let mut child = Command::new(PROGRAM)
        .arg("--home")
        .arg(home)
        .arg("connect")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(input).unwrap();
    drop(stdin);
    child.wait_with_output().unwrap()
}

#[test]
fn connect_carries_standard_input_to_a_granted_target_and_back() {
    let _deadline = common::within(120);
    let port = echo_server();
    let target = format!("127.0.0.1:{port}");
    let grant = format!("tcp/1:{target}");
    let pair = paired("echo", &[&grant]);

    // More than one chunk and more than the stream's first window, so the credit has to flow both ways.
    let input: Vec<u8> = (0..300_000_u32).map(|n| (n % 251) as u8).collect();
    let output = connect_with(pair.client_home.path(), &["box", &target], &input);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, input, "what went in came back whole");

    let id = cabuya(pair.client_home.path(), &["id"]);
    assert!(String::from_utf8_lossy(&id.stdout).starts_with(&pair.client_id));
}

#[test]
fn a_target_that_was_not_granted_is_refused_and_says_so() {
    let _deadline = common::within(120);
    let (allowed, other) = (echo_server(), echo_server());
    let grant = format!("tcp/1:127.0.0.1:{allowed}");
    let pair = paired("refuse", &[&grant]);

    let target = format!("127.0.0.1:{other}");
    let output = connect_with(pair.client_home.path(), &["box", &target], b"hello");
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let why = String::from_utf8_lossy(&output.stderr);
    assert!(why.contains("refused"), "{why}");

    let missing = connect_with(pair.client_home.path(), &["nobody", &target], b"");
    assert!(String::from_utf8_lossy(&missing.stderr).contains("no paired host"));
}

#[test]
fn a_grant_given_later_opens_what_was_refused() {
    let _deadline = common::within(120);
    let port = echo_server();
    let target = format!("127.0.0.1:{port}");
    let pair = paired("later", &[]);
    let refused = connect_with(pair.client_home.path(), &["box", &target], b"x");
    assert!(!refused.status.success());

    let granted = cabuya(
        pair.remote_home.path(),
        &["grant", &pair.client_id, &format!("tcp/1:{target}")],
    );
    assert!(
        granted.status.success(),
        "{}",
        String::from_utf8_lossy(&granted.stderr)
    );
    let output = connect_with(pair.client_home.path(), &["box", &target], b"now");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"now");
}

#[test]
fn a_revoked_peer_loses_its_running_stream_and_cannot_connect_again() {
    let _deadline = common::within(120);
    let port = echo_server();
    let target = format!("127.0.0.1:{port}");
    let pair = paired("revoke", &[&format!("tcp/1:{target}")]);

    let running = Running::start(pair.client_home.path(), &["connect", "box", &target]);
    let mut running = running;
    let mut stdin = running.child.stdin.take().unwrap();
    stdin.write_all(b"ping\n").unwrap();
    stdin.flush().unwrap();
    assert_eq!(running.lines.recv_timeout(WAIT).unwrap(), "ping");
    // Control: the stream stays up until the revocation.
    thread::sleep(Duration::from_millis(2500));
    assert!(running.child.try_wait().unwrap().is_none());

    let revoked = cabuya(pair.remote_home.path(), &["revoke", &pair.client_id]);
    assert!(
        revoked.status.success(),
        "{}",
        String::from_utf8_lossy(&revoked.stderr)
    );
    let status = (0..60)
        .find_map(|_| {
            thread::sleep(Duration::from_millis(250));
            running.child.try_wait().unwrap()
        })
        .expect("the stream ended once the peer was revoked");
    assert!(!status.success());
    let mut why = String::new();
    running
        .child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut why)
        .unwrap();
    assert!(
        why.contains("took the permission away") || why.contains("link was lost"),
        "{why}"
    );

    let again = connect_with(pair.client_home.path(), &["box", &target], b"x");
    assert!(!again.status.success());
}

#[test]
fn forward_maps_a_local_port_onto_a_granted_target() {
    let _deadline = common::within(120);
    let port = echo_server();
    let target = format!("127.0.0.1:{port}");
    let pair = paired("forward", &[&format!("tcp/1:{target}")]);

    let mapping = format!("0={target}");
    let forwarding = Running::start(
        pair.client_home.path(),
        &["forward", "box", "--local", &mapping],
    );
    let listening = forwarding.line("listening");
    let local = listening.split(' ').next().unwrap();

    for message in [&b"first"[..], b"second connection"] {
        let mut connection = TcpStream::connect(local).unwrap();
        connection.set_read_timeout(Some(WAIT)).unwrap();
        connection.write_all(message).unwrap();
        connection.shutdown(Shutdown::Write).unwrap();
        let mut answer = Vec::new();
        connection.read_to_end(&mut answer).unwrap();
        assert_eq!(answer, message);
    }
    drop(forwarding);
}

#[test]
fn a_wrong_pin_pairs_nothing() {
    let _deadline = common::within(120);
    let remote_home = Dir::new("wrongpin", "remote");
    let client_home = Dir::new("wrongpin", "client");
    let remote = Running::start(
        remote_home.path(),
        &["serve", "--listen", "127.0.0.1:0", "--pair"],
    );
    let address = remote.line("listening");
    let pin = remote.line("pin");
    let wrong = if pin == "000000" { "111111" } else { "000000" };

    let joined = cabuya(
        client_home.path(),
        &["pair", "--join", &address, "--name", "box", "--pin", wrong],
    );
    assert!(!joined.status.success());
    let hosts = cabuya(client_home.path(), &["hosts"]);
    assert!(hosts.stdout.is_empty());
    // The guess burned the PIN, so the right one no longer works either.
    let right = cabuya(
        client_home.path(),
        &["pair", "--join", &address, "--name", "box", "--pin", &pin],
    );
    assert!(!right.status.success());
}

#[test]
fn pair_waits_for_one_joiner_grants_it_and_ends() {
    let _deadline = common::within(120);
    let port = echo_server();
    let target = format!("127.0.0.1:{port}");
    let remote_home = Dir::new("oneshot", "remote");
    let client_home = Dir::new("oneshot", "client");
    let grant = format!("tcp/1:{target}");
    let mut waiting = Running::start(
        remote_home.path(),
        &["pair", "--listen", "127.0.0.1:0", "--grant", &grant],
    );
    let address = waiting.line("listening");
    let pin = waiting.line("pin");

    // `serve` is somewhere else than where the pairing happened.
    let serving = Running::start(remote_home.path(), &["serve", "--listen", "127.0.0.1:0"]);
    let served_at = serving.line("listening");
    let joined = cabuya(
        client_home.path(),
        &[
            "pair", "--join", &address, "--name", "box", "--pin", &pin, "--at", &served_at,
        ],
    );
    assert!(
        joined.status.success(),
        "{}",
        String::from_utf8_lossy(&joined.stderr)
    );
    waiting.line("paired");
    assert!(waiting.child.wait().unwrap().success());

    let hosts = String::from_utf8(cabuya(client_home.path(), &["hosts"]).stdout).unwrap();
    assert!(
        hosts.starts_with("box ") && hosts.trim_end().ends_with(&served_at),
        "{hosts}"
    );
    let output = connect_with(client_home.path(), &["box", &target], b"through serve");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"through serve");

    assert!(
        cabuya(client_home.path(), &["forget", "box"])
            .status
            .success()
    );
    assert!(cabuya(client_home.path(), &["hosts"]).stdout.is_empty());
}
