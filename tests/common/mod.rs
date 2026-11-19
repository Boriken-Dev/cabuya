//! A deadline for a test that waits on threads: one that would hang ends the test binary with a message instead.
#![allow(dead_code)]

use std::sync::mpsc::{Sender, channel};
use std::thread;
use std::time::Duration;

/// Ends the deadline when dropped.
pub struct Deadline(Sender<()>);

/// Exits the process with a message when the returned guard outlives `seconds`.
pub fn within(seconds: u64) -> Deadline {
    let (stop, stopped) = channel::<()>();
    let name = thread::current().name().unwrap_or("a test").to_owned();
    thread::spawn(move || {
        if stopped
            .recv_timeout(Duration::from_secs(seconds))
            .is_err_and(|error| matches!(error, std::sync::mpsc::RecvTimeoutError::Timeout))
        {
            eprintln!("DEADLINE: {name} did not finish within {seconds} seconds");
            std::process::exit(101);
        }
    });
    Deadline(stop)
}
