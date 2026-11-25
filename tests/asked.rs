//! `Trust::asked`: the host decides after the proof, once per handshake, under each method.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

mod common;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel as queue};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use cabuya::auth::{Identity, Method, Proof, Trust, prove, prove_out};
use coqui_peer::KeyMaterial;
use coqui_transport::{Channel, Failure};

const SHORT: Duration = Duration::from_secs(20);

/// A link in memory; the dialer's side can damage the second message it sends.
struct Wire {
    out: Sender<Vec<u8>>,
    incoming: Mutex<Receiver<Vec<u8>>>,
    sent: AtomicUsize,
    damage_second: AtomicBool,
}

impl Channel for Wire {
    fn send(&self, message: &[u8]) -> Result<(), Failure> {
        let mut message = message.to_vec();
        if self.sent.fetch_add(1, Ordering::SeqCst) == 1
            && self.damage_second.load(Ordering::SeqCst)
        {
            let last = message.len() - 1;
            message[last] ^= 1;
        }
        self.out.send(message).map_err(|_| Failure::PathLost)
    }

    fn receive(&self, timeout: Option<Duration>) -> Result<Option<Vec<u8>>, Failure> {
        let incoming = self.incoming.lock().unwrap();
        match incoming.recv_timeout(timeout.unwrap_or(SHORT)) {
            Ok(message) => Ok(Some(message)),
            Err(RecvTimeoutError::Disconnected) => Ok(None),
            Err(RecvTimeoutError::Timeout) => Err(Failure::Timeout),
        }
    }

    fn close(&self) {}
}

fn wires(damage_dialer: bool) -> (Wire, Wire) {
    let (a_out, b_in) = queue();
    let (b_out, a_in) = queue();
    let wire = |out, incoming, damage| Wire {
        out,
        incoming: Mutex::new(incoming),
        sent: AtomicUsize::new(0),
        damage_second: AtomicBool::new(damage),
    };
    (wire(a_out, a_in, damage_dialer), wire(b_out, b_in, false))
}

type Calls = Arc<Mutex<Vec<(KeyMaterial, bool, Option<SocketAddr>)>>>;

fn asking(answer: bool) -> (Trust, Calls) {
    let calls = Calls::default();
    let record = calls.clone();
    let trust = Trust::asked(move |key, answering, source| {
        record
            .lock()
            .unwrap()
            .push((key.clone(), answering, source));
        answer
    });
    (trust, calls)
}

fn proofs(method: Method, dial: Trust, answer: Trust) -> (Identity, Identity, Proof, Proof) {
    let (client, server) = (
        Identity::generate(method).unwrap(),
        Identity::generate(method).unwrap(),
    );
    let dialing = Proof::builder(client.clone())
        .responders(dial)
        .build()
        .unwrap();
    let answering = Proof::builder(server.clone())
        .initiators(answer)
        .build()
        .unwrap();
    (client, server, dialing, answering)
}

#[test]
fn asked_admits_and_is_called_once_on_each_side_with_its_role() {
    let _deadline = common::within(120);
    for method in Method::ALL {
        let ((dial, dialed), (answer, answered)) = (asking(true), asking(true));
        let (client, server, dialing, answering) = proofs(method, dial, answer);
        let (near, far) = wires(false);
        thread::scope(|scope| {
            let answer = scope.spawn(|| prove(&answering, &far, SHORT, 1 << 16).unwrap().peer);
            let done = prove_out(&dialing, &near, SHORT, 1 << 16).unwrap();
            assert_eq!(done.peer, server.peer_id());
            assert_eq!(answer.join().unwrap(), client.peer_id());
        });
        assert_eq!(
            *dialed.lock().unwrap(),
            [(server.key_material(), false, None)]
        );
        assert_eq!(
            *answered.lock().unwrap(),
            [(client.key_material(), true, None)]
        );
    }
}

#[test]
fn asked_refusing_ends_the_proof_on_either_side() {
    let _deadline = common::within(120);
    for method in Method::ALL {
        // The answerer refuses the dialer.
        let ((dial, _), (answer, answered)) = (asking(true), asking(false));
        let (client, _, dialing, answering) = proofs(method, dial, answer);
        let (near, far) = wires(false);
        thread::scope(|scope| {
            let refused = scope.spawn(move || {
                let outcome = prove(&answering, &far, SHORT, 1 << 16);
                drop(far);
                outcome
            });
            // Under Noise the dialer has sent its last message by then, so only the answerer must fail.
            let _ = prove_out(&dialing, &near, SHORT, 1 << 16);
            assert!(refused.join().unwrap().is_err());
        });
        assert_eq!(
            *answered.lock().unwrap(),
            [(client.key_material(), true, None)]
        );

        // The dialer refuses the answerer: the answerer is never asked, as the dialer sends no proof of its own.
        let ((dial, dialed), (answer, answered)) = (asking(false), asking(true));
        let (_, server, dialing, answering) = proofs(method, dial, answer);
        let (near, far) = wires(false);
        thread::scope(|scope| {
            let waiting = scope.spawn(move || {
                let outcome = prove(&answering, &far, SHORT, 1 << 16);
                drop(far);
                outcome
            });
            assert!(prove_out(&dialing, &near, SHORT, 1 << 16).is_err());
            drop(near);
            assert!(waiting.join().unwrap().is_err());
        });
        assert_eq!(
            *dialed.lock().unwrap(),
            [(server.key_material(), false, None)]
        );
        assert!(answered.lock().unwrap().is_empty());
    }
}

#[test]
fn asked_is_never_called_for_a_key_whose_proof_failed() {
    let _deadline = common::within(120);
    for method in Method::ALL {
        let ((dial, _), (answer, answered)) = (asking(true), asking(true));
        let (_, _, dialing, answering) = proofs(method, dial, answer);
        let (near, far) = wires(true);
        thread::scope(|scope| {
            let damaged = scope.spawn(move || {
                let outcome = prove(&answering, &far, SHORT, 1 << 16);
                drop(far);
                outcome
            });
            let _ = prove_out(&dialing, &near, SHORT, 1 << 16);
            assert!(damaged.join().unwrap().is_err());
        });
        assert!(answered.lock().unwrap().is_empty(), "{method}");
    }
}

#[test]
fn debug_shows_asked_and_nothing_of_the_closure() {
    assert_eq!(
        format!("{:?}", Trust::asked(|_, _, _| true)),
        r#"Asked("..")"#
    );
}
