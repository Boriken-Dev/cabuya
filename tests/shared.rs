//! One responder entry point for a listener that takes proofs and pairings: a pairing and a proof on separate channels, an
//! unarmed desk, a wrong PIN, and a PIN that outlived its lifetime.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

mod common;

use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel as queue};
use std::thread;
use std::time::Duration;

use cabuya::Error;
use cabuya::auth::{Identity, Method, Proof, Sealed, Trust, prove_out};
use cabuya::pairing::{self, Answered, PairingHost};
use coqui_peer::TrustStore;
use coqui_peer::pair::{CodeAlphabet, DeskLimits, PairingDesk, PairingOptions, Pin};
use coqui_transport::{Channel, Failure};

const SHORT: Duration = Duration::from_secs(20);

struct Wire {
    out: Sender<Vec<u8>>,
    incoming: Mutex<Receiver<Vec<u8>>>,
}

impl Channel for Wire {
    fn send(&self, message: &[u8]) -> Result<(), Failure> {
        self.out
            .send(message.to_vec())
            .map_err(|_| Failure::PathLost)
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

fn wires() -> (Wire, Wire) {
    let (a_out, b_in) = queue();
    let (b_out, a_in) = queue();
    (
        Wire {
            out: a_out,
            incoming: Mutex::new(a_in),
        },
        Wire {
            out: b_out,
            incoming: Mutex::new(b_in),
        },
    )
}

struct Dir(PathBuf);

impl Dir {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("cabuya-shared-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn store(&self, name: &str) -> TrustStore {
        TrustStore::open(&self.0.join(name)).unwrap()
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn a_pairing_and_a_proof_both_end_in_one_answer() {
    let _deadline = common::within(120);
    for method in Method::ALL {
        let dir = Dir::new(method.name());
        let (server, joiner, known) = (
            Identity::generate(method).unwrap(),
            Identity::generate(method).unwrap(),
            Identity::generate(method).unwrap(),
        );
        let (server_store, joiner_store) = (dir.store("server"), dir.store("joiner"));
        let desk = pairing::desk().unwrap();
        let pin = desk.show_pin().unwrap().pin;
        let proof = Proof::builder(server.clone())
            .initiators(Trust::pinned([known.peer_id()]))
            .build()
            .unwrap();
        let host = PairingHost {
            identity: &server,
            store: &server_store,
            desk: &desk,
        };

        let (near, far) = wires();
        let (joined, answered) = thread::scope(|scope| {
            let answering =
                scope.spawn(|| pairing::answer(&far, &proof, 1 << 16, Some(host), None, SHORT));
            let joined = pairing::join_shared(&near, &joiner, pin, &joiner_store, SHORT);
            (joined, answering.join().unwrap())
        });
        joined.unwrap();
        let Answered::Paired(paired) = answered.unwrap() else {
            panic!("a pairing was answered as a proof");
        };
        assert_eq!(coqui_peer::KeyMaterial::id(&paired.peer), joiner.peer_id());
        assert!(server_store.accepted().unwrap().contains(&joiner.peer_id()));

        // A peer that proves, on another connection to the same entry point.
        let dialing = Proof::builder(known.clone())
            .responders(Trust::pinned([server.peer_id()]))
            .build()
            .unwrap();
        let (near, far) = wires();
        thread::scope(|scope| {
            let answering =
                scope.spawn(|| pairing::answer(&far, &proof, 1 << 16, Some(host), None, SHORT));
            let done = prove_out(&dialing, &near, SHORT, 1 << 16).unwrap();
            Sealed::new(&near, done.session)
                .send(b"over the link")
                .unwrap();
            let Answered::Proven(proven) = answering.join().unwrap().unwrap() else {
                panic!("a proof was answered as a pairing");
            };
            assert_eq!(proven.peer, known.peer_id());
            let sealed = Sealed::new(&far, proven.session);
            assert_eq!(sealed.receive(None).unwrap().unwrap(), b"over the link");
        });
    }
}

/// One joiner against `desk` through `answer`: what the joiner and the responder each report.
fn attempt(
    method: Method,
    desk: &PairingDesk,
    pin: &str,
) -> (Result<(), Error>, Result<(), Error>, usize) {
    let dir = Dir::new(&format!("attempt-{method}-{:p}", desk));
    let (server, joiner) = (
        Identity::generate(method).unwrap(),
        Identity::generate(method).unwrap(),
    );
    let (server_store, joiner_store) = (dir.store("server"), dir.store("joiner"));
    let proof = Proof::builder(server.clone()).build().unwrap();
    let host = PairingHost {
        identity: &server,
        store: &server_store,
        desk,
    };
    let pin = Pin::parse(&CodeAlphabet::pin(), pin).unwrap();
    let (near, far) = wires();
    let (joined, answered) = thread::scope(|scope| {
        let answering =
            scope.spawn(|| pairing::answer(&far, &proof, 1 << 16, Some(host), None, SHORT));
        let joined = pairing::join_shared(&near, &joiner, pin, &joiner_store, SHORT);
        (joined, answering.join().unwrap())
    });
    let stored = server_store.accepted().unwrap().len();
    (
        joined.map(|_| ()),
        answered.map(|_| ()).map_err(|unanswered| unanswered.error),
        stored,
    )
}

#[test]
fn an_unarmed_desk_refuses_a_joiner_and_says_so() {
    let _deadline = common::within(120);
    for method in Method::ALL {
        let desk = pairing::desk().unwrap();
        let (joined, answered, stored) = attempt(method, &desk, "000000");
        let why = joined.unwrap_err().to_string();
        assert!(
            why.contains("none is shown") || why.contains("not offered"),
            "{why}"
        );
        assert!(answered.is_err());
        assert_eq!(stored, 0);
    }
}

#[test]
fn a_wrong_pin_uses_the_pin_up() {
    let _deadline = common::within(120);
    for method in Method::ALL {
        let desk = pairing::desk().unwrap();
        let shown = desk.show_pin().unwrap().pin;
        let wrong = if shown.as_str() == "000000" {
            "111111"
        } else {
            "000000"
        };
        let (joined, _, stored) = attempt(method, &desk, wrong);
        assert!(joined.is_err());
        assert_eq!(stored, 0);
        assert!(desk.shown_pin().is_none(), "the PIN outlived a wrong guess");
        let (joined, _, stored) = attempt(method, &desk, shown.as_str());
        assert!(joined.is_err(), "the right PIN paired after a wrong one");
        assert_eq!(stored, 0);
    }
}

#[test]
fn a_pin_past_its_lifetime_is_refused() {
    let _deadline = common::within(120);
    for method in Method::ALL {
        let mut options = PairingOptions::default();
        options.pin_ttl = Some(Duration::from_millis(100));
        let desk = pairing::desk_with(options, DeskLimits::default()).unwrap();
        let shown = desk.show_pin().unwrap().pin;
        thread::sleep(Duration::from_millis(300));
        let (joined, _, stored) = attempt(method, &desk, shown.as_str());
        assert!(joined.is_err(), "an expired PIN paired");
        assert_eq!(stored, 0);
    }
}

#[test]
fn a_marker_is_neither_a_noise_message_nor_json() {
    assert_ne!(pairing::MARKER.len(), 32);
    assert_ne!(pairing::MARKER.first(), Some(&b'{'));
    assert!(pairing::starts_pairing(pairing::MARKER));
    assert!(!pairing::starts_pairing(&[0_u8; 32]));
}

#[test]
fn a_listener_that_does_not_pair_sends_a_joiner_away_and_still_proves() {
    let _deadline = common::within(120);
    let dir = Dir::new("none");
    let (server, joiner) = (
        Identity::generate(Method::Noise).unwrap(),
        Identity::generate(Method::Noise).unwrap(),
    );
    let proof = Proof::builder(server).build().unwrap();
    let store = dir.store("joiner");
    let (near, far) = wires();
    let pin = Pin::parse(&CodeAlphabet::pin(), "123456").unwrap();
    let answered = thread::scope(|scope| {
        let answering = scope
            .spawn(|| pairing::answer(&far, &proof, 1 << 16, None, None, Duration::from_secs(2)));
        let joined = pairing::join_shared(&near, &joiner, pin, &store, Duration::from_secs(2));
        assert!(joined.is_err());
        answering.join().unwrap()
    });
    let unanswered = answered.unwrap_err();
    assert!(unanswered.pairing);
    let Error::Proof(why) = unanswered.error else {
        panic!("not a refusal: {unanswered}");
    };
    assert!(why.contains("not offered"), "{why}");
}
