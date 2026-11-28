//! A link of this crate on one end and a link of `agentic-peer` on the other, over an in-memory channel: both sides, both
//! proof methods, a stream opened each way, and a stream ended by a grant taken away and by an admission refusing.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;
use std::time::Duration;

use coqui_transport::frame::LengthPrefix;
use coqui_transport::{Channel, Failure, StreamChannel, StreamChannelBuilder, memory};

const WAIT: Option<Duration> = Some(Duration::from_secs(20));

fn channels() -> (StreamChannel, StreamChannel) {
    let (one, other) = memory::pipe(1 << 20);
    let build = |stream| {
        StreamChannelBuilder::new()
            .codec(LengthPrefix::u32_le())
            .max_message(1 << 20)
            .stream(stream)
            .build()
            .unwrap()
    };
    (build(one), build(other))
}

/// What the answering side reports about the stream it opened toward the dialer.
type Report = Sender<String>;

/// How the dialing side makes the answering side take a permission away.
struct Controls {
    blocked: Arc<AtomicBool>,
    revoke_grant: Sender<()>,
}

macro_rules! stack {
    ($name:ident, $krate:ident) => {
        mod $name {
            use super::*;
            use $krate::auth::{Identity, Method, Proof, Sealed, Trust, prove, prove_out};
            use $krate::link::{Host, Options, Services, serve_with};
            use $krate::mux::{Limits, Mux, Side, StreamEnd};
            use $krate::peers::{MemoryPeers, NoEvents, PeerRecord, PeerStore};

            pub struct Party {
                identity: Identity,
            }

            pub fn party(noise: bool) -> Party {
                let method = if noise {
                    Method::Noise
                } else {
                    Method::WebCrypto
                };
                Party {
                    identity: Identity::generate(method).unwrap(),
                }
            }

            pub fn id(party: &Party) -> String {
                party.identity.peer_id().to_string()
            }

            /// Proves the answerer, then opens streams to it and ends them one way and another.
            pub fn dial(
                channel: &StreamChannel,
                party: &Party,
                server: String,
                controls: &Controls,
            ) {
                let proof = Proof::builder(party.identity.clone())
                    .responders(Trust::pinned([server.parse().unwrap()]))
                    .build()
                    .unwrap();
                let done = prove_out(&proof, channel, Duration::from_secs(20), 1 << 16).unwrap();
                assert_eq!(done.peer.to_string(), server);
                let sealed = Sealed::new(channel, done.session);
                Mux::run(&sealed, Side::Dialed, Limits::default(), None, |mux| {
                    let echo = mux.open("echo/1").unwrap();
                    echo.send(b"hello").unwrap();
                    assert_eq!(echo.receive(WAIT).unwrap().unwrap(), b"echo:hello");

                    // The answering side opens a stream toward this one.
                    let incoming = mux
                        .accept(WAIT)
                        .unwrap()
                        .expect("a stream from the answerer");
                    assert_eq!(incoming.protocol, "back/1");
                    assert_eq!(incoming.metadata, b"cursor=7");
                    let back = incoming.stream;
                    assert_eq!(back.receive(WAIT).unwrap().unwrap(), b"from the answerer");
                    back.send(b"ack").unwrap();

                    // An admission refusing takes a running stream away, and a new open is refused.
                    let gated = mux.open("gated/1").unwrap();
                    gated.send(b"one").unwrap();
                    assert_eq!(gated.receive(WAIT).unwrap().unwrap(), b"echo:one");
                    controls.blocked.store(true, Ordering::SeqCst);
                    assert_eq!(gated.receive(WAIT), Err(Failure::Permission));
                    assert!(matches!(gated.end(), Some(StreamEnd::Revoked(_))));
                    assert_eq!(
                        mux.open("gated/1").unwrap().receive(WAIT),
                        Err(Failure::Permission)
                    );

                    // A grant taken away ends its stream and leaves the others.
                    controls.revoke_grant.send(()).unwrap();
                    assert_eq!(echo.receive(WAIT), Err(Failure::Permission));
                    assert!(matches!(echo.end(), Some(StreamEnd::Revoked(_))));
                    assert_eq!(
                        mux.open("echo/1").unwrap().receive(WAIT),
                        Err(Failure::Permission)
                    );
                    back.send(b"still open").unwrap();
                });
            }

            /// Proves the dialer, serves what it is granted, and opens one stream toward it.
            pub fn answer(
                channel: &StreamChannel,
                party: &Party,
                client: String,
                blocked: Arc<AtomicBool>,
                revoke_grant: Receiver<()>,
                report: Report,
            ) {
                let proof = Proof::builder(party.identity.clone())
                    .initiators(Trust::pinned([client.parse().unwrap()]))
                    .build()
                    .unwrap();
                let done = prove(&proof, channel, Duration::from_secs(20), 1 << 16).unwrap();
                assert_eq!(done.peer.to_string(), client);
                let peer = client.clone();
                let sealed = Sealed::new(channel, done.session);
                let store = MemoryPeers::default();
                store
                    .put(
                        &PeerRecord::new(&peer)
                            .granting("echo/1")
                            .granting("gated/1"),
                    )
                    .unwrap();
                let echo = || -> $krate::link::Handler {
                    Box::new(|stream, _| {
                        while let Ok(Some(message)) = stream.receive(WAIT) {
                            let mut answer = b"echo:".to_vec();
                            answer.extend_from_slice(&message);
                            if stream.send(&answer).is_err() {
                                break;
                            }
                        }
                    })
                };
                let services = Services::default().with("echo/1", echo()).with_admission(
                    "gated/1",
                    echo(),
                    Box::new(move |_, _| {
                        if blocked.load(Ordering::SeqCst) {
                            Err("blocked")
                        } else {
                            Ok(())
                        }
                    }),
                );
                let mut options = Options::default();
                options.recheck = Duration::from_millis(20);
                let (store_ref, peer_ref) = (&store, &peer);
                serve_with(
                    &sealed,
                    Side::Answered,
                    options,
                    None,
                    &peer,
                    &Host {
                        store: &store,
                        services: &services,
                        events: &NoEvents,
                    },
                    move |mux| {
                        let back = mux.open_with("back/1", b"cursor=7").unwrap();
                        back.send(b"from the answerer").unwrap();
                        report
                            .send(format!(
                                "{:?}",
                                back.receive(WAIT).map(|m| m.map(String::from_utf8))
                            ))
                            .unwrap();
                        if revoke_grant.recv_timeout(Duration::from_secs(30)).is_ok() {
                            store_ref
                                .put(&PeerRecord::new(peer_ref).granting("gated/1"))
                                .unwrap();
                        }
                        // The dialer's last message arrives on the stream this side opened.
                        report
                            .send(format!(
                                "{:?}",
                                back.receive(WAIT).map(|m| m.map(String::from_utf8))
                            ))
                            .unwrap();
                    },
                );
            }
        }
    };
}

stack!(link_side, cabuya);
stack!(peer_side, agentic_peer);

/// One run of a link: `dial` and `answer` are each a different library's side.
fn run(
    dial: impl FnOnce(&StreamChannel, &Controls) + Send,
    answer: impl FnOnce(&StreamChannel, Arc<AtomicBool>, Receiver<()>, Report) + Send,
) {
    let (near, far) = channels();
    let blocked = Arc::new(AtomicBool::new(false));
    let (revoke_grant, revoke_seen) = channel();
    let (report, reports) = channel();
    let controls = Controls {
        blocked: Arc::clone(&blocked),
        revoke_grant,
    };
    thread::scope(|scope| {
        let answering = scope.spawn(|| answer(&far, blocked, revoke_seen, report));
        // A failed assertion on the dialing side must still end the link, or the answering side waits for it.
        let dialed =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| dial(&near, &controls)));
        near.close();
        if let Err(panic) = dialed {
            std::panic::resume_unwind(panic);
        }
        answering.join().unwrap();
    });
    assert_eq!(
        reports.recv().unwrap(),
        "Ok(Some(Ok(\"ack\")))",
        "the dialer answered the stream the answering side opened"
    );
    assert_eq!(
        reports.recv().unwrap(),
        "Ok(Some(Ok(\"still open\")))",
        "and that stream survived the revocations"
    );
}

#[test]
fn this_crate_dials_and_agentic_peer_answers_under_each_method() {
    let _deadline = common::within(90);
    for noise in [true, false] {
        let (dialer, answerer) = (link_side::party(noise), peer_side::party(noise));
        let (dialer_id, answerer_id) = (link_side::id(&dialer), peer_side::id(&answerer));
        run(
            |channel, controls| link_side::dial(channel, &dialer, answerer_id, controls),
            |channel, blocked, revoke, report| {
                peer_side::answer(channel, &answerer, dialer_id, blocked, revoke, report);
            },
        );
    }
}

#[test]
fn agentic_peer_dials_and_this_crate_answers_under_each_method() {
    let _deadline = common::within(90);
    for noise in [true, false] {
        let (dialer, answerer) = (peer_side::party(noise), link_side::party(noise));
        let (dialer_id, answerer_id) = (peer_side::id(&dialer), link_side::id(&answerer));
        run(
            |channel, controls| peer_side::dial(channel, &dialer, answerer_id, controls),
            |channel, blocked, revoke, report| {
                link_side::answer(channel, &answerer, dialer_id, blocked, revoke, report);
            },
        );
    }
}

#[test]
fn endpoints_of_different_methods_do_not_prove_in_either_direction() {
    let _deadline = common::within(90);
    use agentic_peer::auth as theirs;
    use cabuya::auth as ours;

    for (ours_noise, theirs_noise) in [(true, false), (false, true)] {
        let method = |noise| {
            if noise {
                ours::Method::Noise
            } else {
                ours::Method::WebCrypto
            }
        };
        let dialer = ours::Identity::generate(method(ours_noise)).unwrap();
        let answerer = theirs::Identity::generate(if theirs_noise {
            theirs::Method::Noise
        } else {
            theirs::Method::WebCrypto
        })
        .unwrap();
        let dialing = ours::Proof::builder(dialer.clone())
            .responders(ours::Trust::pinned([answerer
                .peer_id()
                .to_string()
                .parse()
                .unwrap()]))
            .build()
            .unwrap();
        let answering = theirs::Proof::builder(answerer.clone())
            .initiators(theirs::Trust::pinned([dialer
                .peer_id()
                .to_string()
                .parse()
                .unwrap()]))
            .build()
            .unwrap();
        let (near, far) = channels();
        thread::scope(|scope| {
            let answered = scope.spawn(|| {
                let outcome = theirs::prove(&answering, &far, Duration::from_secs(5), 1 << 16);
                far.close();
                outcome.is_err()
            });
            let dialed = ours::prove_out(&dialing, &near, Duration::from_secs(5), 1 << 16);
            near.close();
            assert!(dialed.is_err(), "a method is chosen per endpoint");
            assert!(answered.join().unwrap(), "and never negotiated");
        });
    }
}
