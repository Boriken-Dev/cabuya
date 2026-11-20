# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `pairing::answer`, `join_shared`, `MARKER`, `starts_pairing`, `Answered`, `Unanswered` and `PairingHost`: one responder for a listener that takes proofs and pairings. A pairing and a proof begin with the same handshake message under both methods, so a joiner of `join_shared` sends `MARKER` first and `answer` reads the first message and runs `respond` or `prove` by it. `join`, `respond` and `prove` are unchanged.
- `pairing::desk_with(options, limits)`: a desk with the PIN's lifetime, attempts, alphabet and the pending-run bounds set; `desk()` is `desk_with` of the defaults.
- `mux::Stream::grant` and `Stream::window`, `Limits::max_window` and `Limits::auto_window`: a receiver widens one stream beyond the initial window with one credit frame, bounded by `max_window` (default: `window`, so nothing changes), and `auto_window` (off) doubles a stream that received over two thirds of its window between a `ping` and its `pong`. The `broken` check uses the stream's own window. No frame changes: senders already accept credit past what was read. `LinkStream::grant` has a default that answers `Unsupported`. Dart and JavaScript gain `grant`, `window` and `maxWindow`.
- `link/1` documents carry `window` and `max_frame`, and `link::Capabilities` has them as `Option`s, absent from an older peer, so a host can report that the two sides' initial windows differ.
- `forward::Rule`: a grant token `tcp/1:RULE` may match targets by pattern (an exact or `*.suffix` name, `*`, an address or a CIDR network, and a port, a range or `*`); a token naming one target behaves as before. Nothing resolves when a rule is checked, so a name target never matches an address or network rule. `grant_rule`, `Rule::matches`, `grant_token` and `from_grant_token`. `forward` now depends on `coqui-net-acl` for networks and port ranges.
- `forward::Options`, `pump_with`, `listen_with`, `serving_with` and `Target::connect_within`: the connect timeout, message size, stall time and polling intervals are settings with today's values as defaults; zero durations and chunk are refused by `Options::validate`.
- `auth::Trust::Asked` and `Trust::asked(closure)`: the host decides whether a proven key is admitted, under either method, once per handshake and never for a key whose proof failed, with the key, whether this side answered, and the source. It records nothing itself. coqui-peer-noise and coqui-peer-webcrypto are pinned at the revisions that carry the same variant.
- `mux`: named streams inside one link, with credit per stream, metadata on open, a distinct cause for every end, and an optional heartbeat, in Rust, Dart and JavaScript, on one set of vectors.
- `peers` and `link`: claims and grants per peer behind a `PeerStore` trait with audit events, and serving a link under them, failing closed, with a capability document and per-protocol admission.
- `auth` and `pairing` (feature `sealed`): Noise and WebCrypto identities, proof, the sealed channel, and PIN pairing over any channel.
- `forward` (feature `forward`): `tcp/1`, one TCP connection per stream in either direction, each target its own grant.
- The wire is the one `coqui-agentic-peer` speaks (`link/1`, `tcp/1`, every frame byte), so a link of either library talks to a link of the other; `tests/interop.rs` runs both ends under both proof methods.
