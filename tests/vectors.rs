//! The frame vectors are the wire: this crate and `coqui-agentic-peer`, which speaks the same wire, decode every valid vector to a
//! frame that encodes back to its bytes, and refuse every invalid one.
#![allow(clippy::std_instead_of_core, clippy::std_instead_of_alloc)]

const VECTORS: &str = include_str!("../vectors/mux.json");

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap())
        .collect()
}

fn vectors(key: &str) -> Vec<(String, Vec<u8>)> {
    let document: serde_json::Value = serde_json::from_str(VECTORS).unwrap();
    document[key]
        .as_array()
        .unwrap()
        .iter()
        .map(|vector| {
            (
                vector["name"].as_str().unwrap().to_owned(),
                hex(vector["hex"].as_str().unwrap()),
            )
        })
        .collect()
}

#[test]
fn every_valid_vector_round_trips_in_this_crate_and_in_coqui_agentic_peer() {
    let valid = vectors("frames");
    assert!(!valid.is_empty());
    for (name, bytes) in &valid {
        let ours = cabuya::mux::Frame::decode(bytes).unwrap_or_else(|| panic!("{name}"));
        assert_eq!(&ours.encode(), bytes, "{name}");
        let theirs =
            coqui_agentic_peer::mux::Frame::decode(bytes).unwrap_or_else(|| panic!("{name}"));
        assert_eq!(&theirs.encode(), bytes, "{name}");
    }
}

#[test]
fn every_invalid_vector_is_refused_by_both() {
    let invalid = vectors("invalid");
    assert!(!invalid.is_empty());
    for (name, bytes) in &invalid {
        assert!(cabuya::mux::Frame::decode(bytes).is_none(), "{name}");
        assert!(
            coqui_agentic_peer::mux::Frame::decode(bytes).is_none(),
            "{name}"
        );
    }
}

/// With `CABUYA_SOURCE_VECTORS` naming the other library's `vectors/mux.json`, the two files are the same bytes.
#[test]
fn the_vectors_are_byte_identical_to_the_other_librarys() {
    let Some(path) = std::env::var_os("CABUYA_SOURCE_VECTORS") else {
        eprintln!("SKIPPED: CABUYA_SOURCE_VECTORS names no file");
        return;
    };
    assert_eq!(std::fs::read(path).unwrap(), VECTORS.as_bytes());
}
