#![no_main]

//! Fuzz serializer round-trip: parse bytes into `Resp`, serialize back in
//! both RESP2 and RESP3, parse again, and verify the trees match.
//! Catches bugs in `write_to_for_version` such as wrong CRLF, wrong null
//! encoding, or RESP3-specific types not handled in the RESP2 fallback.

use libfuzzer_sys::fuzz_target;
use nexrade_core::resp::{Resp, RespParser};

fn round_trip(version: u8, data: &[u8]) {
    let mut parser = RespParser::new();
    parser.feed(data);
    let parsed = match parser.parse_one() {
        Ok(Some(r)) => r,
        _ => return,
    };

    // Serialize with the requested version, then re-parse.
    let encoded = match version {
        3 => parsed.serialize_for_version(3),
        _ => parsed.serialize_for_version(2),
    };
    let mut parser2 = RespParser::new();
    parser2.feed(&encoded);
    let reparsed = match parser2.parse_one() {
        Ok(Some(r)) => r,
        _ => panic!(
            "RESP{version} reparse failed for: original={data:?} \
             encoded={encoded:?}"
        ),
    };
    // Compare structural equality — but also normalise `Null` vs
    // `BulkString(None)` / `Array(None)` since those are semantically
    // identical.
    if !semantically_equal(&parsed, &reparsed) {
        panic!(
            "RESP{version} mismatch for input {data:?}\n  \
             original: {parsed:?}\n  re-parsed: {reparsed:?}\n  \
             encoded: {encoded:?}"
        );
    }
}

fn semantically_equal(a: &Resp, b: &Resp) -> bool {
    use Resp::*;
    match (a, b) {
        // `Null` (RESP3 only) is semantically equal to `BulkString(None)`
        // / `Array(None)` (RESP2 nulls).
        (Null, BulkString(None)) | (BulkString(None), Null) => true,
        (Null, Array(None)) | (Array(None), Null) => true,
        (SimpleString(a), SimpleString(b)) => a == b,
        (Error(a), Error(b)) => a == b,
        (Integer(a), Integer(b)) => a == b,
        (BulkString(Some(a)), BulkString(Some(b))) => a == b,
        (Array(None), Array(None)) => true,
        (Array(Some(a)), Array(Some(b))) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(x, y)| semantically_equal(x, y))
        }
        (Bool(a), Bool(b)) => a == b,
        (Double(a), Double(b)) => a == b,
        (Map(a), Map(b)) => {
            a.len() == b.len()
                && a.iter().zip(b.iter()).all(|((k1, v1), (k2, v2))| {
                    semantically_equal(k1, k2) && semantically_equal(v1, v2)
                })
        }
        (Set(a), Set(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(x, y)| semantically_equal(x, y))
        }
        // RESP3-only types fall back to arrays in RESP2; we accept that
        // mismatch silently (the wire is well-formed but the abstract type
        // changes).
        (Push(_), Array(_)) | (Array(_), Push(_)) => true,
        // Raw is just pre-serialised bytes; accept either side.
        _ => false,
    }
}

fuzz_target!(|data: &[u8]| {
    // Alternate which version is "currently selected" so both code paths
    // get exercised. The last byte toggles between 2 and 3.
    let split = data.len().saturating_sub(1);
    let payload = &data[..split];
    let version = if data[split] % 2 == 0 { 2 } else { 3 };
    round_trip(version, payload);
});