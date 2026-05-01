#![no_main]

use libfuzzer_sys::fuzz_target;
use nexrade_core::resp::RespParser;

fuzz_target!(|data: &[u8]| {
    let mut parser = RespParser::new();
    parser.feed(data);
    // Parse as many complete messages as possible; errors are not panics.
    loop {
        match parser.parse_one() {
            Ok(Some(_)) => {} // valid message — keep going
            Ok(None)    => break, // incomplete — need more data
            Err(_)      => break, // parse error — not a crash
        }
    }
});
