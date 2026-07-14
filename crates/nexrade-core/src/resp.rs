//! RESP (REdis Serialization Protocol) v2 and v3 parser/serializer.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;
#[cfg(not(target_arch = "wasm32"))]
use std::io::IoSlice;

use crate::error::{NexradeError, Result};

/// A RESP value (RESP2 and RESP3)
#[derive(Debug, Clone, PartialEq)]
pub enum Resp {
    // ── RESP2 types ───────────────────────────────────────────────────────────
    /// Simple string: +OK\r\n
    SimpleString(String),
    /// Error: -ERR message\r\n
    Error(String),
    /// Integer: :42\r\n
    Integer(i64),
    /// Bulk string: $6\r\nfoobar\r\n  or  $-1\r\n (null)
    BulkString(Option<Bytes>),
    /// Array: *3\r\n... or *-1\r\n (null)
    Array(Option<Vec<Resp>>),

    // ── RESP3-only types ──────────────────────────────────────────────────────
    /// RESP3 null: _\r\n
    Null,
    /// RESP3 boolean: #t\r\n / #f\r\n
    Bool(bool),
    /// RESP3 double: ,3.14\r\n
    Double(f64),
    /// RESP3 map: %n\r\n + n key-value pairs
    Map(Vec<(Resp, Resp)>),
    /// RESP3 set: ~n\r\n + n elements
    Set(Vec<Resp>),
    /// RESP3 push: >n\r\n + n elements (out-of-band push messages)
    Push(Vec<Resp>),
    /// Pre-serialized RESP bytes — written as-is, no framing added.
    /// Used by commands that serialize directly into a buffer for performance
    /// (e.g. LRANGE avoids building an intermediate Vec<Resp>).
    Raw(Bytes),
}

impl Resp {
    pub fn ok() -> Self {
        Resp::SimpleString("OK".to_string())
    }

    pub fn null() -> Self {
        Resp::BulkString(None)
    }

    pub fn null_array() -> Self {
        Resp::Array(None)
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Resp::Error(msg.into())
    }

    pub fn bulk(s: impl Into<Bytes>) -> Self {
        Resp::BulkString(Some(s.into()))
    }

    pub fn bulk_str(s: impl Into<String>) -> Self {
        Resp::BulkString(Some(Bytes::from(s.into().into_bytes())))
    }

    pub fn int(n: i64) -> Self {
        Resp::Integer(n)
    }

    pub fn array(items: Vec<Resp>) -> Self {
        Resp::Array(Some(items))
    }

    /// Serialize to RESP bytes
    pub fn serialize(&self) -> Bytes {
        self.serialize_for_version(2)
    }

    /// Serialize to RESP bytes, respecting the negotiated protocol version.
    ///
    /// Differences between RESP2 and RESP3:
    /// - RESP3 encodes null as `_\r\n`; RESP2 uses `$-1\r\n` (bulk) or
    ///   `*-1\r\n` (array).
    /// - RESP3 has native `Map`, `Set`, `Bool`, `Double`, `Push`. When a
    ///   connection has negotiated RESP2 we degrade those to a sensible
    ///   RESP2 equivalent so old clients still see meaningful replies.
    pub fn serialize_for_version(&self, version: u8) -> Bytes {
        let mut buf = SegBuf::with_capacity(64);
        self.write_to_for_version(&mut buf, version);
        buf.finalize();
        if buf.segments.len() == 1 {
            buf.segments.pop().unwrap()
        } else {
            let total: usize = buf.segments.iter().map(|s| s.len()).sum();
            let mut out = BytesMut::with_capacity(total);
            for seg in &buf.segments {
                out.put(seg.as_ref());
            }
            out.freeze()
        }
    }

    pub fn write_to(&self, buf: &mut SegBuf) {
        self.write_to_for_version(buf, 2);
    }

    pub fn write_to_for_version(&self, buf: &mut SegBuf, version: u8) {
        match self {
            Resp::SimpleString(s) => {
                let b = buf.inner();
                b.put_u8(b'+');
                b.put(s.as_bytes());
                b.put(&b"\r\n"[..]);
            }
            Resp::Error(s) => {
                let b = buf.inner();
                b.put_u8(b'-');
                b.put(s.as_bytes());
                b.put(&b"\r\n"[..]);
            }
            Resp::Integer(n) => {
                let b = buf.inner();
                b.put_u8(b':');
                put_i64(b, *n);
                b.put(&b"\r\n"[..]);
            }
            Resp::BulkString(None) => {
                if version >= 3 {
                    buf.inner().put(&b"_\r\n"[..]);
                } else {
                    buf.inner().put(&b"$-1\r\n"[..]);
                }
            }
            Resp::BulkString(Some(data)) => {
                let b = buf.inner();
                b.put_u8(b'$');
                put_usize(b, data.len());
                b.put(&b"\r\n"[..]);
                b.put(data.as_ref());
                b.put(&b"\r\n"[..]);
            }
            Resp::Array(None) => {
                if version >= 3 {
                    buf.inner().put(&b"_\r\n"[..]);
                } else {
                    buf.inner().put(&b"*-1\r\n"[..]);
                }
            }
            Resp::Array(Some(items)) => {
                let b = buf.inner();
                b.put_u8(b'*');
                put_usize(b, items.len());
                b.put(&b"\r\n"[..]);
                for item in items {
                    item.write_to_for_version(buf, version);
                }
            }
            // ── RESP3 ─────────────────────────────────────────────────────────
            Resp::Null => {
                buf.inner().put(&b"_\r\n"[..]);
            }
            Resp::Bool(b) => {
                if version >= 3 {
                    let bf = buf.inner();
                    bf.put_u8(b'#');
                    bf.put_u8(if *b { b't' } else { b'f' });
                    bf.put(&b"\r\n"[..]);
                } else {
                    let bf = buf.inner();
                    bf.put_u8(b':');
                    bf.put_u8(if *b { b'1' } else { b'0' });
                    bf.put(&b"\r\n"[..]);
                }
            }
            Resp::Double(d) => {
                if version >= 3 {
                    let b = buf.inner();
                    b.put_u8(b',');
                    b.put(format!("{}", d).as_bytes());
                    b.put(&b"\r\n"[..]);
                } else {
                    let s = format!("{}", d);
                    let b = buf.inner();
                    b.put_u8(b'$');
                    put_usize(b, s.len());
                    b.put(&b"\r\n"[..]);
                    b.put(s.as_bytes());
                    b.put(&b"\r\n"[..]);
                }
            }
            Resp::Map(pairs) => {
                if version >= 3 {
                    let b = buf.inner();
                    b.put_u8(b'%');
                    put_usize(b, pairs.len());
                    b.put(&b"\r\n"[..]);
                    for (k, v) in pairs {
                        k.write_to_for_version(buf, version);
                        v.write_to_for_version(buf, version);
                    }
                } else {
                    // RESP2 fallback: flat array of [k1, v1, k2, v2, ...].
                    let b = buf.inner();
                    b.put_u8(b'*');
                    put_usize(b, pairs.len() * 2);
                    b.put(&b"\r\n"[..]);
                    for (k, v) in pairs {
                        k.write_to_for_version(buf, version);
                        v.write_to_for_version(buf, version);
                    }
                }
            }
            Resp::Set(items) => {
                if version >= 3 {
                    let b = buf.inner();
                    b.put_u8(b'~');
                    put_usize(b, items.len());
                    b.put(&b"\r\n"[..]);
                    for item in items {
                        item.write_to_for_version(buf, version);
                    }
                } else {
                    let b = buf.inner();
                    b.put_u8(b'*');
                    put_usize(b, items.len());
                    b.put(&b"\r\n"[..]);
                    for item in items {
                        item.write_to_for_version(buf, version);
                    }
                }
            }
            Resp::Push(items) => {
                if version >= 3 {
                    let b = buf.inner();
                    b.put_u8(b'>');
                    put_usize(b, items.len());
                    b.put(&b"\r\n"[..]);
                    for item in items {
                        item.write_to_for_version(buf, version);
                    }
                } else {
                    // RESP2 has no push frames; degrade to a regular array.
                    let b = buf.inner();
                    b.put_u8(b'*');
                    put_usize(b, items.len());
                    b.put(&b"\r\n"[..]);
                    for item in items {
                        item.write_to_for_version(buf, version);
                    }
                }
            }
            Resp::Raw(bytes) => {
                buf.push_raw(bytes.clone());
            }
        }
    }

    /// Serialize a bulk-string array directly into `buf`, without allocating
    /// an intermediate `Vec<Resp>`.
    pub(crate) fn write_bulk_array_into(
        buf: &mut BytesMut,
        iter: impl ExactSizeIterator<Item = impl AsRef<[u8]>> + Clone,
    ) {
        let count = iter.len();
        let data_bytes: usize = iter.clone().map(|e| e.as_ref().len()).sum();
        // 23 bytes covers worst-case array header (*<20 digits>\r\n).
        // 14 bytes per element covers worst-case framing ($<10 digits>\r\n\r\n).
        buf.reserve(23 + count * 14 + data_bytes);

        buf.put_u8(b'*');
        put_usize(buf, count);
        buf.put_slice(b"\r\n");
        for elem in iter {
            let data = elem.as_ref();
            buf.put_u8(b'$');
            put_usize(buf, data.len());
            buf.put_slice(b"\r\n");
            buf.put(data);
            buf.put_slice(b"\r\n");
        }
    }

    /// Try to get as string slice
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Resp::BulkString(Some(b)) => std::str::from_utf8(b).ok(),
            Resp::SimpleString(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Try to get as bytes
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            Resp::BulkString(Some(b)) => Some(b),
            _ => None,
        }
    }
}

impl fmt::Display for Resp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Resp::SimpleString(s) => write!(f, "{}", s),
            Resp::Error(s) => write!(f, "ERR {}", s),
            Resp::Integer(n) => write!(f, "{}", n),
            Resp::BulkString(None) => write!(f, "(nil)"),
            Resp::BulkString(Some(b)) => {
                write!(f, "{}", String::from_utf8_lossy(b))
            }
            Resp::Array(None) => write!(f, "(nil)"),
            Resp::Array(Some(items)) => {
                for (i, item) in items.iter().enumerate() {
                    write!(f, "{}) {}", i + 1, item)?;
                    if i + 1 < items.len() {
                        writeln!(f)?;
                    }
                }
                Ok(())
            }
            Resp::Null => write!(f, "(null)"),
            Resp::Bool(b) => write!(f, "{}", b),
            Resp::Double(d) => write!(f, "{}", d),
            Resp::Map(pairs) => {
                for (i, (k, v)) in pairs.iter().enumerate() {
                    write!(f, "{}) {} => {}", i + 1, k, v)?;
                    if i + 1 < pairs.len() {
                        writeln!(f)?;
                    }
                }
                Ok(())
            }
            Resp::Set(items) | Resp::Push(items) => {
                for (i, item) in items.iter().enumerate() {
                    write!(f, "{}) {}", i + 1, item)?;
                    if i + 1 < items.len() {
                        writeln!(f)?;
                    }
                }
                Ok(())
            }
            Resp::Raw(bytes) => write!(f, "{}", String::from_utf8_lossy(bytes)),
        }
    }
}

/// Write an i64 to `buf` without heap allocation.
#[inline]
fn put_i64(buf: &mut BytesMut, n: i64) {
    let mut tmp = [0u8; 20];
    let mut pos = 20usize;
    let neg = n < 0;
    let mut v = if neg { n.unsigned_abs() } else { n as u64 };
    if v == 0 {
        buf.put_u8(b'0');
        return;
    }
    while v > 0 {
        pos -= 1;
        tmp[pos] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    if neg {
        pos -= 1;
        tmp[pos] = b'-';
    }
    buf.put(&tmp[pos..]);
}

/// Write a usize to `buf` without heap allocation.
#[inline]
fn put_usize(buf: &mut BytesMut, n: usize) {
    put_i64(buf, n as i64);
}

/// Segmented write buffer that supports zero-copy for pre-serialized `Bytes`.
///
/// Inline writes go into an active `BytesMut`. When a raw `Bytes` segment is
/// pushed, the current inline bytes are frozen into a segment and the raw bytes
/// are appended as a separate segment. At flush time, `chunks_vectored` yields
/// one `IoSlice` per segment for `writev(2)`.
pub struct SegBuf {
    active: BytesMut,
    segments: Vec<Bytes>,
    read_pos: usize,
    read_offset: usize,
}

impl SegBuf {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            active: BytesMut::with_capacity(cap),
            segments: Vec::new(),
            read_pos: 0,
            read_offset: 0,
        }
    }

    pub fn inner(&mut self) -> &mut BytesMut {
        &mut self.active
    }

    /// Freeze any pending inline bytes, then append `raw` as a zero-copy segment.
    pub fn push_raw(&mut self, raw: Bytes) {
        if !self.active.is_empty() {
            self.segments.push(self.active.split().freeze());
        }
        self.segments.push(raw);
    }

    pub fn clear(&mut self) {
        self.active.clear();
        self.segments.clear();
        self.read_pos = 0;
        self.read_offset = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty() && self.active.is_empty()
    }

    pub fn finalize(&mut self) {
        if !self.active.is_empty() {
            self.segments.push(self.active.split().freeze());
        }
    }
}

impl Buf for SegBuf {
    fn remaining(&self) -> usize {
        let mut total = self.active.len();
        for (i, seg) in self.segments[self.read_pos..].iter().enumerate() {
            if i == 0 {
                total += seg.len() - self.read_offset;
            } else {
                total += seg.len();
            }
        }
        total
    }

    fn chunk(&self) -> &[u8] {
        if self.read_pos < self.segments.len() {
            &self.segments[self.read_pos][self.read_offset..]
        } else {
            &self.active[..]
        }
    }

    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 {
            if self.read_pos < self.segments.len() {
                let seg = &self.segments[self.read_pos];
                let avail = seg.len() - self.read_offset;
                if cnt < avail {
                    self.read_offset += cnt;
                    return;
                }
                cnt -= avail;
                self.read_pos += 1;
                self.read_offset = 0;
            } else {
                self.active.advance(cnt);
                return;
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let mut n = 0;
        for (i, seg) in self.segments[self.read_pos..].iter().enumerate() {
            if n >= dst.len() {
                break;
            }
            let data = if i == 0 {
                &seg[self.read_offset..]
            } else {
                &seg[..]
            };
            if !data.is_empty() {
                dst[n] = IoSlice::new(data);
                n += 1;
            }
        }
        if n < dst.len() && !self.active.is_empty() {
            dst[n] = IoSlice::new(&self.active[..]);
            n += 1;
        }
        n
    }
}

/// RESP protocol parser
pub struct RespParser {
    buf: BytesMut,
}

impl RespParser {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(4096),
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Try to parse one complete RESP value from the buffer.
    /// Returns Ok(Some(value)) if a complete value was parsed,
    /// Ok(None) if more data is needed,
    /// Err if protocol error.
    pub fn parse_one(&mut self) -> Result<Option<Resp>> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        match parse_resp(&self.buf) {
            Ok((value, consumed)) => {
                self.buf.advance(consumed);
                Ok(Some(value))
            }
            Err(ParseError::Incomplete) => Ok(None),
            Err(ParseError::Invalid(msg)) => Err(NexradeError::ProtocolError(msg)),
        }
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
enum ParseError {
    Incomplete,
    Invalid(String),
}

fn parse_resp(buf: &[u8]) -> std::result::Result<(Resp, usize), ParseError> {
    if buf.is_empty() {
        return Err(ParseError::Incomplete);
    }
    match buf[0] {
        b'+' => parse_simple_string(buf),
        b'-' => parse_error(buf),
        b':' => parse_integer(buf),
        b'$' => parse_bulk_string(buf),
        b'*' => parse_array(buf),
        // RESP3 types
        b'_' => parse_resp3_null(buf),
        b'#' => parse_resp3_bool(buf),
        b',' => parse_resp3_double(buf),
        b'%' => parse_resp3_map(buf),
        b'~' => parse_resp3_set(buf),
        b'>' => parse_resp3_push(buf),
        // Inline commands (for telnet/manual testing)
        _ => parse_inline(buf),
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

fn parse_line(buf: &[u8]) -> std::result::Result<(&[u8], usize), ParseError> {
    match find_crlf(buf) {
        Some(pos) => Ok((&buf[1..pos], pos + 2)),
        None => Err(ParseError::Incomplete),
    }
}

fn parse_simple_string(buf: &[u8]) -> std::result::Result<(Resp, usize), ParseError> {
    let (line, consumed) = parse_line(buf)?;
    Ok((
        Resp::SimpleString(String::from_utf8_lossy(line).to_string()),
        consumed,
    ))
}

fn parse_error(buf: &[u8]) -> std::result::Result<(Resp, usize), ParseError> {
    let (line, consumed) = parse_line(buf)?;
    Ok((
        Resp::Error(String::from_utf8_lossy(line).to_string()),
        consumed,
    ))
}

fn parse_integer(buf: &[u8]) -> std::result::Result<(Resp, usize), ParseError> {
    let (line, consumed) = parse_line(buf)?;
    let s = std::str::from_utf8(line)
        .map_err(|_| ParseError::Invalid("invalid integer encoding".to_string()))?;
    let n: i64 = s
        .parse()
        .map_err(|_| ParseError::Invalid(format!("invalid integer: {}", s)))?;
    Ok((Resp::Integer(n), consumed))
}

fn parse_bulk_string(buf: &[u8]) -> std::result::Result<(Resp, usize), ParseError> {
    let (line, header_len) = parse_line(buf)?;
    let s = std::str::from_utf8(line)
        .map_err(|_| ParseError::Invalid("invalid bulk string length".to_string()))?;
    let len: i64 = s
        .parse()
        .map_err(|_| ParseError::Invalid(format!("invalid bulk length: {}", s)))?;

    if len == -1 {
        return Ok((Resp::BulkString(None), header_len));
    }
    if len < 0 {
        return Err(ParseError::Invalid(format!("invalid bulk length: {}", len)));
    }

    let len = len as usize;
    let total = header_len + len + 2; // +2 for trailing \r\n
    if buf.len() < total {
        return Err(ParseError::Incomplete);
    }

    let data = Bytes::copy_from_slice(&buf[header_len..header_len + len]);
    // Validate trailing CRLF
    if &buf[header_len + len..header_len + len + 2] != b"\r\n" {
        return Err(ParseError::Invalid(
            "missing CRLF after bulk string".to_string(),
        ));
    }

    Ok((Resp::BulkString(Some(data)), total))
}

fn parse_array(buf: &[u8]) -> std::result::Result<(Resp, usize), ParseError> {
    let (line, header_len) = parse_line(buf)?;
    let s = std::str::from_utf8(line)
        .map_err(|_| ParseError::Invalid("invalid array length".to_string()))?;
    let len: i64 = s
        .parse()
        .map_err(|_| ParseError::Invalid(format!("invalid array length: {}", s)))?;

    if len == -1 {
        return Ok((Resp::Array(None), header_len));
    }
    if len < 0 {
        return Err(ParseError::Invalid(format!(
            "invalid array length: {}",
            len
        )));
    }

    let len = len as usize;
    let mut items = Vec::with_capacity(len);
    let mut offset = header_len;

    for _ in 0..len {
        let (item, consumed) = parse_resp(&buf[offset..])?;
        items.push(item);
        offset += consumed;
    }

    Ok((Resp::Array(Some(items)), offset))
}

fn parse_resp3_null(buf: &[u8]) -> std::result::Result<(Resp, usize), ParseError> {
    // _\r\n
    if buf.len() < 3 {
        return Err(ParseError::Incomplete);
    }
    if &buf[1..3] != b"\r\n" {
        return Err(ParseError::Invalid("invalid RESP3 null".to_string()));
    }
    Ok((Resp::Null, 3))
}

fn parse_resp3_bool(buf: &[u8]) -> std::result::Result<(Resp, usize), ParseError> {
    // #t\r\n or #f\r\n
    if buf.len() < 4 {
        return Err(ParseError::Incomplete);
    }
    let val = match buf[1] {
        b't' => true,
        b'f' => false,
        _ => return Err(ParseError::Invalid("invalid RESP3 boolean".to_string())),
    };
    if &buf[2..4] != b"\r\n" {
        return Err(ParseError::Invalid("invalid RESP3 boolean".to_string()));
    }
    Ok((Resp::Bool(val), 4))
}

fn parse_resp3_double(buf: &[u8]) -> std::result::Result<(Resp, usize), ParseError> {
    let (line, consumed) = parse_line(buf)?;
    let s = std::str::from_utf8(line)
        .map_err(|_| ParseError::Invalid("invalid RESP3 double".to_string()))?;
    let d: f64 = s
        .parse()
        .map_err(|_| ParseError::Invalid(format!("invalid double: {}", s)))?;
    Ok((Resp::Double(d), consumed))
}

fn parse_resp3_map(buf: &[u8]) -> std::result::Result<(Resp, usize), ParseError> {
    let (line, header_len) = parse_line(buf)?;
    let s = std::str::from_utf8(line)
        .map_err(|_| ParseError::Invalid("invalid RESP3 map length".to_string()))?;
    let count: usize = s
        .parse()
        .map_err(|_| ParseError::Invalid(format!("invalid map length: {}", s)))?;
    let mut pairs = Vec::with_capacity(count);
    let mut offset = header_len;
    for _ in 0..count {
        let (k, klen) = parse_resp(&buf[offset..])?;
        offset += klen;
        let (v, vlen) = parse_resp(&buf[offset..])?;
        offset += vlen;
        pairs.push((k, v));
    }
    Ok((Resp::Map(pairs), offset))
}

fn parse_resp3_set(buf: &[u8]) -> std::result::Result<(Resp, usize), ParseError> {
    let (line, header_len) = parse_line(buf)?;
    let s = std::str::from_utf8(line)
        .map_err(|_| ParseError::Invalid("invalid RESP3 set length".to_string()))?;
    let count: usize = s
        .parse()
        .map_err(|_| ParseError::Invalid(format!("invalid set length: {}", s)))?;
    let mut items = Vec::with_capacity(count);
    let mut offset = header_len;
    for _ in 0..count {
        let (item, len) = parse_resp(&buf[offset..])?;
        items.push(item);
        offset += len;
    }
    Ok((Resp::Set(items), offset))
}

fn parse_resp3_push(buf: &[u8]) -> std::result::Result<(Resp, usize), ParseError> {
    let (line, header_len) = parse_line(buf)?;
    let s = std::str::from_utf8(line)
        .map_err(|_| ParseError::Invalid("invalid RESP3 push length".to_string()))?;
    let count: usize = s
        .parse()
        .map_err(|_| ParseError::Invalid(format!("invalid push length: {}", s)))?;
    let mut items = Vec::with_capacity(count);
    let mut offset = header_len;
    for _ in 0..count {
        let (item, len) = parse_resp(&buf[offset..])?;
        items.push(item);
        offset += len;
    }
    Ok((Resp::Push(items), offset))
}

fn parse_inline(buf: &[u8]) -> std::result::Result<(Resp, usize), ParseError> {
    match find_crlf(buf) {
        None => Err(ParseError::Incomplete),
        Some(pos) => {
            let line = &buf[..pos];
            let parts = tokenize_inline(line)?;
            if parts.is_empty() {
                return Err(ParseError::Invalid("empty inline command".to_string()));
            }
            Ok((Resp::Array(Some(parts)), pos + 2))
        }
    }
}

/// Tokenize an inline command line, respecting `"..."` and `'...'` quoting.
/// Matches Redis's own inline parser behaviour:
/// - tokens are separated by ASCII spaces
/// - a token may be wrapped in double- or single-quotes; the quotes are stripped
/// - `\"` inside a double-quoted token is an escaped double-quote
/// - an unterminated quote returns `ParseError::Invalid`
fn tokenize_inline(line: &[u8]) -> std::result::Result<Vec<Resp>, ParseError> {
    let mut parts: Vec<Resp> = Vec::with_capacity(4);
    let mut i = 0;

    while i < line.len() {
        // skip leading spaces
        while i < line.len() && line[i] == b' ' {
            i += 1;
        }
        if i >= line.len() {
            break;
        }

        if line[i] == b'"' || line[i] == b'\'' {
            // quoted token: needs an owned buffer for escape processing
            let quote = line[i];
            i += 1; // skip opening quote
            let mut token: Vec<u8> = Vec::new();
            loop {
                if i >= line.len() {
                    return Err(ParseError::Invalid(
                        "unterminated quoted string in inline command".to_string(),
                    ));
                }
                if line[i] == quote {
                    i += 1; // skip closing quote
                    break;
                }
                // backslash escape only inside double-quotes, matching Redis
                if quote == b'"' && line[i] == b'\\' && i + 1 < line.len() {
                    i += 1;
                    token.push(match line[i] {
                        b'"' => b'"',
                        b'\\' => b'\\',
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        other => other,
                    });
                } else {
                    token.push(line[i]);
                }
                i += 1;
            }
            parts.push(Resp::BulkString(Some(Bytes::copy_from_slice(&token))));
        } else {
            // unquoted token: slice directly from the input — no intermediate buffer
            let start = i;
            while i < line.len() && line[i] != b' ' {
                i += 1;
            }
            parts.push(Resp::BulkString(Some(Bytes::copy_from_slice(
                &line[start..i],
            ))));
        }
    }

    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_simple_string() {
        let r = Resp::ok();
        assert_eq!(r.serialize().as_ref(), b"+OK\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let r = Resp::bulk_str("hello");
        assert_eq!(r.serialize().as_ref(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_serialize_null() {
        let r = Resp::null();
        assert_eq!(r.serialize().as_ref(), b"$-1\r\n");
    }

    #[test]
    fn test_parse_simple_string() {
        let mut p = RespParser::new();
        p.feed(b"+OK\r\n");
        let v = p.parse_one().unwrap().unwrap();
        assert_eq!(v, Resp::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_parse_array() {
        let mut p = RespParser::new();
        p.feed(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let v = p.parse_one().unwrap().unwrap();
        match v {
            Resp::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0].as_str(), Some("SET"));
                assert_eq!(items[1].as_str(), Some("foo"));
                assert_eq!(items[2].as_str(), Some("bar"));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_parse_inline() {
        let mut p = RespParser::new();
        p.feed(b"PING\r\n");
        let v = p.parse_one().unwrap().unwrap();
        match v {
            Resp::Array(Some(items)) => {
                assert_eq!(items[0].as_str(), Some("PING"));
            }
            _ => panic!("expected array"),
        }
    }

    fn inline_args(line: &[u8]) -> Vec<String> {
        let parts = tokenize_inline(line).unwrap();
        parts
            .iter()
            .map(|p| p.as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn test_inline_quoted_double() {
        // SET mykey "Hello World"  →  ["SET", "mykey", "Hello World"]
        assert_eq!(
            inline_args(b"SET mykey \"Hello World\""),
            vec!["SET", "mykey", "Hello World"]
        );
    }

    #[test]
    fn test_inline_quoted_single() {
        assert_eq!(
            inline_args(b"SET mykey 'Hello World'"),
            vec!["SET", "mykey", "Hello World"]
        );
    }

    #[test]
    fn test_inline_escape_in_double_quotes() {
        // "say \"hi\""  →  say "hi"
        assert_eq!(
            inline_args(b"SET k \"say \\\"hi\\\"\""),
            vec!["SET", "k", "say \"hi\""]
        );
    }

    #[test]
    fn test_inline_unquoted_no_spaces() {
        assert_eq!(inline_args(b"SET foo bar"), vec!["SET", "foo", "bar"]);
    }

    #[test]
    fn test_inline_extra_spaces() {
        assert_eq!(inline_args(b"SET   foo   bar"), vec!["SET", "foo", "bar"]);
    }

    #[test]
    fn test_inline_unterminated_quote() {
        assert!(tokenize_inline(b"SET key \"unterminated").is_err());
    }
}
