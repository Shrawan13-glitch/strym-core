//! Minimal AMF0 — enough to build the RTMP control plane (`connect`,
//! `createStream`, `publish`, `@setDataFrame`) and to read back the server's
//! `_result` / `onStatus` responses. Not a general-purpose codec.

/// Decoded AMF0 value, kept small on purpose.
///
/// `non_exhaustive`: new AMF0 markers may be mapped in a minor release;
/// matches must keep a wildcard arm.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Val {
    /// IEEE-754 double (AMF0's only numeric type).
    Number(f64),
    /// Boolean.
    Boolean(bool),
    /// UTF-8 string (length-prefixed on the wire).
    String(String),
    /// Anonymous object: ordered field list.
    Object(Vec<(String, Val)>),
    /// Strict array (rarely seen from RTMP servers).
    Array(Vec<Val>),
    /// Null.
    Null,
    /// Undefined (also returned for unknown type markers).
    Undefined,
}

/// AMF0 type marker for an IEEE-754 double.
pub const TYPE_NUMBER: u8 = 0x00;
/// AMF0 type marker for a boolean.
pub const TYPE_BOOLEAN: u8 = 0x01;
/// AMF0 type marker for a UTF-8 string.
pub const TYPE_STRING: u8 = 0x02;
/// AMF0 type marker for an anonymous object.
pub const TYPE_OBJECT: u8 = 0x03;
/// AMF0 type marker for null.
pub const TYPE_NULL: u8 = 0x05;
/// AMF0 type marker for undefined.
pub const TYPE_UNDEFINED: u8 = 0x06;
/// AMF0 type marker for an ECMA (associative) array.
pub const TYPE_ECMA_ARRAY: u8 = 0x08;
/// AMF0 marker terminating an object's field list.
pub const TYPE_OBJECT_END: u8 = 0x09;

/// A field value inside an AMF0 object.
///
/// `non_exhaustive`: new field types may be added in a minor release; matches
/// must keep a wildcard arm.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum ObjVal<'a> {
    /// IEEE-754 double field.
    Num(f64),
    /// Boolean field.
    Bool(bool),
    /// Borrowed UTF-8 string field.
    Str(&'a str),
}

/// Writes AMF0 values into a byte buffer.
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    /// Create an empty writer.
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// Append an IEEE-754 double.
    pub fn number(&mut self, n: f64) -> &mut Self {
        self.buf.push(TYPE_NUMBER);
        self.buf.extend_from_slice(&n.to_bits().to_be_bytes());
        self
    }

    /// Append a boolean.
    pub fn boolean(&mut self, b: bool) -> &mut Self {
        self.buf.push(TYPE_BOOLEAN);
        self.buf.push(b as u8);
        self
    }

    /// Append a length-prefixed UTF-8 string.
    pub fn string(&mut self, s: &str) -> &mut Self {
        self.buf.push(TYPE_STRING);
        self.buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
        self.buf.extend_from_slice(s.as_bytes());
        self
    }

    /// Append a null value.
    pub fn null(&mut self) -> &mut Self {
        self.buf.push(TYPE_NULL);
        self
    }

    /// ECMA array (an associative map). Server-side FLV metadata prefers this
    /// over a plain object.
    pub fn ecma_array(&mut self, entries: &[(&str, f64)]) -> &mut Self {
        self.buf.push(TYPE_ECMA_ARRAY);
        self.buf.extend_from_slice(&(entries.len() as u32).to_be_bytes());
        for (k, v) in entries {
            self.buf.extend_from_slice(&(k.len() as u16).to_be_bytes());
            self.buf.extend_from_slice(k.as_bytes());
            self.number(*v);
        }
        self.buf.extend_from_slice(&[0, 0, TYPE_OBJECT_END]);
        self
    }

    /// Object with mixed-typed fields (connect's app/flashVer/etc.).
    pub fn object(&mut self, entries: &[(&str, ObjVal)]) -> &mut Self {
        self.buf.push(TYPE_OBJECT);
        for (k, v) in entries {
            self.buf.extend_from_slice(&(k.len() as u16).to_be_bytes());
            self.buf.extend_from_slice(k.as_bytes());
            match v {
                ObjVal::Num(n) => {
                    self.number(*n);
                }
                ObjVal::Bool(b) => {
                    self.boolean(*b);
                }
                ObjVal::Str(s) => {
                    self.string(s);
                }
            }
        }
        self.buf.extend_from_slice(&[0, 0, TYPE_OBJECT_END]);
        self
    }

    /// A bare string value without a type marker (used inside `@setDataFrame`).
    pub fn raw_string(&mut self, s: &str) -> &mut Self {
        self.buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
        self.buf.extend_from_slice(s.as_bytes());
        self
    }

    /// Consume the writer, returning the encoded bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}

impl Default for Writer {
    fn default() -> Self {
        Self::new()
    }
}

/// Streaming decoder over a byte slice.
pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// Create a reader over `data`, positioned at the first byte.
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// Read one byte, or `None` at end of input.
    pub fn read_u8(&mut self) -> Option<u8> {
        let b = *self.data.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    /// Read a big-endian u16, or `None` if fewer than 2 bytes remain.
    pub fn read_u16(&mut self) -> Option<u16> {
        let b = self.data.get(self.pos..self.pos + 2)?;
        self.pos += 2;
        Some(u16::from_be_bytes([b[0], b[1]]))
    }

    /// Read a big-endian IEEE-754 double, or `None` if fewer than 8 bytes remain.
    pub fn read_f64(&mut self) -> Option<f64> {
        let b = self.data.get(self.pos..self.pos + 8)?;
        self.pos += 8;
        Some(f64::from_bits(u64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ])))
    }

    /// Read a length-prefixed UTF-8 string, or `None` if truncated.
    pub fn read_utf8(&mut self) -> Option<String> {
        let len = self.read_u16()? as usize;
        let b = self.data.get(self.pos..self.pos + len)?;
        self.pos += len;
        Some(String::from_utf8_lossy(b).into_owned())
    }

    /// Skip a length-prefixed UTF-8 string (e.g. a command name).
    pub fn skip_utf8(&mut self) -> Option<()> {
        let len = self.read_u16()? as usize;
        self.data.get(self.pos..self.pos + len)?;
        self.pos += len;
        Some(())
    }

    /// Read object / ECMA-array entries into `out`. `count` = known entry count
    /// (ECMA arrays) or None to read until the 0x000009 end marker (objects).
    fn read_entries(&mut self, out: &mut Vec<(String, Val)>, count: Option<usize>) -> Option<()> {
        let mut n = 0usize;
        loop {
            if let Some(c) = count {
                if n >= c {
                    // consume the trailing marker for completeness
                    self.read_u16()?;
                    let _ = self.read_u8();
                    break;
                }
            }
            let keylen = self.read_u16()?;
            if keylen == 0 {
                // Object end marker is an empty key + 0x09.
                let t = self.read_u8()?;
                if t == TYPE_OBJECT_END {
                    break;
                }
                // Empty key but a real value: consume it.
                self.pos -= 1;
                out.push((String::new(), self.read_value()?));
            } else {
                let keylen = keylen as usize;
                let raw = self.data.get(self.pos..self.pos + keylen)?;
                let key = String::from_utf8_lossy(raw).into_owned();
                self.pos += keylen;
                out.push((key, self.read_value()?));
            }
            n += 1;
        }
        Some(())
    }

    /// Read one complete AMF0 value, or `None` if the input is truncated or
    /// carries a type marker this minimal codec doesn't understand.
    pub fn read_value(&mut self) -> Option<Val> {
        match self.read_u8()? {
            TYPE_NUMBER => self.read_f64().map(Val::Number),
            TYPE_BOOLEAN => self.read_u8().map(|b| Val::Boolean(b != 0)),
            TYPE_STRING => self.read_utf8().map(Val::String),
            TYPE_OBJECT => {
                let mut out = Vec::new();
                self.read_entries(&mut out, None)?;
                Some(Val::Object(out))
            }
            TYPE_ECMA_ARRAY => {
                let count =
                    u32::from_be_bytes([self.read_u8()?, self.read_u8()?, self.read_u8()?, self.read_u8()?]) as usize;
                let mut out = Vec::new();
                self.read_entries(&mut out, Some(count))?;
                Some(Val::Object(out))
            }
            TYPE_NULL => Some(Val::Null),
            TYPE_UNDEFINED => Some(Val::Undefined),
            _ => None,
        }
    }

    /// Read every remaining value until the input is exhausted or an invalid
    /// marker appears. Never panics, regardless of input.
    pub fn read_all(&mut self) -> Vec<Val> {
        let mut out = Vec::new();
        while let Some(v) = self.read_value() {
            out.push(v);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_values() {
        let mut w = Writer::new();
        w.number(2.0)
            .string("hi")
            .boolean(true)
            .ecma_array(&[("a", 1.0), ("b", 2.0)]);
        let bytes = w.into_bytes();

        let mut r = Reader::new(&bytes);
        assert_eq!(r.read_value(), Some(Val::Number(2.0)));
        assert_eq!(r.read_value(), Some(Val::String("hi".into())));
        assert_eq!(r.read_value(), Some(Val::Boolean(true)));
        assert_eq!(
            r.read_value(),
            Some(Val::Object(vec![
                ("a".into(), Val::Number(1.0)),
                ("b".into(), Val::Number(2.0))
            ]))
        );
    }

    #[test]
    fn truncated_object_is_none_not_panic() {
        let mut w = Writer::new();
        w.object(&[("key", amf0_str("value"))]);
        let bytes = w.into_bytes();
        // Chop the encoding at every length: none may panic.
        for cut in 0..=bytes.len() {
            let mut r = Reader::new(&bytes[..cut]);
            let _ = r.read_value();
        }
    }

    #[test]
    fn truncated_string_is_none() {
        assert_eq!(Reader::new(&[TYPE_STRING, 0x00, 0x10, b'a']).read_value(), None);
        assert_eq!(Reader::new(&[TYPE_STRING, 0xFF]).read_value(), None);
    }

    #[test]
    fn unknown_marker_is_none() {
        assert_eq!(Reader::new(&[0x0C, 0, 0, 0, 0, 0, 0, 0, 0]).read_value(), None);
    }

    #[test]
    fn skip_utf8_respects_bounds() {
        let mut r = Reader::new(&[0x00, 0x05, b'a']);
        assert_eq!(r.skip_utf8(), None);
    }

    fn amf0_str(s: &str) -> ObjVal<'_> {
        ObjVal::Str(s)
    }
}
