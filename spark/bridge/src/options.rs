// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Decoder for the connector's default options wire format.
//!
//! `BridgeProviderFactory.encodeOptions`'s default (`OptionsCodec` on the JVM
//! side) encodes the Spark options map as length-prefixed UTF-8 pairs,
//! sorted by key: big-endian `i32` entry count, then per entry key length,
//! key bytes, value length, value bytes. Key-sorting makes the bytes a pure
//! function of the map contents — the shared-scan determinism contract uses
//! the options bytes as the scan identity.
//!
//! Bridges using the default JVM encoding read their options here:
//!
//! ```ignore
//! let opts = datafusion_spark_bridge::options::decode_options(options_bytes)?;
//! let url = opts.get("url").ok_or("missing required option 'url'")?;
//! ```
//!
//! The two implementations are pinned to each other by the shared fixture in
//! the tests below; `OptionsCodecTest` on the JVM side asserts the same
//! bytes.

use std::collections::BTreeMap;

/// Decode bytes produced by the JVM `OptionsCodec.encode` (or
/// [`encode_options`]). Empty input decodes as an empty map.
pub fn decode_options(bytes: &[u8]) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    if bytes.is_empty() {
        return Ok(out);
    }
    let mut cursor = Cursor { bytes, pos: 0 };
    let count = cursor.read_len("entry count")?;
    for i in 0..count {
        let key = cursor.read_string(&format!("key of entry {i}"))?;
        let value = cursor.read_string(&format!("value of entry {i}"))?;
        out.insert(key, value);
    }
    if cursor.pos != bytes.len() {
        return Err(format!(
            "options blob has {} trailing byte(s) after {count} entries",
            bytes.len() - cursor.pos
        ));
    }
    Ok(out)
}

/// Encode in the same format (key-sorted via `BTreeMap`). Primarily for
/// tests and Rust-side tooling; production encoding normally happens on the
/// JVM driver.
pub fn encode_options(options: &BTreeMap<String, String>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(options.len() as i32).to_be_bytes());
    for (key, value) in options {
        out.extend_from_slice(&(key.len() as i32).to_be_bytes());
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(&(value.len() as i32).to_be_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    out
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Cursor<'_> {
    fn read_len(&mut self, what: &str) -> Result<usize, String> {
        if self.bytes.len() - self.pos < 4 {
            return Err(format!("options blob truncated reading {what}"));
        }
        let raw = i32::from_be_bytes(self.bytes[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        usize::try_from(raw).map_err(|_| format!("negative length for {what}: {raw}"))
    }

    fn read_string(&mut self, what: &str) -> Result<String, String> {
        let len = self.read_len(&format!("length of {what}"))?;
        if self.bytes.len() - self.pos < len {
            return Err(format!("options blob truncated reading {what}"));
        }
        let slice = &self.bytes[self.pos..self.pos + len];
        self.pos += len;
        String::from_utf8(slice.to_vec()).map_err(|e| format!("{what} is not UTF-8: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shared fixture: must stay byte-identical to the one asserted by the
    /// JVM-side `OptionsCodecTest`. {"table": "t1", "url": "grpc://h:1"}
    /// encodes (sorted: table < url) as below.
    fn fixture_bytes() -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&2i32.to_be_bytes());
        for (k, v) in [("table", "t1"), ("url", "grpc://h:1")] {
            b.extend_from_slice(&(k.len() as i32).to_be_bytes());
            b.extend_from_slice(k.as_bytes());
            b.extend_from_slice(&(v.len() as i32).to_be_bytes());
            b.extend_from_slice(v.as_bytes());
        }
        b
    }

    #[test]
    fn decodes_fixture() {
        let map = decode_options(&fixture_bytes()).unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("table").map(String::as_str), Some("t1"));
        assert_eq!(map.get("url").map(String::as_str), Some("grpc://h:1"));
    }

    #[test]
    fn round_trips() {
        let mut map = BTreeMap::new();
        map.insert("b".to_string(), "2".to_string());
        map.insert("a".to_string(), "1".to_string());
        map.insert("unicode".to_string(), "héllo→world".to_string());
        let bytes = encode_options(&map);
        assert_eq!(decode_options(&bytes).unwrap(), map);
    }

    #[test]
    fn empty_input_is_empty_map() {
        assert!(decode_options(&[]).unwrap().is_empty());
        let empty = encode_options(&BTreeMap::new());
        assert!(decode_options(&empty).unwrap().is_empty());
    }

    #[test]
    fn rejects_truncation_and_trailing_bytes() {
        let bytes = fixture_bytes();
        assert!(decode_options(&bytes[..bytes.len() - 1])
            .unwrap_err()
            .contains("truncated"));
        let mut extended = bytes.clone();
        extended.push(0);
        assert!(decode_options(&extended).unwrap_err().contains("trailing"));
    }
}
