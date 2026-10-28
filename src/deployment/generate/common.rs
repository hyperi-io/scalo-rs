// Project:   scalo
// File:      src/deployment/generate/common.rs
// Purpose:   Shared port and one-line text helpers for the generators
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

// ============================================================================
// Helpers
// ============================================================================

/// The `/udp` suffix Docker and Compose need on a UDP port, since a bare port
/// number means TCP to both; empty for any other protocol.
pub(super) fn udp_port_suffix(protocol: &str) -> &'static str {
    if protocol.eq_ignore_ascii_case("udp") {
        "/udp"
    } else {
        ""
    }
}

/// `text` with each control character written as its escape, for a contract
/// value printed onto one line, where a raw newline would start a Dockerfile
/// instruction or a YAML key of its own.
pub(super) fn on_one_line(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c.is_control() {
            out.extend(c.escape_default());
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The generators refuse a control character through `validate`, and this
    /// escape keeps a value on its line if one ever gets past it.
    #[test]
    fn a_control_character_is_written_as_its_escape() {
        let cases = [
            ("plain", "web", "web"),
            ("newline", "on\nRUN id", r"on\nRUN id"),
            ("carriage return", "a\rb", r"a\rb"),
            ("tab", "a\tb", r"a\tb"),
            ("empty", "", ""),
        ];
        for (name, input, want) in cases {
            assert_eq!(on_one_line(input), want, "{name}");
        }
    }
}
