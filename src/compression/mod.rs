// Project:   scalo
// File:      src/compression/mod.rs
// Purpose:   One compression type and engine for every scalo surface
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One compression setting, and the engines behind it, for every scalo surface.
//!
//! Every surface reads the same YAML shape:
//!
//! ```yaml
//! compression: {codec: zstd, level: 3}
//! ```
//!
//! A missing `level` takes the codec's default: zstd 3, gzip 6. LZ4 and
//! snappy take no level. Each surface lists the codecs it takes in an
//! [`Accepts`], and its `validate` refuses anything else with the surface,
//! the codecs it allows and the level range.
//!
//! [`Codec`], [`Compression`] and [`Accepts`] are always compiled, so a config
//! type can hold a setting a dependency applies (librdkafka, tonic). The
//! engines -- `Compressor`, `Encoder` and `Decoder` -- need the `compression`
//! feature, and snappy needs `compression-snappy` as well.
//!
//! The block form (`Compressor`, `Compression::compress`) and the stream form
//! (`Encoder`, `Decoder`) are one format for zstd and gzip. For LZ4 and snappy
//! the block form is the raw block (LZ4 with a 4-byte little-endian length in
//! front) and the stream form is the framed format [`Codec::extension`] names,
//! so bytes written by one form are read by the same form only.
//!
//! See [docs/compression.md](https://github.com/hyperi-io/scalo-rs/blob/main/docs/compression.md).

use std::fmt;
use std::ops::RangeInclusive;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[cfg(feature = "compression")]
mod engine;
#[cfg(feature = "compression")]
mod gzip_codec;
#[cfg(feature = "compression")]
mod lz4_codec;
#[cfg(feature = "compression-snappy")]
mod snappy_codec;
#[cfg(feature = "compression")]
mod zstd_codec;

#[cfg(feature = "compression")]
#[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
pub use engine::{Compressor, Decoder, Encoder};

/// A compression codec.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    /// Bytes pass through unchanged.
    #[default]
    None,
    /// Zstandard, levels 1-22, default 3.
    Zstd,
    /// LZ4, no level.
    Lz4,
    /// gzip (deflate), levels 1-9, default 6.
    Gzip,
    /// Snappy, no level. Its engine needs the `compression-snappy` feature.
    Snappy,
}

impl Codec {
    /// Every codec, in tag order.
    pub const ALL: [Self; 5] = [Self::None, Self::Zstd, Self::Lz4, Self::Gzip, Self::Snappy];

    /// The name in config, logs and error messages.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Zstd => "zstd",
            Self::Lz4 => "lz4",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
        }
    }

    /// The level a [`Compression`] with no level runs at, or `None` for a
    /// codec with no levels.
    #[must_use]
    pub const fn default_level(self) -> Option<i32> {
        match self {
            Self::Zstd => Some(3),
            Self::Gzip => Some(6),
            Self::None | Self::Lz4 | Self::Snappy => None,
        }
    }

    /// The levels the codec takes, or `None` for a codec with no levels.
    #[must_use]
    pub const fn levels(self) -> Option<RangeInclusive<i32>> {
        match self {
            Self::Zstd => Some(1..=22),
            Self::Gzip => Some(1..=9),
            Self::None | Self::Lz4 | Self::Snappy => None,
        }
    }

    /// One byte naming the codec, for a store scalo owns to record what each
    /// entry was written with. Stable across releases.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Zstd => 1,
            Self::Lz4 => 2,
            Self::Gzip => 3,
            Self::Snappy => 4,
        }
    }

    /// The codec a [`tag`](Self::tag) names, or `None` for an unknown byte.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::None),
            1 => Some(Self::Zstd),
            2 => Some(Self::Lz4),
            3 => Some(Self::Gzip),
            4 => Some(Self::Snappy),
            _ => None,
        }
    }

    /// The file suffix, without the dot, for what an `Encoder` writes, or
    /// `None` for [`Codec::None`].
    #[must_use]
    pub const fn extension(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Zstd => Some("zst"),
            Self::Lz4 => Some("lz4"),
            Self::Gzip => Some("gz"),
            Self::Snappy => Some("sz"),
        }
    }

    /// The feature its engine needs, or `None` when it needs none.
    #[must_use]
    pub const fn feature(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Zstd | Self::Lz4 | Self::Gzip => Some("compression"),
            Self::Snappy => Some("compression-snappy"),
        }
    }

    /// Whether this build has the codec's engine.
    #[must_use]
    pub const fn is_compiled(self) -> bool {
        match self {
            Self::None => true,
            Self::Zstd | Self::Lz4 | Self::Gzip => cfg!(feature = "compression"),
            Self::Snappy => cfg!(feature = "compression-snappy"),
        }
    }
}

impl fmt::Display for Codec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for Codec {
    type Err = String;

    /// Reads a [`name`](Self::name), in any case.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|codec| codec.name().eq_ignore_ascii_case(s.trim()))
            .ok_or_else(|| {
                format!(
                    "unknown codec {s:?}, expected one of: {}",
                    names(&Self::ALL)
                )
            })
    }
}

/// A codec and the level it runs at, as one surface's `compression` key.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "config-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Compression {
    /// The codec.
    pub codec: Codec,

    /// The level. Unset, the codec's [`default_level`](Codec::default_level).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<i32>,
}

impl Compression {
    /// No compression.
    pub const NONE: Self = Self::new(Codec::None);

    /// `codec` at its default level.
    #[must_use]
    pub const fn new(codec: Codec) -> Self {
        Self { codec, level: None }
    }

    /// This setting at `level`.
    #[must_use]
    pub const fn with_level(self, level: i32) -> Self {
        Self {
            level: Some(level),
            ..self
        }
    }

    /// The level it runs at: the one set, else the codec's default.
    #[must_use]
    pub const fn effective_level(self) -> Option<i32> {
        match self.level {
            Some(level) => Some(level),
            None => self.codec.default_level(),
        }
    }

    /// Whether bytes pass through unchanged.
    #[must_use]
    pub const fn is_none(self) -> bool {
        matches!(self.codec, Codec::None)
    }

    /// Checks the level against the codec, for a setting no surface owns.
    ///
    /// # Errors
    ///
    /// A level on a codec that takes none, or outside the codec's range.
    pub fn validate(self) -> Result<Self, CompressionError> {
        check_level("compression", self, true)
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.effective_level() {
            Some(level) => write!(f, "{}(level={level})", self.codec),
            None => write!(f, "{}", self.codec),
        }
    }
}

impl From<Codec> for Compression {
    fn from(codec: Codec) -> Self {
        Self::new(codec)
    }
}

/// The codecs one surface takes, and whether it takes a level.
///
/// ```
/// use scalo::compression::{Accepts, Codec, Compression};
///
/// const GRPC: Accepts =
///     Accepts::new("grpc.compression", &[Codec::None, Codec::Gzip, Codec::Zstd]).without_level();
///
/// assert!(GRPC.check(Compression::new(Codec::Lz4)).is_err());
/// assert!(GRPC.check(Compression::new(Codec::Gzip).with_level(9)).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Accepts {
    surface: &'static str,
    codecs: &'static [Codec],
    level: bool,
    engine: bool,
}

impl Accepts {
    /// `codecs` on `surface`, the config key errors name. A level is taken,
    /// and scalo's engine runs the codec, so it must be compiled in.
    #[must_use]
    pub const fn new(surface: &'static str, codecs: &'static [Codec]) -> Self {
        Self {
            surface,
            codecs,
            level: true,
            engine: true,
        }
    }

    /// The surface takes no level, because what applies the codec fixes it.
    #[must_use]
    pub const fn without_level(self) -> Self {
        Self {
            level: false,
            ..self
        }
    }

    /// A dependency applies the codec, so scalo's engine need not be compiled.
    #[must_use]
    pub const fn external(self) -> Self {
        Self {
            engine: false,
            ..self
        }
    }

    /// The config key errors name.
    #[must_use]
    pub const fn surface(&self) -> &'static str {
        self.surface
    }

    /// The codecs listed, before the build's features are applied.
    #[must_use]
    pub const fn codecs(&self) -> &'static [Codec] {
        self.codecs
    }

    /// Whether `codec` is taken in this build.
    #[must_use]
    pub fn takes(&self, codec: Codec) -> bool {
        self.codecs.contains(&codec) && (!self.engine || codec.is_compiled())
    }

    /// `compression`, if this surface takes it.
    ///
    /// # Errors
    ///
    /// A codec the surface does not list, an engine this build lacks, a level
    /// where none is taken, or a level outside the codec's range.
    pub fn check(&self, compression: Compression) -> Result<Compression, CompressionError> {
        let codec = compression.codec;
        if !self.codecs.contains(&codec) {
            return Err(CompressionError::NotAccepted {
                surface: self.surface,
                codec,
                accepted: names(self.codecs),
            });
        }
        if self.engine
            && !codec.is_compiled()
            && let Some(feature) = codec.feature()
        {
            return Err(CompressionError::NotCompiled {
                surface: self.surface,
                codec,
                feature,
            });
        }
        check_level(self.surface, compression, self.level)
    }

    /// The schema of this surface's `compression` key: its `codec` enum
    /// narrowed to the codecs this build takes, and its `level` range.
    #[cfg(feature = "config-schema")]
    #[cfg_attr(docsrs, doc(cfg(feature = "config-schema")))]
    #[must_use]
    pub fn schema(&self) -> schemars::Schema {
        let codecs: Vec<Codec> = self
            .codecs
            .iter()
            .copied()
            .filter(|c| self.takes(*c))
            .collect();
        let codec_names: Vec<&str> = codecs.iter().map(|c| c.name()).collect();
        let mut properties = serde_json::Map::new();
        properties.insert(
            "codec".into(),
            serde_json::json!({
                "type": "string",
                "enum": codec_names,
                "description": format!("Codec for {}.", self.surface),
            }),
        );
        let ranges: Vec<(Codec, RangeInclusive<i32>)> = codecs
            .iter()
            .filter_map(|c| c.levels().map(|r| (*c, r)))
            .collect();
        if self.level
            && let (Some(min), Some(max)) = (
                ranges.iter().map(|(_, r)| *r.start()).min(),
                ranges.iter().map(|(_, r)| *r.end()).max(),
            )
        {
            let per_codec: Vec<String> = ranges
                .iter()
                .map(|(c, r)| format!("{c} {}-{}", r.start(), r.end()))
                .collect();
            properties.insert(
                "level".into(),
                serde_json::json!({
                    "type": ["integer", "null"],
                    "minimum": min,
                    "maximum": max,
                    "description": format!(
                        "Level, by codec: {}. Unset, the codec's default.",
                        per_codec.join(", ")
                    ),
                }),
            );
        }
        schemars::json_schema!({
            "type": "object",
            "additionalProperties": false,
            "required": ["codec"],
            "properties": properties,
        })
    }
}

/// A [`Compression`] a surface refuses.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CompressionError {
    /// The surface does not list the codec.
    #[error("{surface}: codec {codec} is not accepted here, use one of: {accepted}")]
    NotAccepted {
        /// The config key.
        surface: &'static str,
        /// The codec given.
        codec: Codec,
        /// The codecs the surface lists.
        accepted: String,
    },

    /// This build lacks the codec's engine.
    #[error("{surface}: codec {codec} needs scalo's `{feature}` feature")]
    NotCompiled {
        /// The config key.
        surface: &'static str,
        /// The codec given.
        codec: Codec,
        /// The feature to enable.
        feature: &'static str,
    },

    /// A level where none is taken.
    #[error("{surface}: {codec} takes no level here")]
    NoLevel {
        /// The config key.
        surface: &'static str,
        /// The codec given.
        codec: Codec,
    },

    /// A level outside the codec's range.
    #[error("{surface}: {codec} level {level} is outside {min}-{max}")]
    LevelOutOfRange {
        /// The config key.
        surface: &'static str,
        /// The codec given.
        codec: Codec,
        /// The level given.
        level: i32,
        /// The lowest level.
        min: i32,
        /// The highest level.
        max: i32,
    },
}

/// `compression`'s level against its codec, and against `surface` taking one.
fn check_level(
    surface: &'static str,
    compression: Compression,
    surface_takes_level: bool,
) -> Result<Compression, CompressionError> {
    let Some(level) = compression.level else {
        return Ok(compression);
    };
    let codec = compression.codec;
    match codec.levels() {
        Some(range) if surface_takes_level => {
            if range.contains(&level) {
                Ok(compression)
            } else {
                Err(CompressionError::LevelOutOfRange {
                    surface,
                    codec,
                    level,
                    min: *range.start(),
                    max: *range.end(),
                })
            }
        }
        _ => Err(CompressionError::NoLevel { surface, codec }),
    }
}

/// `codecs` by name, comma separated.
fn names(codecs: &[Codec]) -> String {
    codecs
        .iter()
        .map(|c| c.name())
        .collect::<Vec<&str>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_LEVELS: Accepts = Accepts::new("test.compression", &Codec::ALL);

    #[test]
    fn codec_constants_agree() {
        for codec in Codec::ALL {
            assert_eq!(Codec::from_tag(codec.tag()), Some(codec), "{codec}");
            assert_eq!(codec.name().parse::<Codec>(), Ok(codec), "{codec}");
            assert_eq!(
                codec.default_level().is_some(),
                codec.levels().is_some(),
                "{codec}"
            );
            if let (Some(level), Some(range)) = (codec.default_level(), codec.levels()) {
                assert!(range.contains(&level), "{codec}");
            }
            assert_eq!(codec.extension().is_none(), codec == Codec::None, "{codec}");
            assert_eq!(codec.feature().is_none(), codec == Codec::None, "{codec}");
        }
        assert_eq!(Codec::from_tag(5), None);
        assert_eq!("ZSTD".parse::<Codec>(), Ok(Codec::Zstd));
        assert!("brotli".parse::<Codec>().is_err());
        assert_eq!(Compression::default(), Compression::NONE);
        assert_eq!(Compression::new(Codec::Zstd).to_string(), "zstd(level=3)");
        assert_eq!(Compression::new(Codec::Lz4).to_string(), "lz4");
        assert_eq!(Compression::new(Codec::Gzip).effective_level(), Some(6));
        assert_eq!(Compression::new(Codec::Lz4).effective_level(), None);
    }

    /// The YAML shape, and the keys it refuses.
    #[cfg(feature = "yaml")]
    #[test]
    fn yaml_shape() {
        let rows: [(&str, Option<Compression>); 6] = [
            ("codec: zstd\n", Some(Compression::new(Codec::Zstd))),
            (
                "{codec: gzip, level: 9}\n",
                Some(Compression::new(Codec::Gzip).with_level(9)),
            ),
            ("codec: none\n", Some(Compression::NONE)),
            ("{codec: zstd, levl: 3}\n", None),
            ("codec: brotli\n", None),
            ("level: 3\n", None),
        ];
        for (yaml, want) in rows {
            let got: Option<Compression> =
                crate::yaml::from_str(yaml, "test", crate::yaml::Profile::Config).ok();
            assert_eq!(got, want, "{yaml}");
        }
    }

    /// Each refusal names the surface and what it takes.
    #[test]
    fn accepts_checks_codec_level_and_engine() {
        const GRPC: Accepts =
            Accepts::new("grpc.compression", &[Codec::None, Codec::Gzip, Codec::Zstd])
                .without_level();
        const KAFKA: Accepts = Accepts::new("kafka.compression", &Codec::ALL).external();
        let snappy_missing = (!cfg!(feature = "compression-snappy"))
            .then_some("test.compression: codec snappy needs scalo's `compression-snappy` feature");
        let rows: [(Accepts, Compression, Option<&str>); 9] = [
            (GRPC, Compression::new(Codec::None), None),
            (
                GRPC,
                Compression::new(Codec::Lz4),
                Some(
                    "grpc.compression: codec lz4 is not accepted here, use one of: none, gzip, zstd",
                ),
            ),
            (
                GRPC,
                Compression::new(Codec::Gzip).with_level(6),
                Some("grpc.compression: gzip takes no level here"),
            ),
            (
                ALL_LEVELS,
                Compression::new(Codec::Zstd).with_level(23),
                Some("test.compression: zstd level 23 is outside 1-22"),
            ),
            (
                ALL_LEVELS,
                Compression::new(Codec::Gzip).with_level(0),
                Some("test.compression: gzip level 0 is outside 1-9"),
            ),
            (
                ALL_LEVELS,
                Compression::new(Codec::Lz4).with_level(1),
                Some("test.compression: lz4 takes no level here"),
            ),
            (
                ALL_LEVELS,
                Compression::new(Codec::None).with_level(1),
                Some("test.compression: none takes no level here"),
            ),
            (KAFKA, Compression::new(Codec::Snappy), None),
            (ALL_LEVELS, Compression::new(Codec::Snappy), snappy_missing),
        ];
        for (accepts, compression, want) in rows {
            let got = accepts.check(compression).err().map(|e| e.to_string());
            assert_eq!(got.as_deref(), want, "{compression}");
        }
        assert!(
            Compression::new(Codec::Lz4)
                .with_level(1)
                .validate()
                .is_err()
        );
        assert!(
            Compression::new(Codec::Zstd)
                .with_level(22)
                .validate()
                .is_ok()
        );
    }

    #[cfg(feature = "config-schema")]
    #[test]
    fn schema_narrows_codec_and_level() {
        const HTTP: Accepts =
            Accepts::new("http.compression", &[Codec::None, Codec::Gzip, Codec::Zstd]);
        let schema = serde_json::to_value(HTTP.schema()).unwrap();
        let names = if cfg!(feature = "compression") {
            serde_json::json!(["none", "gzip", "zstd"])
        } else {
            serde_json::json!(["none"])
        };
        assert_eq!(schema["properties"]["codec"]["enum"], names);
        if cfg!(feature = "compression") {
            assert_eq!(schema["properties"]["level"]["minimum"], 1);
            assert_eq!(schema["properties"]["level"]["maximum"], 22);
        }
        let no_level = serde_json::to_value(HTTP.without_level().schema()).unwrap();
        assert!(no_level["properties"].get("level").is_none());
    }
}
