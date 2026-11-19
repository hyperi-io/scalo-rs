// Project:   scalo
// File:      benches/delivery_path/args.rs
// Purpose:   Command line of the delivery-path bench
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

use std::path::PathBuf;
use std::time::Duration;

use scalo::transport::test_support::delivery::REGISTRY;

use crate::loads::Load;

pub const USAGE: &str = "\
usage: delivery_path [--approaches a,b] [--loads W1,W2] [--tier 1|2] [--seconds N]
                     [--runs N] [--seed N] [--save FILE]
                     [--compare FILE] [--max-regression F] [--calibrate]";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    One,
    Two,
}

impl Tier {
    pub fn number(self) -> u8 {
        match self {
            Self::One => 1,
            Self::Two => 2,
        }
    }
}

#[derive(Debug)]
pub struct Args {
    pub approaches: Vec<String>,
    pub loads: Vec<Load>,
    pub tier: Tier,
    pub seconds: f64,
    pub runs: usize,
    pub seed: u64,
    pub save: Option<PathBuf>,
    pub compare: Option<PathBuf>,
    pub max_regression: f64,
    pub calibrate: bool,
}

impl Args {
    pub fn parse(command_line: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut args = Self {
            approaches: REGISTRY.iter().map(|(n, _)| (*n).to_string()).collect(),
            loads: Load::ALL.to_vec(),
            tier: Tier::One,
            seconds: 5.0,
            runs: 2,
            seed: 1,
            save: None,
            compare: None,
            max_regression: 0.05,
            calibrate: false,
        };
        let mut words = command_line.peekable();
        while let Some(flag) = words.next() {
            let mut value = || words.next().ok_or_else(|| format!("{flag} needs a value"));
            match flag.as_str() {
                // cargo bench passes --bench to a harness = false target.
                "--bench" => {}
                "--approaches" => {
                    args.approaches = value()?.split(',').map(str::to_string).collect();
                    for name in &args.approaches {
                        if !REGISTRY.iter().any(|(n, _)| n == name) {
                            let known: Vec<&str> = REGISTRY.iter().map(|(n, _)| *n).collect();
                            return Err(format!("no approach {name}: known {}", known.join(",")));
                        }
                    }
                }
                "--loads" => {
                    args.loads = value()?
                        .split(',')
                        .map(|id| Load::parse(id).ok_or_else(|| format!("no load {id}")))
                        .collect::<Result<Vec<Load>, String>>()?;
                }
                "--tier" => {
                    args.tier = match value()?.as_str() {
                        "1" => Tier::One,
                        "2" => Tier::Two,
                        other => return Err(format!("--tier is 1 or 2, not {other}")),
                    };
                }
                "--seconds" => args.seconds = number(&value()?)?,
                "--runs" => args.runs = number(&value()?)?,
                "--seed" => args.seed = number(&value()?)?,
                "--save" => args.save = Some(PathBuf::from(value()?)),
                "--compare" => args.compare = Some(PathBuf::from(value()?)),
                "--max-regression" => args.max_regression = number(&value()?)?,
                "--calibrate" => args.calibrate = true,
                other => return Err(format!("unknown argument {other}")),
            }
        }
        if args.runs < 2 {
            return Err("--runs is at least 2, so every approach has a same-code spread".into());
        }
        if !(args.seconds > 0.0 && args.seconds.is_finite()) {
            return Err("--seconds must be positive".into());
        }
        Ok(args)
    }

    pub fn duration(&self) -> Duration {
        Duration::from_secs_f64(self.seconds)
    }
}

fn number<T: std::str::FromStr>(text: &str) -> Result<T, String> {
    text.parse().map_err(|_| format!("not a number: {text}"))
}
