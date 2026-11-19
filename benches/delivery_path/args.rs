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
                     [--runs N] [--workers N] [--seed N] [--allocs] [--save FILE]
                     [--compare FILE] [--max-regression F]
                     [--cpus LIST] [--broker-cpus LIST] [--calibrate] [--order-check]";

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

#[derive(Debug, Clone)]
pub struct Args {
    pub approaches: Vec<String>,
    pub loads: Vec<Load>,
    pub tier: Tier,
    pub seconds: f64,
    pub runs: usize,
    pub workers: usize,
    pub seed: u64,
    pub allocs: bool,
    pub save: Option<PathBuf>,
    pub compare: Option<PathBuf>,
    pub max_regression: f64,
    pub calibrate: bool,
    pub order_check: bool,
    /// CPUs the runner and its cells are pinned to, as `taskset -c` writes them.
    pub cpus: Option<String>,
    /// The tier 2 broker container's cpuset, as `docker update --cpuset-cpus` takes it.
    pub broker_cpus: Option<String>,
    /// Set in a child process: run this one cell and print its JSON line.
    pub cell_run: Option<usize>,
    /// Set in a tier 2 child: the broker the parent started.
    pub bootstrap: Option<String>,
}

impl Args {
    pub fn parse(command_line: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut args = Self {
            approaches: REGISTRY.iter().map(|(n, _)| (*n).to_string()).collect(),
            loads: Load::ALL.to_vec(),
            tier: Tier::One,
            seconds: 5.0,
            runs: 5,
            workers: 8,
            seed: 1,
            allocs: false,
            save: None,
            compare: None,
            max_regression: 0.05,
            calibrate: false,
            order_check: false,
            cpus: None,
            broker_cpus: None,
            cell_run: None,
            bootstrap: None,
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
                "--workers" => args.workers = number(&value()?)?,
                "--seed" => args.seed = number(&value()?)?,
                "--allocs" => args.allocs = true,
                "--save" => args.save = Some(PathBuf::from(value()?)),
                "--compare" => args.compare = Some(PathBuf::from(value()?)),
                "--max-regression" => args.max_regression = number(&value()?)?,
                "--calibrate" => args.calibrate = true,
                "--order-check" => args.order_check = true,
                "--cpus" => args.cpus = Some(value()?),
                "--broker-cpus" => args.broker_cpus = Some(value()?),
                "--cell-run" => args.cell_run = Some(number(&value()?)?),
                "--bootstrap" => args.bootstrap = Some(value()?),
                other => return Err(format!("unknown argument {other}")),
            }
        }
        if args.runs < 2 {
            return Err("--runs is at least 2, so every approach has a same-code spread".into());
        }
        if args.workers == 0 {
            return Err("--workers is at least 1".into());
        }
        if let Some(list) = &args.cpus {
            let cpus = cpu_list(list)?;
            if args.workers > cpus.len() {
                return Err(format!(
                    "--workers {} is more than the {} CPUs in --cpus {list}",
                    args.workers,
                    cpus.len()
                ));
            }
        }
        if let Some(list) = &args.broker_cpus {
            cpu_list(list)?;
        }
        if !(args.seconds > 0.0 && args.seconds.is_finite()) {
            return Err("--seconds must be positive".into());
        }
        Ok(args)
    }

    pub fn duration(&self) -> Duration {
        Duration::from_secs_f64(self.seconds)
    }

    /// The command line of a child that runs one cell of this invocation.
    pub fn cell_command_line(&self, approach: &str, load: Load, run: usize) -> Vec<String> {
        let mut line = vec![
            "--approaches".to_string(),
            approach.to_string(),
            "--loads".to_string(),
            load.id().to_string(),
            "--tier".to_string(),
            self.tier.number().to_string(),
            "--seconds".to_string(),
            self.seconds.to_string(),
            "--workers".to_string(),
            self.workers.to_string(),
            "--seed".to_string(),
            self.seed.to_string(),
            "--cell-run".to_string(),
            run.to_string(),
        ];
        if self.allocs {
            line.push("--allocs".to_string());
        }
        if let Some(bootstrap) = &self.bootstrap {
            line.push("--bootstrap".to_string());
            line.push(bootstrap.clone());
        }
        line
    }
}

/// The CPUs a list like `16-23` or `0,2,4-7` names.
pub fn cpu_list(list: &str) -> Result<Vec<usize>, String> {
    let mut cpus = Vec::new();
    for part in list.split(',') {
        let (low, high) = part.split_once('-').unwrap_or((part, part));
        let low: usize = number(low.trim())?;
        let high: usize = number(high.trim())?;
        if low > high {
            return Err(format!("CPU range {part} runs backwards"));
        }
        cpus.extend(low..=high);
    }
    cpus.sort_unstable();
    cpus.dedup();
    Ok(cpus)
}

fn number<T: std::str::FromStr>(text: &str) -> Result<T, String> {
    text.parse().map_err(|_| format!("not a number: {text}"))
}
