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

use crate::drive::Retry;
use crate::loads::Load;

pub const USAGE: &str = "\
usage: delivery_path [--approaches a[+retry][@control],b] [--retry none|bounded|hold]
                     [--control] [--loads W1,W2] [--tier 1|2] [--seconds N] [--runs N]
                     [--open-loop F,F] [--workers N] [--seed N] [--allocs] [--save FILE]
                     [--compare FILE] [--max-regression F]
                     [--cpus LIST] [--broker-cpus LIST] [--stop-file FILE] [--warmup SECONDS]
                     [--calibrate] [--order-check]";

/// Exit code of `--compare` when the run cannot be compared.
pub const INCONCLUSIVE: u8 = 3;

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

/// An approach under a retry policy, and the label its cells carry.
#[derive(Debug, Clone, PartialEq)]
pub struct Spec {
    pub name: String,
    pub retry: Retry,
    /// `name+retry`, with `@control` on a same-code control.
    pub label: String,
}

impl Spec {
    /// `name`, `name+retry` or either with `@control`, with `retry` when the
    /// text names none.
    pub fn parse(text: &str, retry: Retry) -> Result<Self, String> {
        let (body, control) = text
            .strip_suffix("@control")
            .map_or((text, false), |b| (b, true));
        let (name, retry) = match body.split_once('+') {
            Some((name, policy)) => (
                name,
                Retry::parse(policy).ok_or_else(|| format!("no retry policy {policy}"))?,
            ),
            None => (body, retry),
        };
        if !REGISTRY.iter().any(|(n, _)| *n == name) {
            let known: Vec<&str> = REGISTRY.iter().map(|(n, _)| *n).collect();
            return Err(format!("no approach {name}: known {}", known.join(",")));
        }
        let suffix = if control { "@control" } else { "" };
        Ok(Self {
            name: name.to_string(),
            retry,
            label: format!("{name}+{}{suffix}", retry.name()),
        })
    }
}

// Each bool is one command-line switch.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone)]
pub struct Args {
    pub approaches: Vec<Spec>,
    pub loads: Vec<Load>,
    pub tier: Tier,
    pub seconds: f64,
    pub runs: usize,
    pub workers: usize,
    pub seed: u64,
    pub allocs: bool,
    /// Offered rates for open-loop cells, as fractions of the slowest
    /// approach's closed-loop records per second under the same load.
    pub open_loop: Vec<f64>,
    pub save: Option<PathBuf>,
    pub compare: Option<PathBuf>,
    pub max_regression: f64,
    pub calibrate: bool,
    pub order_check: bool,
    /// The runner stops before the next cell once this file exists.
    pub stop_file: Option<PathBuf>,
    /// Seconds of a discarded cell before the measured ones.
    pub warmup: f64,
    /// CPUs the runner and its cells are pinned to, as `taskset -c` writes them.
    pub cpus: Option<String>,
    /// The tier 2 broker container's cpuset, as `docker update --cpuset-cpus` takes it.
    pub broker_cpus: Option<String>,
    /// Set in a child process: run this one cell and print its JSON line.
    pub cell_run: Option<usize>,
    /// Set in a child: the offered rate of an open-loop cell, records per second.
    pub rate: Option<f64>,
    /// Set in a child: the cell's drive, `closed` or `open@F`.
    pub mode: String,
    /// Set in a child: build the destinations, run no callers, report RSS.
    pub idle: bool,
    /// Set in a tier 2 child: the broker the parent started.
    pub bootstrap: Option<String>,
}

impl Args {
    pub fn parse(command_line: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut args = Self {
            approaches: Vec::new(),
            loads: Load::ALL.to_vec(),
            tier: Tier::One,
            seconds: 5.0,
            runs: 5,
            workers: 8,
            seed: 1,
            allocs: false,
            open_loop: Vec::new(),
            save: None,
            compare: None,
            max_regression: 0.05,
            calibrate: false,
            order_check: false,
            stop_file: None,
            warmup: -1.0,
            cpus: None,
            broker_cpus: None,
            cell_run: None,
            rate: None,
            mode: "closed".to_string(),
            idle: false,
            bootstrap: None,
        };
        let mut approaches: Option<String> = None;
        let mut retry = Retry::None;
        let mut control = false;
        let mut words = command_line.peekable();
        while let Some(flag) = words.next() {
            let mut value = || words.next().ok_or_else(|| format!("{flag} needs a value"));
            match flag.as_str() {
                // cargo bench passes --bench to a harness = false target.
                "--bench" => {}
                "--approaches" => approaches = Some(value()?),
                "--retry" => {
                    let policy = value()?;
                    retry =
                        Retry::parse(&policy).ok_or_else(|| format!("no retry policy {policy}"))?;
                }
                "--control" => control = true,
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
                "--open-loop" => {
                    args.open_loop = value()?.split(',').map(number::<f64>).collect::<Result<
                        Vec<f64>,
                        String,
                    >>(
                    )?;
                }
                "--save" => args.save = Some(PathBuf::from(value()?)),
                "--compare" => args.compare = Some(PathBuf::from(value()?)),
                "--max-regression" => args.max_regression = number(&value()?)?,
                "--calibrate" => args.calibrate = true,
                "--order-check" => args.order_check = true,
                "--warmup" => args.warmup = number(&value()?)?,
                "--stop-file" => args.stop_file = Some(PathBuf::from(value()?)),
                "--cpus" => args.cpus = Some(value()?),
                "--broker-cpus" => args.broker_cpus = Some(value()?),
                "--cell-run" => args.cell_run = Some(number(&value()?)?),
                "--rate" => args.rate = Some(number(&value()?)?),
                "--mode" => args.mode = value()?,
                "--idle" => args.idle = true,
                "--bootstrap" => args.bootstrap = Some(value()?),
                other => return Err(format!("unknown argument {other}")),
            }
        }
        args.approaches = match approaches {
            Some(list) => list
                .split(',')
                .map(|text| Spec::parse(text, retry))
                .collect::<Result<Vec<Spec>, String>>()?,
            None => REGISTRY
                .iter()
                .map(|(name, _)| Spec::parse(name, retry))
                .collect::<Result<Vec<Spec>, String>>()?,
        };
        if control && let Some(first) = args.approaches.first() {
            let mut twin = first.clone();
            twin.label.push_str("@control");
            args.approaches.push(twin);
        }
        if args.warmup < 0.0 {
            args.warmup = match args.tier {
                Tier::One => 0.0,
                Tier::Two => 20.0,
            };
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
        if args.open_loop.iter().any(|f| !(*f > 0.0 && f.is_finite())) {
            return Err("--open-loop fractions must be positive".into());
        }
        Ok(args)
    }

    pub fn duration(&self) -> Duration {
        Duration::from_secs_f64(self.seconds)
    }

    /// The modes every load runs: closed-loop, then each open-loop fraction.
    pub fn modes(&self) -> Vec<String> {
        let mut modes = vec!["closed".to_string()];
        modes.extend(self.open_loop.iter().map(|f| format!("open@{f}")));
        modes
    }

    /// The command line of a child that runs one cell of this invocation.
    pub fn cell_command_line(
        &self,
        spec: &Spec,
        load: Load,
        run: usize,
        mode: &str,
        rate: Option<f64>,
    ) -> Vec<String> {
        let mut line = vec![
            "--approaches".to_string(),
            spec.label.clone(),
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
            "--mode".to_string(),
            mode.to_string(),
        ];
        if let Some(rate) = rate {
            line.push("--rate".to_string());
            line.push(rate.to_string());
        }
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
