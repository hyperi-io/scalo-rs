// Project:   scalo
// File:      src/rules/eval.rs
// Purpose:   Evaluate a rule set against one record
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One pass per record: each target in order, its rows in declared order, the
//! first row that matches and resolves a value sets it. A row whose `from` or
//! `lookup` finds nothing does not match, and the next row is tried. A row for a
//! later target reads an earlier target's new value. The clamp runs last.

use std::borrow::Cow;
use std::collections::HashSet;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::Arc;

use cel::common::types::CelString;
use cel::common::value::CowVal;

use super::MAX_TARGETS;
use super::compile::{Action, Condition, FieldId, Matcher, RuleSet, Test};
use super::field::{FieldSource, Lookup};

/// A target field of one rule set, from [`RuleSet::target`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TargetId(usize);

/// The row that set a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuleRef<'a> {
    index: usize,
    id: &'a str,
}

impl<'a> RuleRef<'a> {
    /// The row's id.
    #[must_use]
    pub fn id(&self) -> &'a str {
        self.id
    }
}

/// The value a target ends with, and the row that set it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment<'a> {
    value: Cow<'a, str>,
    rule: Option<RuleRef<'a>>,
}

impl<'a> Assignment<'a> {
    /// The value to write into the target field.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }

    /// The value, owned where it was not borrowed.
    #[must_use]
    pub fn into_value(self) -> Cow<'a, str> {
        self.value
    }

    /// The row that set it, `None` when the clamp's default replaced the value.
    #[must_use]
    pub fn rule(&self) -> Option<RuleRef<'a>> {
        self.rule
    }
}

/// What the clamp did to its target.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClampOutcome<'a> {
    /// The final value was in the allowed set and stands.
    Kept,
    /// The final value was outside the allowed set and the default replaced it.
    Replaced {
        /// The value refused.
        was: Cow<'a, str>,
        /// The row that set it, `None` when the record carried it.
        rule: Option<RuleRef<'a>>,
    },
    /// No row set the target and the record had no value, so it took the default.
    Defaulted,
}

/// Per call: the values a target may end with, and the one it takes otherwise.
///
/// The caller keeps `default` inside `allowed`, as the clamp does not check it.
#[derive(Debug)]
pub struct Clamp<'a, S = RandomState> {
    target: TargetId,
    allowed: &'a HashSet<String, S>,
    default: &'a str,
}

impl<'a, S: BuildHasher> Clamp<'a, S> {
    /// Clamp `target` to `allowed`, replacing anything else with `default`.
    #[must_use]
    pub fn new(target: TargetId, allowed: &'a HashSet<String, S>, default: &'a str) -> Self {
        Self {
            target,
            allowed,
            default,
        }
    }
}

/// The values one record's targets end with.
#[derive(Clone)]
pub struct Outcome<'a> {
    set: &'a RuleSet,
    slots: [Option<Assignment<'a>>; MAX_TARGETS],
    clamp: Option<ClampOutcome<'a>>,
}

impl<'a> Outcome<'a> {
    /// The assignment to `target`, `None` when no row set it and the clamp left
    /// the record's own value.
    #[must_use]
    pub fn get(&self, target: TargetId) -> Option<&Assignment<'a>> {
        self.slots.get(target.0)?.as_ref()
    }

    /// The value assigned to `target`.
    #[must_use]
    pub fn value(&self, target: TargetId) -> Option<&str> {
        self.get(target).map(Assignment::value)
    }

    /// Each target field with its assignment, in evaluation order.
    pub fn iter(&self) -> impl Iterator<Item = (&'a str, &Assignment<'a>)> + '_ {
        let set = self.set;
        self.slots
            .iter()
            .zip(set.targets.iter())
            .filter_map(|(slot, target)| slot.as_ref().map(|a| (target.field.as_str(), a)))
    }

    /// What the clamp did, `None` when the call had no clamp.
    #[must_use]
    pub fn clamp(&self) -> Option<&ClampOutcome<'a>> {
        self.clamp.as_ref()
    }

    /// True when no target was assigned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.iter().all(Option::is_none)
    }
}

impl std::fmt::Debug for Outcome<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Outcome")
            .field("assignments", &self.iter().collect::<Vec<_>>())
            .field("clamp", &self.clamp)
            .finish()
    }
}

/// Counts what one batch of evaluations fired and adds it to the set's counters
/// once, at [`flush`](Self::flush) or drop, so threads evaluating in parallel
/// share no counter per record.
#[must_use = "a batch adds its counts to the rule set when it is flushed or dropped"]
pub struct Batch<'s> {
    set: &'s RuleSet,
    hits: Vec<u64>,
    /// One per rule, then one for values the record carried.
    clamped: Vec<u64>,
}

impl<'s> Batch<'s> {
    /// [`RuleSet::evaluate`], counted into this batch.
    pub fn evaluate<'r, R: FieldSource + ?Sized>(&mut self, record: &'r R) -> Outcome<'r>
    where
        's: 'r,
    {
        let set: &'r RuleSet = self.set;
        let outcome = set.run::<R, RandomState>(record, None);
        self.tally(&outcome);
        outcome
    }

    /// [`RuleSet::evaluate_clamped`], counted into this batch.
    pub fn evaluate_clamped<'r, R: FieldSource + ?Sized, S: BuildHasher>(
        &mut self,
        record: &'r R,
        clamp: &Clamp<'r, S>,
    ) -> Outcome<'r>
    where
        's: 'r,
    {
        let set: &'r RuleSet = self.set;
        let outcome = set.run(record, Some(clamp));
        self.tally(&outcome);
        outcome
    }

    fn tally(&mut self, outcome: &Outcome<'_>) {
        visit_counts(outcome, |count| match count {
            Count::Hit(rule) => self.hits[rule] += 1,
            Count::Clamped(rule) => self.clamped[rule.unwrap_or(self.hits.len())] += 1,
        });
    }

    /// Add the counts so far to the set's counters and start again from zero.
    pub fn flush(&mut self) {
        let records = self.hits.len();
        for (rule, n) in self.hits.iter_mut().enumerate() {
            if *n > 0 {
                self.set.usage.add_hits(rule, *n);
                *n = 0;
            }
        }
        for (slot, n) in self.clamped.iter_mut().enumerate() {
            if *n > 0 {
                self.set
                    .usage
                    .add_clamped((slot < records).then_some(slot), *n);
                *n = 0;
            }
        }
    }
}

impl Drop for Batch<'_> {
    fn drop(&mut self) {
        self.flush();
    }
}

enum Count {
    Hit(usize),
    /// `None` for a value the record carried.
    Clamped(Option<usize>),
}

/// Each count one outcome adds: a hit per row that fired, the clamp's refusal.
fn visit_counts(outcome: &Outcome<'_>, mut count: impl FnMut(Count)) {
    for slot in outcome.slots.iter().flatten() {
        if let Some(rule) = slot.rule {
            count(Count::Hit(rule.index));
        }
    }
    if let Some(ClampOutcome::Replaced { rule, .. }) = &outcome.clamp {
        if let Some(rule) = rule {
            count(Count::Hit(rule.index));
        }
        count(Count::Clamped(rule.map(|r| r.index)));
    }
}

/// One usage snapshot row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RuleUsage<'a> {
    /// The row's id.
    pub rule: &'a str,
    /// The target field it sets.
    pub target: &'a str,
    /// Records it set the target on, since the set compiled.
    pub hits: u64,
    /// Of those, records whose value the clamp replaced.
    pub clamped: u64,
}

impl RuleSet {
    /// The target field `field`, `None` when the set does not assign it.
    #[must_use]
    pub fn target(&self, field: &str) -> Option<TargetId> {
        self.targets
            .iter()
            .position(|t| t.field.as_str() == field)
            .map(TargetId)
    }

    /// The field a target assigns.
    #[must_use]
    pub fn target_field(&self, target: TargetId) -> Option<&str> {
        self.targets.get(target.0).map(|t| t.field.as_str())
    }

    /// Evaluate `record`. Each fired row's counter takes one shared atomic add,
    /// so for many records in parallel prefer a [`batch`](Self::batch).
    pub fn evaluate<'a, R: FieldSource + ?Sized>(&'a self, record: &'a R) -> Outcome<'a> {
        let outcome = self.run::<R, RandomState>(record, None);
        self.count_now(&outcome);
        outcome
    }

    /// Evaluate `record`, then hold the clamp's target to `clamp`.
    pub fn evaluate_clamped<'a, R: FieldSource + ?Sized, S: BuildHasher>(
        &'a self,
        record: &'a R,
        clamp: &Clamp<'a, S>,
    ) -> Outcome<'a> {
        let outcome = self.run(record, Some(clamp));
        self.count_now(&outcome);
        outcome
    }

    /// A batch that counts locally and adds to the set's counters once.
    pub fn batch(&self) -> Batch<'_> {
        Batch {
            set: self,
            hits: vec![0; self.rules.len()],
            clamped: vec![0; self.rules.len() + 1],
        }
    }

    /// Per row, in evaluation order: hits and clamps since the set compiled.
    #[must_use]
    pub fn usage(&self) -> Vec<RuleUsage<'_>> {
        self.rules
            .iter()
            .enumerate()
            .map(|(index, rule)| RuleUsage {
                rule: &rule.id,
                target: self.targets[rule.target].field.as_str(),
                hits: self.usage.hits(index),
                clamped: self.usage.clamped(Some(index)),
            })
            .collect()
    }

    /// Records whose own value the clamp replaced, which no row set.
    #[must_use]
    pub fn record_clamped(&self) -> u64 {
        self.usage.clamped(None)
    }

    fn count_now(&self, outcome: &Outcome<'_>) {
        visit_counts(outcome, |count| match count {
            Count::Hit(rule) => self.usage.add_hits(rule, 1),
            Count::Clamped(rule) => self.usage.add_clamped(rule, 1),
        });
    }

    pub(crate) fn run<'a, R: FieldSource + ?Sized, S: BuildHasher>(
        &'a self,
        record: &'a R,
        clamp: Option<&Clamp<'a, S>>,
    ) -> Outcome<'a> {
        let mut slots: [Option<Assignment<'a>>; MAX_TARGETS] = [const { None }; MAX_TARGETS];
        let mut cache = Cache::default();
        for (t, target) in self.targets.iter().enumerate() {
            let pass = Pass {
                set: self,
                target: t,
                record,
            };
            for index in target.rules.clone() {
                let rule = &self.rules[index];
                if !pass.holds(&rule.condition, &slots, &mut cache) {
                    continue;
                }
                if let Some(value) = pass.resolve(&rule.action, &slots, &mut cache) {
                    slots[t] = Some(Assignment {
                        value,
                        rule: Some(RuleRef {
                            index,
                            id: &rule.id,
                        }),
                    });
                    break;
                }
            }
        }
        let clamp = clamp.and_then(|clamp| self.clamp(record, clamp, &mut slots));
        Outcome {
            set: self,
            slots,
            clamp,
        }
    }

    fn clamp<'a, R: FieldSource + ?Sized, S: BuildHasher>(
        &'a self,
        record: &'a R,
        clamp: &Clamp<'a, S>,
        slots: &mut [Option<Assignment<'a>>; MAX_TARGETS],
    ) -> Option<ClampOutcome<'a>> {
        let target = self.targets.get(clamp.target.0)?;
        let slot = &mut slots[clamp.target.0];
        let current = match slot.as_ref() {
            Some(assigned) => Some((assigned.value.clone(), assigned.rule)),
            None => record
                .lookup(&target.field)
                .present_text()
                .map(|value| (value, None)),
        };
        let default = Some(Assignment {
            value: Cow::Borrowed(clamp.default),
            rule: None,
        });
        Some(match current {
            Some((value, _)) if clamp.allowed.contains(value.as_ref()) => ClampOutcome::Kept,
            Some((was, rule)) => {
                *slot = default;
                ClampOutcome::Replaced { was, rule }
            }
            None => {
                *slot = default;
                ClampOutcome::Defaulted
            }
        })
    }
}

/// Recent record reads, so rows testing the same field read it once.
#[derive(Default)]
struct Cache<'a> {
    entries: [Option<(FieldId, Lookup<'a>)>; 4],
    next: usize,
}

impl<'a> Cache<'a> {
    fn get_or(&mut self, field: FieldId, read: impl FnOnce() -> Lookup<'a>) -> Lookup<'a> {
        if let Some((_, found)) = self.entries.iter().flatten().find(|(id, _)| *id == field) {
            return found.clone();
        }
        let found = read();
        self.entries[self.next] = Some((field, found.clone()));
        self.next = (self.next + 1) % self.entries.len();
        found
    }
}

/// One target's pass over its rows.
struct Pass<'a, R: ?Sized> {
    set: &'a RuleSet,
    target: usize,
    record: &'a R,
}

impl<'a, R: FieldSource + ?Sized> Pass<'a, R> {
    /// The value at `field`: an earlier target's new value, else the record's.
    fn read(
        &self,
        field: FieldId,
        slots: &[Option<Assignment<'a>>],
        cache: &mut Cache<'a>,
    ) -> Lookup<'a> {
        let slot = &self.set.fields[field];
        if let Some(earlier) = slot.target.filter(|&i| i < self.target)
            && let Some(assigned) = &slots[earlier]
        {
            return Lookup::Text(assigned.value.clone());
        }
        let record: &'a R = self.record;
        cache.get_or(field, || record.lookup(&slot.path))
    }

    fn holds(
        &self,
        condition: &'a Condition,
        slots: &[Option<Assignment<'a>>],
        cache: &mut Cache<'a>,
    ) -> bool {
        match condition {
            Condition::Always => true,
            Condition::Native(matcher) => self.matches(matcher, slots, cache),
            Condition::Cel(program) => self.cel_holds(program, slots),
        }
    }

    fn matches(
        &self,
        matcher: &'a Matcher,
        slots: &[Option<Assignment<'a>>],
        cache: &mut Cache<'a>,
    ) -> bool {
        match matcher {
            Matcher::Leaf {
                field,
                test,
                negate,
            } => test_holds(test, &self.read(*field, slots, cache)) != *negate,
            Matcher::All { parts, negate } => {
                parts.iter().all(|part| self.matches(part, slots, cache)) != *negate
            }
        }
    }

    fn cel_holds(&self, program: &cel::Program, slots: &[Option<Assignment<'a>>]) -> bool {
        let Some(env) = &self.set.cel_env else {
            return false;
        };
        let resolver = Resolver { pass: self, slots };
        let mut context = cel::Context::with_env(Arc::clone(env));
        context.set_variable_resolver(&resolver);
        matches!(program.execute(&context), Ok(cel::Value::Bool(true)))
    }

    fn resolve(
        &self,
        action: &'a Action,
        slots: &[Option<Assignment<'a>>],
        cache: &mut Cache<'a>,
    ) -> Option<Cow<'a, str>> {
        match action {
            Action::Value(value) => Some(Cow::Borrowed(value)),
            Action::From(field) => self.read(*field, slots, cache).present_text(),
            Action::Lookup { table, from } => {
                let key = self.read(*from, slots, cache).present_text()?;
                self.set.tables[*table]
                    .get(key.as_ref())
                    .map(|value| Cow::Borrowed(value.as_str()))
            }
        }
    }
}

/// Feeds a CEL condition the record's fields and earlier targets' new values.
struct Resolver<'p, 'a, R: ?Sized> {
    pass: &'p Pass<'a, R>,
    slots: &'p [Option<Assignment<'a>>],
}

impl<R: FieldSource + ?Sized> cel::context::VariableResolver for Resolver<'_, '_, R> {
    fn resolve<'b>(&'b self, name: &str) -> Option<CowVal<'b, 'b>> {
        let earlier = self.pass.set.targets[..self.pass.target]
            .iter()
            .position(|t| t.field.as_str() == name);
        if let Some(assigned) = earlier.and_then(|i| self.slots[i].as_ref()) {
            return Some(CowVal::owned(CelString::from(assigned.value.as_ref())));
        }
        let value = self.pass.record.cel_value(name)?;
        value.try_into().ok().map(CowVal::Owned)
    }
}

fn test_holds(check: &Test, found: &Lookup<'_>) -> bool {
    if let Test::Exists = check {
        return match found {
            Lookup::Text(text) => !text.is_empty(),
            Lookup::Composite => true,
            Lookup::Missing | Lookup::Null => false,
        };
    }
    let Lookup::Text(text) = found else {
        return false;
    };
    match check {
        Test::Equals(value) => text.as_ref() == value.as_ref(),
        Test::StartsWith(value) => text.starts_with(value.as_ref()),
        Test::EndsWith(value) => text.ends_with(value.as_ref()),
        Test::Contains(finder) => finder.find(text.as_bytes()).is_some(),
        Test::PrefixAndSuffix { prefix, suffix } => {
            text.starts_with(prefix.as_ref()) && text.ends_with(suffix.as_ref())
        }
        Test::Length { min, max } => {
            let len = if text.is_ascii() {
                text.len()
            } else {
                text.chars().count()
            };
            (*min..=*max).contains(&len)
        }
        Test::Regex(matcher) => matcher.is_match(text.as_bytes()),
        Test::Exists => unreachable!("invariant: exists returned above"),
    }
}
