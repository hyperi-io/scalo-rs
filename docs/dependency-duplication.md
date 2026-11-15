# Dependency duplication

`scripts/dep-dup-check.sh [features]` lists every crate that resolves at more than one version (`cargo tree -d --features <features> -e normal`, `full` by default) and prints the count. It never exits non-zero, and no CI job runs it: duplication is tracked, not gated, because most duplicates arrive transitively from mid-migration splits across the ecosystem.

A duplicate is worth acting on when one copy is a scalo direct dependency and the other arrives through a crate that has an upgrade path. Track it as an issue. A split whose copies both come from third-party crates (`syn`, `thiserror`, `rand`, `hashbrown` and the like) takes no action.

The AWS, Vault, TUI and git stacks sit behind `secrets-aws`, `secrets-vault`, `top` and `directory-config-git`, and must not appear in a default or transport-only build. Spot-check with `cargo tree --no-default-features --features transport-kafka`.
