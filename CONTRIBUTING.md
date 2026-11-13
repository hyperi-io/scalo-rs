# Contributing

We welcome contributions to this project. By contributing, you agree to the
terms outlined below.

## Commit Message Format

HyperI projects use [Conventional Commits](https://www.conventionalcommits.org/)
and [semantic-release](https://semantic-release.gitbook.io/) for automated
versioning and changelog generation. All commits must follow this format:

```text
<type>(<scope>): <subject>

[optional body]

[optional footer(s)]
```

### Types

| Type | Description | Version Bump |
| ------ | ------------- | -------------- |
| `feat` | A new feature | Minor (0.X.0) |
| `fix` | A bug fix | Patch (0.0.X) |
| `docs` | Documentation only | None |
| `style` | Code style (formatting, semicolons, etc.) | None |
| `refactor` | Code change that neither fixes a bug nor adds a feature | None |
| `perf` | Performance improvement | Patch (0.0.X) |
| `test` | Adding or correcting tests | None |
| `build` | Changes to build system or dependencies | None |
| `ci` | Changes to CI configuration | None |
| `chore` | Other changes that don't modify src or test files | None |
| `revert` | Reverts a previous commit | Varies |

### Breaking Changes

For breaking changes that require a major version bump, add `!` after the type
or include `BREAKING CHANGE:` in the footer:

```text
feat!: remove deprecated API endpoints

BREAKING CHANGE: The /v1/users endpoint has been removed. Use /v2/users instead.
```

### Examples

```text
feat(auth): add OAuth2 support for Google login

fix(api): handle null response from upstream service

docs: update installation instructions for Windows

refactor(core)!: restructure module exports

BREAKING CHANGE: Named exports are now used instead of default exports.
```

### Scope

Scope is optional but recommended. Use it to indicate the area of the codebase
affected (e.g., `api`, `auth`, `core`, `cli`, `docs`).

## Semantic Versioning

This project follows [Semantic Versioning 2.0.0](https://semver.org/):

- **MAJOR** (X.0.0): Breaking changes that require users to modify their code
- **MINOR** (0.X.0): New features that are backwards-compatible
- **PATCH** (0.0.X): Bug fixes and minor improvements

Versions are automatically determined by semantic-release based on commit
messages. Do not manually update version numbers.

## Developer Certificate of Origin

This project uses the Developer Certificate of Origin (DCO) to ensure that
contributors have the right to submit their contributions.

By making a contribution to this project, you certify that:

1. The contribution was created in whole or in part by you and you have the
   right to submit it under the license indicated in the file; or

2. The contribution is based upon previous work that, to the best of your
   knowledge, is covered under an appropriate open source license and you
   have the right under that license to submit that work with modifications;
   or

3. The contribution was provided directly to me by some other person who
   certified (1), (2) or (3) and you have not modified it.

4. You understand and agree that this project and the contribution are public
   and that a record of the contribution (including all personal information
   you submit with it, including your sign-off) is maintained indefinitely
   and may be redistributed consistent with this project or the license(s)
   involved.

## How to Sign Off Your Commits

You must sign off each commit to indicate your acceptance of the DCO. Combine
the signoff with your conventional commit message:

```bash
git commit --signoff -m "feat(auth): add two-factor authentication"
```

This produces:

```text
feat(auth): add two-factor authentication

Signed-off-by: Your Name <your.email@example.com>
```

Make sure your Git configuration has your correct name and email:

```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

## License for Contributions

All contributions to this project are licensed under the Apache License,
Version 2.0, the same license that covers the project.

## How to Contribute

1. **Fork the repository** and create your branch from `main`
2. **Make your changes** following the commit message format above
3. **Sign off your commits** with the DCO
4. **Test your changes** to ensure they work as expected
5. **Submit a pull request** with a clear description of what you've done

### Pull Request Checklist

- [ ] Commits follow the conventional commit format
- [ ] All commits are signed off (DCO)
- [ ] Tests pass (if applicable)
- [ ] Documentation is updated (if applicable)

## Run the checks locally before you push

HyperI projects gate every push through CI (lint, format, tests, secret scan,
dependency audit, build). Run the SAME checks locally first so your change lands
green instead of bouncing:

```bash
hyperi-ci check
```

`hyperi-ci` is the HyperI CI CLI (public on PyPI). If you do not have it, install
it once with `uv tool install hyperi-ci` (or `pipx install hyperi-ci`), or run it
ad hoc with `uvx hyperi-ci check`. It runs the project's full local validation --
the same suite CI runs -- and reports what to fix. Run it before every push and before opening a
pull request. It is the single best way to give your change the best chance of
surviving CI.

To run the tests by hand, use `cargo nextest run --all-features`, not plain `cargo test`. The suite shares process-global state (the config cascade, the metrics recorder, the health registry), so it needs nextest's one process per test, and under `cargo test` tests fail for that reason alone.

## CI/CD Workflow

When your pull request is merged to `main`:

1. **semantic-release** analyses commit messages since the last release
2. Determines the next version number based on commit types
3. Generates/updates the CHANGELOG
4. Creates a new GitHub release with release notes
5. Publishes the package (if applicable)

This happens automatically - no manual intervention required.

## Questions

If you have questions about contributing, please open an issue or contact us.

## Context

### What this is

HyperI's shared Rust library -- config cascade, logging, metrics, health,
self-regulation, transports, spool and DLQ, secrets and the deployment-contract
generators -- published as `scalo` on crates.io under Apache-2.0.

It is scalo-py's SISTER, not its twin: same conventions, DIFFERENT API, separate
repo. Never assume parity.

### Where things live

| Path | What it holds |
| ------ | --------------- |
| `src/<module>/` | One directory per module, gated by the feature of the same name |
| `docs/core-pillars/` | config, logging, metrics, health, tracing, shutdown, lifecycle |
| `docs/pipeline/` | spool, DLQ, tiered sink, worker pool, batch engine, scaling |
| `benches/` | Criterion benches -- config, logger, engine, strmatch, auth, loadgen |
| `docs/architecture.md` | Layering, the crate dependency graph, and what scalo-py has that this does not |

### Commands that prove a change

```bash
make quality   # fmt, clippy, audit
make test      # the suite
make bench     # criterion
```

Read the per-job CI result, never a local summary.

### What tends to bite

| Don't | Do | Why |
| ------- | ---- | ----- |
| Declare rustflags under `[build]` | Put them under `[target.<triple>]` | The ARC pod sets `CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS`, which counts as a target entry, and cargo reads `build.rustflags` ONLY when no target entry exists. A committed, correct-looking config.toml ships without its flags and nothing fails |
| Negate a file inside an excluded directory in `.gitignore` | Exclude with `.cargo/*` so the negation can apply | `.cargo/` excludes the directory, so `!.cargo/config.toml` is inert and the file never commits |
| Read an instruction count as proof a target-cpu applied | Compare VEX to legacy-SSE encodings within one binary | Crates with runtime dispatch compile AVX2 paths whatever `target-cpu` says, so a raw BMI2 or ymm count measures what the binary LINKS, not what the compiler was told |

### Where this sits

Generated from `dfe-infra/suite.yaml` via `dfe-stack suite`. Seven DFE Rust
consumers declare this crate: receiver, loader, fetcher, archiver, transform-vrl,
transform-vector and transform-elastic.
