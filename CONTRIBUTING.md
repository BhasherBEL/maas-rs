# Contributing

Personal R&D project; Issues and discussion welcome, PRs will be reviewed (but no timeline, as this is a solo-maintainer project).

The routing and design philosophy described in the [README](README.md) is non-negotiable: surfacing multiple options rather than one answer, multi-objective ranking without a single blended score, and reliability as a first-class criterion. Changes that quietly collapse the planner back into a single-answer engine are out of scope.

A few practical notes:

- Open an issue before a large change, so we can agree on direction first.
- Keep tunable constants in `config.yaml`, not hardcoded (see `AGENTS.md`).
- Every behaviour change should come with tests; run `cargo test` and `cargo clippy` before opening a PR.
