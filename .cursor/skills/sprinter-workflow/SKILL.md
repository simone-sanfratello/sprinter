---
name: sprinter-workflow
description: Maintains the sprinter Tokio task-queue crate—coordinator invariants, QueueError-based error handling, failure_injection tests, tarpaulin/clippy, and README/API alignment. Use when editing sprinter, fixing queue bugs, adding dedupe or wait helpers, running coverage, or auditing panics.
---

# Sprinter maintenance workflow

## Before changing behavior

1. Read `failure_injection` and coordinator loop in `src/lib.rs`: CAS on `tick`, no mutex across dedupe `await`, `Notify` for worker completion.
2. Confirm lifecycle: `set_push_done` required for session `Done`; document new stalls if API changes.

## After changing `src/lib.rs`

1. Run `cargo test` and `cargo clippy --all-targets`.
2. Run `cargo tarpaulin --config tarpaulin.toml` if touching execution paths or `failure_injection`.
3. New tests: add `let _s = crate::test_serial();` **unless** the test starts with `HookTestSession::new()`.

## Error-handling bar

- No `unwrap`/`expect`/`panic!` on recoverable paths in library code.
- Prefer `QueueError` and rollback (`index` / internal `queue`) where `_push` or `tick` fails after partial progress.

## Useful edge cases to preserve (regression fodder)

- `wait_for_tasks_done` only after `set_push_done` (timeouts without it are expected in tests).
- `set_push_done` without any `push` may never reach `Done` (coordinator never started).
- `Queue::new(0)` stalls execution.
- Duplicate `task_id` after a finished task until `reset`.
- `wait_for_results` map must not contain `MARKER_TASK_ID_PUSH_DONE` (`"#"`).

## Versioning

- Breaking public API → bump semver in `Cargo.toml` and sync `README.md` signatures.

## References

- Project rules: `.cursor/rules/sprinter*.mdc`
