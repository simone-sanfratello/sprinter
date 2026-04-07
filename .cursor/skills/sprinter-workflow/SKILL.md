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
- **Commit style:** Prefer [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, breaking footers / `!`) so release automation can pick the next semver via [git-cliff](https://git-cliff.org/) (`cliff.toml`).

## Releases and crates.io (GitHub Actions)

### GitHub repository settings (required for *Release Prepare*)

If **Release Prepare** fails on **Commit and push release branch** or **Open pull request**, check:

1. **Settings → Actions → General → Workflow permissions** — set **Read and write permissions** (not “Read repository contents and packages”). Read-only defaults break `git push` for `release/v*` branches and `git push` for baseline tags.
2. On the same page, enable **Allow GitHub Actions to create and approve pull requests**. Without this, `gh pr create` often returns `Resource not accessible by integration`.
3. **Rulesets / branch protection** — ensure pushes from **GitHub Actions** (or the default `GITHUB_TOKEN`) are not blocked for the actions that need to push branches and tags. If your org forbids it, use a **fine-grained or classic PAT** with `contents: write` and `pull-requests: write`, stored as a repository secret (e.g. `RELEASE_AUTOMATION_TOKEN`), and pass it to `actions/checkout` (`token: ${{ secrets.RELEASE_AUTOMATION_TOKEN }}`) and set `GH_TOKEN` / `GITHUB_TOKEN` in the push/PR steps to that secret.

### Workflows

Three workflows in `.github/workflows/`:

1. **`release-prepare.yml`** — *Release (prepare branch)* — `workflow_dispatch` with bump `auto|patch|minor|major`. If no `v*` tag exists, the workflow creates and pushes **`v<Cargo.toml version>`** at the commit that introduced that `version = "…"` line (`git log -S`). **Writes `CHANGELOG.md` via git-cliff** (do not keep a hand-authored copy on `main`), bumps `Cargo.toml` with `cargo set-version`, pushes `release/vX.Y.Z` and opens a PR to `main`. If that release branch **already exists** (e.g. a previous run pushed it but failed later), the workflow **resets** it from the current default branch and **`git push --force-with-lease`** so you can re-run without deleting the branch manually.
2. **`release-finalize.yml`** — *Release (tag + GitHub Release)* — runs when a PR **from** `release/v*` **into the default branch** is **merged** (same-repo only). Creates the `vX.Y.Z` tag on the merge commit and a GitHub Release with notes sliced from the `CHANGELOG.md` introduced by that merge.
3. **`deploy-crates.yml`** — *Deploy (crates.io)* — `workflow_dispatch`; optional `tag` input (default: latest `v*` by sort). Runs `cargo publish --locked`. Needs repo secret **`CARGO_REGISTRY_TOKEN`** ([crates.io token](https://crates.io/settings/tokens)).

**Baseline tag:** If that automatic `-S` lookup fails (no such commit), create `v<Cargo.toml version>` on the correct commit manually and push it before running *Release Prepare* again.

## References

- Project rules: `.cursor/rules/sprinter*.mdc`
