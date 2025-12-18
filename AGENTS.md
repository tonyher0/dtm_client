<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

# Repository Guidelines

## Project Structure & Module Organization

- `Cargo.toml`: crate metadata and dependencies (Rust edition 2024).
- `src/lib.rs`: library entry point and public API surface.
- Unit tests live inline under `#[cfg(test)] mod tests { ... }` in the module they cover.
- (Optional) Integration tests belong in `tests/*.rs` when you need black-box tests against the public API.

## Build, Test, and Development Commands

- `cargo build`: compile the crate.
- `cargo test`: run unit/integration tests.
- `cargo fmt`: format code using `rustfmt`.
- `cargo clippy -- -D warnings`: run linting and fail on warnings.
- `cargo doc --open`: generate and view API docs locally.

## Coding Style & Naming Conventions

- Formatting: use `cargo fmt` (do not hand-format). Indentation is rustfmt-default (4 spaces).
- Naming:
  - `snake_case` for modules, functions, variables, and files (e.g., `dtm_client.rs`, `parse_response`).
  - `CamelCase` for types/traits (e.g., `ClientConfig`).
  - Prefer explicit, domain-oriented names over abbreviations.
- Error handling: prefer `Result<T, E>` with meaningful error types/messages; avoid `unwrap()` outside tests.

## Testing Guidelines

- Framework: Rust built-in test harness (`#[test]`).
- Keep tests deterministic and fast; avoid network access by default.
- Conventions:
  - Unit tests: `mod tests` near the code under test.
  - Integration tests: `tests/<feature>_test.rs` with descriptive test names.

## Commit & Pull Request Guidelines

- Git history may be empty; use a consistent convention going forward:
  - Prefer Conventional Commits: `feat: ...`, `fix: ...`, `chore: ...`, `docs: ...`, `test: ...`.
- PRs should include:
  - Clear description and rationale, plus linked issue/ticket if applicable.
  - Test coverage updates (or an explanation if not applicable).
  - Evidence of local checks: `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test`.

## Agent-Specific Notes

- Keep changes minimal and scoped; avoid introducing new dependencies unless required.
- If adding new modules, expose only what’s needed via `src/lib.rs` and keep public API intentional.
