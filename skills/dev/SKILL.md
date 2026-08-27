---
name: dev
description: Use when the user wants to add, fix, or refactor a method inside the pysisense package itself (not write a script that calls it) — e.g. "add a new method to Dashboard", "fix a bug in access_management/users.py", "add a new pysisense module", "the docstring checker is failing", or any request that touches a file under `pysisense/<module>/*.py`. Not for writing automation scripts that use pysisense as a library — that's the `script` skill.
---

# Contributing to pysisense itself

This skill is for editing **pysisense's own source** — the SDK, not scripts that call it. `CLAUDE.md` at this repo's root is the authoritative source for every convention referenced below (mixin pattern, docstring format, PATCH safety, Pydantic patterns, logging/secrets policy, OS-routing, testing approach, naming). It's already auto-loaded as project instructions whenever Claude works in this repo, so this skill doesn't restate it — it's a workflow checklist on top of it, covering the steps that are easy to skip under time pressure.

If you land here but the actual task is "write a script using pysisense," switch to the `script` skill instead — that's a different audience (script authors, not SDK maintainers) with different conventions (comments are welcome, dry-run safety matters, project scaffolding applies).

## Workflow: adding or changing a public method

1. **Find the right mixin file.** Use the mixin lookup table in `CLAUDE.md` (per-package, per-file, per-method). If the table doesn't have it — the table has known staleness in at least one spot (some `access_management` methods are misattributed to `custom_code`) — grep `pysisense/<module>/*.py` directly rather than trusting the table blindly.
2. **Write the method** following `CLAUDE.md`'s conventions:
   - Full type hints, builtin generics (`dict[str, Any]`, not `Dict`).
   - NumPy-style docstring: summary, short description, `Parameters`, `Returns`, optionally `Raises`/`Notes`. No `Example`/`Examples` blocks, no mention of MCP/tools/LLMs/agents/external systems, only approved `(format: ...)` tags (`email`, `uuid`, `date`, `ipv4`, `ipv6`).
   - Return `{"error": "..."}` on failure — don't raise, unless the module already raises consistently.
   - If it's an update/patch payload: PATCH safety rules apply — `exclude_unset=True, exclude_none=True`, never inject an implicit `groups: []`, resolve `role`/`groups` names to IDs only when explicitly provided (fail fast with a clear error if resolution fails), Pydantic model with `extra="forbid"`.
   - If OS-specific: Linux is the `else` branch, Windows is the `if self.api_client.operating_system == "windows":` conditional, log `os=` in the debug line, and guard Linux-only methods with an explicit `{"error": "...not supported on Windows..."}` rather than silently hitting the wrong endpoint.
   - Logging: `debug` for step-by-step decisions/endpoints/counts, `info` for success summaries, `error` for failures with status code + safe summary. Never log tokens/passwords/secrets — pass full payloads through `redact_secrets()` first if they must be logged.
   - No `SisenseClient` import in the mixin file; no `__init__`; start the file with `from __future__ import annotations` if it doesn't already.
3. **Run the docstring checker on the touched file(s)** — this is required, not optional:
   ```bash
   uv run python tools/check_docstrings.py pysisense/<module>/<file>.py
   ```
4. **Lint and format:**
   ```bash
   uv run ruff check --fix pysisense/<module>/<file>.py
   uv run ruff format pysisense/<module>/<file>.py
   ```
5. **Add or extend a unit test** in `tests/unit/`, using the fake-injection pattern — never mock `SisenseClient` itself; inject a fake `api_client` with fixture dicts inline in the test file.
6. **Docs/examples sync — do this every time a method is touched, even if it looks like nothing changed.** For each touched method, explicitly check and note whether it needs an update, and why or why not — don't skip the check itself just because the change seems purely internal (e.g. deduplicating a helper) with no visible output change:
   - `docs/<module>.md` — parameter table and return shape.
   - `examples/<module>_example.md` — add or update a usage snippet. This is not optional for a new public method; it's the first place users look.
7. **New method or new module?** Also update the mixin lookup table in `CLAUDE.md` (and its mirror in `.cursor/rules/project-overview.mdc`). A new module additionally needs: an entry in the `CLAUDE.md` Modules table, a class in the canonical init pattern if it's top-level, and its own `docs/<module>.md` + `examples/<module>_example.md`.
8. **Commit**: conventional commits (`feat`/`fix`/`refactor`/`docs`/`test`/`chore`/`perf`), present-tense imperative (`add user validation`, not `added`). Branch from `dev`, not `main`.

## Classifying a behavior change as breaking

When a method's return value changes shape (de-duplication, consistency fixes, refactors — not just new features):

- **More information than before** (an extra field, a previously-unresolved value now resolved, a record that used to be silently dropped now included) → safe to apply directly, no separate sign-off needed.
- **Less information than before** (a field that used to carry a real value now comes back `None`/missing, a fallback that used to preserve something now loses it, a record that used to appear is now silently skipped) → treat as a breaking change. Flag it explicitly and hold for the user's decision — never apply it silently, even if it looks like a minor edge case.

## Quality bar

- Preserve existing behavior unless the task is explicitly a bug fix.
- Prefer small, safe refactors over rewrites; match the existing style/naming/logging pattern of the module you're editing.
- Don't introduce cross-module logic unless explicitly requested; use `_`-prefixed helpers for internal shared logic, never exposed on the public class.
- Public SDK methods are a stability contract — treat signature/behavior changes with more care than an internal helper.

## Quick command reference

```bash
uv sync --dev                                  # install dev deps
uv run pytest -m "not integration"             # unit tests
uv run pytest tests/unit/unit_test_x.py -k name  # one test
uv run ruff check .                            # lint
uv run ruff check --fix .                      # lint + autofix
uv run ruff format .                           # format
uv run python tools/check_docstrings.py        # docstring check, whole package
```
