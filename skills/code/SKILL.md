---
name: code
description: Implementation stage for a pysisense source change — writes the actual code to satisfy the tests from `build` and the scope from `plan`. Usually run by `orchestrator` right after `build`; use directly only when tests already exist and the user just wants the implementation written to match them.
---

# code — implementation

Write the implementation that makes `build`'s tests pass, within `plan`'s scope and `struct`'s impact analysis. This is the only stage that touches `pysisense/` source directly.

## Conventions (this repo's `CLAUDE.md` is authoritative — this is the checklist, not a restatement)

- Correct mixin file (per `plan.md`'s scope), no `__init__` in mixin files, no `SisenseClient` import in mixins, `from __future__ import annotations` at the top.
- Full type hints, builtin generics (`dict[str, Any]`, not `Dict`).
- NumPy-style docstring: summary, short description, `Parameters`, `Returns`, optionally `Raises`/`Notes`. No `Example`/`Examples` blocks, no mention of MCP/tools/LLMs/agents/external systems, only approved `(format: ...)` tags (`email`, `uuid`, `date`, `ipv4`, `ipv6`).
- Return `{"error": "..."}` on failure — don't raise, unless the module already raises consistently.
- Update/patch payloads: `exclude_unset=True, exclude_none=True`, never inject an implicit `groups: []`, resolve `role`/`groups` names to IDs only when explicitly provided, Pydantic model with `extra="forbid"`.
- OS-specific routing: Linux is the `else` branch, Windows is the `if self.api_client.operating_system == "windows":` conditional, log `os=` in the debug line, guard Linux-only methods with an explicit `{"error": "...not supported on Windows..."}`.
- Logging: `debug` for step-by-step decisions, `info` for success summaries, `error` for failures with status code + safe summary. Never log secrets; pass full payloads through `redact_secrets()` first if they must be logged.
- If `struct` recommended a deprecation path rather than delete-and-recreate, implement the deprecation (keep the old method, forward to the new one, note the deprecation in its docstring) — don't quietly delete anyway.

## Docs/examples sync — part of "done," not an afterthought

Per `plan.md`'s definition-of-done checklist: update `docs/<module>.md` (parameter table, return shape) and `examples/<module>_example.md` (a usage snippet — required for any new public method) in the **same** pass as the code change, even if the change looks internal-only. If a new method/module, also update the mixin lookup table in `CLAUDE.md` (and its mirror in `.cursor/rules/project-overview.mdc`).

Explicitly note, for each touched method, whether docs/examples needed an update and why or why not — don't skip the check itself just because a change seems purely internal.

## Handoff

Update `.pysisense-dev/cycle.json`: `code_files` list, `current_stage: rethink_code`. `rethink` runs next to check the implementation actually stayed inside scope.
