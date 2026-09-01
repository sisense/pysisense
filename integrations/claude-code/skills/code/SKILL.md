---
name: code
description: Writes the actual implementation for a pysisense source change, to satisfy the tests from `build` and the scope from `plan`. Runs right after `build`, as part of `orchestrator`'s cycle. Invoke it directly only when tests already exist and the user just wants matching code written.
---

# code: implementation

Write the implementation that makes `build`'s tests pass, within `plan`'s scope and `struct`'s impact analysis. This is the only stage that touches `pysisense/` source directly.

## Conventions (this repo's `CLAUDE.md` is authoritative; this is the checklist, not a restatement)

- Correct mixin file (per `plan.md`'s scope), no `__init__` in mixin files, no `SisenseClient` import in mixins, `from __future__ import annotations` at the top.
- Full type hints, builtin generics (`dict[str, Any]`, not `Dict`). Enum-valued string params use `Literal[...]` (e.g. `Literal["extract", "live"]`), not a bare `str`.
- NumPy-style docstring: summary, short description, `Parameters`, `Returns`, optionally `Raises`/`Notes`. No `Example`/`Examples` blocks, no mention of MCP/tools/LLMs/agents/external systems, only approved `(format: ...)` tags (`email`, `uuid`, `date`, `ipv4`, `ipv6`).
- Return `{"ok": False, "error": "...", "status_code": <int, when available>}` on failure, never raise, unless the module already raises consistently. Build HTTP failures via `_extract_error_message()` (`pysisense/utils.py`) only; a hand-built validation/not-found failure must still carry the `"ok": False` marker explicitly.
- Never bracket-access a value from an API response (`data["script"]`). The Sisense API omits keys entirely instead of sending them empty. Use `.get(...)` with an explicit fallback, and don't assume an error body is JSON, some Sisense error responses are HTML.
- If the method exists in both live and extract flavors, verify both against real behavior, not just one and an assumption about the other. They routinely differ in path, identifier (title vs oid), and required fields.
- New method taking a dict payload: plain named parameters by default. Only use a dict when it mirrors an API body 1:1, has PATCH "send only what changes" semantics, or its fields vary by a discriminator (e.g. per-provider connection params). Every dict payload needs a TypedDict contract in `pysisense/payloads.py` (two-class inheritance: a `total=True` base for required keys, a `total=False` subclass for optional ones), unless it's genuinely free-form (JAQL, Blox JSON, metadata queries, encryption bodies), in which case say so in the docstring instead.
- Renaming a method: keep the old name as a deprecated alias for one minor version, decorated with `@typing_extensions.deprecated("use <new_name>")`, plus a one-line deprecation note in the docstring for humans. Never delete the old name outright in the same release as the rename.
- New top-level SDK class: add it to `pysisense.FACADES`, not just `__all__`.
- Update/patch payloads: `exclude_unset=True, exclude_none=True`, never inject an implicit `groups: []`, resolve `role`/`groups` names to IDs only when explicitly provided, Pydantic model with `extra="forbid"`.
- OS-specific routing: Linux is the `else` branch, Windows is the `if self.api_client.operating_system == "windows":` conditional, log `os=` in the debug line, guard Linux-only methods with an explicit failure dict rather than silently hitting the wrong endpoint.
- Logging: `debug` for step-by-step decisions, `info` for success summaries, `error` for failures with status code + safe summary. Never log secrets; pass full payloads through `redact_secrets()` first if they must be logged.
- If `struct` recommended a deprecation path rather than delete-and-recreate, implement the deprecation (keep the old method, forward to the new one, note the deprecation in its docstring). Don't quietly delete anyway.
- Breaking change of any kind: add an entry to `CHANGELOG.md` and `docs/upgrading.md` in the same pass, not just a commit message. Neither users nor a future assistant can see a shape change that's only recorded in git history.

## Docs/examples sync: part of "done," not an afterthought

Per `plan.md`'s definition-of-done checklist: update `docs/<module>.md` (parameter table, return shape) and `examples/<module>_example.md` (a usage snippet, required for any new public method) in the **same** pass as the code change, even if the change looks internal-only. If a new method/module, also update the mixin lookup table in `CLAUDE.md` (and its mirror in `.cursor/rules/project-overview.mdc`).

Explicitly note, for each touched method, whether docs/examples needed an update and why or why not. Don't skip the check itself just because a change seems purely internal.

## Handoff

Update `.pysisense-dev/cycle.json`: `code_files` list, `current_stage: rethink_code`. `rethink` runs next to check the implementation actually stayed inside scope.
