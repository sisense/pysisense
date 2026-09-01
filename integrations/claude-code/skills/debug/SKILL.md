---
name: debug
description: Checks whether a reported bug actually happens, before any fix gets planned. Use this for requests shaped like a bug report, such as "X is broken", "this returns the wrong thing", or "check if Y actually happens". Skip it for new-feature or refactor requests, those go straight to `plan`. Normally run automatically by `orchestrator` as the first step; invoke it directly to just verify a suspected bug without starting a full fix cycle.
---

# debug: confirm it actually happens

Before planning a fix, reproduce the reported problem. Planning and implementing a fix for something that doesn't actually reproduce (a misdiagnosis, a stale report, a misunderstanding of expected behavior) wastes the whole downstream cycle and can produce a "fix" for the wrong thing.

## When this runs

Only for requests shaped like a bug report or fix: something is described as broken, wrong, failing, or the user asks to check whether a specific behavior actually happens. A new feature, a refactor, or an explicit "add X" request skips this stage entirely and goes straight to `plan`.

## What to do

1. **Reproduce it.** Write or run the smallest thing that demonstrates the reported behavior: a targeted `pytest` invocation against an existing test, a minimal ad-hoc script using the fake-injection pattern (never mock `SisenseClient`), or, if the user has a live environment, the exact call they described. If the report describes a return shape or error that doesn't match this repo's current source, check `pysisense.__version__` before assuming it's a bug, the reporter's environment may pin an older release. `docs/upgrading.md` has a symptom → cause → fix table for exactly this.
2. **Capture the actual result.** The real error message, traceback, or wrong return value, not a paraphrase.
3. **Compare against what was reported.** Does it match exactly, partially, or not at all?
4. **If it reproduces (fully or partially):** note the conditions under which it does (e.g. "only when the role was deleted after the user was created"), and a best-effort root-cause hypothesis (file/line) if one is apparent from the repro. This scopes `plan`. Don't fully diagnose the fix here, just enough to hand off a concrete, verified starting point. Treat this as a hypothesis, not a confirmed root cause, especially when the real failure point is an external system (a live Sisense server) whose exact behavior can't be verified from source alone.
5. **If it does not reproduce:** stop. Do not guess at a fix for a problem that isn't observably happening.

## Output

Write `.pysisense-dev/debug.md`:

- **Reported behavior**: the user's description, restated precisely.
- **Reproduction**: exact steps/command used.
- **Actual result**: what was actually observed.
- **Reproduced?**: yes / partially (with conditions) / no.
- **Root-cause hypothesis**: if apparent; otherwise say so explicitly rather than guessing.

## Handoff

- **Reproduced (fully or partially):** update `.pysisense-dev/cycle.json`: `current_stage: plan`, add `"debug"` to `stages_completed`, `debug_result: "reproduced"` or `"partial"`. `plan` reads `debug.md` next and scopes around the confirmed conditions instead of re-diagnosing from scratch.
- **Not reproduced:** update `cycle.json`: `status: needs_input`, `debug_result: "not_reproduced"`, `needs_input.reason` describing exactly what was tried and what happened instead. Halt. Ask the user for more detail (exact steps, environment, recent change) or confirmation before proceeding. Never fall through to `plan` on an unreproduced report.
