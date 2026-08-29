---
name: validate
description: Runs the test suite written in `build` against the implementation from `code`, and reports pass/fail into the pysisense dev-cycle shared state. Usually run by `orchestrator` right after `code`'s `rethink` gate clears; use directly to just re-run tests for a change already in progress.
---

# validate — run the tests

Run exactly the test(s) `build` wrote (plus the existing full unit suite, to catch regressions elsewhere), and record the result.

```bash
uv run pytest -m "not integration"                       # full unit suite
uv run pytest <path/to/new_or_changed_test_file.py> -v    # the specific test(s) from build
```

## On failure

Do not patch the test to match broken code, and do not silently rewrite the implementation without recording what changed. Update `.pysisense-dev/cycle.json`: `validate_result: fail`, and add a note describing the actual failure (assertion, traceback summary). Hand back to `code` — `orchestrator` re-enters the `code` stage, not `build`, unless the test itself was wrong (rare — that's a `build` mistake, not a `code` one, and should be called out explicitly if so).

## On success

Update `.pysisense-dev/cycle.json`: `validate_result: pass`, `current_stage: ci`.
