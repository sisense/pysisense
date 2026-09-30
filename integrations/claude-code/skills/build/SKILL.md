---
name: build
description: Writes the tests for a pysisense change before any implementation exists, so "passing" is defined up front. Runs after `plan` and `struct`, as part of `orchestrator`'s cycle. Invoke it directly when the user explicitly wants tests written first, ahead of implementation.
---

# build: tests first

Write the test(s) that define success for this change, before touching implementation code. Read `.pysisense-dev/plan.md` and `.pysisense-dev/struct.md` first. The test(s) must cover exactly what `plan`'s definition-of-done and `struct`'s affected-files list describe, nothing more or less.

## Conventions (from this repo's testing rules)

- Tests live in `tests/unit/`, one file per module area, e.g. `tests/unit/unit_test_wellcheck.py`.
- **Never mock `SisenseClient`.** Inject a fake `api_client`:
  ```python
  class FakeApiClient:
      def get(self, url, **kwargs):
          return self._fixture_data.get(url, {})
  ```
- Keep fixture dicts inline in the test file. Don't load from external JSON.
- For a changed method (not a new one), extend the existing test file/function rather than duplicating it, unless the existing test would otherwise conflate two behaviors.
- If `struct` classified the change as breaking, the test suite must include a case that pins down the *new* documented behavior explicitly (so it can't silently regress again), not just a happy-path check.
- Integration tests (`@pytest.mark.integration`) are out of scope here unless the user is explicitly working against a live Sisense instance. Default to unit tests only.

## Output

The new/updated test file(s), plus an update to `.pysisense-dev/cycle.json`: append the file path(s) to `test_files`, `current_stage: code`.

At this point the tests should **fail** (or not exist as passing). There's no implementation yet. That's expected; `validate` is what confirms pass/fail later, not this stage.
