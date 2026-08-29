---
name: ci
description: Emulates this repo's CI pipeline locally (lint, format, docstrings, tests) so a pysisense source change doesn't fail CI on first push. Usually run by `orchestrator` right after `validate` passes; use directly before pushing any manual change, even one made outside the dev cycle.
---

# ci — local CI emulation

Run the same checks this repo's CI pipeline runs, in the same order, before considering the cycle done:

```bash
uv run ruff check .                       # lint
uv run ruff format --check .              # format — also formats Python fences inside .md files (docs/examples/skills)
uv run python tools/check_docstrings.py   # docstring/type-hint conventions
uv run pytest -m "not integration"        # unit tests
```

If any of these fail:
- `ruff format --check` failures are almost always safe to auto-fix: `uv run ruff format .`, then re-run `--check` to confirm.
- `ruff check` failures need a real read — don't blanket `--fix` without checking what changed.
- Docstring checker failures point at the specific file/method; fix the docstring, don't suppress the check.
- Test failures send this back to `validate`/`code`, not forward.

## Output

Update `.pysisense-dev/cycle.json`: `ci_result: pass` or `fail` (with which check failed, if any), `current_stage: report` once all four pass clean.
