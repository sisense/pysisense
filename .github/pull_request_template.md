## Summary

<!-- What does this PR do, and why? -->

## Type of change

- [ ] `feat`: new feature
- [ ] `fix`: bug fix
- [ ] `refactor`: no feature/fix change
- [ ] `docs`: documentation only
- [ ] `test`: tests only
- [ ] `chore`: build, deps, config

## Checklist

- [ ] Branched from `dev`
- [ ] Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/#summary)
- [ ] `uv run ruff check .` and `uv run ruff format .` pass
- [ ] `uv run python tools/check_docstrings.py` passes
- [ ] `uv run pytest -m "not integration"` passes
- [ ] Public methods have NumPy-style docstrings with type hints
- [ ] No secrets, tokens, or credentials hardcoded or logged

### If a public method was added or changed

- [ ] Added/updated the mixin lookup table in `CLAUDE.md` and `.cursor/rules/project-overview.mdc`
- [ ] Added/updated a snippet in `examples/<module>_example.md`
- [ ] Added/updated the parameter table and return shape in `docs/<module>.md`

## Testing

<!-- How was this verified? Include commands run and, if applicable, integration test results. -->

## Related issues

<!-- Link related issues, e.g. Closes #123 -->
