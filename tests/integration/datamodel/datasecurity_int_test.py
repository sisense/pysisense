"""Integration tests for datasecurity write paths against a live Sisense instance.

Live-verified findings these encode (2026-08 sandbox, L2025.x):
- the extract write route is POST (PUT does not exist — HTML 404)
- writes require the cube to be BUILT/RUNNING (extract) or PUBLISHED (live);
  draft models fail ("[object Object]" / "Elasticube has not been found")
- rules must carry allMembers (+ live/fullname for live models); members must
  be a list of strings; server-managed fields are rejected on write

Local-only: requires config.yaml (gitignored); never runs in CI. Both tests
are self-cleaning lifecycles: add one behavior-neutral rule (allMembers=True,
default share — same visibility as having no rule) to a fresh column, verify
it, delete it via delete_datasecurity, and verify the model is back to its
original state.
"""

import os

import pytest

from pysisense.datamodel import DataModel
from pysisense.sisenseclient import SisenseClient

CONFIG_PATH = os.environ.get("PYSISENSE_CONFIG_PATH", "config.yaml")


def _make_datamodel() -> DataModel:
    if not os.path.exists(CONFIG_PATH):
        pytest.skip(f"Config file '{CONFIG_PATH}' not found; skipping integration test.")
    return DataModel(api_client=SisenseClient(config_file=CONFIG_PATH, debug=False))


def _find_model(dm: DataModel, wanted_type: str, wanted_statuses: set[str]) -> dict:
    models = dm.get_all_datamodel()
    if not isinstance(models, list):
        pytest.skip(f"Could not list datamodels: {models}")
    match = next(
        (m for m in models if str(m.get("type", "")).lower() == wanted_type and str(m.get("status", "")).lower() in wanted_statuses),
        None,
    )
    if match is None:
        pytest.skip(f"No {wanted_type.upper()} datamodel with status in {sorted(wanted_statuses)} found (writes require a built/published model).")
    return match


def _fresh_column(dm: DataModel, title: str) -> tuple[str, str, str]:
    """Pick a column that has no existing datasecurity rule."""
    schema = dm.get_model_schema(title)
    if not isinstance(schema, list) or not schema:
        pytest.skip(f"Could not read schema for '{title}': {schema}")
    ruled = {(r.get("table"), r.get("column")) for r in dm.get_datasecurity_raw(title) if isinstance(r, dict)}
    for row in schema:
        if (row["table_name"], row["column_name"]) not in ruled:
            ctype = str(row.get("column_type", "text")).lower()
            datatype = "numeric" if ctype in ("integer", "double", "bigint", "decimal", "float", "real") else "datetime" if ctype == "datetime" else "text"
            return row["table_name"], row["column_name"], datatype
    pytest.skip(f"Every column of '{title}' already has a datasecurity rule.")


def _neutral_rule(table: str, column: str, datatype: str) -> dict:
    # allMembers=True + default share = everyone sees everything, which is the
    # same visibility as having no rule at all — behavior-neutral while present.
    return {"table": table, "column": column, "datatype": datatype, "shares": [{"type": "default"}], "members": [], "exclusionary": False, "allMembers": True}


@pytest.mark.integration
def test_extract_datasecurity_add_and_delete_lifecycle() -> None:
    """update_datasecurity (POST, add semantics) + delete_datasecurity on a
    RUNNING extract cube. Reproduces the stale-PUT-route finding if the verb
    regresses, and the draft-cube failure if run against unbuilt models.
    """
    dm = _make_datamodel()
    model = _find_model(dm, "extract", {"running"})
    title = model["title"]
    before = dm.get_datasecurity_raw(title)
    table, column, datatype = _fresh_column(dm, title)

    result = dm.update_datasecurity(title, [_neutral_rule(table, column, datatype)])
    assert isinstance(result, dict | list)
    assert "error" not in result, f"update_datasecurity failed on running cube '{title}': {result}"

    added = dm.get_datasecurity_raw(title)
    assert len(added) == len(before) + 1, f"rule not added: {added}"

    cleanup = dm.delete_datasecurity(title, table, column)
    assert cleanup == {"success": True}, f"cleanup failed — REMOVE THE RULE ON {title} {table}.{column} MANUALLY: {cleanup}"

    final = dm.get_datasecurity_raw(title)
    assert len(final) == len(before), f"model not back to original state: {final}"


@pytest.mark.integration
def test_live_datasecurity_add_many_and_delete_lifecycle() -> None:
    """set_live_datasecurity_add_many + delete_datasecurity on a PUBLISHED
    live model. Reproduces the 422-required-fields and draft-model findings.
    """
    dm = _make_datamodel()
    model = _find_model(dm, "live", {"published"})
    title = model["title"]
    before = dm.get_datasecurity_raw(title)
    if not isinstance(before, list):
        pytest.skip(f"Could not read datasecurity for live model '{title}': {before}")
    table, column, datatype = _fresh_column(dm, title)

    result = dm.set_live_datasecurity_add_many(title, [_neutral_rule(table, column, datatype)])
    assert isinstance(result, dict | list)
    assert "error" not in result, f"addMany failed on published live model '{title}': {result}"

    added = dm.get_datasecurity_raw(title)
    assert len(added) == len(before) + 1, f"rule not added: {added}"

    cleanup = dm.delete_datasecurity(title, table, column)
    assert cleanup == {"success": True}, f"cleanup failed — REMOVE THE RULE ON {title} {table}.{column} MANUALLY: {cleanup}"

    final = dm.get_datasecurity_raw(title)
    assert len(final) == len(before), f"model not back to original state: {final}"
