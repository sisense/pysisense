"""Integration tests for datasecurity write paths against a live Sisense instance.

These encode live findings from FES-side testing (2026-08 sandbox, L2025.x):
- update_datasecurity PUT route returned an HTML 404 on an extract model
- set_live_datasecurity_add_many failed 422 (missing allMembers/live/fullname)
  and then "Elasticube has not been found" on a live model (title-vs-oid suspect)

Local-only: requires config.yaml (gitignored); never runs in CI. The live
addMany test WRITES a datasecurity rule and is additionally gated behind
PYSISENSE_RUN_DATASECURITY_WRITES=1 because the SDK has no rule-delete method
to clean up after itself. The extract round-trip PUTs back the model's own
existing rules unchanged, so it is state-neutral.
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


def _find_model_by_type(dm: DataModel, wanted_type: str) -> dict:
    models = dm.get_all_datamodel()
    if not isinstance(models, list):
        pytest.skip(f"Could not list datamodels: {models}")
    match = next((m for m in models if str(m.get("type", "")).lower() == wanted_type), None)
    if match is None:
        pytest.skip(f"No {wanted_type.upper()} datamodel found on the instance.")
    return match


@pytest.mark.integration
def test_update_datasecurity_route_accepts_put_on_extract_model() -> None:
    """Finding: PUT /api/elasticubes/localhost/{title}/datasecurity returned
    an HTML 404 ('Cannot PUT ...') while the GET on the same path works.

    State-neutral round-trip: reads the model's existing rules and PUTs the
    exact same payload back. Skips when the model has no rules to round-trip.
    A failure here reproduces the stale-route finding and carries the real
    status/body via the error contract.
    """
    dm = _make_datamodel()
    models = dm.get_all_datamodel()
    if not isinstance(models, list):
        pytest.skip(f"Could not list datamodels: {models}")

    title, raw_rules = None, None
    for model in models:
        if str(model.get("type", "")).lower() != "extract":
            continue
        candidate_rules = dm.get_datasecurity_raw(model["title"], datamodel_type="extract")
        if isinstance(candidate_rules, list) and candidate_rules:
            title, raw_rules = model["title"], candidate_rules
            break
    if title is None:
        pytest.skip("No extract model with existing datasecurity rules found to round-trip.")

    result = dm.update_datasecurity(title, raw_rules)

    assert isinstance(result, dict)
    assert "error" not in result, f"update_datasecurity round-trip failed on '{title}': {result}"


@pytest.mark.integration
def test_set_live_datasecurity_add_many_reaches_the_live_model() -> None:
    """Finding: with the documented field list the API returned 422 (missing
    allMembers/live/fullname); with those added it failed 'Elasticube has not
    been found' — suspicion: the URL uses the title where the live API wants
    the oid.

    WRITE TEST — leaves a rule on the live model (no delete API). Gated behind
    PYSISENSE_RUN_DATASECURITY_WRITES=1. Builds the least-invasive rule
    (allMembers=True on the first real table/column) with the full field list
    observed as required. Assertion failures carry the raw response as
    evidence for the endpoint investigation.
    """
    if os.environ.get("PYSISENSE_RUN_DATASECURITY_WRITES") != "1":
        pytest.skip("Set PYSISENSE_RUN_DATASECURITY_WRITES=1 to run the live datasecurity write test.")

    dm = _make_datamodel()
    model = _find_model_by_type(dm, "live")
    title = model["title"]

    schema_rows = dm.get_model_schema(title)
    if not isinstance(schema_rows, list) or not schema_rows:
        pytest.skip(f"Could not read schema for live model '{title}': {schema_rows}")
    first = schema_rows[0]

    rule = {
        "table": first["table_name"],
        "column": first["column_name"],
        "datatype": str(first.get("column_type", "text")).lower(),
        "members": [],
        "exclusionary": False,
        "shares": [],
        # Required by the live API per the 422 response (2026-08, L2025.x):
        "allMembers": True,
        "live": True,
        "fullname": f"live:{title}",
    }

    result = dm.set_live_datasecurity_add_many(title, [rule])

    assert isinstance(result, dict)
    assert "must have required property" not in str(result), f"Live API rejected the rule fields — docstring field list is wrong: {result}"
    assert "has not been found" not in str(result), f"Live model not found by title-based URL (title-vs-oid suspect confirmed): {result}"
    assert "error" not in result, f"set_live_datasecurity_add_many failed on '{title}': {result}"
