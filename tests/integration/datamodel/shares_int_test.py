"""Integration tests for data model share write paths against a live Sisense instance.

Live-verified findings these encode (2026-08 sandbox, L2025.x):
- EXTRACT permissions are read/written by title via
  /api/elasticubes/localhost/{title}/permissions (GET + PUT); LIVE uses a
  PATCH by oid instead
- BOTH vocabularies key share entries by "partyId" — the "party" key (used
  by datasecurity rule shares on extract models) is silently DROPPED by the
  permissions PUT: the write returns 200 but the entry never lands
- entries for INACTIVE users are also silently dropped (HTTP 200, entry
  never lands) — the SDK skips them with a warning instead of submitting
- add_datamodel_shares supports EXTRACT models (the pre-2.0 "Fixing Bug ...
  will be fixed in V2" early-return is retired)

Local-only: requires config.yaml (gitignored); never runs in CI. The test is
a self-cleaning lifecycle: snapshot the existing permissions, add one share
for a user that has none, verify it appears, then restore the snapshot via
update_datamodel_permissions_extract and verify the model is back to its
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


def _find_running_extract(dm: DataModel) -> dict:
    models = dm.get_all_datamodel()
    if not isinstance(models, list):
        pytest.skip(f"Could not list datamodels: {models}")
    match = next(
        (m for m in models if str(m.get("type", "")).lower() == "extract" and str(m.get("status", "")).lower() == "running"),
        None,
    )
    if match is None:
        pytest.skip("No RUNNING extract datamodel found (permission writes require a built cube).")
    return match


def _party_id(entry: dict) -> str | None:
    return entry.get("partyId", entry.get("party"))


@pytest.mark.integration
def test_add_datamodel_shares_extract_lifecycle() -> None:
    """add_datamodel_shares on a RUNNING extract cube: snapshot -> add ->
    verify -> restore. Regresses loudly if the extract path stops writing
    "partyId"-keyed entries (a "party"-keyed entry is silently dropped) or
    the PUT-by-title route changes.
    """
    dm = _make_datamodel()
    model = _find_running_extract(dm)
    title = model["title"]

    snapshot = dm.get_datamodel_permissions_extract(title)
    assert isinstance(snapshot, list), f"could not snapshot permissions for '{title}': {snapshot}"

    shared_ids = {_party_id(e) for e in snapshot if isinstance(e, dict)}

    # Prefer an unshared ACTIVE user (Sisense silently drops shares for
    # inactive ones); fall back to an unshared group.
    users_response = dm.api_client.get("/api/v1/users")
    if users_response is None or users_response.status_code != 200:
        pytest.skip("Could not list users to pick a share candidate.")
    user = next((u for u in users_response.json() if u.get("_id") not in shared_ids and u.get("email") and u.get("active") is True), None)
    if user is not None:
        candidate_id, share_def = user["_id"], {"name": user["email"], "type": "user", "permission": "USE"}
    else:
        groups_response = dm.api_client.get("/api/v1/groups")
        if groups_response is None or groups_response.status_code != 200:
            pytest.skip("Could not list groups to pick a share candidate.")
        group = next((g for g in groups_response.json() if g.get("_id") not in shared_ids and g.get("name") not in ("Everyone", "All users in system")), None)
        if group is None:
            pytest.skip(f"No unshared active user or group available for '{title}'.")
        candidate_id, share_def = group["_id"], {"name": group["name"], "type": "group", "permission": "USE"}

    result = dm.add_datamodel_shares(title, [share_def])
    assert isinstance(result, dict)
    assert result.get("ok") is not False and "error" not in result, f"add_datamodel_shares failed on extract cube '{title}': {result}"

    after = dm.get_datamodel_permissions_extract(title)
    assert isinstance(after, list), f"could not re-read permissions: {after}"
    assert len(after) == len(snapshot) + 1, f"share not added: {after}"
    assert any(_party_id(e) == candidate_id for e in after if isinstance(e, dict)), f"added party missing from readback: {after}"

    cleanup = dm.update_datamodel_permissions_extract(title, snapshot)
    assert "error" not in cleanup, f"cleanup failed — RESTORE PERMISSIONS ON '{title}' MANUALLY: {cleanup}"

    final = dm.get_datamodel_permissions_extract(title)
    assert isinstance(final, list)
    assert len(final) == len(snapshot), f"model not back to original state: {final}"
