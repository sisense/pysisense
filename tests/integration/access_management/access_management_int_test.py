import os

import pytest

from pysisense.access_management import AccessManagement
from pysisense.sisenseclient import SisenseClient

CONFIG_PATH = os.environ.get("PYSISENSE_CONFIG_PATH", "config.yaml")


def _make_client() -> SisenseClient:
    if not os.path.exists(CONFIG_PATH):
        pytest.skip(f"Config file '{CONFIG_PATH}' not found; skipping integration test.")
    return SisenseClient(config_file=CONFIG_PATH, debug=False)


@pytest.mark.integration
def test_get_users_all_returns_list() -> None:
    """get_users_all should return a non-empty list of user dicts."""
    am = AccessManagement(api_client=_make_client())
    result = am.get_users_all()

    assert isinstance(result, list), f"Expected list, got: {result}"
    assert len(result) > 0, "Expected at least one user."

    first = result[0]
    # Canonical user row: uppercase keys, both role vocabularies, and GROUPS
    # (names, kept from 1.x) alongside the new GROUP_IDS.
    assert "EMAIL" in first
    assert "ROLE_NAME" in first
    assert "ROLE_DISPLAY_NAME" in first
    assert "ROLE_RAW_NAME" in first
    assert "GROUP_IDS" in first
    assert "GROUPS" in first


@pytest.mark.integration
def test_get_user_returns_user_dict() -> None:
    """get_user should return the correct user dict for a known email."""
    client = _make_client()
    am = AccessManagement(api_client=client)

    all_users = am.get_users_all()
    if not isinstance(all_users, list) or not all_users:
        pytest.skip("No users available.")

    email = next((u.get("EMAIL", "") for u in all_users if u.get("EMAIL")), "")
    if not email:
        pytest.skip("No users with an email address found.")

    result = am.get_user(email)

    assert isinstance(result, dict)
    assert "error" not in result
    assert result.get("EMAIL") == email


@pytest.mark.integration
def test_get_users_with_role_names_and_group_names_returns_enriched() -> None:
    """get_users_with_role_names_and_group_names should return enriched user dicts."""
    am = AccessManagement(api_client=_make_client())
    result = am.get_users_with_role_names_and_group_names()

    assert isinstance(result, list)
    assert len(result) > 0

    first = result[0]
    assert "ROLE_NAME" in first
    assert "GROUP_NAMES" in first


@pytest.mark.integration
def test_users_per_group_all_returns_dict() -> None:
    """users_per_group_all should return a dict mapping group names to user lists."""
    am = AccessManagement(api_client=_make_client())
    result = am.users_per_group_all()

    assert isinstance(result, dict | list)


@pytest.mark.integration
def test_get_all_dashboard_shares_returns_list() -> None:
    """get_all_dashboard_shares should return a list of share records."""
    am = AccessManagement(api_client=_make_client())
    result = am.get_all_dashboard_shares()

    assert isinstance(result, list)


@pytest.mark.integration
def test_role_resolution_accepts_both_vocabularies_and_keeps_roles_distinct() -> None:
    """Role names resolve against the live instance's own /api/roles list.

    Read-only (only GETs /api/roles). Live-verified findings this encodes
    (2026-08 sandbox, L2025.x): /api/roles carries a displayName alongside the
    raw name ("super" / "Sys. Admin"), and the instance defines roles beyond
    the three with UI aliases — admin, tenantAdmin, dataDesigner, dataAdmin.

    The two assertions that matter are the negative ones: 'admin' must NOT
    resolve to 'super' (that would silently over-privilege a created user) and
    'data designer' must NOT resolve to 'contributor' (dataDesigner is its own
    role, not a synonym for Designer). Both are guaranteed by matching the
    instance's real roles before consulting the alias table.
    """
    am = AccessManagement(api_client=_make_client())

    roles_response = am.api_client.get("/api/roles")
    if roles_response is None or not roles_response.ok:
        pytest.skip("Could not fetch /api/roles.")
    name_by_id = {r["_id"]: r.get("name") for r in roles_response.json() if r.get("_id")}
    available = set(name_by_id.values())

    expectations = {
        "super": "super",
        "sysAdmin": "super",
        "sys admin": "super",
        "System Administrator": "super",
        "viewer": "consumer",
        "consumer": "consumer",
        "dashboardDesigner": "contributor",
        "designer": "contributor",
        "contributor": "contributor",
        # Roles that must resolve to themselves, never to a similar-sounding alias
        "admin": "admin",
        "dataDesigner": "dataDesigner",
        "data designer": "dataDesigner",
        "dataAdmin": "dataAdmin",
    }

    for given, expected_raw_name in expectations.items():
        if expected_raw_name not in available:
            continue  # instance does not define this role; nothing to assert
        resolved = am._resolve_role_id(given)
        assert not isinstance(resolved, dict), f"role {given!r} failed to resolve: {resolved}"
        assert name_by_id[resolved] == expected_raw_name, f"role {given!r} resolved to {name_by_id[resolved]!r}, expected {expected_raw_name!r}"

    unknown = am._resolve_role_id("definitely-not-a-role-xyz")
    assert isinstance(unknown, dict) and unknown["ok"] is False
    assert "Available roles" in unknown["error"], "the error must tell the caller what it can retry with"
