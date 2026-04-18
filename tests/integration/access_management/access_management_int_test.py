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
    # get_users_all returns transformed dicts with uppercase keys
    assert "EMAIL" in first
    assert "ROLE_NAME" in first or "GROUPS" in first


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

    assert isinstance(result, (dict, list))


@pytest.mark.integration
def test_get_all_dashboard_shares_returns_list() -> None:
    """get_all_dashboard_shares should return a list of share records."""
    am = AccessManagement(api_client=_make_client())
    result = am.get_all_dashboard_shares()

    assert isinstance(result, list)
