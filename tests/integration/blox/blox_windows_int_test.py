"""Windows-specific integration tests for the Blox module.

IMPORTANT
---------
These tests target a Windows Sisense deployment. They:
- Require ``PYSISENSE_WINDOWS_CONFIG_PATH`` to point to a config.yaml with
  ``operating_system: windows`` and a valid Windows server token.
- Use the ``@pytest.mark.windows`` marker — run ONLY on the designated Windows
  test server, not in normal CI or against production.
- May create or modify Blox actions on the target instance. The Windows test
  server is a non-production environment; permanent changes are acceptable.

Run only with:
    pytest -m "integration and windows" tests/integration/blox/blox_windows_int_test.py

Skip in normal CI with:
    pytest -m "not windows"
"""

import os

import pytest

from pysisense.blox import Blox
from pysisense.sisenseclient import SisenseClient

WINDOWS_CONFIG_PATH = os.environ.get("PYSISENSE_WINDOWS_CONFIG_PATH", "config_windows.yaml")

_TEST_ACTION = {
    "type": "pysisense_integration_test",
    "title": "pysisense Integration Test Action",
    "description": "Created by pysisense Windows integration test suite. Safe to delete.",
    "icon": "",
    "url": "",
    "permissions": [],
}


def _make_windows_client() -> SisenseClient:
    if not os.path.exists(WINDOWS_CONFIG_PATH):
        pytest.skip(f"Windows config file '{WINDOWS_CONFIG_PATH}' not found. Set PYSISENSE_WINDOWS_CONFIG_PATH to run Windows integration tests.")
    client = SisenseClient(config_file=WINDOWS_CONFIG_PATH, debug=False)
    if client.operating_system != "windows":
        pytest.skip(f"Config at '{WINDOWS_CONFIG_PATH}' does not set operating_system=windows. Ensure config.yaml contains 'operating_system: windows'.")
    return client


@pytest.mark.integration
@pytest.mark.windows
def test_client_has_windows_operating_system() -> None:
    """SisenseClient built from Windows config should report operating_system='windows'."""
    client = _make_windows_client()
    assert client.operating_system == "windows"


@pytest.mark.integration
@pytest.mark.windows
def test_get_blox_actions_uses_windows_endpoint() -> None:
    """get_blox_actions on a Windows client should succeed via the Windows endpoint.

    On Windows the endpoint is GET /api/v1/getCustomActions/actions rather
    than the Linux endpoint /api/v1/blox/getCustomActions.
    """
    blox = Blox(api_client=_make_windows_client())
    result = blox.get_blox_actions()

    assert isinstance(result, list), f"Expected list, got: {result}"
    if result:
        assert "error" not in result[0], f"Unexpected error in first result: {result[0]}"


@pytest.mark.integration
@pytest.mark.windows
def test_save_blox_action_on_windows() -> None:
    """save_blox_action should persist an action to the Windows Sisense instance.

    This test creates a new Blox action on the Windows server. It is safe to
    leave the action in place (the test server is non-production), but the
    action can be cleaned up manually or by running
    test_delete_blox_action_on_windows.
    """
    blox = Blox(api_client=_make_windows_client())
    result = blox.save_blox_action(_TEST_ACTION)

    assert "error" not in result, f"save_blox_action failed: {result}"


@pytest.mark.integration
@pytest.mark.windows
def test_delete_blox_action_on_windows() -> None:
    """delete_blox_action should remove the test action created by the save test.

    Run this after test_save_blox_action_on_windows to clean up.
    """
    blox = Blox(api_client=_make_windows_client())
    result = blox.delete_blox_action(_TEST_ACTION["type"])

    assert "error" not in result, f"delete_blox_action failed: {result}"


@pytest.mark.integration
@pytest.mark.windows
def test_get_blox_actions_round_trip_after_save() -> None:
    """Save a test action then verify it appears in get_blox_actions.

    NOTE: This test intentionally leaves the saved action on the server.
    Run test_delete_blox_action_on_windows separately to clean up.
    """
    blox = Blox(api_client=_make_windows_client())

    save_result = blox.save_blox_action(_TEST_ACTION)
    assert "error" not in save_result, f"save_blox_action failed: {save_result}"

    actions = blox.get_blox_actions()
    assert isinstance(actions, list), f"get_blox_actions failed: {actions}"

    types = [a.get("type") for a in actions]
    assert _TEST_ACTION["type"] in types, f"Saved action type '{_TEST_ACTION['type']}' not found in action list: {types}"
