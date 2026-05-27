import os

import pytest

from pysisense.blox import Blox
from pysisense.sisenseclient import SisenseClient

CONFIG_PATH = os.environ.get("PYSISENSE_CONFIG_PATH", "config.yaml")


def _make_client() -> SisenseClient:
    if not os.path.exists(CONFIG_PATH):
        pytest.skip(f"Config file '{CONFIG_PATH}' not found; skipping integration test.")
    return SisenseClient(config_file=CONFIG_PATH, debug=False)


@pytest.mark.integration
def test_get_blox_actions_returns_list() -> None:
    """get_blox_actions should return a list with no error key."""
    blox = Blox(api_client=_make_client())
    result = blox.get_blox_actions()

    assert isinstance(result, list), f"Expected list, got: {result}"
    if result:
        assert "error" not in result[0], f"Unexpected error in response: {result[0]}"


@pytest.mark.integration
def test_save_blox_action_round_trips_existing_action() -> None:
    """save_blox_action should accept an action fetched from the same instance."""
    blox = Blox(api_client=_make_client())

    actions = blox.get_blox_actions()
    assert isinstance(actions, list), f"get_blox_actions failed: {actions}"

    if not actions:
        pytest.skip("No Blox actions found on this instance; skipping save test.")

    action = actions[0]
    assert "error" not in action, f"Unexpected error in fetched action: {action}"

    result = blox.save_blox_action(action)

    assert isinstance(result, dict), f"Expected dict, got: {result}"
    assert "error" not in result, f"save_blox_action returned error: {result}"
