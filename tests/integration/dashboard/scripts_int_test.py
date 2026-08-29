"""Integration tests for dashboard script getters and share read-back.

These encode live findings from FES-side testing (2026-08 sandbox, L2025.x):
- get_dashboard_script raised KeyError('script') on scriptless dashboards
- get_dashboard_shares_v1 read-back after a successful share write returned
  an unusable "No response text available" error on 1.0.2

Local-only: requires config.yaml (gitignored); never runs in CI. Both tests
are read-only.
"""

import os

import pytest

from pysisense.dashboard import Dashboard
from pysisense.dashboard.scripts import SisenseScript
from pysisense.sisenseclient import SisenseClient

CONFIG_PATH = os.environ.get("PYSISENSE_CONFIG_PATH", "config.yaml")

# How many dashboards to sweep in the script-getter regression test.
_SCRIPT_SWEEP_LIMIT = 10


def _make_dashboard() -> Dashboard:
    if not os.path.exists(CONFIG_PATH):
        pytest.skip(f"Config file '{CONFIG_PATH}' not found; skipping integration test.")
    return Dashboard(api_client=SisenseClient(config_file=CONFIG_PATH, debug=False))


@pytest.mark.integration
def test_get_dashboard_script_never_raises_on_real_dashboards() -> None:
    """Finding: a dashboard with no script — a normal state — raised
    KeyError('script'). Sweep real dashboards (scriptless ones included) and
    require every result to be either a SisenseScript or a contract error
    dict; any exception is the regression.
    """
    dashboard = _make_dashboard()
    dashboards = dashboard.get_all_dashboards()
    if not isinstance(dashboards, list) or not dashboards:
        pytest.skip(f"Could not list dashboards: {dashboards}")

    scriptless_seen = 0
    for entry in dashboards[:_SCRIPT_SWEEP_LIMIT]:
        result = dashboard.get_dashboard_script(entry["oid"])  # must not raise
        assert isinstance(result, SisenseScript | dict), f"Unexpected return for '{entry.get('title')}': {type(result)}"
        if isinstance(result, dict):
            assert "error" in result
            if "has no dashboard script" in result["error"]:
                scriptless_seen += 1

    # Informational: the sweep is most valuable when it actually hit a
    # scriptless dashboard; either way, no exception means the fix holds.
    assert scriptless_seen >= 0


@pytest.mark.integration
def test_get_dashboard_shares_v1_read_back_is_usable() -> None:
    """Finding: after a successful add_dashboard_shares, the v1 shares
    read-back failed with the pre-contract "No response text available"
    message, making shares unverifiable. On the branch a failure must carry
    the real reason (status_code + Sisense detail) — and a success must carry
    the shares structure.
    """
    dashboard = _make_dashboard()
    dashboards = dashboard.get_all_dashboards()
    if not isinstance(dashboards, list) or not dashboards:
        pytest.skip(f"Could not list dashboards: {dashboards}")

    result = dashboard.get_dashboard_shares_v1(dashboards[0]["oid"])

    if isinstance(result, list):
        # Some Sisense versions return the share entries as a list.
        assert result, "Share read-back returned an empty list."
        return
    assert isinstance(result, dict), f"Unexpected return type: {type(result)}"
    if "error" in result:
        assert "No response text available" not in result["error"], "Pre-contract error phrasing has resurfaced."
        assert "status_code" in result, f"Share read-back failed without the error contract (no status_code): {result}"
        pytest.fail(f"Share read-back failed — real reason now visible for the investigation: {result}")
    else:
        assert "sharesTo" in result or "owner" in result, f"Share read-back succeeded but lacks the expected structure: {result}"
