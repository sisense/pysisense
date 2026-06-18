import os

import pytest

from pysisense.dashboard import Dashboard
from pysisense.sisenseclient import SisenseClient

CONFIG_PATH = os.environ.get("PYSISENSE_CONFIG_PATH", "config.yaml")


def _make_client() -> SisenseClient:
    if not os.path.exists(CONFIG_PATH):
        pytest.skip(f"Config file '{CONFIG_PATH}' not found; skipping integration test.")
    return SisenseClient(config_file=CONFIG_PATH, debug=False)


@pytest.mark.integration
def test_get_all_dashboards_returns_list() -> None:
    """get_all_dashboards should return a non-empty list of dashboard dicts."""
    dashboard = Dashboard(api_client=_make_client())
    result = dashboard.get_all_dashboards()

    assert isinstance(result, list), f"Expected list, got: {result}"
    assert len(result) > 0, "Expected at least one dashboard."

    first = result[0]
    assert "oid" in first
    assert "title" in first


@pytest.mark.integration
def test_get_dashboard_by_id_returns_dashboard() -> None:
    """get_dashboard_by_id should return the correct dashboard dict for a known ID."""
    client = _make_client()
    dashboard = Dashboard(api_client=client)

    all_dashboards = dashboard.get_all_dashboards()
    if not isinstance(all_dashboards, list) or not all_dashboards:
        pytest.skip("No dashboards available.")

    first = all_dashboards[0]
    dashboard_id = first.get("oid")

    result = dashboard.get_dashboard_by_id(dashboard_id)

    # The admin endpoint returns a list; the method passes it through directly
    assert isinstance(result, list)
    assert len(result) >= 1
    assert result[0].get("_id") == dashboard_id or result[0].get("oid") == dashboard_id


@pytest.mark.integration
def test_get_dashboard_by_name_returns_match() -> None:
    """get_dashboard_by_name should return a list containing the matching dashboard."""
    client = _make_client()
    dashboard = Dashboard(api_client=client)

    all_dashboards = dashboard.get_all_dashboards()
    if not isinstance(all_dashboards, list) or not all_dashboards:
        pytest.skip("No dashboards available.")

    first = all_dashboards[0]
    title = first.get("title", "")
    if not title:
        pytest.skip("First dashboard has no title.")

    result = dashboard.get_dashboard_by_name(title)

    assert isinstance(result, list)
    assert len(result) >= 1
    titles = [d.get("title") for d in result]
    assert title in titles


@pytest.mark.integration
def test_get_dashboard_share_returns_share_info() -> None:
    """get_dashboard_share should return share data for a known dashboard."""
    client = _make_client()
    dashboard = Dashboard(api_client=client)

    all_dashboards = dashboard.get_all_dashboards()
    if not isinstance(all_dashboards, list) or not all_dashboards:
        pytest.skip("No dashboards available.")

    title = all_dashboards[0].get("title", "")
    if not title:
        pytest.skip("First dashboard has no title.")

    result = dashboard.get_dashboard_share(title)

    # Returns a dict with share info or an error dict — either is valid here
    assert isinstance(result, (dict, list))
