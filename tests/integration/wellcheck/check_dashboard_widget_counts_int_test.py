import os
from typing import Any, List

import pytest

from pysisense.sisenseclient import SisenseClient
from pysisense.wellcheck import WellCheck


CONFIG_PATH = os.environ.get("PYSISENSE_CONFIG_PATH", "config.yaml")


def _config_exists() -> bool:
    return os.path.exists(CONFIG_PATH)


@pytest.mark.integration
def test_check_dashboard_widget_counts_integration_smoke() -> None:
    """
    Basic integration test for check_dashboard_widget_counts.

    This test:
      - Instantiates SisenseClient from a config file.
      - Retrieves a list of dashboards via the admin API.
      - Picks one dashboard reference (title or ID).
      - Calls check_dashboard_widget_counts and verifies the shape
        and types of the result.
    """
    if not _config_exists():
        pytest.skip(f"Config file '{CONFIG_PATH}' not found; skipping integration test.")

    client = SisenseClient(config_file=CONFIG_PATH, debug=False)
    wellcheck = WellCheck(api_client=client, debug=False)

    # Fetch some dashboards to use as references
    response = client.get("/api/v1/dashboards/admin?dashboardType=owner")
    assert response is not None, "Expected a response from the dashboards admin endpoint."

    dashboards: List[Any] = response.json()
    if not dashboards:
        pytest.skip("No dashboards returned from admin endpoint; skipping integration test.")

    first = dashboards[0]
    ref = first.get("title") or first.get("oid")
    assert isinstance(ref, str) and ref, "Dashboard reference must be a non-empty string."

    results = wellcheck.check_dashboard_widget_counts(dashboards=[ref])

    assert isinstance(results, list)
    assert len(results) == 1

    row = results[0]
    assert "dashboard_id" in row
    assert "dashboard_title" in row
    assert "widget_count" in row
    assert isinstance(row["widget_count"], int)
