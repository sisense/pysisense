from typing import Any, Dict

import pytest

from pysisense.sisenseclient import SisenseClient
from pysisense.dashboard import Dashboard
from pysisense.wellcheck import WellCheck


@pytest.fixture(scope="session")
def sisense_client() -> SisenseClient:
    """
    Return a SisenseClient instance for integration tests.

    A lightweight connectivity check is performed; if the instance is not
    reachable or dashboards cannot be fetched, integration tests are skipped.
    """
    client = SisenseClient(debug=False)

    try:
        response = client.get("/api/v1/dashboards/admin?dashboardType=owner")
    except Exception as exc:
        pytest.skip(f"Sisense instance not reachable for integration tests: {exc}")

    if response is None or response.status_code != 200:
        status = getattr(response, "status_code", None)
        pytest.skip(
            f"Dashboards endpoint unavailable for integration tests (status={status})."
        )

    return client


@pytest.fixture(scope="session")
def example_dashboard(sisense_client: SisenseClient) -> Dict[str, Any]:
    """
    Fetch a single dashboard to serve as a test fixture.

    Uses the Dashboard helper to fetch all dashboards and selects the first one.
    If no dashboards are available, integration tests depending on this fixture
    are skipped.
    """
    dashboard_helper = Dashboard(api_client=sisense_client, debug=False)
    dashboards = dashboard_helper.get_all_dashboards()

    # Helper may return {"error": "..."} instead of a list.
    if isinstance(dashboards, dict) and dashboards.get("error"):
        pytest.skip(
            f"Unable to fetch dashboards for integration tests: {dashboards['error']}"
        )

    if not isinstance(dashboards, list) or not dashboards:
        pytest.skip("No dashboards available for integration tests.")

    first = dashboards[0]
    dashboard_id = first.get("oid")
    dashboard_title = first.get("title", "")

    if not dashboard_id:
        pytest.skip("Selected dashboard has no 'oid' field.")

    return {"id": dashboard_id, "title": dashboard_title}


@pytest.fixture(scope="session")
def wellcheck(sisense_client: SisenseClient) -> WellCheck:
    """
    Return a WellCheck instance wired to the real SisenseClient.
    """
    return WellCheck(api_client=sisense_client, debug=False)


def test_check_dashboard_structure_with_dashboard_id(
    wellcheck: WellCheck,
    example_dashboard: Dict[str, Any],
) -> None:
    """
    Run check_dashboard_structure with a real dashboard ID and validate
    the basic shape of the result.
    """
    dashboard_id = example_dashboard["id"]

    result = wellcheck.check_dashboard_structure(dashboards=[dashboard_id])

    assert isinstance(result, list)
    assert len(result) == 1

    row = result[0]

    assert row["dashboard_id"] == dashboard_id
    assert isinstance(row["dashboard_title"], str)

    for field in ("pivot_count", "tabber_count", "accordion_count", "jtd_count"):
        assert field in row
        assert isinstance(row[field], int)
        assert row[field] >= 0


def test_check_dashboard_structure_with_dashboard_title(
    wellcheck: WellCheck,
    example_dashboard: Dict[str, Any],
) -> None:
    """
    Run check_dashboard_structure with the dashboard title instead of the ID.

    This exercises the name-based resolution path against the live instance.
    """
    dashboard_title = example_dashboard["title"]
    dashboard_id = example_dashboard["id"]

    if not dashboard_title:
        pytest.skip("Selected dashboard has an empty title.")

    result = wellcheck.check_dashboard_structure(dashboards=[dashboard_title])

    assert isinstance(result, list)
    assert len(result) == 1

    row = result[0]

    # Name-based resolution should still produce the same ID.
    assert row["dashboard_id"] == dashboard_id
    assert isinstance(row["dashboard_title"], str)

    for field in ("pivot_count", "tabber_count", "accordion_count", "jtd_count"):
        assert field in row
        assert isinstance(row[field], int)
        assert row[field] >= 0
