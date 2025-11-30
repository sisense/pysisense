from typing import Any, Dict, List, Optional

from pysisense.wellcheck import WellCheck


class FakeLogger:
    def __init__(self) -> None:
        self.messages: List[Dict[str, Any]] = []

    def _log(self, level: str, msg: str, **extra: Any) -> None:
        entry: Dict[str, Any] = {"level": level, "msg": msg}
        if extra:
            entry["extra"] = extra
        self.messages.append(entry)

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("debug", msg, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("info", msg, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("warning", msg, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("error", msg, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        # In tests this is treated as an error-style log
        self._log("exception", msg, **kwargs)


class FakeResponse:
    def __init__(self, status_code: int, json_data: Any) -> None:
        self.status_code = status_code
        self._json_data = json_data
        self.text = ""

    def json(self) -> Any:
        return self._json_data


class FakeApiClient:
    """
    Minimal fake SisenseClient for testing WellCheck.

    Supports:
      - get(endpoint)
      - logger
    """

    def __init__(self, responses: Dict[str, FakeResponse], logger: FakeLogger) -> None:
        self._responses = responses
        self.logger = logger

    def get(self, endpoint: str) -> Optional[FakeResponse]:
        return self._responses.get(endpoint)


class FakeDashboard:
    """
    Fake Dashboard helper that only implements resolve_dashboard_reference.
    """

    def __init__(self, mapping: Dict[str, Dict[str, Any]]) -> None:
        """
        Parameters
        ----------
        mapping : dict
            Mapping from reference string to a dict with keys:
            - dashboard_id
            - dashboard_title
        """
        self._mapping = mapping

    def resolve_dashboard_reference(self, dashboard_ref: str) -> Dict[str, Any]:
        if dashboard_ref in self._mapping:
            entry = self._mapping[dashboard_ref]
            return {
                "success": True,
                "status_code": 200,
                "dashboard_id": entry["dashboard_id"],
                "dashboard_title": entry["dashboard_title"],
                "error": None,
            }

        return {
            "success": False,
            "status_code": 404,
            "dashboard_id": None,
            "dashboard_title": None,
            "error": f"Dashboard reference '{dashboard_ref}' not found",
        }


class FakeDatamodel:
    """
    Fake Datamodel helper that only implements resolve_datamodel_reference.
    """

    def __init__(self, mapping: Dict[str, Dict[str, Any]]) -> None:
        """
        Parameters
        ----------
        mapping : dict
            Mapping from reference string to a dict with keys:
            - datamodel_id
            - datamodel_title
        """
        self._mapping = mapping

    def resolve_datamodel_reference(self, datamodel_ref: str) -> Dict[str, Any]:
        if datamodel_ref in self._mapping:
            entry = self._mapping[datamodel_ref]
            return {
                "success": True,
                "status_code": 200,
                "datamodel_id": entry["datamodel_id"],
                "datamodel_title": entry["datamodel_title"],
                "error": None,
            }

        return {
            "success": False,
            "status_code": 404,
            "datamodel_id": None,
            "datamodel_title": None,
            "error": f"Datamodel reference '{datamodel_ref}' not found",
        }


class WellCheckTestHarness(WellCheck):
    """
    Thin subclass that lets tests inject fake api_client, dashboard, and datamodel
    without triggering the real Dashboard / Datamodel / SisenseClient initialization.
    """

    def __init__(
        self,
        api_client: FakeApiClient,
        dashboard: FakeDashboard,
        datamodel: Optional[FakeDatamodel] = None,
    ) -> None:
        self.api_client = api_client
        self.logger = api_client.logger
        self.dashboard = dashboard
        if datamodel is not None:
            self.datamodel = datamodel


# ---------------------------------------------------------------------------
# Tests for check_dashboard_structure
# ---------------------------------------------------------------------------


def test_check_dashboard_structure_returns_empty_when_no_dashboards() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)
    dashboard = FakeDashboard(mapping={})

    wellcheck = WellCheckTestHarness(api_client=api_client, dashboard=dashboard)

    result = wellcheck.check_dashboard_structure(dashboards=None)

    assert result == []
    # Ensure an error was logged about missing dashboard references
    assert any(
        m["level"] == "error"
        and "At least one dashboard reference" in m["msg"]
        for m in logger.messages
    )


def test_check_dashboard_structure_skips_unresolved_references() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)
    # No entries in mapping -> all references will fail to resolve
    dashboard = FakeDashboard(mapping={})

    wellcheck = WellCheckTestHarness(api_client=api_client, dashboard=dashboard)

    result = wellcheck.check_dashboard_structure(dashboards=["missing_dashboard"])

    # Nothing resolved or processed
    assert result == []
    # There should be at least one warning about skipping the reference
    assert any(
        m["level"] == "warning"
        and "Skipping dashboard reference 'missing_dashboard'" in m["msg"]
        for m in logger.messages
    )


def test_check_dashboard_structure_counts_widgets_and_jtds() -> None:
    """
    Single dashboard:
      - 1 pivot widget
      - 1 tabber widget
      - 1 JTD via options.drillTarget
      - 1 JTD via script (dashboardId inside prism.jumpToDashboard block)
      - 1 accordion detected via accordionConfig
    """
    logger = FakeLogger()

    dashboard_id = "D123456789012345678901234"
    endpoint = f"/api/dashboards/{dashboard_id}?adminAccess=true"

    # Script only needs to contain a jumpToDashboard block now
    script = 'prism.jumpToDashboard(widget, { dashboardId: "123456789012345678901234" });'

    dashboard_payload = {
        "oid": dashboard_id,
        "title": "Test Dashboard",
        "widgets": [
            {
                "oid": "W1",
                "type": "pivot",
                "options": {
                    "drillTarget": {"oid": "CHILD_DASH_1"},
                },
                "script": script,
                "accordionConfig": {
                    "isEnabled": True,
                    "openOnPageLoad": False,
                    "dashboardUrl": "",
                    "dashboardName": "_accrd_test_2",
                    "useDashboardName": True,
                    "backgroundColor": "#ffcb05",
                    "dashboardFiltersInheritance": False,
                    "dashboardFiltersSelection": [],
                    "widgetFiltersInheritance": False,
                },
            },
            {
                "oid": "W2",
                "type": "WidgetsTabber",
            },
        ],
    }

    responses = {
        endpoint: FakeResponse(status_code=200, json_data=dashboard_payload),
    }

    api_client = FakeApiClient(responses=responses, logger=logger)
    dashboard = FakeDashboard(
        mapping={
            dashboard_id: {
                "dashboard_id": dashboard_id,
                "dashboard_title": "Test Dashboard",
            },
            "Test Dashboard": {
                "dashboard_id": dashboard_id,
                "dashboard_title": "Test Dashboard",
            },
        }
    )

    wellcheck = WellCheckTestHarness(api_client=api_client, dashboard=dashboard)

    result = wellcheck.check_dashboard_structure(dashboards=[dashboard_id])

    assert len(result) == 1
    row = result[0]

    assert row["dashboard_id"] == dashboard_id
    assert row["dashboard_title"] == "Test Dashboard"
    assert row["pivot_count"] == 1
    assert row["tabber_count"] == 1
    assert row["jtd_count"] == 2  # 1 via options, 1 via script
    assert row["accordion_count"] == 1

    # Summary logs – keep assertions tolerant to wording
    assert any(
        m["level"] == "info"
        and "Total dashboards processed" in m["msg"]
        for m in logger.messages
    )
    assert any(
        m["level"] == "info"
        and "Total JTD" in m["msg"]
        for m in logger.messages
    )


# ---------------------------------------------------------------------------
# Tests for check_dashboard_widget_counts
# ---------------------------------------------------------------------------


def test_check_dashboard_widget_counts_returns_empty_when_no_dashboards() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)
    dashboard = FakeDashboard(mapping={})

    wellcheck = WellCheckTestHarness(api_client=api_client, dashboard=dashboard)

    result = wellcheck.check_dashboard_widget_counts(dashboards=None)

    assert result == []
    assert any(
        m["level"] == "error"
        and "At least one dashboard reference" in m["msg"]
        for m in logger.messages
    )


def test_check_dashboard_widget_counts_skips_unresolved_references() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)
    dashboard = FakeDashboard(mapping={})

    wellcheck = WellCheckTestHarness(api_client=api_client, dashboard=dashboard)

    result = wellcheck.check_dashboard_widget_counts(dashboards=["missing_dashboard"])

    assert result == []
    assert any(
        m["level"] == "warning"
        and "Skipping dashboard reference 'missing_dashboard'" in m["msg"]
        for m in logger.messages
    )


def test_check_dashboard_widget_counts_counts_widgets() -> None:
    logger = FakeLogger()

    dashboard_id = "D123456789012345678901234"
    endpoint = f"/api/dashboards/{dashboard_id}?adminAccess=true"

    dashboard_payload = {
        "oid": dashboard_id,
        "title": "Widget Count Dashboard",
        "widgets": [
            {"oid": "W1", "type": "chart"},
            {"oid": "W2", "type": "pivot"},
        ],
    }

    responses = {
        endpoint: FakeResponse(status_code=200, json_data=dashboard_payload),
    }

    api_client = FakeApiClient(responses=responses, logger=logger)
    dashboard = FakeDashboard(
        mapping={
            dashboard_id: {
                "dashboard_id": dashboard_id,
                "dashboard_title": "Widget Count Dashboard",
            },
            "Widget Count Dashboard": {
                "dashboard_id": dashboard_id,
                "dashboard_title": "Widget Count Dashboard",
            },
        }
    )

    wellcheck = WellCheckTestHarness(api_client=api_client, dashboard=dashboard)

    result = wellcheck.check_dashboard_widget_counts(dashboards=[dashboard_id])

    # Functional checks
    assert len(result) == 1
    row = result[0]

    assert row["dashboard_id"] == dashboard_id
    assert row["dashboard_title"] == "Widget Count Dashboard"
    assert row["widget_count"] == 2

    # Just verify that at least one info log was written
    assert any(m["level"] == "info" for m in logger.messages)


# ---------------------------------------------------------------------------
# Tests for check_pivot_widget_fields
# ---------------------------------------------------------------------------


def test_check_pivot_widget_fields_returns_empty_when_no_dashboards() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)
    dashboard = FakeDashboard(mapping={})

    wellcheck = WellCheckTestHarness(api_client=api_client, dashboard=dashboard)

    result = wellcheck.check_pivot_widget_fields(dashboards=None)

    # No dashboards -> empty result
    assert result == []
    # Ensure an error was logged about missing dashboard references
    assert any(
        m["level"] == "error"
        and "At least one dashboard reference" in m["msg"]
        for m in logger.messages
    )


def test_check_pivot_widget_fields_skips_unresolved_references() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)
    # No entries in mapping -> all references will fail to resolve
    dashboard = FakeDashboard(mapping={})

    wellcheck = WellCheckTestHarness(api_client=api_client, dashboard=dashboard)

    result = wellcheck.check_pivot_widget_fields(dashboards=["missing_dashboard"])

    # Nothing resolved or processed
    assert result == []

    # At least one warning should be present
    assert any(m["level"] == "warning" for m in logger.messages)


def test_check_pivot_widget_fields_counts_pivot_widgets_and_fields() -> None:
    """
    Single dashboard:
      - 1 pivot widget with 21 fields (> default threshold of 20)
      - 1 non-pivot widget that should be ignored
    """
    logger = FakeLogger()

    dashboard_id = "D123456789012345678901234"
    endpoint = f"/api/dashboards/{dashboard_id}?adminAccess=true"

    # 21 > default threshold 20
    pivot_items = [{"field": f"F{i}"} for i in range(21)]

    dashboard_payload = {
        "oid": dashboard_id,
        "title": "Pivot Field Dashboard",
        "widgets": [
            {
                "oid": "W1",
                "type": "pivot",
                "metadata": {
                    "panels": [
                        {
                            "items": pivot_items,
                        }
                    ]
                },
            },
            {
                "oid": "W2",
                "type": "indicator",
                "metadata": {"panels": [{"items": [{"field": "X"}]}]},
            },
        ],
    }

    responses = {
        endpoint: FakeResponse(status_code=200, json_data=dashboard_payload),
    }

    api_client = FakeApiClient(responses=responses, logger=logger)
    dashboard = FakeDashboard(
        mapping={
            dashboard_id: {
                "dashboard_id": dashboard_id,
                "dashboard_title": "Pivot Field Dashboard",
            },
            "Pivot Field Dashboard": {
                "dashboard_id": dashboard_id,
                "dashboard_title": "Pivot Field Dashboard",
            },
        }
    )

    wellcheck = WellCheckTestHarness(api_client=api_client, dashboard=dashboard)

    # Use default threshold from implementation (> 20)
    result = wellcheck.check_pivot_widget_fields(dashboards=[dashboard_id])

    # Exactly one pivot widget above threshold
    assert len(result) == 1
    row = result[0]

    assert row["dashboard_id"] == dashboard_id
    assert row["dashboard_title"] == "Pivot Field Dashboard"
    assert row["widget_id"] == "W1"
    assert row["has_more_fields"] is True
    assert row["field_count"] == 21

    # At least one info-level log should exist (exact text not enforced)
    assert any(m["level"] == "info" for m in logger.messages)


def test_check_pivot_widget_fields_logs_when_no_pivot_widgets() -> None:
    logger = FakeLogger()

    dashboard_id = "D000000000000000000000000"
    endpoint = f"/api/dashboards/{dashboard_id}?adminAccess=true"

    dashboard_payload = {
        "oid": dashboard_id,
        "title": "No Pivot Dashboard",
        "widgets": [
            {
                "oid": "W1",
                "type": "chart",
                "metadata": {"panels": [{"items": [{"field": "A"}]}]},
            }
        ],
    }

    responses = {
        endpoint: FakeResponse(status_code=200, json_data=dashboard_payload),
    }

    api_client = FakeApiClient(responses=responses, logger=logger)
    dashboard = FakeDashboard(
        mapping={
            dashboard_id: {
                "dashboard_id": dashboard_id,
                "dashboard_title": "No Pivot Dashboard",
            },
            "No Pivot Dashboard": {
                "dashboard_id": dashboard_id,
                "dashboard_title": "No Pivot Dashboard",
            },
        }
    )

    wellcheck = WellCheckTestHarness(api_client=api_client, dashboard=dashboard)

    result = wellcheck.check_pivot_widget_fields(dashboards=[dashboard_id])

    # No pivot widgets => no rows
    assert result == []

    # There should be at least one info log (the "no pivot widgets" style message)
    assert any(m["level"] == "info" for m in logger.messages)


# ---------------------------------------------------------------------------
# Tests for check_datamodel_custom_tables
# ---------------------------------------------------------------------------


def test_check_datamodel_custom_tables_returns_empty_when_no_datamodels() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)

    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(mapping={})

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_custom_tables(datamodels=None)

    assert result == []
    # Ensure an error was logged about missing data model references
    assert any(
        m["level"] == "error"
        and (
            "At least one data model reference" in m["msg"]
            or "At least one datamodel reference" in m["msg"]
        )
        for m in logger.messages
    )


def test_check_datamodel_custom_tables_detects_union_in_custom_tables() -> None:
    """
    Single fake data model with:
      - 3 tables in total
      - 2 custom tables (one with UNION, one without)
      - 1 non-custom table
    """
    logger = FakeLogger()

    datamodel_id = "DM123456789012345678901234"
    endpoint = f"/api/v2/datamodels/{datamodel_id}/schema"

    schema_payload = {
        "datasets": [
            {
                "oid": "DS1",
                "schema": {
                    "tables": [
                        {
                            "name": "custom_no_union",
                            "type": "custom",
                            "expression": {
                                "expression": "SELECT * FROM orders"
                            },
                        },
                        {
                            "name": "custom_with_union",
                            "type": "custom",
                            "expression": {
                                "expression": "SELECT * FROM a UNION SELECT * FROM b"
                            },
                        },
                        {
                            "name": "physical_table",
                            "type": "physical",
                        },
                    ]
                },
            }
        ]
    }

    responses = {
        endpoint: FakeResponse(status_code=200, json_data=schema_payload),
    }

    api_client = FakeApiClient(responses=responses, logger=logger)

    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(
        mapping={
            # Resolve either by ID or by title to the same datamodel_id
            datamodel_id: {
                "datamodel_id": datamodel_id,
                "datamodel_title": "Sales Model",
            },
            "Sales Model": {
                "datamodel_id": datamodel_id,
                "datamodel_title": "Sales Model",
            },
        }
    )

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_custom_tables(datamodels=[datamodel_id])

    # Expect 2 rows, one per custom table
    assert len(result) == 2

    # Convert list to dict for easier assertions
    by_table = {row["table"]: row for row in result}

    assert by_table["custom_no_union"]["data_model"] == "Sales Model"
    assert by_table["custom_no_union"]["has_union"] == "no"

    assert by_table["custom_with_union"]["data_model"] == "Sales Model"
    assert by_table["custom_with_union"]["has_union"] == "yes"

    # Summary logs should reflect processed counts
    assert any(
        m["level"] == "info"
        and "Processed 1 data models." in m["msg"]
        for m in logger.messages
    )
    assert any(
        m["level"] == "info"
        and "Processed 3 tables." in m["msg"]
        for m in logger.messages
    )
    assert any(
        m["level"] == "info"
        and "Processed 2 custom tables." in m["msg"]
        for m in logger.messages
    )
    assert any(
        m["level"] == "info"
        and "Found 1 custom tables using 'union'." in m["msg"]
        for m in logger.messages
    )


# ---------------------------------------------------------------------------
# Tests for check_datamodel_island_tables
# ---------------------------------------------------------------------------


def test_check_datamodel_island_tables_returns_empty_when_no_datamodels() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)

    # Dashboard helper is not used here, but the harness expects it
    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(mapping={})

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_island_tables(datamodels=None)

    assert result == []
    # An error about missing datamodel references should be logged
    assert any(
        m["level"] == "error"
        and "datamodel reference" in m["msg"]
        for m in logger.messages
    )


def test_check_datamodel_island_tables_skips_unresolved_references() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)

    # No entries in mapping -> all references will fail to resolve
    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(mapping={})

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_island_tables(
        datamodels=["missing_datamodel"]
    )

    # Nothing resolved or processed
    assert result == []
    # There should be a warning about skipping the reference
    assert any(
        m["level"] == "warning"
        and "Skipping datamodel reference 'missing_datamodel'" in m["msg"]
        for m in logger.messages
    )


def test_check_datamodel_island_tables_finds_island_tables() -> None:
    """
    Single datamodel with three tables:
      - T1 and T2 appear in relations -> not islands
      - T3 never appears in relations -> island
    """
    logger = FakeLogger()

    datamodel_id = "DM123456789012345678901234"
    datamodel_title = "Test Datamodel"

    # Fake schema payload returned by /api/v2/datamodels/{id}/schema
    schema_endpoint = f"/api/v2/datamodels/{datamodel_id}/schema"

    schema_payload = {
        "oid": datamodel_id,
        "title": datamodel_title,
        "relations": [
            {
                "oid": "REL1",
                "columns": [
                    {"table": "T1"},
                    {"table": "T2"},
                ],
            }
        ],
        "datasets": [
            {
                "oid": "DS1",
                "schema": {
                    "tables": [
                        {
                            "name": "FactTable",
                            "oid": "T1",
                            "type": "fact",
                        },
                        {
                            "name": "DimTable",
                            "oid": "T2",
                            "type": "dim",
                        },
                        {
                            "name": "IslandTable",
                            "oid": "T3",
                            "type": "dim",
                        },
                    ]
                },
            }
        ],
    }

    responses = {
        schema_endpoint: FakeResponse(status_code=200, json_data=schema_payload),
    }

    api_client = FakeApiClient(responses=responses, logger=logger)

    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(
        mapping={
            datamodel_id: {
                "datamodel_id": datamodel_id,
                "datamodel_title": datamodel_title,
            },
            datamodel_title: {
                "datamodel_id": datamodel_id,
                "datamodel_title": datamodel_title,
            },
        }
    )

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_island_tables(datamodels=[datamodel_id])

    # Exactly one island table (T3) should be returned
    assert len(result) == 1
    row = result[0]

    assert row["datamodel"] == datamodel_title
    assert row["datamodel_oid"] == datamodel_id
    assert row["table"] == "IslandTable"
    assert row["table_oid"] == "T3"
    assert row["type"] == "dim"
    assert row["relation"] == "no"

    # Summary logs should mention processed datamodels and island tables
    assert any(
        m["level"] == "info"
        and "Processed 1 data models" in m["msg"]
        for m in logger.messages
    )
    assert any(
        m["level"] == "info"
        and "Island tables" in m["msg"]
        for m in logger.messages
    )


# ---------------------------------------------------------------------------
# Tests for check_datamodel_rls_datatypes
# ---------------------------------------------------------------------------


def test_check_datamodel_rls_datatypes_returns_empty_when_no_datamodels() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)

    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(mapping={})

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_rls_datatypes(datamodels=None)

    assert result == []
    # Ensure an error was logged about missing datamodel references
    assert any(
        m["level"] == "error"
        and "At least one datamodel reference" in m["msg"]
        for m in logger.messages
    )


def test_check_datamodel_rls_datatypes_skips_unresolved_references() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)

    dashboard = FakeDashboard(mapping={})
    # No entries in mapping -> all references fail to resolve
    datamodel = FakeDatamodel(mapping={})

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_rls_datatypes(
        datamodels=["missing_datamodel"]
    )

    # Nothing resolved or processed
    assert result == []
    # There should be a warning about skipping the reference
    assert any(
        m["level"] == "warning"
        and "Skipping datamodel reference 'missing_datamodel'" in m["msg"]
        for m in logger.messages
    )


def test_check_datamodel_rls_datatypes_detects_numeric_and_non_numeric() -> None:
    """
    Single datamodel with RLS rules:
      - One numeric rule
      - One non-numeric rule
      - One duplicate non-numeric rule (should be de-duplicated)
    """
    logger = FakeLogger()

    datamodel_id = "DM123456789012345678901234"
    datamodel_title = "Sales Model"
    datamodel_server = "EC1"

    # Schema payload returned by /api/v2/datamodels/{id}/schema
    schema_endpoint = f"/api/v2/datamodels/{datamodel_id}/schema"
    schema_payload = {
        "oid": datamodel_id,
        "title": datamodel_title,
        "type": "extract",
        "server": datamodel_server,
    }

    # RLS payload returned by /api/elasticubes/{server}/{title}/datasecurity
    rls_endpoint = f"/api/elasticubes/{datamodel_server}/{datamodel_title}/datasecurity"
    rls_payload = [
        {
            "table": "Orders",
            "column": "CustomerId",
            "datatype": "numeric",
        },
        {
            "table": "Orders",
            "column": "Region",
            "datatype": "string",
        },
        {
            # Duplicate of the previous rule – should be de-duplicated
            "table": "Orders",
            "column": "Region",
            "datatype": "string",
        },
    ]

    responses = {
        schema_endpoint: FakeResponse(status_code=200, json_data=schema_payload),
        rls_endpoint: FakeResponse(status_code=200, json_data=rls_payload),
    }

    api_client = FakeApiClient(responses=responses, logger=logger)

    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(
        mapping={
            # Resolve by ID
            datamodel_id: {
                "datamodel_id": datamodel_id,
                "datamodel_title": datamodel_title,
            },
            # Or by title
            datamodel_title: {
                "datamodel_id": datamodel_id,
                "datamodel_title": datamodel_title,
            },
        }
    )

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_rls_datatypes(datamodels=[datamodel_id])

    # We expect two unique rules (one numeric, one non-numeric)
    assert len(result) == 2

    by_key = {(row["table"], row["column"]): row for row in result}

    assert by_key[("Orders", "CustomerId")]["datamodel"] == datamodel_title
    assert by_key[("Orders", "CustomerId")]["datatype"] == "numeric"

    assert by_key[("Orders", "Region")]["datamodel"] == datamodel_title
    assert by_key[("Orders", "Region")]["datatype"] == "string"

    # Summary logs should reflect processed counts
    assert any(
        m["level"] == "info"
        and "Processed 1 data models." in m["msg"]
        for m in logger.messages
    )
    assert any(
        m["level"] == "info"
        and "Processed 2 data security rules." in m["msg"]
        for m in logger.messages
    )
    assert any(
        m["level"] == "info"
        and "Found 1 non-numeric data security rules." in m["msg"]
        for m in logger.messages
    )


# ---------------------------------------------------------------------------
# Tests for check_datamodel_import_queries
# ---------------------------------------------------------------------------


def test_check_datamodel_import_queries_returns_empty_when_no_datamodels() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)

    # Dashboard helper is required by the harness but not used here
    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(mapping={})

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_import_queries(datamodels=None)

    assert result == []
    # Ensure an error was logged about missing datamodel references
    assert any(
        m["level"] == "error" and "datamodel reference" in m["msg"]
        for m in logger.messages
    )


def test_check_datamodel_import_queries_skips_unresolved_references() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)

    # No entries in mapping -> all references will fail to resolve
    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(mapping={})

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_import_queries(
        datamodels=["missing_datamodel"]
    )

    # Nothing resolved or processed
    assert result == []
    # There should be a warning about skipping the reference
    assert any(
        m["level"] == "warning"
        and "Skipping datamodel reference 'missing_datamodel'" in m["msg"]
        for m in logger.messages
    )


def test_check_datamodel_import_queries_detects_import_queries() -> None:
    """
    Single datamodel with three tables:
      - table_no_import: no importQuery
      - table_with_import: has importQuery
      - table_null_config: configOptions is null
    """
    logger = FakeLogger()

    datamodel_id = "DM123456789012345678901234"
    datamodel_title = "Import Query Model"

    schema_endpoint = f"/api/v2/datamodels/{datamodel_id}/schema"

    schema_payload = {
        "oid": datamodel_id,
        "title": datamodel_title,
        "datasets": [
            {
                "oid": "DS1",
                "schema": {
                    "tables": [
                        {
                            "name": "table_no_import",
                            "configOptions": {},
                        },
                        {
                            "name": "table_with_import",
                            "configOptions": {
                                "importQuery": "SELECT * FROM some_table"
                            },
                        },
                        {
                            "name": "table_null_config",
                            "configOptions": None,
                        },
                    ]
                },
            }
        ],
    }

    responses = {
        schema_endpoint: FakeResponse(status_code=200, json_data=schema_payload),
    }

    api_client = FakeApiClient(responses=responses, logger=logger)

    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(
        mapping={
            datamodel_id: {
                "datamodel_id": datamodel_id,
                "datamodel_title": datamodel_title,
            },
            datamodel_title: {
                "datamodel_id": datamodel_id,
                "datamodel_title": datamodel_title,
            },
        }
    )

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_import_queries(datamodels=[datamodel_id])

    # We expect one row per table (3)
    assert len(result) == 3

    # Convert to dict keyed by table name for easier assertions
    by_table = {row["table"]: row for row in result}

    assert by_table["table_no_import"]["data_model"] == datamodel_title
    assert by_table["table_no_import"]["has_import_query"] == "no"

    assert by_table["table_with_import"]["data_model"] == datamodel_title
    assert by_table["table_with_import"]["has_import_query"] == "yes"

    assert by_table["table_null_config"]["data_model"] == datamodel_title
    # configOptions is null -> still included, but with "no"
    assert by_table["table_null_config"]["has_import_query"] == "no"

    # Summary logs should reflect processed counts
    assert any(
        m["level"] == "info"
        and "Processed 1 data models." in m["msg"]
        for m in logger.messages
    )
    assert any(
        m["level"] == "info"
        and "Processed 3 tables." in m["msg"]
        for m in logger.messages
    )
    assert any(
        m["level"] == "info"
        and "Found 1 tables with import queries." in m["msg"]
        for m in logger.messages
    )


# ---------------------------------------------------------------------------
# Tests for check_datamodel_m2m_relationships
# ---------------------------------------------------------------------------


def test_check_datamodel_m2m_relationships_returns_empty_when_no_datamodels() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)

    # Dashboard is unused here but required by the harness
    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(mapping={})

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_m2m_relationships(datamodels=None)

    assert result == []
    assert any(
        m["level"] == "error"
        and "At least one datamodel reference" in m["msg"]
        for m in logger.messages
    )


def test_check_datamodel_m2m_relationships_skips_unresolved_references() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)

    # No entries in mapping -> all references fail to resolve
    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(mapping={})

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_m2m_relationships(
        datamodels=["missing_datamodel"]
    )

    assert result == []
    assert any(
        m["level"] == "warning"
        and "Skipping datamodel reference 'missing_datamodel'" in m["msg"]
        for m in logger.messages
    )


def test_check_datamodel_m2m_relationships_detects_m2m_pairs() -> None:
    """
    Single datamodel with one relation between:
      - LeftTable.LeftKey
      - RightTable.RightKey

    Both sides return > 1 duplicate key row from the aggregate queries,
    so the pair should be flagged as many-to-many (is_m2m=True).
    """
    logger = FakeLogger()

    datamodel_id = "DM1"
    datamodel_title = "Sales Model"

    # Endpoints used by the implementation
    relations_endpoint = f"/api/v2/datamodels/{datamodel_id}/schema/relations"
    left_table_endpoint = (
        f"/api/v2/datamodels/{datamodel_id}/schema/datasets/DS1/tables/T1"
    )
    right_table_endpoint = (
        f"/api/v2/datamodels/{datamodel_id}/schema/datasets/DS2/tables/T2"
    )
    datasource_endpoint = f"/api/datasources/{datamodel_title}/sql"

    # 1) Relations payload: a single relation with two columns (one left, one right)
    relations_payload = [
        {
            "oid": "REL1",
            "columns": [
                {"dataset": "DS1", "table": "T1", "column": "C1"},
                {"dataset": "DS2", "table": "T2", "column": "C2"},
            ],
        }
    ]

    # 2) Table details payloads for left and right tables
    left_table_payload = {
        "name": "LeftTable",
        "columns": [
            {"oid": "C1", "name": "LeftKey"},
        ],
    }
    right_table_payload = {
        "name": "RightTable",
        "columns": [
            {"oid": "C2", "name": "RightKey"},
        ],
    }

    # 3) CSV responses for the aggregate queries
    #    Two data rows + header => count = 2 (> 1) for each side.
    left_query = (
        "select [LeftKey], count([LeftKey]) as key_count1 "
        "from [LeftTable] "
        "group by [LeftKey] "
        "having count([LeftKey]) > 1"
    )
    right_query = (
        "select [RightKey], count([RightKey]) as key_count2 "
        "from [RightTable] "
        "group by [RightKey] "
        "having count([RightKey]) > 1"
    )

    left_csv_resp = FakeResponse(status_code=200, json_data={})
    left_csv_resp.text = "LeftKey,key_count1\nA,2\nB,3\n"

    right_csv_resp = FakeResponse(status_code=200, json_data={})
    right_csv_resp.text = "RightKey,key_count2\nX,2\nY,4\n"

    # We key responses by (endpoint, query) where query is None for non-SQL calls
    responses = {
        (relations_endpoint, None): FakeResponse(
            status_code=200, json_data=relations_payload
        ),
        (left_table_endpoint, None): FakeResponse(
            status_code=200, json_data=left_table_payload
        ),
        (right_table_endpoint, None): FakeResponse(
            status_code=200, json_data=right_table_payload
        ),
        (datasource_endpoint, left_query): left_csv_resp,
        (datasource_endpoint, right_query): right_csv_resp,
    }

    class FakeApiClientWithParams:
        """
        Local fake API client that supports the `params` argument used by
        the M2M check when calling the SQL endpoint.
        """

        def __init__(self, responses, logger) -> None:
            self._responses = responses
            self.logger = logger

        def get(self, endpoint, params=None):
            query = None
            if params and "query" in params:
                query = params["query"]
            return self._responses.get((endpoint, query))

    api_client = FakeApiClientWithParams(responses=responses, logger=logger)

    # Dashboard helper is unused for this test
    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(
        mapping={
            datamodel_id: {
                "datamodel_id": datamodel_id,
                "datamodel_title": datamodel_title,
            },
            datamodel_title: {
                "datamodel_id": datamodel_id,
                "datamodel_title": datamodel_title,
            },
        }
    )

    wellcheck = WellCheckTestHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
    )

    result = wellcheck.check_datamodel_m2m_relationships(datamodels=[datamodel_id])

    # Exactly one relation pair -> one row in the result
    assert len(result) == 1
    row = result[0]

    assert row["data_model"] == datamodel_title
    assert row["left_table"] == "LeftTable"
    assert row["left_column"] == "LeftKey"
    assert row["right_table"] == "RightTable"
    assert row["right_column"] == "RightKey"
    assert row["is_m2m"] is True

    # Check that the original print-style info log was preserved
    assert any(
        m["level"] == "info"
        and datamodel_title in m["msg"]
        and "LeftTable" in m["msg"]
        and "RightTable" in m["msg"]
        for m in logger.messages
    )

    # Summary logs should mention processed datamodels and M2M count
    assert any(
        m["level"] == "info"
        and "Processed 1 data models" in m["msg"]
        for m in logger.messages
    )
    assert any(
        m["level"] == "info"
        and "Found 1 many-to-many relationships" in m["msg"]
        for m in logger.messages
    )


# ---------------------------------------------------------------------------
# Tests for run_full_wellcheck
# ---------------------------------------------------------------------------


def test_run_full_wellcheck_aggregates_results_and_invokes_subchecks() -> None:
    logger = FakeLogger()
    api_client = FakeApiClient(responses={}, logger=logger)

    # Dashboard/datamodel helpers (not actually used by the stubbed methods,
    # but required by the harness constructor)
    dashboard = FakeDashboard(mapping={})
    datamodel = FakeDatamodel(mapping={})

    class FakeAccessManagement:
        def __init__(self) -> None:
            self.called_with: Optional[List[str]] = None

        def get_unused_columns_bulk(
            self,
            datamodels: Optional[List[str]] = None,
        ) -> List[Dict[str, Any]]:
            self.called_with = datamodels
            # Return a simple stub row
            return [
                {
                    "datamodel_name": "DM1",
                    "table": "T_unused",
                    "column": "C_unused",
                    "used": False,
                }
            ]

    class FullWellCheckHarness(WellCheckTestHarness):
        """
        Harness that overrides all sub-check methods so the test can
        verify calls and control the returned data.
        """

        def __init__(
            self,
            api_client: FakeApiClient,
            dashboard: FakeDashboard,
            datamodel: FakeDatamodel,
            access_mgmt: FakeAccessManagement,
        ) -> None:
            super().__init__(api_client=api_client, dashboard=dashboard, datamodel=datamodel)
            # IMPORTANT: Match the attribute name used in WellCheck.run_full_wellcheck
            self.access_mgmt = access_mgmt

            # Track what each sub-check was called with
            self.structure_called_with: Optional[List[str]] = None
            self.widget_counts_called_with: Optional[List[str]] = None
            self.pivot_fields_called_with: Optional[Dict[str, Any]] = None
            self.custom_tables_called_with: Optional[List[str]] = None
            self.island_tables_called_with: Optional[List[str]] = None
            self.rls_called_with: Optional[List[str]] = None
            self.import_queries_called_with: Optional[List[str]] = None
            self.m2m_called_with: Optional[List[str]] = None

        # Dashboard-level checks
        def check_dashboard_structure(
            self,
            dashboards: Optional[List[str]] = None,
        ) -> List[Dict[str, Any]]:
            self.structure_called_with = dashboards
            return [{"dashboard_id": "D1", "pivot_count": 1}]

        def check_dashboard_widget_counts(
            self,
            dashboards: Optional[List[str]] = None,
        ) -> List[Dict[str, Any]]:
            self.widget_counts_called_with = dashboards
            return [{"dashboard_id": "D1", "widget_count": 3}]

        def check_pivot_widget_fields(
            self,
            dashboards: Optional[List[str]] = None,
            max_fields: int = 20,
        ) -> List[Dict[str, Any]]:
            self.pivot_fields_called_with = {
                "dashboards": dashboards,
                "max_fields": max_fields,
            }
            return [
                {
                    "dashboard_id": "D1",
                    "widget_id": "W1",
                    "has_more_fields": True,
                    "field_count": 25,
                }
            ]

        # Datamodel-level checks
        def check_datamodel_custom_tables(
            self,
            datamodels: Optional[List[str]] = None,
        ) -> List[Dict[str, Any]]:
            self.custom_tables_called_with = datamodels
            return [
                {"data_model": "DM1", "table": "T_custom", "has_union": "no"}
            ]

        def check_datamodel_island_tables(
            self,
            datamodels: Optional[List[str]] = None,
        ) -> List[Dict[str, Any]]:
            self.island_tables_called_with = datamodels
            return [
                {
                    "datamodel": "DM1",
                    "datamodel_oid": "DM1",
                    "table": "IslandTable",
                    "table_oid": "T3",
                    "type": "dim",
                    "relation": "no",
                }
            ]

        def check_datamodel_rls_datatype(
            self,
            datamodels: Optional[List[str]] = None,
        ) -> List[Dict[str, Any]]:
            self.rls_called_with = datamodels
            return [
                {
                    "datamodel": "DM1",
                    "table": "T_rls",
                    "column": "C_rls",
                    "datatype": "string",
                }
            ]

        def check_datamodel_import_queries(
            self,
            datamodels: Optional[List[str]] = None,
        ) -> List[Dict[str, Any]]:
            self.import_queries_called_with = datamodels
            return [
                {"data_model": "DM1", "table": "T_import", "has_import_query": "yes"}
            ]

        def check_datamodel_m2m_relationships(
            self,
            datamodels: Optional[List[str]] = None,
        ) -> List[Dict[str, Any]]:
            self.m2m_called_with = datamodels
            return [
                {
                    "datamodel": "DM1",
                    "left_table": "Fact",
                    "left_column": "FactId",
                    "right_table": "Dim",
                    "right_column": "FactId",
                    "is_m2m": True,
                }
            ]

    access_mgmt = FakeAccessManagement()
    wellcheck = FullWellCheckHarness(
        api_client=api_client,
        dashboard=dashboard,
        datamodel=datamodel,
        access_mgmt=access_mgmt,
    )

    dashboards_input = ["DashA", "DashB"]
    datamodels_input = ["DM1", "DM2"]

    report = wellcheck.run_full_wellcheck(
        dashboards=dashboards_input,
        datamodels=datamodels_input,
        max_pivot_fields=30,
    )

    # ------------------------------------------------------------------
    # Verify sub-checks were invoked with normalized arguments
    # ------------------------------------------------------------------
    assert set(wellcheck.structure_called_with or []) == set(dashboards_input)
    assert set(wellcheck.widget_counts_called_with or []) == set(dashboards_input)

    assert wellcheck.pivot_fields_called_with is not None
    assert set(wellcheck.pivot_fields_called_with["dashboards"] or []) == set(
        dashboards_input
    )
    assert wellcheck.pivot_fields_called_with["max_fields"] == 30

    assert set(wellcheck.custom_tables_called_with or []) == set(datamodels_input)
    assert set(wellcheck.island_tables_called_with or []) == set(datamodels_input)
    assert set(wellcheck.rls_called_with or []) == set(datamodels_input)
    assert set(wellcheck.import_queries_called_with or []) == set(datamodels_input)
    assert set(wellcheck.m2m_called_with or []) == set(datamodels_input)

    # Unused columns delegated to AccessManagement
    assert set(access_mgmt.called_with or []) == set(datamodels_input)

    # ------------------------------------------------------------------
    # Verify report structure and contents
    # ------------------------------------------------------------------
    assert "dashboards" in report
    assert "datamodels" in report

    dashboards_section = report["dashboards"]
    datamodels_section = report["datamodels"]

    # Dashboard subsections
    assert dashboards_section["structure"] == [{"dashboard_id": "D1", "pivot_count": 1}]
    assert dashboards_section["widget_counts"] == [
        {"dashboard_id": "D1", "widget_count": 3}
    ]
    assert dashboards_section["pivot_widget_fields"] == [
        {
            "dashboard_id": "D1",
            "widget_id": "W1",
            "has_more_fields": True,
            "field_count": 25,
        }
    ]

    # Datamodel subsections
    assert datamodels_section["custom_tables"] == [
        {"data_model": "DM1", "table": "T_custom", "has_union": "no"}
    ]
    assert datamodels_section["island_tables"][0]["table"] == "IslandTable"
    assert datamodels_section["rls_datatypes"][0]["column"] == "C_rls"
    assert datamodels_section["import_queries"][0]["has_import_query"] == "yes"
    assert datamodels_section["m2m_relationships"][0]["is_m2m"] is True

    # Unused columns subsection – comes from FakeAccessManagement
    assert datamodels_section["unused_columns"] == [
        {
            "datamodel_name": "DM1",
            "table": "T_unused",
            "column": "C_unused",
            "used": False,
        }
    ]
