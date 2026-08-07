"""Unit tests for pysisense.report_manager.ReportManager."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.report_manager import ReportManager

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------

_REPORT = {
    "_id": "5A929ac648c9EcebAf0DE08e",
    "name": "Weekly Sales",
    "owner": "85A8bb184e1ec985b7F4b877",
    "enabled": True,
    "priority": "normal",
}

_REPORTS_PAGE_LAST = {
    "data": [_REPORT],
    "pagination": {"page": 1, "limit": 100, "isLastPage": True, "totalCount": 1},
}

_NOT_FOUND = FakeResponse(404, {"error": {"code": 0, "message": "Report not found", "status": 404}})
_PLUGIN_DISABLED = FakeResponse(404, None, content=b"")
_SERVICE_UNAVAILABLE = FakeResponse(504, "Gateway Timeout")

# Report Manager's schema validation requires all three keys, even though
# only one format may actually be enabled.
_VALID_REPORT_TYPE = {"PDF": True, "CSV": False, "URL": False}

# Report Manager's schema validation requires overwriteExisting to be
# present, even when the report has no file-share destination configured.
_VALID_RUN_ON_FINISH = {"fileShare": {"overwriteExisting": False}}


def _make_report_manager(get_responses=None, post_responses=None, patch_responses=None, delete_responses=None):
    """Build a ReportManager instance backed by FakeApiClient."""
    logger = FakeLogger()
    client = FakeApiClient(
        get_responses=get_responses,
        post_responses=post_responses,
        patch_responses=patch_responses,
        delete_responses=delete_responses,
        logger=logger,
    )
    return ReportManager(api_client=client)


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------


class TestReportManagerInit:
    def test_creates_with_fake_client(self):
        rm = _make_report_manager()
        assert rm is not None
        assert hasattr(rm, "api_client")
        assert hasattr(rm, "logger")


# ---------------------------------------------------------------------------
# get_reports
# ---------------------------------------------------------------------------


class TestGetReports:
    def test_returns_list_on_success(self):
        rm = _make_report_manager(get_responses={"/api/v1/report_manager/reports": FakeResponse(200, _REPORTS_PAGE_LAST)})
        result = rm.get_reports()
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["name"] == "Weekly Sales"

    def test_paginates_across_multiple_pages(self):
        page_1 = {"data": [_REPORT], "pagination": {"page": 1, "isLastPage": False}}
        page_2 = {"data": [dict(_REPORT, _id="other-id")], "pagination": {"page": 2, "isLastPage": True}}
        calls = {"n": 0}

        class _PagedClient(FakeApiClient):
            def get(self, url, params=None, **kwargs):
                calls["n"] += 1
                return FakeResponse(200, page_1 if calls["n"] == 1 else page_2)

        rm = ReportManager(api_client=_PagedClient(logger=FakeLogger()))
        result = rm.get_reports()
        assert len(result) == 2
        assert calls["n"] == 2

    def test_returns_error_on_none_response(self):
        rm = _make_report_manager()  # no responses configured -> None
        result = rm.get_reports()
        assert isinstance(result, dict)
        assert "error" in result

    def test_returns_error_when_plugin_not_enabled(self):
        rm = _make_report_manager(get_responses={"/api/v1/report_manager/reports": _PLUGIN_DISABLED})
        result = rm.get_reports()
        assert "error" in result
        assert "Report Manager plugin" in result["error"]

    def test_returns_error_when_service_unavailable(self):
        rm = _make_report_manager(get_responses={"/api/v1/report_manager/reports": _SERVICE_UNAVAILABLE})
        result = rm.get_reports()
        assert "error" in result
        assert "unavailable" in result["error"]
        assert "Report Manager plugin" not in result["error"]


# ---------------------------------------------------------------------------
# get_report
# ---------------------------------------------------------------------------


class TestGetReport:
    def test_returns_report_on_success(self):
        rm = _make_report_manager(get_responses={"/api/v1/report_manager/reports/5A929ac648c9EcebAf0DE08e": FakeResponse(200, _REPORT)})
        result = rm.get_report("5A929ac648c9EcebAf0DE08e")
        assert result["name"] == "Weekly Sales"

    def test_returns_error_when_not_found(self):
        rm = _make_report_manager(get_responses={"/api/v1/report_manager/reports/missing": _NOT_FOUND})
        result = rm.get_report("missing")
        assert "error" in result

    def test_genuine_not_found_does_not_hint_plugin_disabled(self):
        # A 404 with Report Manager's own structured error body is a real
        # "not found" (the plugin handled the request) — not a signal that
        # the plugin itself is missing.
        rm = _make_report_manager(get_responses={"/api/v1/report_manager/reports/missing": _NOT_FOUND})
        result = rm.get_report("missing")
        assert "Report Manager plugin" not in result["error"]

    def test_returns_error_when_plugin_not_enabled(self):
        rm = _make_report_manager(get_responses={"/api/v1/report_manager/reports/abc": _PLUGIN_DISABLED})
        result = rm.get_report("abc")
        assert "error" in result
        assert "Report Manager plugin" in result["error"]


# ---------------------------------------------------------------------------
# create_report
# ---------------------------------------------------------------------------


class TestCreateReport:
    def test_creates_report_on_success(self):
        rm = _make_report_manager(post_responses={"/api/v1/report_manager/reports": FakeResponse(201, "created-id")})
        result = rm.create_report({"name": "Weekly Sales", "reportType": _VALID_REPORT_TYPE, "runOnFinish": _VALID_RUN_ON_FINISH})
        assert result == {"result": "created-id"}

    def test_defaults_omitted_fields_when_creating(self):
        captured = {}

        class _CapturingClient(FakeApiClient):
            def post(self, url, data=None, **kwargs):
                captured["data"] = data
                return FakeResponse(201, "created-id")

        rm = ReportManager(api_client=_CapturingClient(logger=FakeLogger()))
        rm.create_report({"name": "Weekly Sales"})
        sent = captured["data"][0]
        assert sent["events"] == []
        assert sent["reportType"] == {"PDF": False, "CSV": False, "URL": False}
        assert sent["runOnFinish"] == {"fileShare": {"overwriteExisting": False}}

    def test_returns_error_for_invalid_payload(self):
        rm = _make_report_manager()
        result = rm.create_report({"unknownField": "x"})
        assert "error" in result

    def test_returns_error_for_missing_required_name(self):
        rm = _make_report_manager()
        result = rm.create_report({"enabled": True, "reportType": _VALID_REPORT_TYPE, "runOnFinish": _VALID_RUN_ON_FINISH})
        assert "error" in result

    def test_returns_error_for_incomplete_report_type(self):
        # Report Manager's schema requires PDF, CSV, and URL all to be
        # present when reportType is provided — supplying only one is not enough.
        rm = _make_report_manager()
        result = rm.create_report({"name": "Weekly Sales", "reportType": {"PDF": True}, "runOnFinish": _VALID_RUN_ON_FINISH})
        assert "error" in result

    def test_returns_error_for_incomplete_run_on_finish(self):
        # overwriteExisting must be present when runOnFinish is provided,
        # even with no fileShare destination.
        rm = _make_report_manager()
        result = rm.create_report({"name": "Weekly Sales", "reportType": _VALID_REPORT_TYPE, "runOnFinish": {"fileShare": {}}})
        assert "error" in result

    def test_returns_error_when_plugin_not_enabled(self):
        rm = _make_report_manager(post_responses={"/api/v1/report_manager/reports": _PLUGIN_DISABLED})
        result = rm.create_report({"name": "Weekly Sales", "reportType": _VALID_REPORT_TYPE, "runOnFinish": _VALID_RUN_ON_FINISH})
        assert "error" in result
        assert "Report Manager plugin" in result["error"]


# ---------------------------------------------------------------------------
# update_report
# ---------------------------------------------------------------------------


class TestUpdateReport:
    def test_updates_report_on_success(self):
        rm = _make_report_manager(patch_responses={"/api/v1/report_manager/reports/abc": FakeResponse(200, "ok")})
        result = rm.update_report("abc", {"enabled": False})
        assert result == {"result": "ok"}

    def test_no_fields_provided_makes_no_call(self):
        rm = _make_report_manager()
        result = rm.update_report("abc", {})
        assert result == {"success": True, "changed": False}

    def test_returns_error_for_invalid_payload(self):
        rm = _make_report_manager()
        result = rm.update_report("abc", {"unknownField": "x"})
        assert "error" in result

    def test_returns_error_when_not_found(self):
        rm = _make_report_manager(patch_responses={"/api/v1/report_manager/reports/missing": _NOT_FOUND})
        result = rm.update_report("missing", {"enabled": False})
        assert "error" in result

    def test_returns_error_when_plugin_not_enabled(self):
        rm = _make_report_manager(patch_responses={"/api/v1/report_manager/reports/abc": _PLUGIN_DISABLED})
        result = rm.update_report("abc", {"enabled": False})
        assert "error" in result
        assert "Report Manager plugin" in result["error"]


# ---------------------------------------------------------------------------
# delete_report
# ---------------------------------------------------------------------------


class TestDeleteReport:
    def test_deletes_report_on_success(self):
        rm = _make_report_manager(delete_responses={"/api/v1/report_manager/reports/abc": FakeResponse(204, None)})
        result = rm.delete_report("abc")
        assert result == {"success": True}

    def test_returns_error_when_not_found(self):
        rm = _make_report_manager(delete_responses={"/api/v1/report_manager/reports/missing": _NOT_FOUND})
        result = rm.delete_report("missing")
        assert "error" in result

    def test_returns_error_when_plugin_not_enabled(self):
        rm = _make_report_manager(delete_responses={"/api/v1/report_manager/reports/abc": _PLUGIN_DISABLED})
        result = rm.delete_report("abc")
        assert "error" in result
        assert "Report Manager plugin" in result["error"]


# ---------------------------------------------------------------------------
# run_report
# ---------------------------------------------------------------------------


class TestRunReport:
    def test_runs_report_on_success(self):
        rm = _make_report_manager(post_responses={"/api/v1/report_manager/reports/abc/run": FakeResponse(200, {"success": True})})
        result = rm.run_report("abc")
        assert result == {"success": True}

    def test_returns_error_when_permission_denied(self):
        rm = _make_report_manager(post_responses={"/api/v1/report_manager/reports/abc/run": FakeResponse(403, {"error": "forbidden"})})
        result = rm.run_report("abc")
        assert "error" in result

    def test_returns_error_when_plugin_not_enabled(self):
        rm = _make_report_manager(post_responses={"/api/v1/report_manager/reports/abc/run": _PLUGIN_DISABLED})
        result = rm.run_report("abc")
        assert "error" in result
        assert "Report Manager plugin" in result["error"]
