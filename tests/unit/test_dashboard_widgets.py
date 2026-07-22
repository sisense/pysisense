"""Unit tests for pysisense.dashboard.widgets (DashboardWidgetsMixin)."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.dashboard import Dashboard


class FakeResponseEmpty(FakeResponse):
    """FakeResponse with an empty body — simulates a 200 with no JSON content."""

    def __init__(self, status_code: int) -> None:
        super().__init__(status_code, None)
        self.content = b""


# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------

_DASHBOARD = {
    "oid": "dash123",
    "title": "Sales Report",
    "owner": "owner_id",
    "shares": [],
}

_CHART_WIDGET = {
    "oid": "widget456",
    "_id": "widget456",
    "owner": "owner_id",
    "userId": "owner_id",
    "title": "Chart Widget",
    "type": "chart",
    "created": "2024-01-01",
    "lastUpdated": "2024-01-02",
    "instanceType": "chart",
    "dashboardid": "dash123",
    "metadata": {"panels": []},
}

_BLOX_WIDGET = {
    **_CHART_WIDGET,
    "oid": "widget_blox",
    "title": "BloX Widget",
    "type": "BloX",
    "instanceType": "BloX",
}

_CHART_WIDGET_2 = {
    **_CHART_WIDGET,
    "oid": "widget789",
    "title": "Chart Widget 2",
}

_SERVER_MANAGED = {"oid", "_id", "owner", "userId", "created", "lastUpdated", "instanceType", "dashboardid"}


def _make_dash(get_responses=None, post_responses=None, put_responses=None, patch_responses=None):
    logger = FakeLogger()
    client = FakeApiClient(
        get_responses=get_responses,
        post_responses=post_responses,
        put_responses=put_responses,
        patch_responses=patch_responses,
        logger=logger,
    )
    return Dashboard(api_client=client)


# ---------------------------------------------------------------------------
# get_widget_by_id
# ---------------------------------------------------------------------------


class TestGetWidgetById:
    def test_returns_widget_on_success(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _CHART_WIDGET)})
        result = dash.get_widget_by_id("dash123", "widget456")
        assert result["oid"] == "widget456"
        assert result["type"] == "chart"

    def test_returns_error_on_none_response(self):
        dash = _make_dash()
        result = dash.get_widget_by_id("dash123", "widget456")
        assert "error" in result

    def test_returns_error_on_404(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(404, {"message": "not found"})})
        result = dash.get_widget_by_id("dash123", "widget456")
        assert "error" in result

    def test_returns_error_on_403(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(403, {"message": "forbidden"})})
        result = dash.get_widget_by_id("dash123", "widget456")
        assert "error" in result

    def test_admin_access_true_by_default(self):
        # Default is admin_access=True; FakeApiClient strips query params so the same key resolves
        dash = _make_dash(get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _CHART_WIDGET)})
        result = dash.get_widget_by_id("dash123", "widget456")
        assert "error" not in result

    def test_admin_access_false_resolves_same_key(self):
        # With admin_access=False the ?adminAccess param is omitted; FakeApiClient matches by base path
        dash = _make_dash(get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _CHART_WIDGET)})
        result = dash.get_widget_by_id("dash123", "widget456", admin_access=False)
        assert result["oid"] == "widget456"


# ---------------------------------------------------------------------------
# update_widget
# ---------------------------------------------------------------------------


class _TrackingPutClient(FakeApiClient):
    """FakeApiClient that records the data passed to put()."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.put_payloads: list[dict] = []

    def put(self, url, data=None, **kwargs):
        self.put_payloads.append(dict(data or {}))
        return FakeResponse(200, {"title": "ok"})


class TestUpdateWidget:
    def test_returns_updated_widget_on_success(self):
        updated = {**_CHART_WIDGET, "title": "Updated Title"}
        dash = _make_dash(put_responses={"/api/dashboards/dash123/widgets/widget456": FakeResponse(200, updated)})
        result = dash.update_widget("dash123", "widget456", dict(_CHART_WIDGET))
        assert "error" not in result
        assert result["title"] == "Updated Title"

    def test_returns_success_dict_when_response_has_no_body(self):
        # Sisense returns 200 with an empty body on some instances/versions
        dash = _make_dash(put_responses={"/api/dashboards/dash123/widgets/widget456": FakeResponseEmpty(200)})
        result = dash.update_widget("dash123", "widget456", dict(_CHART_WIDGET))
        assert result == {"success": True}

    def test_strips_all_server_managed_fields(self):
        logger = FakeLogger()
        client = _TrackingPutClient(logger=logger)
        dash = Dashboard(api_client=client)

        dash.update_widget("dash123", "widget456", dict(_CHART_WIDGET))

        assert len(client.put_payloads) == 1
        payload = client.put_payloads[0]
        for field in _SERVER_MANAGED:
            assert field not in payload, f"Server-managed field '{field}' should have been stripped"

    def test_preserves_non_managed_fields(self):
        logger = FakeLogger()
        client = _TrackingPutClient(logger=logger)
        dash = Dashboard(api_client=client)

        widget = {**_CHART_WIDGET, "title": "Keep This", "metadata": {"panels": [{"key": "val"}]}}
        dash.update_widget("dash123", "widget456", widget)

        payload = client.put_payloads[0]
        assert payload["title"] == "Keep This"
        assert payload["metadata"] == {"panels": [{"key": "val"}]}

    def test_returns_error_on_none_response(self):
        dash = _make_dash()
        result = dash.update_widget("dash123", "widget456", dict(_CHART_WIDGET))
        assert "error" in result

    def test_returns_error_on_non_200(self):
        dash = _make_dash(put_responses={"/api/dashboards/dash123/widgets/widget456": FakeResponse(403, {"message": "forbidden"})})
        result = dash.update_widget("dash123", "widget456", dict(_CHART_WIDGET))
        assert "error" in result

    def test_handles_widget_without_server_managed_fields(self):
        minimal = {"title": "Minimal", "type": "chart"}
        dash = _make_dash(put_responses={"/api/dashboards/dash123/widgets/widget456": FakeResponse(200, minimal)})
        result = dash.update_widget("dash123", "widget456", minimal)
        assert "error" not in result


# ---------------------------------------------------------------------------
# find_widgets_by_type
# ---------------------------------------------------------------------------


class TestFindWidgetsByType:
    def test_finds_matching_widgets_across_all_dashboards(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/dash123/widgets": FakeResponse(200, [_CHART_WIDGET, _BLOX_WIDGET]),
            }
        )
        results = dash.find_widgets_by_type("BloX")
        assert len(results) == 1
        assert results[0]["widget_type"] == "BloX"
        assert results[0]["widget_id"] == "widget_blox"

    def test_does_not_return_non_matching_types(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/dash123/widgets": FakeResponse(200, [_CHART_WIDGET, _BLOX_WIDGET]),
            }
        )
        results = dash.find_widgets_by_type("pivot")
        assert results == []

    def test_accepts_bare_string_as_single_dashboard(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/dash123/widgets": FakeResponse(200, [_CHART_WIDGET]),
            }
        )
        results = dash.find_widgets_by_type("chart", dashboards="Sales Report")
        assert len(results) == 1

    def test_accepts_dashboard_list(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/dash123/widgets": FakeResponse(200, [_CHART_WIDGET, _CHART_WIDGET_2]),
            }
        )
        results = dash.find_widgets_by_type("chart", dashboards=["Sales Report"])
        assert len(results) == 2

    def test_max_results_caps_output(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/dash123/widgets": FakeResponse(200, [_CHART_WIDGET, _CHART_WIDGET_2]),
            }
        )
        results = dash.find_widgets_by_type("chart", max_results=1)
        assert len(results) == 1

    def test_skips_dashboard_when_widget_fetch_fails(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                # No widget endpoint → None → dashboard skipped
            }
        )
        results = dash.find_widgets_by_type("chart")
        assert results == []

    def test_returns_empty_when_dashboard_list_fetch_fails(self):
        dash = _make_dash()  # No responses → None
        results = dash.find_widgets_by_type("chart")
        assert results == []

    def test_result_contains_expected_keys(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/dash123/widgets": FakeResponse(200, [_CHART_WIDGET]),
            }
        )
        results = dash.find_widgets_by_type("chart")
        assert len(results) == 1
        assert set(results[0].keys()) == {"dashboard_id", "dashboard_title", "widget_id", "widget_title", "widget_type"}

    def test_populates_result_fields_correctly(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/dash123/widgets": FakeResponse(200, [_CHART_WIDGET]),
            }
        )
        results = dash.find_widgets_by_type("chart")
        r = results[0]
        assert r["dashboard_id"] == "dash123"
        assert r["dashboard_title"] == "Sales Report"
        assert r["widget_id"] == "widget456"
        assert r["widget_title"] == "Chart Widget"
        assert r["widget_type"] == "chart"

    def test_skips_unresolvable_dashboard_reference(self):
        # When a dashboard reference cannot be resolved, it is skipped
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, []),  # No dashboards → resolve fails
            }
        )
        results = dash.find_widgets_by_type("chart", dashboards=["NonExistentDash"])
        assert results == []

    def test_admin_access_false_uses_non_admin_dashboard_list(self):
        # With admin_access=False the enumeration falls back to the user-visible list
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/dash123/widgets": FakeResponse(200, [_CHART_WIDGET]),
            }
        )
        results = dash.find_widgets_by_type("chart", admin_access=False)
        assert len(results) == 1

    def test_admin_access_true_enumerates_via_admin_endpoint(self):
        # The default enumeration must hit the admin list, not the user-visible list
        class _RecordingClient(FakeApiClient):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.get_urls: list[str] = []

            def get(self, url, **kwargs):
                self.get_urls.append(url)
                return super().get(url, **kwargs)

        client = _RecordingClient(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/dash123/widgets": FakeResponse(200, [_CHART_WIDGET]),
            },
            logger=FakeLogger(),
        )
        dash = Dashboard(api_client=client)
        results = dash.find_widgets_by_type("chart")
        assert len(results) == 1
        assert client.get_urls[0] == "/api/v1/dashboards/admin?dashboardType=owner"
