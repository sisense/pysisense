"""Unit tests for pysisense.dashboard.Dashboard."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

import pysisense.dashboard.scripts as scripts_module
from pysisense.dashboard import Dashboard
from pysisense.dashboard.scripts import SisenseScript

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------
_DASHBOARD = {
    "oid": "dash123",
    "title": "Sales Report",
    "owner": "owner_id",
    "shares": [],
    "widgets": [],
    "filters": [],
    "layout": {},
}

_USER = {
    "_id": "user123",
    "userName": "jdoe",
    "firstName": "John",
    "lastName": "Doe",
    "email": "jdoe@example.com",
    "active": True,
    "role": {"_id": "role1", "name": "consumer"},
    "groups": [],
    "roleId": "role1",
}


def _make_dash(get_responses=None, post_responses=None, put_responses=None, patch_responses=None):
    """Build a Dashboard backed by FakeApiClient."""
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
# Init
# ---------------------------------------------------------------------------


class TestDashboardInit:
    def test_creates_with_fake_client(self):
        dash = _make_dash()
        assert dash is not None
        assert hasattr(dash, "api_client")
        assert hasattr(dash, "access_mgmt")
        assert hasattr(dash, "logger")


# ---------------------------------------------------------------------------
# get_all_dashboards
# ---------------------------------------------------------------------------


class TestGetAllDashboards:
    def test_returns_list_on_success(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD])})
        result = dash.get_all_dashboards()
        assert isinstance(result, list)
        assert result[0]["oid"] == "dash123"

    def test_returns_error_dict_on_none_response(self):
        dash = _make_dash()  # no responses → None
        result = dash.get_all_dashboards()
        assert "error" in result

    def test_returns_error_dict_on_non_200(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(403, {"message": "forbidden"})})
        result = dash.get_all_dashboards()
        assert "error" in result


# ---------------------------------------------------------------------------
# get_dashboard_by_id
# ---------------------------------------------------------------------------


class TestGetDashboardById:
    def test_returns_dashboard_on_success(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD])})
        result = dash.get_dashboard_by_id("dash123")
        assert isinstance(result, list)
        assert result[0]["oid"] == "dash123"

    def test_returns_error_on_none_response(self):
        dash = _make_dash()
        result = dash.get_dashboard_by_id("dash123")
        assert "error" in result

    def test_returns_error_when_empty_result(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [])})
        result = dash.get_dashboard_by_id("dash123")
        assert "error" in result


# ---------------------------------------------------------------------------
# get_dashboard_by_name
# ---------------------------------------------------------------------------


class TestGetDashboardByName:
    def test_returns_dashboard_on_success(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD])})
        result = dash.get_dashboard_by_name("Sales Report")
        assert isinstance(result, list)
        assert result[0]["title"] == "Sales Report"

    def test_returns_error_when_empty_result(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [])})
        result = dash.get_dashboard_by_name("NoSuchDash")
        assert "error" in result

    def test_returns_error_on_none_response(self):
        dash = _make_dash()
        result = dash.get_dashboard_by_name("Sales Report")
        assert "error" in result


# ---------------------------------------------------------------------------
# add_dashboard_script
# ---------------------------------------------------------------------------


class TestAddDashboardScript:
    def test_returns_success_string_on_put_200(self):
        script = '{"script": "console.log(1);"}'
        dash = _make_dash(put_responses={"/api/dashboards/dash123": FakeResponse(200, {})})
        result = dash.add_dashboard_script("dash123", script)
        assert "successfully" in result.lower()

    def test_returns_error_string_on_put_failure(self):
        script = '{"script": "console.log(1);"}'
        dash = _make_dash(put_responses={"/api/dashboards/dash123": FakeResponse(500, {"error": "fail"})})
        result = dash.add_dashboard_script("dash123", script)
        assert result.startswith("Error:")

    def test_returns_error_string_on_invalid_json_script(self):
        dash = _make_dash()
        # If the script is not valid JSON and not a plain string, it stays a string
        # Pass something that causes JSONDecodeError on json.loads
        result = dash.add_dashboard_script("dash123", "{bad json{{")
        assert result.startswith("Error:")


# ---------------------------------------------------------------------------
# add_widget_script
# ---------------------------------------------------------------------------


class TestAddWidgetScript:
    def test_returns_success_string_on_put_200(self):
        script = '{"script": "console.log(widget);"}'
        dash = _make_dash(
            put_responses={"/api/dashboards/dash123": FakeResponse(200, {})},
            post_responses={"/api/v1/dashboards/dash123": FakeResponse(204, {})},
        )
        result = dash.add_widget_script("dash123", "widget456", script)
        assert "successfully" in result.lower()

    def test_returns_error_string_on_500_put_response(self):
        # Use a 500 response (not None) to avoid a source-code NPE on status_code
        script = '{"script": "console.log(widget);"}'
        dash = _make_dash(put_responses={"/api/dashboards/dash123": FakeResponse(500, {"error": "fail"})})
        result = dash.add_widget_script("dash123", "widget456", script)
        assert result.startswith("Error:")


# ---------------------------------------------------------------------------
# add_dashboard_shares
# ---------------------------------------------------------------------------


class TestAddDashboardShares:
    def test_returns_no_new_shares_message_when_already_shared(self):
        existing_shares = {"sharesTo": [{"shareId": "user123", "type": "user", "rule": "EDIT"}]}
        dash = _make_dash(
            get_responses={
                # For get_user lookup inside access_mgmt
                "/api/v1/users": FakeResponse(200, [_USER]),
                # For fetching existing shares
                "/api/shares/dashboard/dash123": FakeResponse(200, existing_shares),
            }
        )
        result = dash.add_dashboard_shares(
            "dash123",
            [{"type": "user", "name": "jdoe@example.com", "rule": "EDIT"}],
        )
        assert "No new or updated shares" in result

    def test_returns_error_string_when_share_fetch_fails(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/users": FakeResponse(200, [_USER]),
            }
            # No share endpoint → None → error
        )
        result = dash.add_dashboard_shares(
            "dash123",
            [{"type": "user", "name": "jdoe@example.com", "rule": "EDIT"}],
        )
        assert result.startswith("Error:")


# ---------------------------------------------------------------------------
# get_dashboard_columns
# ---------------------------------------------------------------------------


class TestGetDashboardColumns:
    def test_returns_empty_list_when_dashboard_not_found(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [])})
        result = dash.get_dashboard_columns("NoSuchDash")
        assert result == []

    def test_returns_column_list_on_success(self):
        export_data = [
            {
                "title": "Sales Report",
                "filters": [],
                "widgets": [
                    {
                        "title": "Revenue",
                        "metadata": {"panels": [{"items": [{"jaql": {"dim": "[orders].[amount]"}}]}]},
                    }
                ],
                "layout": {},
            }
        ]
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/export": FakeResponse(200, export_data),
            }
        )
        result = dash.get_dashboard_columns("Sales Report")
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# get_dashboard_share
# ---------------------------------------------------------------------------


class TestGetDashboardShare:
    def test_returns_empty_list_when_dashboard_has_no_shares(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD])})
        result = dash.get_dashboard_share("Sales Report")
        assert result == []

    def test_returns_empty_list_when_dashboard_not_found(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [])})
        result = dash.get_dashboard_share("NoSuchDash")
        assert result == []


# ---------------------------------------------------------------------------
# resolve_dashboard_reference
# ---------------------------------------------------------------------------


class TestResolveDashboardReference:
    def test_resolves_by_name_when_not_a_24_char_id(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD])})
        result = dash.resolve_dashboard_reference("Sales Report")
        assert result["success"] is True
        assert result["dashboard_id"] == "dash123"

    def test_returns_failure_when_not_found(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [])})
        result = dash.resolve_dashboard_reference("NoSuchDash")
        assert result["success"] is False
        assert result["dashboard_id"] is None

    def test_resolves_by_id_when_24_char_hex(self):
        dash_id = "a" * 24
        dash_with_id = {**_DASHBOARD, "oid": dash_id}
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [dash_with_id])})
        result = dash.resolve_dashboard_reference(dash_id)
        assert result["success"] is True
        assert result["dashboard_id"] == dash_id


# ---------------------------------------------------------------------------
# script retrieval + rendering
# ---------------------------------------------------------------------------


class TestGetDashboardScript:
    def test_returns_script_object(self, monkeypatch):
        class DummyScript:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        export_data = {
            "oid": "dash123",
            "title": "Sales Report",
            "lastOpened": "2025-01-01T00:00:00.000Z",
            "script": "console.log('hello');",
            "widgets": [],
        }
        monkeypatch.setattr(scripts_module, "SisenseScript", DummyScript)
        dash = _make_dash()
        # Method delegates through self.dashboard.export_dashboard(...)
        dash.dashboard = dash
        dash.export_dashboard = lambda dashboard_id: export_data

        result = dash.get_dashboard_script("dash123")

        assert isinstance(result, DummyScript)
        assert result.kwargs["title"] == "Sales Report"
        assert result.kwargs["script"] == "console.log('hello');"

    def test_returns_error_dict_when_export_fails(self):
        dash = _make_dash()
        dash.dashboard = dash
        dash.export_dashboard = lambda dashboard_id: {"error": "failed to export"}

        result = dash.get_dashboard_script("dash123")

        assert isinstance(result, dict)
        assert result["error"] == "failed to export"


class TestGetWidgetScript:
    def test_returns_script_object_for_widget(self, monkeypatch):
        class DummyScript:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        export_data = {
            "oid": "dash123",
            "title": "Sales Report",
            "lastOpened": "2025-01-01T00:00:00.000Z",
            "script": "",
            "widgets": [
                {
                    "oid": "widget456",
                    "title": "Revenue by Region",
                    "type": "chart/column",
                    "script": "console.log('widget');",
                }
            ],
        }
        monkeypatch.setattr(scripts_module, "SisenseScript", DummyScript)
        dash = _make_dash()
        dash.dashboard = dash
        dash.export_dashboard = lambda dashboard_id: export_data

        result = dash.get_widget_script("dash123", "widget456")

        assert isinstance(result, DummyScript)
        assert result.kwargs["title"] == "Revenue by Region"
        assert result.kwargs["type"] == "chart/column"

    def test_returns_error_dict_when_widget_not_found(self):
        export_data = {
            "oid": "dash123",
            "title": "Sales Report",
            "lastOpened": "2025-01-01T00:00:00.000Z",
            "script": "",
            "widgets": [],
        }
        dash = _make_dash()
        dash.dashboard = dash
        dash.export_dashboard = lambda dashboard_id: export_data

        result = dash.get_widget_script("dash123", "widget456")

        assert isinstance(result, dict)
        assert "error" in result


class TestScriptRendering:
    def test_to_text_beautifies_javascript(self):
        """``to_text`` runs jsbeautifier (4-space indent) on script + footer."""
        script = SisenseScript(
            url="/app/main/dashboards/dash123",
            title="Sales Report",
            type=None,
            script="function foo(){if(true){return 1;}}",
            template=r"/\*no-such-banner\*/",
            footer="// Dashboard Title: {title}",
        )

        text = script.to_text()

        assert "function foo()" in text
        assert "    return 1" in text
        assert "// Dashboard Title: Sales Report" in text

    def test_to_md_includes_heading_and_code_block(self):
        script = SisenseScript(
            url="/app/main/dashboards/dash123",
            title="Sales Report",
            type=None,
            script="console.log('x');",
            template=r"/\*unused\*/",
            footer="// Dashboard Title: {title}",
        )

        markdown = script.to_md()

        assert markdown.startswith("# Sales Report")
        assert "```js" in markdown
        assert "console.log('x');" in markdown

    def test_to_file_writes_rendered_text(self, tmp_path):
        script = SisenseScript(
            url="/app/main/dashboards/dash123",
            title="Sales Report",
            type=None,
            script="console.log('x');",
            template=r"/\*unused\*/",
            footer="// Dashboard Title: {title}",
        )
        output = tmp_path / "script.js"

        script.to_file(str(output))

        assert output.exists()
        content = output.read_text()
        assert "console.log('x');" in content
