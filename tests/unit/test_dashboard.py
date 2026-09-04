"""Unit tests for pysisense.dashboard.Dashboard."""

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse

import pysisense.dashboard.scripts as scripts_module
from pysisense.dashboard import Dashboard
from pysisense.dashboard.scripts import SisenseScript


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
# get_dashboard_widgets
# ---------------------------------------------------------------------------


_WIDGETS = [
    {"oid": "w1", "title": "Chart A", "type": "chart/column"},
    {"oid": "w2", "title": "Pivot B", "type": "pivot"},
]

_EXPORT_DASH = {**_DASHBOARD, "widgets": _WIDGETS}


class TestGetDashboardWidgets:
    def test_returns_widget_list_on_success(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/export": FakeResponse(200, [_EXPORT_DASH]),
            }
        )
        result = dash.get_dashboard_widgets("Sales Report")
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["oid"] == "w1"

    def test_returns_empty_list_when_widgets_missing(self):
        export_no_widgets = {**_DASHBOARD, "widgets": []}
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/export": FakeResponse(200, [export_no_widgets]),
            }
        )
        result = dash.get_dashboard_widgets("Sales Report")
        assert result == []

    def test_normalizes_widgets_object_map(self):
        export_map = {**_DASHBOARD, "widgets": {"w1": _WIDGETS[0], "w2": _WIDGETS[1]}}
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/export": FakeResponse(200, [export_map]),
            }
        )
        result = dash.get_dashboard_widgets("Sales Report")
        assert isinstance(result, list)
        assert len(result) == 2
        assert {w["oid"] for w in result} == {"w1", "w2"}

    def test_returns_error_when_reference_unresolved(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [])})
        result = dash.get_dashboard_widgets("NoSuchDash")
        assert isinstance(result, dict)
        assert "error" in result

    def test_returns_error_on_export_none_response(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/export": None,
            }
        )
        result = dash.get_dashboard_widgets("dash123")
        assert isinstance(result, dict)
        assert "error" in result

    def test_returns_error_on_export_non_200(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/export": FakeResponse(403, {"message": "forbidden"}),
            }
        )
        result = dash.get_dashboard_widgets("Sales Report")
        assert isinstance(result, dict)
        assert "error" in result

    def test_returns_error_when_widgets_wrong_type(self):
        bad_export = {**_DASHBOARD, "widgets": "not-a-list-or-map"}
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/export": FakeResponse(200, [bad_export]),
            }
        )
        result = dash.get_dashboard_widgets("Sales Report")
        assert isinstance(result, dict)
        assert "error" in result

    def test_resolves_24_char_oid_with_admin_then_export(self):
        dash_id = "a" * 24
        dash_row = {**_DASHBOARD, "oid": dash_id}
        export_dash = {**dash_row, "widgets": _WIDGETS}
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [dash_row]),
                "/api/v1/dashboards/export": FakeResponse(200, [export_dash]),
            }
        )
        result = dash.get_dashboard_widgets(dash_id)
        assert isinstance(result, list)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# add_dashboard_script
# ---------------------------------------------------------------------------


class TestAddDashboardScript:
    def test_returns_success_dict_on_put_200(self):
        script = '{"script": "console.log(1);"}'
        dash = _make_dash(put_responses={"/api/dashboards/dash123": FakeResponse(200, {})})
        result = dash.add_dashboard_script("dash123", script)
        assert result["success"] is True
        assert "successfully" in result["message"].lower()

    def test_returns_error_dict_on_put_failure(self):
        script = '{"script": "console.log(1);"}'
        dash = _make_dash(put_responses={"/api/dashboards/dash123": FakeResponse(500, {"error": "fail"})})
        result = dash.add_dashboard_script("dash123", script)
        assert result["ok"] is False
        assert "fail" in result["error"]
        assert result["status_code"] == 500

    def test_returns_error_dict_on_invalid_json_script(self):
        dash = _make_dash()
        # Pass something that causes JSONDecodeError on json.loads
        result = dash.add_dashboard_script("dash123", "{bad json{{")
        assert result["ok"] is False
        assert "valid JSON" in result["error"]

    def test_returns_ownership_hint_on_404_when_no_executing_user(self):
        # Confirmed correct against a live instance — see
        # micael_similar_methods_fixes.md, Dashboard Scripts module.
        # add_dashboard_script's PUT /api/dashboards/{id} returns 404 for an
        # inaccessible dashboard, while add_widget_script's PUT
        # /api/dashboards/{id}/widgets/{widget_id} returns 403 for the same
        # case — a real, observed difference between the two endpoints, not
        # a bug in either method's status-code check.
        script = '{"script": "console.log(1);"}'
        dash = _make_dash(put_responses={"/api/dashboards/dash123": FakeResponse(404, {"error": "not found"})})
        result = dash.add_dashboard_script("dash123", script)
        assert "executing_user" in result["error"]

    def test_does_not_return_ownership_hint_on_403_when_no_executing_user(self):
        # Contrast case: unlike add_widget_script, a 403 here currently falls
        # through to the generic error message, not the ownership hint.
        script = '{"script": "console.log(1);"}'
        dash = _make_dash(put_responses={"/api/dashboards/dash123": FakeResponse(403, {"error": "forbidden"})})
        result = dash.add_dashboard_script("dash123", script)
        assert "executing_user" not in result["error"]


# ---------------------------------------------------------------------------
# add_widget_script
# ---------------------------------------------------------------------------


class TestAddWidgetScript:
    def test_returns_success_dict_on_put_200(self):
        script = '{"script": "console.log(widget);"}'
        dash = _make_dash(
            put_responses={"/api/dashboards/dash123": FakeResponse(200, {})},
            post_responses={"/api/v1/dashboards/dash123": FakeResponse(204, {})},
        )
        result = dash.add_widget_script("dash123", "widget456", script)
        assert result["success"] is True
        assert "successfully" in result["message"].lower()

    def test_returns_error_dict_on_500_put_response(self):
        script = '{"script": "console.log(widget);"}'
        dash = _make_dash(put_responses={"/api/dashboards/dash123": FakeResponse(500, {"error": "fail"})})
        result = dash.add_widget_script("dash123", "widget456", script)
        assert result["ok"] is False
        assert "fail" in result["error"]
        assert result["status_code"] == 500

    def test_returns_ownership_hint_on_403_when_no_executing_user(self):
        # Pins CURRENT behavior — see test_returns_ownership_hint_on_404_when_no_executing_user
        # in TestAddDashboardScript for the corresponding contrast case.
        script = '{"script": "console.log(widget);"}'
        dash = _make_dash(put_responses={"/api/dashboards/dash123": FakeResponse(403, {"error": "forbidden"})})
        result = dash.add_widget_script("dash123", "widget456", script)
        assert "executing_user" in result["error"]

    def test_does_not_return_ownership_hint_on_404_when_no_executing_user(self):
        # Contrast case: unlike add_dashboard_script, a 404 here currently falls
        # through to the generic error message, not the ownership hint.
        script = '{"script": "console.log(widget);"}'
        dash = _make_dash(put_responses={"/api/dashboards/dash123": FakeResponse(404, {"error": "not found"})})
        result = dash.add_widget_script("dash123", "widget456", script)
        assert "executing_user" not in result["error"]


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
        assert result["success"] is True
        assert "No new or updated shares" in result["message"]
        assert result["new_shares"] == 0
        assert result["updated_shares"] == 0

    def test_returns_error_dict_when_share_fetch_fails(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/users": FakeResponse(200, [_USER]),
            }
            # No share endpoint → None → connection-failure error dict
        )
        result = dash.add_dashboard_shares(
            "dash123",
            [{"type": "user", "name": "jdoe@example.com", "rule": "EDIT"}],
        )
        assert result["ok"] is False
        assert "connection failed" in result["error"]


# ---------------------------------------------------------------------------
# get_dashboard_columns
# ---------------------------------------------------------------------------


class TestGetDashboardColumns:
    def test_returns_error_dict_when_dashboard_not_found(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [])})
        result = dash.get_dashboard_columns("NoSuchDash")
        assert result["ok"] is False
        assert "NoSuchDash" in result["error"]

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
        # [orders].[amount] is the two-bracket form; the old strip("[]").split(".")
        # read it as table "orders]" / column "[amount".
        assert result == [{"dashboard_name": "Sales Report", "source": "widget", "widget_id": "Unknown Widget", "table": "orders", "column": "amount"}]

    def test_widget_id_comes_from_the_widget_oid_and_calendar_dedupes(self):
        export_data = [
            {
                "title": "Sales Report",
                "filters": [{"levels": [{"dim": "[orders.Date (Calendar)]"}]}],
                "widgets": [
                    {"oid": "w1", "metadata": {"panels": [{"items": [{"jaql": {"dim": "[orders.Date]"}}]}]}},
                    {"oid": "w2", "metadata": {"panels": [{"items": [{"jaql": {"dim": "[[region.r_name]"}}]}]}},
                ],
                "layout": {"columns": [{"cells": [{"subcells": [{"elements": [{"widgetid": "layout-says-otherwise"}]}]}]}]},
            }
        ]
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD]),
                "/api/v1/dashboards/export": FakeResponse(200, export_data),
            }
        )
        result = dash.get_dashboard_columns("Sales Report")
        assert [(r["source"], r["widget_id"], r["table"], r["column"]) for r in result] == [
            ("filter", "N/A", "orders", "Date (Calendar)"),  # w1's [orders.Date] dedupes against it
            ("widget", "w2", "[region", "r_name"),  # table name starting with "[" survives
        ]


# ---------------------------------------------------------------------------
# get_dashboard_share
# ---------------------------------------------------------------------------


class TestGetDashboardShare:
    def test_returns_empty_list_when_dashboard_has_no_shares(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [_DASHBOARD])})
        result = dash.get_dashboard_share("Sales Report")
        assert result == []

    def test_returns_error_dict_when_dashboard_not_found(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, [])})
        result = dash.get_dashboard_share("NoSuchDash")
        assert result["ok"] is False
        assert "NoSuchDash" in result["error"]

    def test_resolves_user_and_group_shares_via_shared_access_mgmt_helper(self):
        # Regression: get_dashboard_share now resolves shares via
        # AccessManagement._get_user_email_and_group_name_maps() (self.access_mgmt)
        # instead of its own direct /api/v1/users + /api/v1/groups fetch —
        # confirms the shared helper produces the same resolved/unresolved
        # distinction as before, across the class boundary.
        dashboard_with_shares = {**_DASHBOARD, "shares": [{"type": "user", "shareId": "user123"}, {"type": "group", "shareId": "grp1"}, {"type": "user", "shareId": "u_missing"}]}
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/admin": FakeResponse(200, [dashboard_with_shares]),
                "/api/v1/users": FakeResponse(200, [_USER]),
                "/api/v1/groups": FakeResponse(200, [{"_id": "grp1", "name": "Engineers"}]),
            }
        )
        result = dash.get_dashboard_share("Sales Report")
        assert result == [
            {"type": "user", "name": _USER["email"]},
            {"type": "group", "name": "Engineers"},
        ]


# ---------------------------------------------------------------------------
# get_dashboard_shares_v1
# ---------------------------------------------------------------------------


class TestGetDashboardSharesV1:
    def test_returns_shares_dict_on_success(self):
        shares_payload = {"sharesTo": [{"shareId": "user123", "type": "user", "rule": "edit"}]}
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/dash123/shares": FakeResponse(200, shares_payload),
            }
        )
        result = dash.get_dashboard_shares_v1("dash123")
        assert result["sharesTo"][0]["shareId"] == "user123"

    def test_returns_error_dict_on_none_response(self):
        dash = _make_dash()
        result = dash.get_dashboard_shares_v1("dash123")
        assert "error" in result

    def test_returns_error_dict_on_non_200(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/dash123/shares": FakeResponse(403, {"message": "forbidden"}),
            }
        )
        result = dash.get_dashboard_shares_v1("dash123")
        assert "error" in result

    def test_retries_without_admin_access_when_api_rejects_the_param(self):
        # Some Sisense versions reject adminAccess with a strict-schema 422;
        # the method must retry the bare endpoint. Exact-URL keys let the fake
        # serve different responses per variant.
        shares_list = [{"userName": "a@b.com", "rule": "edit"}]
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/dash123/shares?adminAccess=true": FakeResponse(422, {"message": "must NOT have additional properties"}),
                "/api/v1/dashboards/dash123/shares": FakeResponse(200, shares_list),
            }
        )
        result = dash.get_dashboard_shares_v1("dash123")
        assert result == shares_list


# ---------------------------------------------------------------------------
# can_be_owned
# ---------------------------------------------------------------------------


class TestCanBeOwned:
    def test_returns_response_on_success(self):
        body = {"canBeOwned": True}
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/dash123/can_be_owned": FakeResponse(200, body),
            }
        )
        result = dash.can_be_owned("dash123")
        assert result["canBeOwned"] is True

    def test_returns_error_dict_on_failure(self):
        dash = _make_dash(
            get_responses={
                "/api/v1/dashboards/dash123/can_be_owned": FakeResponse(404, {"message": "not found"}),
            }
        )
        result = dash.can_be_owned("dash123")
        assert "error" in result


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

    def test_scriptless_widget_returns_explicit_message_not_keyerror(self):
        export_data = {
            "oid": "dash123",
            "title": "Sales Report",
            "widgets": [{"oid": "widget456", "title": "Revenue by Region", "type": "chart/column"}],
        }
        dash = _make_dash()
        dash.dashboard = dash
        dash.export_dashboard = lambda dashboard_id: export_data

        result = dash.get_widget_script("dash123", "widget456")

        assert result == {"ok": False, "error": "Widget 'Revenue by Region' has no widget script."}

    def test_export_omitting_script_field_falls_back_to_direct_widget_fetch(self, monkeypatch):
        # Some Sisense versions omit 'script' (and 'title') from export widgets
        # entirely — the getter must fetch the widget directly.
        class DummyScript:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        export_data = {
            "oid": "dash123",
            "title": "Sales Report",
            "widgets": [{"oid": "widget456", "type": "chart/pie"}],  # no script key
        }
        dash = _make_dash()
        dash.dashboard = dash
        dash.export_dashboard = lambda dashboard_id: export_data
        dash.get_widget_by_id = lambda d, w: {"oid": "widget456", "title": "Pie", "type": "chart/pie", "script": "console.log('w');"}
        monkeypatch.setattr(scripts_module, "SisenseScript", DummyScript)

        result = dash.get_widget_script("dash123", "widget456")

        assert isinstance(result, DummyScript)
        assert result.kwargs["script"] == "console.log('w');"
        assert result.kwargs["title"] == "Pie"

    def test_export_without_widgets_key_reports_widget_not_found(self):
        dash = _make_dash()
        dash.dashboard = dash
        dash.export_dashboard = lambda dashboard_id: {"oid": "dash123", "title": "Sales Report"}

        result = dash.get_widget_script("dash123", "widget456")

        assert "not found" in result["error"]


class TestGetDashboardScriptNoScript:
    def test_scriptless_dashboard_returns_explicit_message_not_keyerror(self):
        dash = _make_dash()
        dash.dashboard = dash
        dash.export_dashboard = lambda dashboard_id: {"oid": "dash123", "title": "Sales Report"}

        result = dash.get_dashboard_script("dash123")

        assert result == {"ok": False, "error": "Dashboard 'Sales Report' has no dashboard script."}


class TestBeautifyJsCode:
    def test_returns_string(self):
        script = SisenseScript(
            url="/app/main/dashboards/dash123",
            title="Sales Report",
            type=None,
            script="x",
            template=r"/\*unused\*/",
            footer="// Dashboard Title: {title}",
        )
        result = script._beautify_js_code("function foo(){return 1;}")
        assert isinstance(result, str)


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


# ---------------------------------------------------------------------------
# get_dashboards
# ---------------------------------------------------------------------------


class TestGetDashboards:
    def test_returns_list_on_success(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards": FakeResponse(200, [_DASHBOARD])})
        result = dash.get_dashboards()
        assert isinstance(result, list)
        assert result[0]["oid"] == "dash123"

    def test_returns_error_on_none_response(self):
        dash = _make_dash()
        result = dash.get_dashboards()
        assert "error" in result

    def test_returns_error_on_non_200(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards": FakeResponse(403, {})})
        result = dash.get_dashboards()
        assert "error" in result


# ---------------------------------------------------------------------------
# publish_dashboard
# ---------------------------------------------------------------------------


class TestPublishDashboard:
    def test_returns_success_on_204(self):
        dash = _make_dash(post_responses={"/api/v1/dashboards/dash123/publish": FakeResponse(204, {})})
        result = dash.publish_dashboard("dash123")
        assert result == {"success": True}

    def test_returns_json_body_on_200(self):
        body = {"published": True}
        dash = _make_dash(post_responses={"/api/v1/dashboards/dash123/publish": FakeResponse(200, body)})
        result = dash.publish_dashboard("dash123")
        assert result == body

    def test_returns_error_on_none_response(self):
        dash = _make_dash()
        result = dash.publish_dashboard("dash123")
        assert "error" in result

    def test_returns_error_on_400(self):
        dash = _make_dash(post_responses={"/api/v1/dashboards/dash123/publish": FakeResponse(400, {})})
        result = dash.publish_dashboard("dash123")
        assert "error" in result

    def test_admin_access_false_does_not_append_param(self):
        # Without adminAccess the endpoint key changes — no response wired means None → error
        dash = _make_dash()
        result = dash.publish_dashboard("dash123", admin_access=False)
        assert "error" in result

    def test_force_true_still_succeeds(self):
        dash = _make_dash(post_responses={"/api/v1/dashboards/dash123/publish": FakeResponse(200, {"published": True})})
        result = dash.publish_dashboard("dash123", force=True)
        assert "error" not in result

    def test_plain_call_first_then_admin_access_on_403(self):
        # Live-observed: only the owner may publish; some versions honour adminAccess for admins, this one rejects it (422).
        client = _RecordingDashClient(
            post_responses={"/api/v1/dashboards/dash123/publish": [FakeResponse(403, {"error": {"code": 403, "message": "Access denied"}}), FakeResponse(200, {"published": True})]},
            logger=FakeLogger(),
        )
        result = Dashboard(api_client=client).publish_dashboard("dash123")
        assert result == {"published": True}
        assert [u for u, _ in client.posted] == ["/api/v1/dashboards/dash123/publish", "/api/v1/dashboards/dash123/publish?adminAccess=true"]

    def test_admin_retry_rejected_with_422_returns_the_original_403(self):
        client = _RecordingDashClient(
            post_responses={
                "/api/v1/dashboards/dash123/publish": [
                    FakeResponse(403, {"error": {"code": 403, "message": "Access denied"}}),
                    FakeResponse(422, {"error": {"message": "must NOT have additional properties"}}),
                ]
            },
            logger=FakeLogger(),
        )
        result = Dashboard(api_client=client).publish_dashboard("dash123")
        assert result["ok"] is False and result["status_code"] == 403 and "Access denied" in result["error"]

    def test_admin_access_false_does_not_retry(self):
        client = _RecordingDashClient(post_responses={"/api/v1/dashboards/dash123/publish": FakeResponse(403, {"error": {"message": "Access denied"}})}, logger=FakeLogger())
        result = Dashboard(api_client=client).publish_dashboard("dash123", admin_access=False)
        assert result["status_code"] == 403 and len(client.posted) == 1


# ---------------------------------------------------------------------------
# rename_dashboard
# ---------------------------------------------------------------------------


class TestRenameDashboard:
    def test_returns_updated_dashboard_on_success(self):
        updated = {**_DASHBOARD, "title": "New Title"}
        dash = _make_dash(patch_responses={"/api/dashboards/dash123": FakeResponse(200, updated)})
        result = dash.rename_dashboard("dash123", "New Title")
        assert result["title"] == "New Title"

    def test_returns_error_on_none_response(self):
        dash = _make_dash()
        result = dash.rename_dashboard("dash123", "New Name")
        assert "error" in result

    def test_returns_error_on_failure(self):
        dash = _make_dash(patch_responses={"/api/dashboards/dash123": FakeResponse(500, {"error": "fail"})})
        result = dash.rename_dashboard("dash123", "New Name")
        assert "error" in result

    def test_returns_success_dict_when_response_has_no_body(self):
        # Sisense returns 200 with an empty body on some instances/versions
        dash = _make_dash(patch_responses={"/api/dashboards/dash123": FakeResponseEmpty(200)})
        result = dash.rename_dashboard("dash123", "New Name")
        assert result == {"success": True}


# ---------------------------------------------------------------------------
# move_dashboard_to_folder
# ---------------------------------------------------------------------------


class TestMoveDashboardToFolder:
    def test_returns_updated_dashboard_on_success(self):
        updated = {**_DASHBOARD, "parentFolder": "folder456"}
        dash = _make_dash(patch_responses={"/api/dashboards/dash123": FakeResponse(200, updated)})
        result = dash.move_dashboard_to_folder("dash123", "folder456")
        assert result["parentFolder"] == "folder456"

    def test_returns_error_on_none_response(self):
        dash = _make_dash()
        result = dash.move_dashboard_to_folder("dash123", "folder999")
        assert "error" in result

    def test_returns_error_on_failure(self):
        dash = _make_dash(patch_responses={"/api/dashboards/dash123": FakeResponse(500, {"error": "fail"})})
        result = dash.move_dashboard_to_folder("dash123", "folder456")
        assert "error" in result

    def test_returns_success_dict_when_response_has_no_body(self):
        # Sisense returns 200 with an empty body on some instances/versions
        dash = _make_dash(patch_responses={"/api/dashboards/dash123": FakeResponseEmpty(200)})
        result = dash.move_dashboard_to_folder("dash123", "folder456")
        assert result == {"success": True}


# ---------------------------------------------------------------------------
# change_dashboard_owner
# ---------------------------------------------------------------------------


class TestChangeDashboardOwner:
    def test_returns_response_on_success(self):
        body = {"ok": True}
        dash = _make_dash(post_responses={"/api/v1/dashboards/dash123/change_owner": FakeResponse(200, body)})
        result = dash.change_dashboard_owner("dash123", "new_owner_id")
        assert result == body

    def test_returns_error_on_none_response(self):
        dash = _make_dash()
        result = dash.change_dashboard_owner("dash123", "new_owner_id")
        assert "error" in result

    def test_returns_error_on_403(self):
        dash = _make_dash(post_responses={"/api/v1/dashboards/dash123/change_owner": FakeResponse(403, {"message": "forbidden"})})
        result = dash.change_dashboard_owner("dash123", "new_owner_id")
        assert "error" in result

    def test_returns_error_on_500(self):
        dash = _make_dash(post_responses={"/api/v1/dashboards/dash123/change_owner": FakeResponse(500, {"error": "fail"})})
        result = dash.change_dashboard_owner("dash123", "new_owner_id")
        assert "error" in result

    def test_returns_success_dict_when_response_has_no_body(self):
        # Sisense returns 200 with an empty body on some instances/versions
        dash = _make_dash(post_responses={"/api/v1/dashboards/dash123/change_owner": FakeResponseEmpty(200)})
        result = dash.change_dashboard_owner("dash123", "new_owner_id")
        assert result == {"success": True}


# ---------------------------------------------------------------------------
# add_dashboard_script — ownership restoration via try/finally
# ---------------------------------------------------------------------------

_EXPANDED_USER = {
    "_id": "api_user_id",
    "userName": "api",
    "email": "api@example.com",
    "firstName": "Api",
    "lastName": "User",
    "role": {"_id": "r1", "name": "consumer"},
    "groups": [],
}


class _TrackingPostClient(FakeApiClient):
    """FakeApiClient that records every POST url (before query-params)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.post_urls: list[str] = []

    def post(self, url, data=None, **kwargs):
        self.post_urls.append(url.split("?")[0])
        return super().post(url, data=data, **kwargs)


def _make_ownership_script_client(put_response: FakeResponse) -> _TrackingPostClient:
    """Build a tracking client set up for the ownership-swap script flow."""
    logger = FakeLogger()
    return _TrackingPostClient(
        get_responses={
            "/api/v1/dashboards/admin": FakeResponse(200, [{"owner": "original_owner_id"}]),
            "/api/shares/dashboard/dash123": FakeResponse(200, {"sharesTo": []}),
            "/api/v1/users": FakeResponse(200, [_EXPANDED_USER]),
        },
        post_responses={
            "/api/v1/dashboards/dash123/change_owner": FakeResponse(200, {}),
            "/api/shares/dashboard/dash123": FakeResponse(200, {}),
        },
        put_responses={"/api/dashboards/dash123": put_response},
        logger=logger,
    )


class TestAddDashboardScriptOwnershipRestored:
    def test_ownership_restored_on_write_success(self):
        client = _make_ownership_script_client(FakeResponse(200, {}))
        dash = Dashboard(api_client=client)
        result = dash.add_dashboard_script("dash123", '{"script": "console.log(1);"}', executing_user="api@example.com")

        assert result["success"] is True
        change_owner_calls = [u for u in client.post_urls if "change_owner" in u]
        assert len(change_owner_calls) == 2  # take + restore

    def test_ownership_restored_when_write_fails(self):
        client = _make_ownership_script_client(FakeResponse(500, {"error": "write failed"}))
        dash = Dashboard(api_client=client)
        result = dash.add_dashboard_script("dash123", '{"script": "console.log(1);"}', executing_user="api@example.com")

        assert result["ok"] is False
        # Restoration must still have been attempted despite the failed write
        change_owner_calls = [u for u in client.post_urls if "change_owner" in u]
        assert len(change_owner_calls) == 2
        share_restore_calls = [u for u in client.post_urls if "shares/dashboard" in u]
        assert len(share_restore_calls) >= 1


# ---------------------------------------------------------------------------
# get_dashboards_by_datasource
# ---------------------------------------------------------------------------

_DS = lambda title: {"title": title, "fullname": f"LocalHost/{title}", "live": False}  # noqa: E731
_LISTING = [
    {
        "oid": "d1",
        "title": "Sales Overview",
        "owner": "u1",
        "parentFolder": "f1",
        "lastUpdated": "2026-01-01T00:00:00Z",
        "datasource": _DS("Sample ECommerce"),
        "widgetsDatasources": [_DS("Sample ECommerce")],
    },
    {"oid": "d1", "title": "Sales Overview", "owner": "u1", "datasource": _DS("Sample ECommerce"), "widgetsDatasources": []},  # the listing repeats oids
    {"oid": "d2", "title": "Mixed", "owner": "u2", "datasource": _DS("Other Model"), "widgetsDatasources": [_DS("Other Model"), _DS("sample ecommerce")]},
    {"oid": "d3", "title": "Unrelated", "owner": "u2", "datasource": _DS("Other Model"), "widgetsDatasources": [_DS("Other Model")]},
    {"oid": "d4", "title": "No Summary", "owner": "u1", "datasource": _DS("Other Model"), "widgetsDatasources": []},
    {
        "oid": "d5",
        "title": "Live",
        "owner": "u1",
        "datasource": {"title": "Sample ECommerce", "id": "live:Sample ECommerce", "fullname": "live:Sample ECommerce", "live": True},
        "widgetsDatasources": None,
    },
    None,
]
_USERS = [{"_id": "u1", "email": "one@example.com"}, {"_id": "u2", "email": "two@example.com"}]


def _make_discovery(listing=_LISTING, export=None, users=_USERS, **extra):
    responses = {"/api/v1/dashboards/admin": FakeResponse(200, listing), "/api/v1/users": FakeResponse(200, users), **extra}
    if export is not None:
        responses["/api/v1/dashboards/export"] = FakeResponse(200, export)
    return _make_dash(get_responses=responses)


class TestGetDashboardsByDatasource:
    def test_matches_dashboard_level_and_widget_level_case_insensitively(self):
        rows = _make_discovery().get_dashboards_by_datasource("sample ecommerce")
        assert [(r["dashboard_id"], r["match"]) for r in rows] == [("d5", "dashboard"), ("d1", "dashboard"), ("d2", "widget")]

    def test_rows_carry_owner_email_folder_and_timestamps(self):
        row = next(r for r in _make_discovery().get_dashboards_by_datasource("Sample ECommerce") if r["dashboard_id"] == "d1")
        assert row == {
            "dashboard_id": "d1",
            "title": "Sales Overview",
            "owner": "u1",
            "owner_email": "one@example.com",
            "datasource_title": "Sample ECommerce",
            "match": "dashboard",
            "folder_id": "f1",
            "last_updated": "2026-01-01T00:00:00Z",
        }

    def test_duplicate_listing_rows_collapse_to_one(self):
        rows = _make_discovery().get_dashboards_by_datasource("Sample ECommerce")
        assert [r["dashboard_id"] for r in rows].count("d1") == 1

    def test_accepts_a_model_oid_by_resolving_its_title(self):
        dash = _make_discovery(**{"/api/v2/datamodels/abc-oid/schema": FakeResponse(200, {"oid": "abc-oid", "title": "Sample ECommerce"})})
        assert len(dash.get_dashboards_by_datasource("abc-oid")) == 3

    def test_deep_scan_exports_only_unsummarized_dashboards(self):
        export = [{"oid": "d4", "widgets": [{"oid": "w", "datasource": _DS("Sample ECommerce")}]}]
        rows = _make_discovery(export=export).get_dashboards_by_datasource("Sample ECommerce", deep=True)
        assert ("d4", "widget") in [(r["dashboard_id"], r["match"]) for r in rows]

    def test_without_deep_the_unsummarized_dashboard_is_not_found(self):
        rows = _make_discovery().get_dashboards_by_datasource("Sample ECommerce")
        assert "d4" not in [r["dashboard_id"] for r in rows]

    def test_no_match_is_an_empty_list(self):
        assert _make_discovery().get_dashboards_by_datasource("Nobody Uses Me") == []

    def test_owner_lookup_failure_leaves_email_none(self):
        rows = _make_discovery(users=None).get_dashboards_by_datasource("Sample ECommerce")
        assert rows and all(r["owner_email"] is None for r in rows)

    def test_listing_failure_returns_error_dict(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(500, {"message": "boom"})})
        result = dash.get_dashboards_by_datasource("Sample ECommerce")
        assert result["ok"] is False and result["status_code"] == 500

    def test_deep_export_failure_returns_error_dict(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/admin": FakeResponse(200, _LISTING), "/api/v1/dashboards/export": None})
        assert dash.get_dashboards_by_datasource("Sample ECommerce", deep=True)["ok"] is False

    def test_empty_reference_is_an_error(self):
        assert _make_discovery().get_dashboards_by_datasource("")["ok"] is False


# ---------------------------------------------------------------------------
# duplicate_dashboard
# ---------------------------------------------------------------------------

_EXPORTED = {"oid": "src1", "title": "Sales Report", "filters": [], "widgets": [{"oid": "w1"}, {"oid": "w2"}], "layout": {}}


class _RecordingDashClient(FakeApiClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.posted: list[tuple[str, object]] = []
        self.patched: list[tuple[str, object]] = []

    def post(self, url, data=None, **kwargs):
        self.posted.append((url, data))
        return super().post(url, data=data, **kwargs)

    def patch(self, url, data=None, **kwargs):
        self.patched.append((url, data))
        return super().patch(url, data=data, **kwargs)


def _make_duplicator(import_response=None, import_status=200, export=_EXPORTED):
    created = {"oid": "new1", "title": "Sales Report_perspective_stage", "parentFolder": None, "widgets": [{"oid": "a"}, {"oid": "b"}]}
    body = import_response if import_response is not None else {"succeded": [created], "failed": []}
    client = _RecordingDashClient(
        get_responses={
            "/api/v1/dashboards/src1": FakeResponse(200, [{"oid": "src1", "title": "Sales Report"}]),
            "/api/v1/dashboards/admin": FakeResponse(200, [{"oid": "src1", "title": "Sales Report"}]),
            "/api/v1/dashboards/export": FakeResponse(200, [export]),
        },
        post_responses={"/api/v1/dashboards/import/bulk": FakeResponse(import_status, body)},
        logger=FakeLogger(),
    )
    return Dashboard(api_client=client), client


class TestDuplicateDashboard:
    def test_exports_retitles_and_imports_with_duplicate_action(self):
        dash, client = _make_duplicator()
        result = dash.duplicate_dashboard("src1")
        assert result == {"success": True, "dashboard_id": "new1", "title": "Sales Report_perspective_stage", "source_dashboard_id": "src1", "source_title": "Sales Report", "widget_count": 2}
        url, payload = client.posted[0]
        assert url == "/api/v1/dashboards/import/bulk?action=duplicate"
        assert payload[0]["title"] == "Sales Report_perspective_stage" and payload[0]["widgets"] == _EXPORTED["widgets"]

    def test_unknown_source_is_an_error(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/nope": FakeResponse(404, {}), "/api/v1/dashboards/admin": FakeResponse(200, [])})
        assert dash.duplicate_dashboard("nope")["ok"] is False

    def test_import_failure_status_is_an_error(self):
        dash, _ = _make_duplicator(import_status=500, import_response={"message": "boom"})
        assert dash.duplicate_dashboard("src1")["ok"] is False

    def test_import_reporting_failed_is_an_error(self):
        dash, _ = _make_duplicator(import_response={"succeded": [], "failed": [{"title": "Sales Report_perspective_stage", "error": "quota"}]})
        result = dash.duplicate_dashboard("src1")
        assert result["ok"] is False and "quota" in result["error"]

    def test_import_returning_only_the_source_oid_is_an_error(self):
        dash, _ = _make_duplicator(import_response={"succeded": [{"oid": "src1"}], "failed": []})
        assert dash.duplicate_dashboard("src1")["ok"] is False


# ---------------------------------------------------------------------------
# replace_datasource
# ---------------------------------------------------------------------------

_CATALOGUE = [
    {"title": "Sample ECommerce", "fullname": "localhost/Sample ECommerce", "id": "localhost_aSampleIAAaECommerce", "address": "LocalHost", "database": "aSampleIAAaECommerce", "live": False},
    {"title": "fes_assistant", "fullname": "live:fes_assistant", "id": "live:fes_assistant", "live": True},
    {"title": "Listed Perspective", "fullname": "LocalHost/Listed Perspective", "id": "localhost_aSampleIAAaECommerce", "address": "LocalHost", "database": "aSampleIAAaECommerce", "live": False},
]
_PERSPECTIVE_LIST = [
    {"oid": "p1", "name": "Fresh Live Persp", "parentOid": "dm-live", "datamodelOid": "dm-live"},
    {"oid": "p2", "name": "Fresh Extract Persp", "parentOid": "dm-ext", "datamodelOid": "dm-ext"},
    {"oid": "p0", "name": "Default", "isDefault": True, "parentOid": None, "datamodelOid": "dm-ext"},
]


def _make_swapper(dashboard_ds, widgets, post_statuses=(200, 200), after_ds=None, after_widgets=None, owner="u9", applies_on="owner", widget_lookup_first=False, publish_status=204):
    """Dashboard whose reads flip to the 'after' state at the stage where Sisense applies the change.

    Reads of the dashboard document happen in this order: resolver, method, owner-stage poll,
    admin-stage poll. ``applies_on`` is "owner", "admin" or None (never applies).
    """
    doc_before = {"oid": "6a99ada4ea52ffb5c87c5ba3", "title": "Board", "owner": owner, "datasource": dashboard_ds, "filters": []}
    doc_after = dict(doc_before, datasource=after_ds if after_ds is not None else dashboard_ds)
    after_w = after_widgets if after_widgets is not None else widgets
    b, a = FakeResponse(200, [doc_before]), FakeResponse(200, [doc_after])
    docs = {"owner": [b, b, a], "admin": [b, b, b, a], None: [b]}[applies_on]
    # Widget exports are read once per poll (plus once up front when from_datasource is used);
    # they flip to 'after' at the stage where the change applies.
    before_w, after_w_resp = FakeResponse(200, [dict(doc_before, widgets=widgets)]), FakeResponse(200, [dict(doc_after, widgets=after_w)])
    exports = ([before_w] if widget_lookup_first else []) + {"owner": [after_w_resp], "admin": [before_w, after_w_resp], None: [before_w]}[applies_on]
    client = _RecordingDashClient(
        get_responses={
            "/api/v1/dashboards/admin": docs,
            "/api/v1/dashboards/export": exports,
            "/api/datasources": FakeResponse(200, _CATALOGUE),
            "/api/v2/perspectives": FakeResponse(200, _PERSPECTIVE_LIST),
            "/api/v2/datamodels/dm-live/schema": FakeResponse(200, {"oid": "dm-live", "title": "fes_assistant"}),
            "/api/v2/datamodels/dm-ext/schema": FakeResponse(200, {"oid": "dm-ext", "title": "Sample ECommerce"}),
            "/api/v1/users": FakeResponse(200, [{"_id": "u9", "email": "owner@example.com"}]),
        },
        post_responses={
            "/api/v1/dashboards/": [FakeResponse(s, None if s in (200, 204) else {"message": "no"}) for s in post_statuses],
            "/api/v1/dashboards/6a99ada4ea52ffb5c87c5ba3/publish": FakeResponse(publish_status, None),
        },
        logger=FakeLogger(),
    )
    return Dashboard(api_client=client), client


_LIVE_DS = {"title": "fes_assistant", "id": "live:fes_assistant", "fullname": "live:fes_assistant", "live": True}
_ECOM_DS = _CATALOGUE[0]
_FRESH_LIVE = {"title": "Fresh Live Persp", "id": "live:Fresh Live Persp", "fullname": "live:Fresh Live Persp", "live": True}


class TestReplaceDatasource:
    @pytest.fixture(autouse=True)
    def _fast_polling(self, monkeypatch):
        monkeypatch.setattr("pysisense.dashboard.core.time.sleep", lambda seconds: None)
        monkeypatch.setattr(Dashboard, "_SWAP_POLL_ATTEMPTS", 1)

    def test_live_source_uses_live_server_segment_and_bare_body(self):
        widgets = [{"oid": "w1", "datasource": _LIVE_DS}, {"oid": "w2", "datasource": _ECOM_DS}]
        after = [{"oid": "w1", "datasource": _FRESH_LIVE}, {"oid": "w2", "datasource": _ECOM_DS}]
        dash, client = _make_swapper(_LIVE_DS, widgets, after_ds=_FRESH_LIVE, after_widgets=after)
        result = dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "Fresh Live Persp")
        url, body = client.posted[0]
        assert url == "/api/v1/dashboards/live/fes_assistant/replace_datasource?dashboardId=6a99ada4ea52ffb5c87c5ba3"
        assert body == _FRESH_LIVE  # built from the parent, since the catalogue does not list it yet
        assert result["success"] is True and result["published"] is True
        assert result["previous_datasource"] == _LIVE_DS and result["new_datasource"] == _FRESH_LIVE
        assert result["widgets_updated"] == 1 and result["widgets_unchanged"] == ["Sample ECommerce"]
        assert client.posted[-1][0] == "/api/v1/dashboards/6a99ada4ea52ffb5c87c5ba3/publish"

    def test_publish_can_be_skipped(self):
        dash, client = _make_swapper(_LIVE_DS, [], after_ds=_FRESH_LIVE)
        result = dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "Fresh Live Persp", publish=False)
        assert result["success"] is True and result["published"] is False and "publish_error" not in result
        assert not any("/publish" in u for u, _ in client.posted)

    def test_failed_publish_does_not_undo_a_successful_swap(self):
        dash, _ = _make_swapper(_LIVE_DS, [], after_ds=_FRESH_LIVE, publish_status=500)
        result = dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "Fresh Live Persp")
        assert result["success"] is True and result["published"] is False and "publish" in result["publish_error"].lower()

    def test_publish_refused_to_a_non_owner_names_the_owner(self):
        dash, _ = _make_swapper(_LIVE_DS, [], after_ds=_FRESH_LIVE, publish_status=403)
        result = dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "Fresh Live Persp")
        assert result["success"] is True and result["published"] is False and result["owner"] == "owner@example.com"

    def test_extract_source_uses_address_and_encodes_the_title(self):
        target = {
            "fullname": "LocalHost/Fresh Extract Persp",
            "id": "localhost_aSampleIAAaECommerce",
            "address": "LocalHost",
            "database": "aSampleIAAaECommerce",
            "live": False,
            "title": "Fresh Extract Persp",
        }
        dash, client = _make_swapper(_ECOM_DS, [{"oid": "w1", "datasource": _ECOM_DS}], after_ds=target, after_widgets=[{"oid": "w1", "datasource": target}])
        result = dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "fresh extract persp")
        url, body = client.posted[0]
        assert url == "/api/v1/dashboards/LocalHost/Sample%20ECommerce/replace_datasource?dashboardId=6a99ada4ea52ffb5c87c5ba3"
        assert body == target and result["success"] is True

    def test_a_model_is_used_as_the_catalogue_lists_it(self):
        dash, client = _make_swapper(_LIVE_DS, [], after_ds=_ECOM_DS)
        dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "sample ecommerce")
        assert client.posted[0][1] == _ECOM_DS

    def test_a_perspective_is_built_from_its_parent_even_when_the_catalogue_lists_it(self):
        # "Listed Perspective" is in the catalogue AND in the perspectives list (extract parent dm-ext)
        listed = dict(_PERSPECTIVE_LIST[1], name="Listed Perspective", oid="p3")
        dash, client = _make_swapper(_LIVE_DS, [], after_ds=_CATALOGUE[2])
        client._get["/api/v2/perspectives"] = FakeResponse(200, _PERSPECTIVE_LIST + [listed])
        dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "Listed Perspective")
        assert client.posted[0][1] == {
            "fullname": "LocalHost/Listed Perspective",
            "id": "localhost_aSampleIAAaECommerce",
            "address": "LocalHost",
            "database": "aSampleIAAaECommerce",
            "live": False,
            "title": "Listed Perspective",
        }

    def test_silent_no_op_as_owner_is_retried_with_admin_access(self):
        # Live-observed: Sisense answers 200 to a non-owner and changes nothing; adminAccess=true applies it.
        dash, client = _make_swapper(_LIVE_DS, [], after_ds=_FRESH_LIVE, applies_on="admin")
        result = dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "Fresh Live Persp")
        assert [u for u, _ in client.posted if "replace_datasource" in u] == [
            "/api/v1/dashboards/live/fes_assistant/replace_datasource?dashboardId=6a99ada4ea52ffb5c87c5ba3",
            "/api/v1/dashboards/live/fes_assistant/replace_datasource?dashboardId=6a99ada4ea52ffb5c87c5ba3&adminAccess=true",
        ]
        assert result["success"] is True

    def test_owner_call_that_applies_is_not_repeated(self):
        dash, client = _make_swapper(_LIVE_DS, [], after_ds=_FRESH_LIVE, applies_on="owner")
        assert dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "Fresh Live Persp")["success"] is True
        assert len([u for u, _ in client.posted if "replace_datasource" in u]) == 1

    def test_no_change_after_both_attempts_fails_and_names_the_owner(self):
        dash, client = _make_swapper(_LIVE_DS, [], applies_on=None)
        result = dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "Fresh Live Persp")
        assert result["ok"] is False and result["owner"] == "owner@example.com"
        assert "still shows datasource 'fes_assistant'" in result["error"]
        assert len(client.posted) == 2

    def test_a_real_403_names_the_owner_with_sisense_text(self):
        dash, _ = _make_swapper(_LIVE_DS, [], post_statuses=(403,))
        result = dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "Fresh Live Persp")
        assert result["ok"] is False and result["status_code"] == 403 and result["owner"] == "owner@example.com"
        assert "owner@example.com" not in result["error"]  # the error text is Sisense's, not ours

    def test_from_datasource_targets_a_widget_level_datasource(self):
        widgets = [{"oid": "w1", "datasource": _LIVE_DS}, {"oid": "w2", "datasource": _ECOM_DS}]
        dash, client = _make_swapper(_LIVE_DS, widgets, after_widgets=[{"oid": "w1", "datasource": _LIVE_DS}, {"oid": "w2", "datasource": _CATALOGUE[2]}], widget_lookup_first=True)
        result = dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "Listed Perspective", from_datasource="Sample ECommerce")
        assert client.posted[0][0] == "/api/v1/dashboards/LocalHost/Sample%20ECommerce/replace_datasource?dashboardId=6a99ada4ea52ffb5c87c5ba3"
        assert result["success"] is True and result["previous_datasource"] == _ECOM_DS

    def test_from_datasource_not_on_the_dashboard(self):
        dash, client = _make_swapper(_LIVE_DS, [{"oid": "w1", "datasource": _LIVE_DS}], widget_lookup_first=True)
        assert dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "Listed Perspective", from_datasource="Nowhere")["ok"] is False and client.posted == []

    def test_same_datasource_is_refused(self):
        dash, client = _make_swapper(_LIVE_DS, [])
        assert dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "FES_ASSISTANT")["ok"] is False and client.posted == []

    def test_unknown_datasource_is_refused(self):
        dash, client = _make_swapper(_LIVE_DS, [])
        result = dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "no-such")
        assert result["ok"] is False and "neither a data model nor a perspective" in result["error"] and client.posted == []

    def test_api_failure(self):
        dash, _ = _make_swapper(_LIVE_DS, [], post_statuses=(500,))
        assert dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "Fresh Live Persp")["ok"] is False

    def test_invalid_token_is_not_an_ownership_problem(self):
        # 401 = authentication; no admin retry, no owner key, Sisense's message passed through
        dash, client = _make_swapper(_LIVE_DS, [], post_statuses=(401,))
        client._post["/api/v1/dashboards/"] = FakeResponse(401, {"error": {"code": 5002, "message": "Invalid token.", "status": 401, "httpMessage": "Unauthorized"}})
        result = dash.replace_datasource("6a99ada4ea52ffb5c87c5ba3", "Fresh Live Persp")
        assert result["ok"] is False and result["status_code"] == 401 and "owner" not in result
        assert "Invalid token" in result["error"]
        assert len(client.posted) == 1

    def test_unknown_dashboard(self):
        dash = _make_dash(get_responses={"/api/v1/dashboards/nope": FakeResponse(404, {}), "/api/v1/dashboards/admin": FakeResponse(200, [])})
        assert dash.replace_datasource("nope", "x")["ok"] is False
