"""Unit tests for pysisense.blox.widgets (BloxWidgetsMixin)."""

import copy

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.blox import Blox

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------

_BLOX_WIDGET_TEMPLATE = {
    "oid": "widget456",
    "_id": "widget456",
    "owner": "owner_id",
    "userId": "owner_id",
    "title": "My BloX Widget",
    "type": "BloX",
    "created": "2024-01-01",
    "lastUpdated": "2024-01-02",
    "instanceType": "BloX",
    "dashboardid": "dash123",
    "style": {
        "currentCard": {"style": "body { color: red; }", "body": [{"type": "TextBlock", "text": "Hello"}]},
        "currentConfig": {"fontFamily": "Arial", "fontSizes": {"default": 16}},
    },
}

_NON_BLOX_WIDGET = {**_BLOX_WIDGET_TEMPLATE, "type": "chart", "instanceType": "chart"}


def _blox_widget() -> dict:
    """Return a fresh deep copy of the BloX widget fixture to avoid shared-state mutations."""
    return copy.deepcopy(_BLOX_WIDGET_TEMPLATE)


def _make_blox(get_responses=None, post_responses=None, put_responses=None):
    logger = FakeLogger()
    client = FakeApiClient(
        get_responses=get_responses,
        post_responses=post_responses,
        put_responses=put_responses,
        logger=logger,
    )
    return Blox(api_client=client)


# ---------------------------------------------------------------------------
# get_blox_widget_style
# ---------------------------------------------------------------------------


class TestGetBloxWidgetStyle:
    def test_returns_card_and_config_on_success(self):
        blox = _make_blox(get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _blox_widget())})
        result = blox.get_blox_widget_style("dash123", "widget456")
        assert result == {
            "currentCard": {"style": "body { color: red; }", "body": [{"type": "TextBlock", "text": "Hello"}]},
            "currentConfig": {"fontFamily": "Arial", "fontSizes": {"default": 16}},
        }

    def test_returns_error_for_non_blox_widget_type(self):
        blox = _make_blox(get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _NON_BLOX_WIDGET)})
        result = blox.get_blox_widget_style("dash123", "widget456")
        assert "error" in result
        assert "BloX" in result["error"]

    def test_returns_error_on_none_response(self):
        blox = _make_blox()
        result = blox.get_blox_widget_style("dash123", "widget456")
        assert "error" in result

    def test_returns_error_on_403(self):
        blox = _make_blox(get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(403, {"message": "forbidden"})})
        result = blox.get_blox_widget_style("dash123", "widget456")
        assert "error" in result

    def test_returns_error_on_404(self):
        blox = _make_blox(get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(404, {"message": "not found"})})
        result = blox.get_blox_widget_style("dash123", "widget456")
        assert "error" in result

    def test_returns_empty_dicts_when_style_block_absent(self):
        widget_no_style = {**_blox_widget(), "style": {}}
        blox = _make_blox(get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, widget_no_style)})
        result = blox.get_blox_widget_style("dash123", "widget456")
        assert result == {"currentCard": {}, "currentConfig": {}}

    def test_returns_empty_dicts_when_style_key_missing(self):
        widget_no_style_key = {k: v for k, v in _blox_widget().items() if k != "style"}
        blox = _make_blox(get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, widget_no_style_key)})
        result = blox.get_blox_widget_style("dash123", "widget456")
        assert result == {"currentCard": {}, "currentConfig": {}}


# ---------------------------------------------------------------------------
# update_blox_widget_style — no ownership swap
# ---------------------------------------------------------------------------


class TestUpdateBloxWidgetStyleNoOwnership:
    def test_updates_card_only(self):
        blox = _make_blox(
            get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _blox_widget())},
            put_responses={"/api/dashboards/dash123/widgets/widget456": FakeResponse(200, {})},
        )
        new_card = {"style": "body { color: blue; }", "body": []}
        result = blox.update_blox_widget_style("dash123", "widget456", current_card=new_card)
        assert "error" not in result
        assert result["currentCard"] == new_card
        assert result["currentConfig"] == {"fontFamily": "Arial", "fontSizes": {"default": 16}}

    def test_updates_config_only(self):
        blox = _make_blox(
            get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _blox_widget())},
            put_responses={"/api/dashboards/dash123/widgets/widget456": FakeResponse(200, {})},
        )
        new_config = {"fontFamily": "Roboto", "fontSizes": {"default": 14}}
        result = blox.update_blox_widget_style("dash123", "widget456", current_config=new_config)
        assert "error" not in result
        assert result["currentConfig"] == new_config
        assert result["currentCard"] == {"style": "body { color: red; }", "body": [{"type": "TextBlock", "text": "Hello"}]}

    def test_updates_both_objects(self):
        blox = _make_blox(
            get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _blox_widget())},
            put_responses={"/api/dashboards/dash123/widgets/widget456": FakeResponse(200, {})},
        )
        new_card = {"style": "body { color: green; }"}
        new_config = {"fontFamily": "Georgia"}
        result = blox.update_blox_widget_style("dash123", "widget456", current_card=new_card, current_config=new_config)
        assert result == {"currentCard": new_card, "currentConfig": new_config}

    def test_read_modify_write_roundtrip(self):
        # The documented flow: get the objects, tweak a field, pass them back
        blox = _make_blox(
            get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _blox_widget())},
            put_responses={"/api/dashboards/dash123/widgets/widget456": FakeResponse(200, {})},
        )
        style = blox.get_blox_widget_style("dash123", "widget456")
        style["currentCard"]["style"] = "body { color: blue; }"
        result = blox.update_blox_widget_style("dash123", "widget456", current_card=style["currentCard"])
        assert result["currentCard"]["style"] == "body { color: blue; }"
        assert result["currentCard"]["body"] == [{"type": "TextBlock", "text": "Hello"}]

    def test_returns_current_style_when_no_objects_provided(self):
        # No PUT should be made — returns current style immediately
        blox = _make_blox(
            get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _blox_widget())},
        )
        result = blox.update_blox_widget_style("dash123", "widget456")
        assert result == {
            "currentCard": {"style": "body { color: red; }", "body": [{"type": "TextBlock", "text": "Hello"}]},
            "currentConfig": {"fontFamily": "Arial", "fontSizes": {"default": 16}},
        }

    def test_returns_error_for_non_blox_widget(self):
        blox = _make_blox(get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _NON_BLOX_WIDGET)})
        result = blox.update_blox_widget_style("dash123", "widget456", current_card={"style": "body {}"})
        assert "error" in result
        assert "BloX" in result["error"]

    def test_returns_error_on_none_fetch_response(self):
        blox = _make_blox()
        result = blox.update_blox_widget_style("dash123", "widget456", current_card={"style": "body {}"})
        assert "error" in result

    def test_returns_error_on_put_failure(self):
        blox = _make_blox(
            get_responses={"/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _blox_widget())},
            put_responses={"/api/dashboards/dash123/widgets/widget456": FakeResponse(500, {"error": "fail"})},
        )
        result = blox.update_blox_widget_style("dash123", "widget456", current_card={"style": "body {}"})
        assert "error" in result


# ---------------------------------------------------------------------------
# update_blox_widget_style — with ownership swap
# ---------------------------------------------------------------------------


class _TrackingApiClient(FakeApiClient):
    """FakeApiClient that records every POST url (before query-params)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.post_urls: list[str] = []

    def post(self, url, data=None, **kwargs):
        self.post_urls.append(url.split("?")[0])
        return super().post(url, data=data, **kwargs)


def _make_ownership_client(put_response: FakeResponse) -> _TrackingApiClient:
    """Build a tracking client set up for the ownership-swap flow."""
    logger = FakeLogger()
    return _TrackingApiClient(
        get_responses={
            "/api/v1/dashboards/admin": FakeResponse(200, [{"owner": "original_owner"}]),
            "/api/shares/dashboard/dash123": FakeResponse(200, {"sharesTo": []}),
            "/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _blox_widget()),
        },
        post_responses={
            "/api/v1/dashboards/dash123/change_owner": FakeResponse(200, {}),
            "/api/shares/dashboard/dash123": FakeResponse(200, {}),
        },
        put_responses={
            "/api/dashboards/dash123/widgets/widget456": put_response,
        },
        logger=logger,
    )


class TestUpdateBloxWidgetStyleOwnership:
    def test_ownership_taken_and_restored_on_success(self):
        client = _make_ownership_client(FakeResponse(200, {}))
        blox = Blox(api_client=client)

        result = blox.update_blox_widget_style("dash123", "widget456", current_card={"style": "body {}"}, executing_user_id="my_user_id")

        assert "error" not in result
        # change_owner called twice: once to take, once to restore
        change_owner_calls = [u for u in client.post_urls if "change_owner" in u]
        assert len(change_owner_calls) == 2

    def test_ownership_restored_when_write_fails(self):
        client = _make_ownership_client(FakeResponse(500, {"error": "write failed"}))
        blox = Blox(api_client=client)

        result = blox.update_blox_widget_style("dash123", "widget456", current_card={"style": "body {}"}, executing_user_id="my_user_id")

        assert "error" in result
        # Ownership restoration must still have been attempted
        change_owner_calls = [u for u in client.post_urls if "change_owner" in u]
        assert len(change_owner_calls) == 2
        share_restore_calls = [u for u in client.post_urls if "shares/dashboard" in u]
        assert len(share_restore_calls) >= 1

    def test_returns_error_when_take_ownership_fails(self):
        # Simulate failure fetching the original owner
        logger = FakeLogger()
        client = _TrackingApiClient(
            get_responses={
                "/api/v1/dashboards/dash123/widgets/widget456": FakeResponse(200, _blox_widget()),
                # No admin endpoint → None → take_ownership fails
            },
            logger=logger,
        )
        blox = Blox(api_client=client)
        result = blox.update_blox_widget_style("dash123", "widget456", current_card={"style": "body {}"}, executing_user_id="my_user_id")

        assert "error" in result
        # No ownership change or restoration should have been called
        change_owner_calls = [u for u in client.post_urls if "change_owner" in u]
        assert len(change_owner_calls) == 0
