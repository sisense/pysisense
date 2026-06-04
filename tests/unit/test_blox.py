"""Unit tests for pysisense.blox.Blox."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.blox import Blox


class FakeResponseEmpty(FakeResponse):
    """FakeResponse with an empty body — simulates a 200 with no JSON content."""

    def __init__(self, status_code: int) -> None:
        super().__init__(status_code, None)
        self.content = b""


# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------

_ACTION_A = {"type": "OpenDashboard"}
_ACTION_B = {"type": "ApplyFilter"}

_GET_ACTIONS_RESPONSE = FakeResponse(200, [_ACTION_A, _ACTION_B])
_GET_ACTIONS_EMPTY = FakeResponse(200, [])
_SAVE_OK_200 = FakeResponse(200, {"status": "saved"})
_SAVE_OK_201 = FakeResponse(201, {"status": "created"})
_SAVE_FAIL = FakeResponse(500, {"error": "internal server error"})
_DELETE_FAIL = FakeResponse(500, {"error": "internal server error"})


def _make_blox(get_responses=None, post_responses=None):
    """Build a Blox instance backed by FakeApiClient."""
    logger = FakeLogger()
    client = FakeApiClient(
        get_responses=get_responses,
        post_responses=post_responses,
        logger=logger,
    )
    return Blox(api_client=client)


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------


class TestBloxInit:
    def test_creates_with_fake_client(self):
        b = _make_blox()
        assert b is not None
        assert hasattr(b, "api_client")
        assert hasattr(b, "logger")


# ---------------------------------------------------------------------------
# get_blox_actions
# ---------------------------------------------------------------------------


class TestGetBloxActions:
    def test_returns_list_on_success(self):
        b = _make_blox(get_responses={"/api/v1/blox/getCustomActions": _GET_ACTIONS_RESPONSE})
        result = b.get_blox_actions()
        assert isinstance(result, list)
        assert len(result) == 2

    def test_returns_empty_list_on_200_with_no_actions(self):
        b = _make_blox(get_responses={"/api/v1/blox/getCustomActions": _GET_ACTIONS_EMPTY})
        result = b.get_blox_actions()
        assert result == []

    def test_returns_error_on_none_response(self):
        b = _make_blox()  # no responses → None
        result = b.get_blox_actions()
        assert isinstance(result, list)
        assert "error" in result[0]

    def test_returns_error_on_non_200(self):
        b = _make_blox(get_responses={"/api/v1/blox/getCustomActions": FakeResponse(403, {"message": "forbidden"})})
        result = b.get_blox_actions()
        assert "error" in result[0]

    def test_returns_error_on_500(self):
        b = _make_blox(get_responses={"/api/v1/blox/getCustomActions": FakeResponse(500, {"error": "server error"})})
        result = b.get_blox_actions()
        assert "error" in result[0]

    def test_error_message_includes_status_code(self):
        b = _make_blox(get_responses={"/api/v1/blox/getCustomActions": FakeResponse(403, {})})
        result = b.get_blox_actions()
        assert "403" in result[0]["error"]

    def test_returns_error_when_response_is_not_a_list(self):
        b = _make_blox(get_responses={"/api/v1/blox/getCustomActions": FakeResponse(200, {"actions": []})})
        result = b.get_blox_actions()
        assert "error" in result[0]


# ---------------------------------------------------------------------------
# save_blox_action
# ---------------------------------------------------------------------------


class TestSaveBloxAction:
    def test_returns_response_json_on_200(self):
        b = _make_blox(post_responses={"/api/v1/blox/saveCustomAction": _SAVE_OK_200})
        result = b.save_blox_action(_ACTION_A)
        assert isinstance(result, dict)
        assert "error" not in result

    def test_returns_response_json_on_201(self):
        b = _make_blox(post_responses={"/api/v1/blox/saveCustomAction": _SAVE_OK_201})
        result = b.save_blox_action(_ACTION_A)
        assert isinstance(result, dict)
        assert "error" not in result

    def test_returns_error_on_none_response(self):
        b = _make_blox()  # no responses → None
        result = b.save_blox_action(_ACTION_A)
        assert "error" in result

    def test_returns_error_on_non_200_201(self):
        b = _make_blox(post_responses={"/api/v1/blox/saveCustomAction": _SAVE_FAIL})
        result = b.save_blox_action(_ACTION_A)
        assert "error" in result

    def test_returns_error_on_400(self):
        b = _make_blox(post_responses={"/api/v1/blox/saveCustomAction": FakeResponse(400, {"message": "bad request"})})
        result = b.save_blox_action(_ACTION_A)
        assert "error" in result

    def test_error_message_includes_status_code(self):
        b = _make_blox(post_responses={"/api/v1/blox/saveCustomAction": FakeResponse(422, {})})
        result = b.save_blox_action(_ACTION_A)
        assert "422" in result["error"]

    def test_error_message_includes_action_type(self):
        b = _make_blox()  # no response → None
        result = b.save_blox_action(_ACTION_A)
        assert "OpenDashboard" in result["error"]

    def test_action_without_type_uses_unnamed_in_error(self):
        b = _make_blox()  # no response → None
        result = b.save_blox_action({})
        assert "<unnamed>" in result["error"]

    def test_returns_success_dict_when_response_has_no_body(self):
        # Sisense returns 200 with an empty body on successful save
        b = _make_blox(post_responses={"/api/v1/blox/saveCustomAction": FakeResponseEmpty(200)})
        result = b.save_blox_action(_ACTION_A)
        assert result == {"success": True}


# ---------------------------------------------------------------------------
# delete_blox_action
# ---------------------------------------------------------------------------


class TestDeleteBloxAction:
    def test_returns_success_dict_when_response_has_no_body(self):
        # Sisense returns 200 with an empty body on successful delete
        b = _make_blox(post_responses={"/api/v1/blox/deleteCustomAction": FakeResponseEmpty(200)})
        result = b.delete_blox_action("OpenDashboard")
        assert result == {"success": True}

    def test_returns_response_json_on_200(self):
        b = _make_blox(post_responses={"/api/v1/blox/deleteCustomAction": FakeResponse(200, {"status": "deleted"})})
        result = b.delete_blox_action("OpenDashboard")
        assert isinstance(result, dict)
        assert "error" not in result

    def test_returns_error_on_none_response(self):
        b = _make_blox()  # no responses → None
        result = b.delete_blox_action("OpenDashboard")
        assert "error" in result

    def test_returns_error_on_non_200(self):
        b = _make_blox(post_responses={"/api/v1/blox/deleteCustomAction": _DELETE_FAIL})
        result = b.delete_blox_action("OpenDashboard")
        assert "error" in result

    def test_returns_error_on_400(self):
        b = _make_blox(post_responses={"/api/v1/blox/deleteCustomAction": FakeResponse(400, {"message": "bad request"})})
        result = b.delete_blox_action("OpenDashboard")
        assert "error" in result

    def test_error_message_includes_status_code(self):
        b = _make_blox(post_responses={"/api/v1/blox/deleteCustomAction": FakeResponse(404, {})})
        result = b.delete_blox_action("OpenDashboard")
        assert "404" in result["error"]

    def test_error_message_includes_action_type(self):
        b = _make_blox()  # no response → None
        result = b.delete_blox_action("OpenDashboard")
        assert "OpenDashboard" in result["error"]


# ---------------------------------------------------------------------------
# Windows OS guards
# ---------------------------------------------------------------------------


def _make_blox_windows(post_responses=None, get_responses=None):
    logger = FakeLogger()
    client = FakeApiClient(
        get_responses=get_responses or {},
        post_responses=post_responses or {},
        logger=logger,
        operating_system="windows",
    )
    return Blox(api_client=client)


class TestWindowsGuards:
    def test_save_blox_action_returns_error_on_windows(self):
        b = _make_blox_windows()
        result = b.save_blox_action({"type": "MyAction"})
        assert "error" in result
        assert "Windows" in result["error"]

    def test_delete_blox_action_returns_error_on_windows(self):
        b = _make_blox_windows()
        result = b.delete_blox_action("MyAction")
        assert "error" in result
        assert "Windows" in result["error"]

    def test_get_blox_actions_succeeds_on_windows_endpoint(self):
        b = _make_blox_windows(get_responses={"/api/v1/getCustomActions/actions": FakeResponse(200, [{"type": "A"}])})
        result = b.get_blox_actions()
        assert isinstance(result, list)
        assert len(result) == 1
