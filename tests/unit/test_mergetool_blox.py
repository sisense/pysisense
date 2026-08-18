"""Unit tests for pysisense.mergetool.blox.BloxMergeMixin."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.mergetool import MergeTool

_ACTION_A = {"type": "SendEmail", "title": "Send Email"}
_ACTION_B = {"type": "RefreshDashboard", "title": "Refresh"}


class CapturingFakeApiClient(FakeApiClient):
    """FakeApiClient that also records every POST call for assertions."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls: list[tuple[str, str, object]] = []

    def post(self, url, data=None, **kwargs):
        self.calls.append(("POST", url, data))
        return super().post(url, data=data, **kwargs)


def _make_merge(src_get=None, tgt_get=None, tgt_post=None, capture_target=False, target_os="linux"):
    """Build a MergeTool instance backed by separate FakeApiClient source/target."""
    src = FakeApiClient(get_responses=src_get, logger=FakeLogger())
    tgt_cls = CapturingFakeApiClient if capture_target else FakeApiClient
    tgt = tgt_cls(get_responses=tgt_get, post_responses=tgt_post, logger=FakeLogger(), operating_system=target_os)
    return MergeTool(source_client=src, target_client=tgt)


# ---------------------------------------------------------------------------
# migrate_blox_actions — validation and fetch failures
# ---------------------------------------------------------------------------


class TestMigrateBloxActionsValidation:
    def test_windows_target_returns_failed_summary(self):
        merge = _make_merge(target_os="windows")
        result = merge.migrate_blox_actions()
        assert result["ok"] is False
        assert result["status"] == "failed"

    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_get={"/api/v1/blox/getCustomActions": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_blox_actions()
        assert result["ok"] is False
        assert result["status"] == "failed"


# ---------------------------------------------------------------------------
# migrate_blox_actions — type filtering
# ---------------------------------------------------------------------------


class TestMigrateBloxActionsFiltering:
    def test_missing_type_is_reported_as_failed(self):
        merge = _make_merge(
            src_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [_ACTION_A])},
            tgt_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [])},
            tgt_post={"/api/v1/blox/saveCustomAction": FakeResponse(201, {"success": True})},
        )
        result = merge.migrate_blox_actions(action_types=["SendEmail", "Ghost"])
        failed_types = {f["type"] for f in result["failed"]}
        assert "Ghost" in failed_types
        assert result["source_count"] == 1

    def test_no_matching_actions_is_noop(self):
        merge = _make_merge(src_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [_ACTION_A])})
        result = merge.migrate_blox_actions(action_types=["Ghost"])
        assert result["ok"] is True
        assert result["status"] == "noop"


# ---------------------------------------------------------------------------
# migrate_blox_actions — conflict handling (skip / overwrite / duplicate)
# ---------------------------------------------------------------------------


class TestMigrateBloxActionsConflictHandling:
    def test_skip_leaves_existing_action_unchanged(self):
        merge = _make_merge(
            src_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [_ACTION_A])},
            tgt_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [_ACTION_A])},
        )
        result = merge.migrate_blox_actions(action_types=["SendEmail"], action="skip")
        assert result["skipped"] == [{"type": "SendEmail", "reason": "Already exists on target."}]
        assert result["ok"] is True
        assert result["status"] == "success"

    def test_overwrite_deletes_existing_then_saves(self):
        merge = _make_merge(
            src_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [_ACTION_A])},
            tgt_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [_ACTION_A])},
            tgt_post={
                "/api/v1/blox/deleteCustomAction": FakeResponse(200, {"success": True}),
                "/api/v1/blox/saveCustomAction": FakeResponse(201, {"success": True}),
            },
        )
        result = merge.migrate_blox_actions(action_types=["SendEmail"], action="overwrite")
        assert result["succeeded"] == [{"type": "SendEmail"}]

    def test_duplicate_creates_regardless_of_conflict(self):
        merge = _make_merge(
            src_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [_ACTION_A])},
            tgt_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [_ACTION_A])},
            tgt_post={"/api/v1/blox/saveCustomAction": FakeResponse(201, {"success": True})},
        )
        result = merge.migrate_blox_actions(action_types=["SendEmail"], action="duplicate")
        assert result["succeeded"] == [{"type": "SendEmail"}]
        assert result["skipped"] == []

    def test_save_failure_marks_action_failed(self):
        merge = _make_merge(
            src_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [_ACTION_A])},
            tgt_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [])},
            tgt_post={"/api/v1/blox/saveCustomAction": FakeResponse(400, {"error": "bad request"})},
        )
        result = merge.migrate_blox_actions(action_types=["SendEmail"])
        assert result["failed"][0]["type"] == "SendEmail"


# ---------------------------------------------------------------------------
# migrate_blox_actions — concurrency
# ---------------------------------------------------------------------------


class TestMigrateBloxActionsConcurrency:
    def test_concurrent_actions_all_saved(self):
        merge = _make_merge(
            src_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [_ACTION_A, _ACTION_B])},
            tgt_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [])},
            tgt_post={"/api/v1/blox/saveCustomAction": FakeResponse(201, {"success": True})},
            capture_target=True,
        )
        result = merge.migrate_blox_actions(concurrency=2)
        assert result["ok"] is True
        assert result["succeeded_count"] == 2
        save_calls = [c for c in merge.target_client.calls if c[1] == "/api/v1/blox/saveCustomAction"]
        assert len(save_calls) == 2


# ---------------------------------------------------------------------------
# migrate_all_blox_actions
# ---------------------------------------------------------------------------


class TestMigrateAllBloxActions:
    def test_migrates_every_action_and_forwards_concurrency(self):
        merge = _make_merge(
            src_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [_ACTION_A, _ACTION_B])},
            tgt_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [])},
            tgt_post={"/api/v1/blox/saveCustomAction": FakeResponse(201, {"success": True})},
        )
        result = merge.migrate_all_blox_actions(concurrency=2)
        assert result["source_count"] == 2
        assert result["succeeded_count"] == 2

    def test_no_source_actions_is_noop(self):
        merge = _make_merge(src_get={"/api/v1/blox/getCustomActions": FakeResponse(200, [])})
        result = merge.migrate_all_blox_actions()
        assert result["ok"] is True
        assert result["status"] == "noop"
