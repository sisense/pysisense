"""Unit tests for pysisense.mergetool.dashboards.DashboardMergeMixin."""

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.mergetool import MergeTool

_SOURCE_DASHBOARD_SALES = {"oid": "dash1", "title": "Sales", "owner": "src_user_1", "parentFolder": "src_folder_1"}
_SOURCE_DASHBOARD_MARKETING = {"oid": "dash2", "title": "Marketing", "owner": "src_user_1", "parentFolder": None}

_EXPORTED_SALES = {
    "oid": "dash1",
    "title": "Sales",
    "owner": "src_user_1",
    "parentFolder": "src_folder_1",
    "shares": [
        {"type": "user", "shareId": "src_user_1", "rule": "edit"},
        {"type": "group", "shareId": "src_group_1", "rule": "view"},
    ],
    "widgets": [
        {"datasource": {"address": "SourceHost", "title": "MyCube", "fullname": "SourceHost/MyCube"}},
    ],
}

_EXPORTED_MARKETING = {"oid": "dash2", "title": "Marketing", "owner": "src_user_1", "shares": []}

_SOURCE_USERS = [{"_id": "src_user_1", "email": "alice@example.com"}]
_SOURCE_GROUPS = [{"_id": "src_group_1", "name": "Sales Team"}]
_TARGET_USERS = [{"_id": "tgt_user_1", "email": "alice@example.com"}]
_TARGET_GROUPS = [{"_id": "tgt_group_1", "name": "Sales Team"}]
_SOURCE_FOLDERS = [{"oid": "src_folder_1", "name": "Sales Folder", "parentId": None}]
_TARGET_FOLDERS = [{"oid": "tgt_folder_1", "name": "Sales Folder", "parentId": None}]

_IMPORT_SUCCESS = FakeResponse(200, {"succeded": [{"oid": "dash1", "title": "Sales"}], "failed": []})


class CapturingFakeApiClient(FakeApiClient):
    """FakeApiClient that also records every POST/PATCH call for assertions."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls: list[tuple[str, str, object]] = []

    def post(self, url, data=None, **kwargs):
        self.calls.append(("POST", url, data))
        return super().post(url, data=data, **kwargs)

    def patch(self, url, data=None, **kwargs):
        self.calls.append(("PATCH", url, data))
        return super().patch(url, data=data, **kwargs)


def _make_merge(src_get=None, src_post=None, tgt_get=None, tgt_post=None, tgt_patch=None, capture_target=False):
    """Build a MergeTool instance backed by separate FakeApiClient source/target."""
    src = FakeApiClient(get_responses=src_get, post_responses=src_post, logger=FakeLogger())
    tgt_cls = CapturingFakeApiClient if capture_target else FakeApiClient
    tgt = tgt_cls(get_responses=tgt_get, post_responses=tgt_post, patch_responses=tgt_patch, logger=FakeLogger())
    return MergeTool(source_client=src, target_client=tgt)


def _basic_source(dashboards, exported_by_id=None, extra=None):
    """Source GET fixtures with the standard users/groups/folders lists."""
    src_get = {
        "/api/v1/dashboards/admin": FakeResponse(200, dashboards),
        "/api/v1/users": FakeResponse(200, _SOURCE_USERS),
        "/api/v1/groups": FakeResponse(200, _SOURCE_GROUPS),
        "/api/v1/folders": FakeResponse(200, _SOURCE_FOLDERS),
    }
    if exported_by_id:
        for dash_id, exported in exported_by_id.items():
            src_get[f"/api/v1/dashboards/export?dashboardIds={dash_id}&adminAccess=true"] = FakeResponse(200, [exported])
    if extra:
        src_get.update(extra)
    return src_get


def _basic_target(existing_dashboards=None, tgt_get_extra=None, tgt_post=None):
    tgt_get = {
        "/api/v1/dashboards/admin": FakeResponse(200, existing_dashboards or []),
        "/api/v1/users": FakeResponse(200, _TARGET_USERS),
        "/api/v1/groups": FakeResponse(200, _TARGET_GROUPS),
        "/api/v1/folders": FakeResponse(200, _TARGET_FOLDERS),
        "/api/shares/dashboard/dash1": FakeResponse(200, {"sharesTo": []}),
    }
    if tgt_get_extra:
        tgt_get.update(tgt_get_extra)
    return tgt_get, (tgt_post or {"/api/v1/dashboards/import/bulk": _IMPORT_SUCCESS})


# ---------------------------------------------------------------------------
# migrate_dashboards — validation and fetch failures
# ---------------------------------------------------------------------------


class TestMigrateDashboardsValidation:
    def test_both_ids_and_names_raises(self):
        merge = _make_merge()
        with pytest.raises(ValueError, match="not both"):
            merge.migrate_dashboards(dashboard_ids=["dash1"], dashboard_names=["Sales"])

    def test_neither_ids_nor_names_raises(self):
        merge = _make_merge()
        with pytest.raises(ValueError, match="Provide either"):
            merge.migrate_dashboards()

    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_get={"/api/v1/dashboards/admin": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_dashboards(dashboard_ids=["dash1"])
        assert result["ok"] is False
        assert result["status"] == "failed"

    def test_source_fetch_none_response_returns_failed_summary(self):
        merge = _make_merge(src_get={"/api/v1/dashboards/admin": None})
        result = merge.migrate_dashboards(dashboard_ids=["dash1"])
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# migrate_dashboards — id/name filtering
# ---------------------------------------------------------------------------


class TestMigrateDashboardsFiltering:
    def test_missing_id_is_reported_as_failed(self):
        merge = _make_merge(src_get=_basic_source([_SOURCE_DASHBOARD_SALES]))
        result = merge.migrate_dashboards(dashboard_ids=["dash1", "ghost"])
        failed_oids = {f["source_oid"] for f in result["failed"]}
        assert "ghost" in failed_oids

    def test_missing_name_is_reported_as_failed(self):
        merge = _make_merge(src_get=_basic_source([_SOURCE_DASHBOARD_SALES]))
        result = merge.migrate_dashboards(dashboard_names=["Sales", "Ghost Dashboard"])
        failed_titles = {f["title"] for f in result["failed"]}
        assert "Ghost Dashboard" in failed_titles

    def test_no_matching_dashboards_is_noop(self):
        merge = _make_merge(src_get=_basic_source([_SOURCE_DASHBOARD_SALES]))
        result = merge.migrate_dashboards(dashboard_ids=["ghost"])
        assert result["ok"] is True
        assert result["status"] == "noop"


# ---------------------------------------------------------------------------
# migrate_dashboards — conflict handling
# ---------------------------------------------------------------------------


class TestMigrateDashboardsConflictHandling:
    def test_skip_leaves_existing_dashboard_unchanged(self):
        tgt_get, tgt_post = _basic_target(existing_dashboards=[{"oid": "dash1", "title": "Sales"}])
        merge = _make_merge(src_get=_basic_source([_SOURCE_DASHBOARD_SALES]), tgt_get=tgt_get, tgt_post=tgt_post)
        result = merge.migrate_dashboards(dashboard_ids=["dash1"], action="skip")
        assert result["skipped"] == [{"title": "Sales", "source_oid": "dash1", "reason": "Already exists on target."}]
        assert result["ok"] is True
        assert result["status"] == "success"

    def test_overwrite_reimports_existing_dashboard(self):
        tgt_get, tgt_post = _basic_target(existing_dashboards=[{"oid": "dash1", "title": "Sales"}])
        merge = _make_merge(
            src_get=_basic_source([_SOURCE_DASHBOARD_SALES], exported_by_id={"dash1": _EXPORTED_SALES}),
            tgt_get=tgt_get,
            tgt_post=tgt_post,
        )
        result = merge.migrate_dashboards(dashboard_ids=["dash1"], action="overwrite")
        assert result["succeeded"] == [{"title": "Sales", "oid": "dash1", "source_oid": "dash1"}]

    def test_duplicate_always_imports_regardless_of_conflict(self):
        tgt_get, tgt_post = _basic_target(existing_dashboards=[{"oid": "dash1", "title": "Sales"}])
        merge = _make_merge(
            src_get=_basic_source([_SOURCE_DASHBOARD_SALES], exported_by_id={"dash1": _EXPORTED_SALES}),
            tgt_get=tgt_get,
            tgt_post=tgt_post,
        )
        result = merge.migrate_dashboards(dashboard_ids=["dash1"], action="duplicate")
        assert result["succeeded"] == [{"title": "Sales", "oid": "dash1", "source_oid": "dash1"}]
        assert result["skipped"] == []


# ---------------------------------------------------------------------------
# migrate_dashboards — export/import failure handling
# ---------------------------------------------------------------------------


class TestMigrateDashboardsExportImportFailures:
    def test_export_failure_marks_dashboard_failed(self):
        tgt_get, tgt_post = _basic_target()
        merge = _make_merge(
            src_get=_basic_source(
                [_SOURCE_DASHBOARD_SALES],
                extra={"/api/v1/dashboards/export?dashboardIds=dash1&adminAccess=true": FakeResponse(500, {"error": "boom"})},
            ),
            tgt_get=tgt_get,
            tgt_post=tgt_post,
        )
        result = merge.migrate_dashboards(dashboard_ids=["dash1"])
        assert result["failed"][0]["source_oid"] == "dash1"
        assert "Export failed" in result["failed"][0]["reason"]

    def test_import_error_marks_dashboard_failed(self):
        tgt_get, _ = _basic_target()
        merge = _make_merge(
            src_get=_basic_source([_SOURCE_DASHBOARD_SALES], exported_by_id={"dash1": _EXPORTED_SALES}),
            tgt_get=tgt_get,
            tgt_post={"/api/v1/dashboards/import/bulk": FakeResponse(400, {"error": "bad request"})},
        )
        result = merge.migrate_dashboards(dashboard_ids=["dash1"])
        assert result["failed"][0]["source_oid"] == "dash1"
        assert "Import failed" in result["failed"][0]["reason"]

    def test_import_without_succeded_entries_marks_dashboard_failed(self):
        tgt_get, _ = _basic_target()
        merge = _make_merge(
            src_get=_basic_source([_SOURCE_DASHBOARD_SALES], exported_by_id={"dash1": _EXPORTED_SALES}),
            tgt_get=tgt_get,
            tgt_post={"/api/v1/dashboards/import/bulk": FakeResponse(200, {"succeded": [], "failed": [{"oid": "dash1"}]})},
        )
        result = merge.migrate_dashboards(dashboard_ids=["dash1"])
        assert result["failed"][0]["reason"] == "Import did not report success."


# ---------------------------------------------------------------------------
# migrate_dashboards — full success path (datasource rewrite, owner, shares, folder)
# ---------------------------------------------------------------------------


class TestMigrateDashboardsFullSuccess:
    def test_success_rewrites_datasource_and_remaps_owner_shares_folder(self):
        tgt_get, tgt_post = _basic_target()
        merge = _make_merge(
            src_get=_basic_source([_SOURCE_DASHBOARD_SALES], exported_by_id={"dash1": _EXPORTED_SALES}),
            tgt_get=tgt_get,
            tgt_post={**tgt_post, "/api/v1/dashboards/dash1/change_owner": FakeResponse(200, {"success": True}), "/api/shares/dashboard/dash1": FakeResponse(200, {"success": True})},
            tgt_patch={"/api/dashboards/dash1": FakeResponse(200, {"success": True})},
            capture_target=True,
        )
        result = merge.migrate_dashboards(dashboard_ids=["dash1"], action="overwrite")

        assert result["succeeded"] == [{"title": "Sales", "oid": "dash1", "source_oid": "dash1"}]

        calls = merge.target_client.calls
        import_call = next(c for c in calls if c[0] == "POST" and c[1].startswith("/api/v1/dashboards/import/bulk"))
        imported_payload = import_call[2][0]
        assert imported_payload["widgets"][0]["datasource"]["address"] == "LocalHost"
        assert imported_payload["widgets"][0]["datasource"]["fullname"] == "LocalHost/MyCube"

        assert any(c[0] == "POST" and "change_owner" in c[1] for c in calls)
        assert any(c[0] == "POST" and c[1].startswith("/api/shares/dashboard/dash1") for c in calls)
        assert any(c[0] == "PATCH" and c[1] == "/api/dashboards/dash1" and c[2] == {"parentFolder": "tgt_folder_1"} for c in calls)

    def test_unresolved_owner_and_missing_target_folder_does_not_fail_migration(self):
        tgt_get, tgt_post = _basic_target(tgt_get_extra={"/api/v1/folders": FakeResponse(200, [])})
        merge = _make_merge(
            src_get=_basic_source([_SOURCE_DASHBOARD_MARKETING], exported_by_id={"dash2": _EXPORTED_MARKETING}),
            tgt_get=tgt_get,
            tgt_post={**tgt_post, "/api/v1/dashboards/import/bulk": FakeResponse(200, {"succeded": [{"oid": "dash2", "title": "Marketing"}], "failed": []})},
        )
        result = merge.migrate_dashboards(dashboard_ids=["dash2"])
        assert result["succeeded"] == [{"title": "Marketing", "oid": "dash2", "source_oid": "dash2"}]


# ---------------------------------------------------------------------------
# migrate_all_dashboards
# ---------------------------------------------------------------------------


class TestMigrateAllDashboards:
    def test_migrates_every_source_dashboard(self):
        tgt_get, tgt_post = _basic_target()
        merge = _make_merge(
            src_get=_basic_source(
                [_SOURCE_DASHBOARD_SALES, _SOURCE_DASHBOARD_MARKETING],
                exported_by_id={"dash1": _EXPORTED_SALES, "dash2": _EXPORTED_MARKETING},
            ),
            tgt_get=tgt_get,
            tgt_post={
                **tgt_post,
                "/api/v1/dashboards/dash1/change_owner": FakeResponse(200, {"success": True}),
                "/api/shares/dashboard/dash1": FakeResponse(200, {"success": True}),
            },
            tgt_patch={"/api/dashboards/dash1": FakeResponse(200, {"success": True})},
        )
        result = merge.migrate_all_dashboards()
        assert result["source_count"] == 2
        assert result["succeeded_count"] == 2

    def test_no_source_dashboards_is_noop(self):
        merge = _make_merge(src_get={"/api/v1/dashboards/admin": FakeResponse(200, [])})
        result = merge.migrate_all_dashboards()
        assert result["ok"] is True
        assert result["status"] == "noop"

    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_get={"/api/v1/dashboards/admin": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_all_dashboards()
        assert result["ok"] is False
        assert result["status"] == "failed"
