"""Unit tests for pysisense.mergetool.groups.GroupsMergeMixin."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.mergetool import MergeTool

_SALES_GROUP = {"_id": "group000000000000000001", "name": "Sales", "tenantId": "tenant-system"}
_FINANCE_GROUP = {"_id": "group000000000000000002", "name": "Finance", "tenantId": "tenant-system"}
_ADMINS_GROUP = {"_id": "group000000000000000003", "name": "Admins", "tenantId": "tenant-system"}
_EVERYONE_GROUP = {"_id": "group000000000000000004", "name": "Everyone", "tenantId": "tenant-system"}
_TENANT_GROUP = {"_id": "group000000000000000005", "name": "Acme Corp", "tenantId": "tenant-acme"}

_TENANTS = [
    {"_id": "tenant-system", "name": "system"},
    {"_id": "tenant-acme", "name": "Acme Corp"},
]


def _make_merge(
    src_get=None,
    src_post=None,
    src_delete=None,
    tgt_get=None,
    tgt_post=None,
    tgt_delete=None,
):
    """Build a MergeTool instance backed by separate FakeApiClient source/target."""
    src = FakeApiClient(get_responses=src_get, post_responses=src_post, delete_responses=src_delete, logger=FakeLogger())
    tgt = FakeApiClient(get_responses=tgt_get, post_responses=tgt_post, delete_responses=tgt_delete, logger=FakeLogger())
    return MergeTool(source_client=src, target_client=tgt)


# ---------------------------------------------------------------------------
# migrate_groups — fetch failures
# ---------------------------------------------------------------------------


class TestMigrateGroupsFetchFailures:
    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_get={"/api/v1/groups": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_groups(group_names=["Sales"])
        assert result["ok"] is False
        assert result["status"] == "failed"
        assert result["source_count"] == 0

    def test_source_fetch_none_response_returns_failed_summary(self):
        merge = _make_merge(src_get={"/api/v1/groups": None})
        result = merge.migrate_groups(group_names=["Sales"])
        assert result["ok"] is False
        assert result["status"] == "failed"


# ---------------------------------------------------------------------------
# migrate_groups — name filtering
# ---------------------------------------------------------------------------


class TestMigrateGroupsNameFiltering:
    def test_missing_name_is_reported_as_failed(self):
        merge = _make_merge(
            src_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])},
            tgt_get={"/api/v1/groups": FakeResponse(200, [])},
            tgt_post={"/api/v1/groups/bulk": FakeResponse(201, [{"name": "Sales"}])},
        )
        result = merge.migrate_groups(group_names=["Sales", "Ghost"])
        failed_names = {f["name"] for f in result["failed"]}
        assert "Ghost" in failed_names
        assert result["source_count"] == 1

    def test_no_matching_groups_is_noop(self):
        merge = _make_merge(src_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])})
        result = merge.migrate_groups(group_names=["Ghost"])
        assert result["ok"] is True
        assert result["status"] == "noop"
        assert result["source_count"] == 0

    def test_none_group_names_migrates_everything_on_source(self):
        merge = _make_merge(
            src_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP, _FINANCE_GROUP])},
            tgt_get={"/api/v1/groups": FakeResponse(200, [])},
            tgt_post={"/api/v1/groups/bulk": FakeResponse(201, [{"name": "Sales"}, {"name": "Finance"}])},
        )
        result = merge.migrate_groups(group_names=None)
        assert result["source_count"] == 2
        assert result["succeeded_count"] == 2


# ---------------------------------------------------------------------------
# migrate_groups — conflict handling (skip / overwrite / duplicate)
# ---------------------------------------------------------------------------


class TestMigrateGroupsConflictHandling:
    def test_skip_leaves_existing_group_unchanged(self):
        merge = _make_merge(
            src_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])},
            tgt_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])},
        )
        result = merge.migrate_groups(group_names=["Sales"], action="skip")
        assert result["skipped"] == [{"name": "Sales", "reason": "Already exists on target."}]
        assert result["succeeded_count"] == 0
        assert result["ok"] is True
        assert result["status"] == "success"

    def test_overwrite_deletes_existing_then_recreates(self):
        merge = _make_merge(
            src_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])},
            tgt_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])},
            tgt_delete={f"/api/v1/groups/{_SALES_GROUP['_id']}": FakeResponse(200, {})},
            tgt_post={"/api/v1/groups/bulk": FakeResponse(201, [{"name": "Sales"}])},
        )
        result = merge.migrate_groups(group_names=["Sales"], action="overwrite")
        assert result["succeeded"] == [{"name": "Sales"}]
        assert result["skipped"] == []

    def test_overwrite_delete_failure_still_proceeds_with_create(self):
        merge = _make_merge(
            src_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])},
            tgt_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])},
            tgt_delete={f"/api/v1/groups/{_SALES_GROUP['_id']}": FakeResponse(500, {"error": "cannot delete"})},
            tgt_post={"/api/v1/groups/bulk": FakeResponse(201, [{"name": "Sales"}])},
        )
        result = merge.migrate_groups(group_names=["Sales"], action="overwrite")
        assert result["succeeded"] == [{"name": "Sales"}]

    def test_duplicate_creates_regardless_of_conflict(self):
        merge = _make_merge(
            src_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])},
            tgt_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])},
            tgt_post={"/api/v1/groups/bulk": FakeResponse(201, [{"name": "Sales"}])},
        )
        result = merge.migrate_groups(group_names=["Sales"], action="duplicate")
        assert result["succeeded"] == [{"name": "Sales"}]
        assert result["skipped"] == []

    def test_all_skipped_with_no_failures_is_success(self):
        merge = _make_merge(
            src_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])},
            tgt_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])},
        )
        result = merge.migrate_groups(group_names=["Sales"], action="skip")
        assert result["ok"] is True
        assert result["status"] == "success"


# ---------------------------------------------------------------------------
# migrate_groups — bulk create response handling
# ---------------------------------------------------------------------------


class TestMigrateGroupsBulkCreate:
    def test_bulk_create_success_marks_all_succeeded(self):
        merge = _make_merge(
            src_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP, _FINANCE_GROUP])},
            tgt_get={"/api/v1/groups": FakeResponse(200, [])},
            tgt_post={"/api/v1/groups/bulk": FakeResponse(201, [{"name": "Sales"}, {"name": "Finance"}])},
        )
        result = merge.migrate_groups(group_names=["Sales", "Finance"])
        assert result["ok"] is True
        assert result["status"] == "success"
        assert {s["name"] for s in result["succeeded"]} == {"Sales", "Finance"}

    def test_bulk_create_failure_marks_all_failed(self):
        merge = _make_merge(
            src_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])},
            tgt_get={"/api/v1/groups": FakeResponse(200, [])},
            tgt_post={"/api/v1/groups/bulk": FakeResponse(400, {"error": "bad request"})},
        )
        result = merge.migrate_groups(group_names=["Sales"])
        assert result["ok"] is False
        assert result["status"] == "failed"
        assert result["failed"][0]["name"] == "Sales"

    def test_target_group_fetch_failure_treated_as_no_conflicts(self):
        merge = _make_merge(
            src_get={"/api/v1/groups": FakeResponse(200, [_SALES_GROUP])},
            tgt_get={"/api/v1/groups": FakeResponse(500, {"error": "boom"})},
            tgt_post={"/api/v1/groups/bulk": FakeResponse(201, [{"name": "Sales"}])},
        )
        result = merge.migrate_groups(group_names=["Sales"])
        assert result["succeeded"] == [{"name": "Sales"}]


# ---------------------------------------------------------------------------
# migrate_all_groups — filtering
# ---------------------------------------------------------------------------


class TestMigrateAllGroups:
    def test_excludes_builtin_groups_and_other_tenants(self):
        merge = _make_merge(
            src_get={
                "/api/v1/groups": FakeResponse(200, [_SALES_GROUP, _ADMINS_GROUP, _EVERYONE_GROUP, _TENANT_GROUP]),
                "/api/v1/tenants": FakeResponse(200, _TENANTS),
            },
            tgt_get={"/api/v1/groups": FakeResponse(200, [])},
            tgt_post={"/api/v1/groups/bulk": FakeResponse(201, [{"name": "Sales"}])},
        )
        result = merge.migrate_all_groups()
        assert result["source_count"] == 1
        assert result["succeeded"] == [{"name": "Sales"}]

    def test_no_tenants_endpoint_skips_tenant_filtering(self):
        merge = _make_merge(
            src_get={
                "/api/v1/groups": FakeResponse(200, [_SALES_GROUP, _TENANT_GROUP]),
                "/api/v1/tenants": FakeResponse(404, {"error": "not found"}),
            },
            tgt_get={"/api/v1/groups": FakeResponse(200, [])},
            tgt_post={"/api/v1/groups/bulk": FakeResponse(201, [{"name": "Sales"}, {"name": "Acme Corp"}])},
        )
        result = merge.migrate_all_groups()
        assert result["source_count"] == 2

    def test_no_source_groups_is_noop(self):
        merge = _make_merge(src_get={"/api/v1/groups": FakeResponse(200, [])})
        result = merge.migrate_all_groups()
        assert result["ok"] is True
        assert result["status"] == "noop"

    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_get={"/api/v1/groups": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_all_groups()
        assert result["ok"] is False
        assert result["status"] == "failed"

    def test_all_excluded_is_noop(self):
        merge = _make_merge(
            src_get={
                "/api/v1/groups": FakeResponse(200, [_ADMINS_GROUP, _EVERYONE_GROUP]),
                "/api/v1/tenants": FakeResponse(200, _TENANTS),
            },
        )
        result = merge.migrate_all_groups()
        assert result["ok"] is True
        assert result["status"] == "noop"
