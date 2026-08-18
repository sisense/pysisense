"""Unit tests for pysisense.mergetool.datamodels.DatamodelsMergeMixin."""

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.mergetool import MergeTool

_SALES_EXTRACT = {"oid": "dm1", "title": "Sales Cube", "type": "extract", "status": "done", "sizeInMb": 10}
_MARKETING_LIVE = {"oid": "dm2", "title": "Marketing Live", "type": "live", "status": "done", "sizeInMb": 0}

_EXPORTED_SALES = {
    "oid": "dm1",
    "title": "Sales Cube",
    "datasets": [{"connection": {"provider": "Athena", "parameters": "secret-creds"}}],
}


def _ecm_response(datamodels: list[dict]) -> FakeResponse:
    return FakeResponse(200, {"data": {"elasticubesMetadata": datamodels}})


class CapturingFakeApiClient(FakeApiClient):
    """FakeApiClient that also records every POST/PUT/PATCH call for assertions."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls: list[tuple[str, str, object]] = []

    def post(self, url, data=None, **kwargs):
        self.calls.append(("POST", url, data))
        return super().post(url, data=data, **kwargs)

    def put(self, url, data=None, **kwargs):
        self.calls.append(("PUT", url, data))
        return super().put(url, data=data, **kwargs)

    def patch(self, url, data=None, **kwargs):
        self.calls.append(("PATCH", url, data))
        return super().patch(url, data=data, **kwargs)


def _make_merge(src_get=None, src_post=None, tgt_get=None, tgt_post=None, tgt_put=None, tgt_patch=None, capture_target=False):
    """Build a MergeTool instance backed by separate FakeApiClient source/target."""
    src = FakeApiClient(get_responses=src_get, post_responses=src_post, logger=FakeLogger())
    tgt_cls = CapturingFakeApiClient if capture_target else FakeApiClient
    tgt = tgt_cls(get_responses=tgt_get, post_responses=tgt_post, put_responses=tgt_put, patch_responses=tgt_patch, logger=FakeLogger())
    return MergeTool(source_client=src, target_client=tgt)


def _basic_source(datamodels, export_response=None):
    """Source GET/POST fixtures: the ecm listing plus (optionally) a fixed export response."""
    src_post = {"/api/v2/ecm/": _ecm_response(datamodels)}
    src_get = {}
    if export_response is not None:
        src_get["/api/v2/datamodel-exports/schema"] = export_response
    return src_get, src_post


def _basic_target(existing_datamodels=None, tgt_post_extra=None):
    tgt_post = {"/api/v2/ecm/": _ecm_response(existing_datamodels or [])}
    if tgt_post_extra:
        tgt_post.update(tgt_post_extra)
    return {}, tgt_post


# ---------------------------------------------------------------------------
# migrate_datamodels — validation and fetch failures
# ---------------------------------------------------------------------------


class TestMigrateDatamodelsValidation:
    def test_both_ids_and_names_raises(self):
        merge = _make_merge()
        with pytest.raises(ValueError, match="not both"):
            merge.migrate_datamodels(datamodel_ids=["dm1"], datamodel_names=["Sales Cube"])

    def test_neither_ids_nor_names_raises(self):
        merge = _make_merge()
        with pytest.raises(ValueError, match="Provide either"):
            merge.migrate_datamodels()

    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_post={"/api/v2/ecm/": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_datamodels(datamodel_ids=["dm1"])
        assert result["ok"] is False
        assert result["status"] == "failed"
        assert result["source_count"] == 0


# ---------------------------------------------------------------------------
# migrate_datamodels — id/name filtering
# ---------------------------------------------------------------------------


class TestMigrateDatamodelsFiltering:
    def test_missing_id_is_reported_as_failed(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], export_response=FakeResponse(200, _EXPORTED_SALES))
        _, tgt_post = _basic_target(tgt_post_extra={"/api/v2/datamodel-imports/schema": FakeResponse(201, {"oid": "dm1"})})
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post)
        result = merge.migrate_datamodels(datamodel_ids=["dm1", "ghost"])
        failed_ids = {f["source_oid"] for f in result["failed"]}
        assert "ghost" in failed_ids

    def test_missing_name_is_reported_as_failed(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], export_response=FakeResponse(200, _EXPORTED_SALES))
        _, tgt_post = _basic_target(tgt_post_extra={"/api/v2/datamodel-imports/schema": FakeResponse(201, {"oid": "dm1"})})
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post)
        result = merge.migrate_datamodels(datamodel_names=["Sales Cube", "Ghost Cube"])
        failed_titles = {f["title"] for f in result["failed"]}
        assert "Ghost Cube" in failed_titles

    def test_no_matching_datamodels_is_noop(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT])
        merge = _make_merge(src_get=src_get, src_post=src_post)
        result = merge.migrate_datamodels(datamodel_ids=["ghost"])
        assert result["ok"] is True
        assert result["status"] == "noop"


# ---------------------------------------------------------------------------
# migrate_datamodels — conflict handling
# ---------------------------------------------------------------------------


class TestMigrateDatamodelsConflictHandling:
    def test_skip_leaves_existing_datamodel_unchanged(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT])
        _, tgt_post = _basic_target(existing_datamodels=[_SALES_EXTRACT])
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post)
        result = merge.migrate_datamodels(datamodel_ids=["dm1"], action="skip")
        assert result["skipped"] == [{"title": "Sales Cube", "source_oid": "dm1", "reason": "Already exists on target."}]
        assert result["ok"] is True
        assert result["status"] == "success"

    def test_overwrite_targets_existing_oid_and_imports(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], export_response=FakeResponse(200, _EXPORTED_SALES))
        _, tgt_post = _basic_target(
            existing_datamodels=[_SALES_EXTRACT],
            tgt_post_extra={"/api/v2/datamodel-imports/schema?datamodelId=dm1": FakeResponse(201, {"oid": "dm1"})},
        )
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post)
        result = merge.migrate_datamodels(datamodel_ids=["dm1"], action="overwrite")
        assert result["succeeded"] == [{"title": "Sales Cube", "source_oid": "dm1", "target_id": "dm1"}]

    def test_duplicate_always_imports_regardless_of_conflict(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], export_response=FakeResponse(200, _EXPORTED_SALES))
        _, tgt_post = _basic_target(
            existing_datamodels=[_SALES_EXTRACT],
            tgt_post_extra={"/api/v2/datamodel-imports/schema?newTitle=Sales Cube (Duplicate)": FakeResponse(201, {"oid": "dm3"})},
        )
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post)
        result = merge.migrate_datamodels(datamodel_ids=["dm1"], action="duplicate")
        assert result["succeeded"] == [{"title": "Sales Cube", "source_oid": "dm1", "target_id": "dm3"}]
        assert result["skipped"] == []

    def test_overwrite_without_conflict_creates_plainly(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], export_response=FakeResponse(200, _EXPORTED_SALES))
        _, tgt_post = _basic_target(tgt_post_extra={"/api/v2/datamodel-imports/schema": FakeResponse(201, {"oid": "dm1"})})
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post)
        result = merge.migrate_datamodels(datamodel_ids=["dm1"], action="overwrite")
        assert result["succeeded"] == [{"title": "Sales Cube", "source_oid": "dm1", "target_id": "dm1"}]


# ---------------------------------------------------------------------------
# migrate_datamodels — export/import failure handling
# ---------------------------------------------------------------------------


class TestMigrateDatamodelsExportImportFailures:
    def test_export_failure_marks_datamodel_failed(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], export_response=FakeResponse(500, {"error": "boom"}))
        _, tgt_post = _basic_target()
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post)
        result = merge.migrate_datamodels(datamodel_ids=["dm1"])
        assert result["failed"][0]["source_oid"] == "dm1"
        assert "Export failed" in result["failed"][0]["reason"]

    def test_import_error_marks_datamodel_failed(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], export_response=FakeResponse(200, _EXPORTED_SALES))
        _, tgt_post = _basic_target(tgt_post_extra={"/api/v2/datamodel-imports/schema": FakeResponse(400, {"error": "bad request"})})
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post)
        result = merge.migrate_datamodels(datamodel_ids=["dm1"])
        assert result["failed"][0]["source_oid"] == "dm1"
        assert "Import failed" in result["failed"][0]["reason"]

    def test_already_exists_conflict_gives_clear_reason(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], export_response=FakeResponse(200, _EXPORTED_SALES))
        _, tgt_post = _basic_target(tgt_post_extra={"/api/v2/datamodel-imports/schema": FakeResponse(400, {"title": "ElasticubeAlreadyExists"})})
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post)
        result = merge.migrate_datamodels(datamodel_ids=["dm1"])
        assert "already exists on the target with a different ID" in result["failed"][0]["reason"]


# ---------------------------------------------------------------------------
# migrate_datamodels — connection remapping
# ---------------------------------------------------------------------------


class TestMigrateDatamodelsConnectionRemapping:
    def test_unmapped_provider_strips_connection_parameters(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], export_response=FakeResponse(200, _EXPORTED_SALES))
        _, tgt_post = _basic_target(tgt_post_extra={"/api/v2/datamodel-imports/schema": FakeResponse(201, {"oid": "dm1"})})
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post, capture_target=True)

        merge.migrate_datamodels(datamodel_ids=["dm1"])

        calls = merge.target_client.calls
        import_call = next(c for c in calls if c[0] == "POST" and c[1].startswith("/api/v2/datamodel-imports/schema"))
        assert import_call[2]["datasets"][0]["connection"]["parameters"] == ""

    def test_mapped_provider_repoints_connection(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], export_response=FakeResponse(200, _EXPORTED_SALES))
        _, tgt_post = _basic_target(tgt_post_extra={"/api/v2/datamodel-imports/schema": FakeResponse(201, {"oid": "dm1"})})
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post, capture_target=True)

        merge.migrate_datamodels(datamodel_ids=["dm1"], provider_connection_map={"Athena": "target-conn-oid"})

        calls = merge.target_client.calls
        import_call = next(c for c in calls if c[0] == "POST" and c[1].startswith("/api/v2/datamodel-imports/schema"))
        assert import_call[2]["datasets"][0]["connection"] == {"oid": "target-conn-oid", "provider": "Athena"}


# ---------------------------------------------------------------------------
# migrate_datamodels — concurrency
# ---------------------------------------------------------------------------


class TestMigrateDatamodelsConcurrency:
    def test_concurrent_datamodels_all_imported(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT, _MARKETING_LIVE], export_response=FakeResponse(200, _EXPORTED_SALES))
        _, tgt_post = _basic_target(tgt_post_extra={"/api/v2/datamodel-imports/schema": FakeResponse(201, {"oid": "new"})})
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post, capture_target=True)

        result = merge.migrate_datamodels(datamodel_ids=["dm1", "dm2"], concurrency=2)

        assert result["ok"] is True
        assert result["succeeded_count"] == 2
        import_calls = [c for c in merge.target_client.calls if c[0] == "POST" and c[1] == "/api/v2/datamodel-imports/schema"]
        assert len(import_calls) == 2


# ---------------------------------------------------------------------------
# migrate_all_datamodels
# ---------------------------------------------------------------------------


class TestMigrateAllDatamodels:
    def test_migrates_every_source_datamodel(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT, _MARKETING_LIVE], export_response=FakeResponse(200, _EXPORTED_SALES))
        _, tgt_post = _basic_target(tgt_post_extra={"/api/v2/datamodel-imports/schema": FakeResponse(201, {"oid": "new"})})
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post)
        result = merge.migrate_all_datamodels()
        assert result["source_count"] == 2
        assert result["succeeded_count"] == 2

    def test_no_source_datamodels_is_noop(self):
        merge = _make_merge(src_post={"/api/v2/ecm/": _ecm_response([])})
        result = merge.migrate_all_datamodels()
        assert result["ok"] is True
        assert result["status"] == "noop"

    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_post={"/api/v2/ecm/": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_all_datamodels()
        assert result["ok"] is False
        assert result["status"] == "failed"


# ---------------------------------------------------------------------------
# migrate_datamodels — shares migration
# ---------------------------------------------------------------------------


class TestMigrateDatamodelsShares:
    def test_extract_shares_are_remapped_and_put(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], export_response=FakeResponse(200, _EXPORTED_SALES))
        src_get["/api/v1/users"] = FakeResponse(200, [{"_id": "src_user_1", "email": "alice@example.com"}])
        src_get["/api/v1/groups"] = FakeResponse(200, [])
        src_get["/api/elasticubes/localhost/Sales Cube/permissions"] = FakeResponse(200, {"shares": [{"type": "user", "partyId": "src_user_1", "permission": "a"}]})

        tgt_get = {
            "/api/v1/users": FakeResponse(200, [{"_id": "tgt_user_1", "email": "alice@example.com"}]),
            "/api/v1/groups": FakeResponse(200, []),
        }
        _, tgt_post = _basic_target(tgt_post_extra={"/api/v2/datamodel-imports/schema": FakeResponse(201, {"oid": "dm1"})})
        tgt_put = {"/api/elasticubes/localhost/Sales Cube/permissions": FakeResponse(200, {})}

        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post, tgt_put=tgt_put, capture_target=True)

        result = merge.migrate_datamodels(datamodel_ids=["dm1"], shares=True)

        assert result["succeeded_count"] == 1
        calls = merge.target_client.calls
        put_call = next(c for c in calls if c[0] == "PUT" and c[1] == "/api/elasticubes/localhost/Sales Cube/permissions")
        assert put_call[2] == [{"partyId": "tgt_user_1", "type": "user", "permission": "a"}]

    def test_shares_disabled_by_default_makes_no_share_calls(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], export_response=FakeResponse(200, _EXPORTED_SALES))
        _, tgt_post = _basic_target(tgt_post_extra={"/api/v2/datamodel-imports/schema": FakeResponse(201, {"oid": "dm1"})})
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_post=tgt_post, capture_target=True)

        result = merge.migrate_datamodels(datamodel_ids=["dm1"], shares=False)

        assert result["succeeded_count"] == 1
        assert all(c[0] != "PUT" for c in merge.target_client.calls)
