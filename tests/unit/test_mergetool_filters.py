"""Unit tests for pysisense.mergetool.filters.FiltersMergeMixin."""

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.mergetool import MergeTool

_SALES_EXTRACT = {"oid": "dm1", "title": "Sales Cube", "type": "extract", "status": "done", "sizeInMb": 10}

_SOURCE_FILTER = {
    "_id": "src-filter-1",
    "title": "East Region",
    "datasource": "Sales Cube",
    "filter": {"members": ["East"]},
    "created": "2024-01-01",
    "lastUpdated": "2024-01-02",
}

_PLAIN_DIMENSION = {"title": "Region", "datasource": "Sales Cube"}


def _ecm_response(datamodels: list[dict]) -> FakeResponse:
    return FakeResponse(200, {"data": {"elasticubesMetadata": datamodels}})


class CapturingFakeApiClient(FakeApiClient):
    """FakeApiClient that also records every POST call for assertions."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls: list[tuple[str, str, object]] = []

    def post(self, url, data=None, **kwargs):
        self.calls.append(("POST", url, data))
        return super().post(url, data=data, **kwargs)


def _make_merge(src_get=None, src_post=None, tgt_get=None, tgt_post=None, capture_target=False):
    """Build a MergeTool instance backed by separate FakeApiClient source/target."""
    src = FakeApiClient(get_responses=src_get, post_responses=src_post, logger=FakeLogger())
    tgt_cls = CapturingFakeApiClient if capture_target else FakeApiClient
    tgt = tgt_cls(get_responses=tgt_get, post_responses=tgt_post, logger=FakeLogger())
    return MergeTool(source_client=src, target_client=tgt)


def _basic_source(datamodels, dimensions_by_datasource=None):
    src_post = {"/api/v2/ecm/": _ecm_response(datamodels)}
    src_get = dict(dimensions_by_datasource or {})
    return src_get, src_post


def _basic_target(existing_datamodels=None, tgt_get_extra=None, tgt_post_extra=None):
    tgt_post = {"/api/v2/ecm/": _ecm_response(existing_datamodels or [])}
    if tgt_post_extra:
        tgt_post.update(tgt_post_extra)
    tgt_get = dict(tgt_get_extra or {})
    return tgt_get, tgt_post


# ---------------------------------------------------------------------------
# migrate_saved_filters — validation and fetch failures
# ---------------------------------------------------------------------------


class TestMigrateSavedFiltersValidation:
    def test_both_ids_and_names_raises(self):
        merge = _make_merge()
        with pytest.raises(ValueError, match="not both"):
            merge.migrate_saved_filters(datamodel_ids=["dm1"], datamodel_names=["Sales Cube"])

    def test_neither_ids_nor_names_raises(self):
        merge = _make_merge()
        with pytest.raises(ValueError, match="Provide either"):
            merge.migrate_saved_filters()

    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_post={"/api/v2/ecm/": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_saved_filters(datamodel_ids=["dm1"])
        assert result["ok"] is False
        assert result["status"] == "failed"
        assert result["source_count"] == 0


# ---------------------------------------------------------------------------
# migrate_saved_filters — id/name filtering and target existence
# ---------------------------------------------------------------------------


class TestMigrateSavedFiltersFiltering:
    def test_missing_id_is_reported_as_failed(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT])
        tgt_get, tgt_post = _basic_target()
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post)
        result = merge.migrate_saved_filters(datamodel_ids=["dm1", "ghost"])
        reasons = [f["reason"] for f in result["failed"]]
        assert any("ghost" in r for r in reasons)

    def test_datamodel_not_on_target_is_skipped(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT])
        tgt_get, tgt_post = _basic_target(existing_datamodels=[])
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post)
        result = merge.migrate_saved_filters(datamodel_ids=["dm1"])
        assert result["skipped"] == [{"datamodel": "Sales Cube", "filter": None, "reason": "Data model not found on target. Migrate the data model first."}]
        assert result["status"] == "noop"
        assert result["failed_count"] == 0


# ---------------------------------------------------------------------------
# migrate_saved_filters — no filters on source
# ---------------------------------------------------------------------------


class TestMigrateSavedFiltersNoFilters:
    def test_dimensions_without_filter_key_are_ignored(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], dimensions_by_datasource={"/api/metadata/dimensions": FakeResponse(200, [_PLAIN_DIMENSION])})
        tgt_get, tgt_post = _basic_target(existing_datamodels=[_SALES_EXTRACT])
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post)
        result = merge.migrate_saved_filters(datamodel_ids=["dm1"])
        assert result["succeeded"] == []
        assert result["skipped"] == []
        assert result["source_count"] == 0
        assert result["status"] == "noop"

    def test_fetch_failure_marks_datamodel_failed(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], dimensions_by_datasource={"/api/metadata/dimensions": FakeResponse(500, {"error": "boom"})})
        tgt_get, tgt_post = _basic_target(existing_datamodels=[_SALES_EXTRACT])
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post)
        result = merge.migrate_saved_filters(datamodel_ids=["dm1"])
        assert result["failed"][0]["datamodel"] == "Sales Cube"
        assert "Failed to fetch saved filters" in result["failed"][0]["reason"]


# ---------------------------------------------------------------------------
# migrate_saved_filters — creation, skip, and conflict handling
# ---------------------------------------------------------------------------


class TestMigrateSavedFiltersCreate:
    def test_filter_is_stripped_and_posted(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], dimensions_by_datasource={"/api/metadata/dimensions": FakeResponse(200, [_SOURCE_FILTER])})
        tgt_get, tgt_post = _basic_target(
            existing_datamodels=[_SALES_EXTRACT],
            tgt_get_extra={"/api/metadata/dimensions": FakeResponse(200, [])},
            tgt_post_extra={"/api/metadata": FakeResponse(201, {"title": "East Region"})},
        )
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post, capture_target=True)

        result = merge.migrate_saved_filters(datamodel_ids=["dm1"])

        assert result["succeeded"] == [{"datamodel": "Sales Cube", "filter": "East Region"}]
        post_call = next(c for c in merge.target_client.calls if c[0] == "POST" and c[1] == "/api/metadata")
        written_payload = post_call[2]
        assert "_id" not in written_payload
        assert "created" not in written_payload
        assert "lastUpdated" not in written_payload
        assert written_payload["filter"] == {"members": ["East"]}

    def test_existing_title_is_skipped_by_default(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], dimensions_by_datasource={"/api/metadata/dimensions": FakeResponse(200, [_SOURCE_FILTER])})
        tgt_get, tgt_post = _basic_target(
            existing_datamodels=[_SALES_EXTRACT],
            tgt_get_extra={"/api/metadata/dimensions": FakeResponse(200, [{"title": "East Region", "filter": {"members": ["East"]}}])},
        )
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post)

        result = merge.migrate_saved_filters(datamodel_ids=["dm1"])

        assert result["skipped"] == [{"datamodel": "Sales Cube", "filter": "East Region", "reason": "Already exists on target."}]
        assert result["succeeded"] == []

    def test_duplicate_action_ignores_existing_title(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], dimensions_by_datasource={"/api/metadata/dimensions": FakeResponse(200, [_SOURCE_FILTER])})
        tgt_get, tgt_post = _basic_target(
            existing_datamodels=[_SALES_EXTRACT],
            tgt_post_extra={"/api/metadata": FakeResponse(201, {"title": "East Region"})},
        )
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post)

        result = merge.migrate_saved_filters(datamodel_ids=["dm1"], action="duplicate")

        assert result["succeeded"] == [{"datamodel": "Sales Cube", "filter": "East Region"}]

    def test_create_failure_marks_filter_failed(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], dimensions_by_datasource={"/api/metadata/dimensions": FakeResponse(200, [_SOURCE_FILTER])})
        tgt_get, tgt_post = _basic_target(
            existing_datamodels=[_SALES_EXTRACT],
            tgt_get_extra={"/api/metadata/dimensions": FakeResponse(200, [])},
            tgt_post_extra={"/api/metadata": FakeResponse(500, {"error": "boom"})},
        )
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post)

        result = merge.migrate_saved_filters(datamodel_ids=["dm1"])

        assert result["failed"][0]["filter"] == "East Region"
        assert result["status"] == "failed"


# ---------------------------------------------------------------------------
# migrate_all_saved_filters
# ---------------------------------------------------------------------------


class TestMigrateAllSavedFilters:
    def test_migrates_every_source_datamodel(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], dimensions_by_datasource={"/api/metadata/dimensions": FakeResponse(200, [_SOURCE_FILTER])})
        tgt_get, tgt_post = _basic_target(
            existing_datamodels=[_SALES_EXTRACT],
            tgt_get_extra={"/api/metadata/dimensions": FakeResponse(200, [])},
            tgt_post_extra={"/api/metadata": FakeResponse(201, {"title": "East Region"})},
        )
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post)
        result = merge.migrate_all_saved_filters()
        assert result["source_count"] == 1
        assert result["succeeded_count"] == 1

    def test_no_source_datamodels_is_noop(self):
        merge = _make_merge(src_post={"/api/v2/ecm/": _ecm_response([])})
        result = merge.migrate_all_saved_filters()
        assert result["ok"] is True
        assert result["status"] == "noop"

    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_post={"/api/v2/ecm/": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_all_saved_filters()
        assert result["ok"] is False
        assert result["status"] == "failed"
