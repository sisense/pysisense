"""Unit tests for pysisense.mergetool.datasecurity.DatasecurityMergeMixin."""

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.mergetool import MergeTool

_SALES_EXTRACT = {"oid": "dm1", "title": "Sales Cube", "type": "extract", "status": "done", "sizeInMb": 10}
_MARKETING_LIVE = {"oid": "dm2", "title": "Marketing Live", "type": "live", "status": "done", "sizeInMb": 0}

_EXTRACT_RULE = {
    "table": "Customers",
    "column": "Region",
    "datatype": "text",
    "members": ["East"],
    "exclusionary": False,
    "shares": [{"type": "user", "partyId": "src_user_1"}, {"type": "default"}],
}

_LIVE_RULE = {
    "table": "Customers",
    "column": "Region",
    "datatype": "text",
    "members": ["East"],
    "exclusionary": False,
    "shares": [{"type": "group", "partyId": "src_group_1"}],
}


def _ecm_response(datamodels: list[dict]) -> FakeResponse:
    return FakeResponse(200, {"data": {"elasticubesMetadata": datamodels}})


class CapturingFakeApiClient(FakeApiClient):
    """FakeApiClient that also records every POST/PUT call for assertions."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls: list[tuple[str, str, object]] = []

    def post(self, url, data=None, **kwargs):
        self.calls.append(("POST", url, data))
        return super().post(url, data=data, **kwargs)

    def put(self, url, data=None, **kwargs):
        self.calls.append(("PUT", url, data))
        return super().put(url, data=data, **kwargs)


def _make_merge(src_get=None, src_post=None, tgt_get=None, tgt_post=None, tgt_put=None, capture_target=False):
    """Build a MergeTool instance backed by separate FakeApiClient source/target."""
    src = FakeApiClient(get_responses=src_get, post_responses=src_post, logger=FakeLogger())
    tgt_cls = CapturingFakeApiClient if capture_target else FakeApiClient
    tgt = tgt_cls(get_responses=tgt_get, post_responses=tgt_post, put_responses=tgt_put, logger=FakeLogger())
    return MergeTool(source_client=src, target_client=tgt)


def _basic_source(datamodels, users=None, groups=None, datasecurity_by_url=None):
    src_post = {"/api/v2/ecm/": _ecm_response(datamodels)}
    src_get = {
        "/api/v1/users": FakeResponse(200, users or []),
        "/api/v1/groups": FakeResponse(200, groups or []),
    }
    if datasecurity_by_url:
        src_get.update(datasecurity_by_url)
    return src_get, src_post


def _basic_target(existing_datamodels=None, users=None, groups=None, schema_by_title=None, tgt_post_extra=None, tgt_put_extra=None):
    tgt_post = {"/api/v2/ecm/": _ecm_response(existing_datamodels or [])}
    if tgt_post_extra:
        tgt_post.update(tgt_post_extra)
    tgt_get = {
        "/api/v1/users": FakeResponse(200, users or []),
        "/api/v1/groups": FakeResponse(200, groups or []),
    }
    if schema_by_title:
        tgt_get["/api/v2/datamodels/schema"] = schema_by_title
    tgt_put = dict(tgt_put_extra or {})
    return tgt_get, tgt_post, tgt_put


# ---------------------------------------------------------------------------
# migrate_datasecurity — validation and fetch failures
# ---------------------------------------------------------------------------


class TestMigrateDatasecurityValidation:
    def test_both_ids_and_names_raises(self):
        merge = _make_merge()
        with pytest.raises(ValueError, match="not both"):
            merge.migrate_datasecurity(datamodel_ids=["dm1"], datamodel_names=["Sales Cube"])

    def test_neither_ids_nor_names_raises(self):
        merge = _make_merge()
        with pytest.raises(ValueError, match="Provide either"):
            merge.migrate_datasecurity()

    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_post={"/api/v2/ecm/": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_datasecurity(datamodel_ids=["dm1"])
        assert result["ok"] is False
        assert result["status"] == "failed"
        assert result["source_count"] == 0


# ---------------------------------------------------------------------------
# migrate_datasecurity — id/name filtering and target existence
# ---------------------------------------------------------------------------


class TestMigrateDatasecurityFiltering:
    def test_missing_id_is_reported_as_failed(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT])
        tgt_get, tgt_post, tgt_put = _basic_target()
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post, tgt_put=tgt_put)
        result = merge.migrate_datasecurity(datamodel_ids=["dm1", "ghost"])
        failed_ids = {f["source_oid"] for f in result["failed"]}
        assert "ghost" in failed_ids

    def test_no_matching_datamodels_is_noop(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT])
        merge = _make_merge(src_get=src_get, src_post=src_post)
        result = merge.migrate_datasecurity(datamodel_ids=["ghost"])
        assert result["ok"] is True
        assert result["status"] == "noop"

    def test_datamodel_not_on_target_is_skipped(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT])
        tgt_get, tgt_post, tgt_put = _basic_target(existing_datamodels=[])
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post, tgt_put=tgt_put)
        result = merge.migrate_datasecurity(datamodel_ids=["dm1"])
        assert result["skipped"] == [{"title": "Sales Cube", "source_oid": "dm1", "reason": "Data model not found on target. Migrate the data model first."}]
        assert result["status"] == "success"
        assert result["failed_count"] == 0


# ---------------------------------------------------------------------------
# migrate_datasecurity — no rules on source
# ---------------------------------------------------------------------------


class TestMigrateDatasecurityNoRules:
    def test_empty_rules_on_source_is_skipped(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], datasecurity_by_url={"/api/elasticubes/localhost/Sales Cube/datasecurity": FakeResponse(200, [])})
        tgt_get, tgt_post, tgt_put = _basic_target(existing_datamodels=[_SALES_EXTRACT])
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post, tgt_put=tgt_put)
        result = merge.migrate_datasecurity(datamodel_ids=["dm1"])
        assert result["skipped"] == [{"title": "Sales Cube", "source_oid": "dm1", "reason": "No datasecurity rules found on source."}]

    def test_fetch_failure_marks_datamodel_failed(self):
        src_get, src_post = _basic_source([_SALES_EXTRACT], datasecurity_by_url={"/api/elasticubes/localhost/Sales Cube/datasecurity": FakeResponse(500, {"error": "boom"})})
        tgt_get, tgt_post, tgt_put = _basic_target(existing_datamodels=[_SALES_EXTRACT])
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post, tgt_put=tgt_put)
        result = merge.migrate_datasecurity(datamodel_ids=["dm1"])
        assert result["failed"][0]["source_oid"] == "dm1"
        assert "Failed to fetch datasecurity rules" in result["failed"][0]["reason"]


# ---------------------------------------------------------------------------
# migrate_datasecurity — extract (EXTRACT) rule migration and share remapping
# ---------------------------------------------------------------------------


class TestMigrateDatasecurityExtract:
    def test_rules_are_remapped_and_put(self):
        src_get, src_post = _basic_source(
            [_SALES_EXTRACT],
            users=[{"_id": "src_user_1", "email": "alice@example.com"}],
            datasecurity_by_url={"/api/elasticubes/localhost/Sales Cube/datasecurity": FakeResponse(200, [_EXTRACT_RULE])},
        )
        tgt_get, tgt_post, tgt_put = _basic_target(
            existing_datamodels=[_SALES_EXTRACT],
            users=[{"_id": "tgt_user_1", "email": "alice@example.com"}],
            schema_by_title=FakeResponse(200, {"title": "Sales Cube", "type": "extract"}),
            tgt_post_extra={"/api/elasticubes/localhost/Sales Cube/datasecurity": FakeResponse(200, {})},
        )
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post, tgt_put=tgt_put, capture_target=True)

        result = merge.migrate_datasecurity(datamodel_ids=["dm1"])

        assert result["succeeded"] == [{"title": "Sales Cube", "source_oid": "dm1", "rule_count": 1}]
        put_call = next(c for c in merge.target_client.calls if c[0] == "POST" and c[1] == "/api/elasticubes/localhost/Sales Cube/datasecurity")
        written_rule = put_call[2][0]
        assert written_rule["shares"] == [{"type": "user", "party": "tgt_user_1"}, {"type": "default"}]
        assert written_rule["table"] == "Customers"
        assert written_rule["members"] == ["East"]

    def test_unresolvable_user_share_is_dropped(self):
        src_get, src_post = _basic_source(
            [_SALES_EXTRACT],
            users=[{"_id": "src_user_1", "email": "alice@example.com"}],
            datasecurity_by_url={"/api/elasticubes/localhost/Sales Cube/datasecurity": FakeResponse(200, [_EXTRACT_RULE])},
        )
        tgt_get, tgt_post, tgt_put = _basic_target(
            existing_datamodels=[_SALES_EXTRACT],
            users=[],
            schema_by_title=FakeResponse(200, {"title": "Sales Cube", "type": "extract"}),
            tgt_post_extra={"/api/elasticubes/localhost/Sales Cube/datasecurity": FakeResponse(200, {})},
        )
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post, tgt_put=tgt_put, capture_target=True)

        merge.migrate_datasecurity(datamodel_ids=["dm1"])

        put_call = next(c for c in merge.target_client.calls if c[0] == "POST" and c[1] == "/api/elasticubes/localhost/Sales Cube/datasecurity")
        assert put_call[2][0]["shares"] == [{"type": "default"}]

    def test_write_failure_marks_datamodel_failed(self):
        src_get, src_post = _basic_source(
            [_SALES_EXTRACT],
            datasecurity_by_url={"/api/elasticubes/localhost/Sales Cube/datasecurity": FakeResponse(200, [_EXTRACT_RULE])},
        )
        tgt_get, tgt_post, tgt_put = _basic_target(
            existing_datamodels=[_SALES_EXTRACT],
            schema_by_title=FakeResponse(200, {"title": "Sales Cube", "type": "extract"}),
            tgt_post_extra={"/api/elasticubes/localhost/Sales Cube/datasecurity": FakeResponse(500, {"error": "boom"})},
        )
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post, tgt_put=tgt_put)
        result = merge.migrate_datasecurity(datamodel_ids=["dm1"])
        assert result["failed"][0]["source_oid"] == "dm1"


# ---------------------------------------------------------------------------
# migrate_datasecurity — live rule migration
# ---------------------------------------------------------------------------


class TestMigrateDatasecurityLive:
    def test_live_rules_use_partyid_and_add_many(self):
        src_get, src_post = _basic_source(
            [_MARKETING_LIVE],
            groups=[{"_id": "src_group_1", "name": "Sales Team"}],
            datasecurity_by_url={"/api/v1/elasticubes/live/Marketing Live/datasecurity": FakeResponse(200, [_LIVE_RULE])},
        )
        tgt_get, tgt_post, tgt_put = _basic_target(
            existing_datamodels=[_MARKETING_LIVE],
            groups=[{"_id": "tgt_group_1", "name": "Sales Team"}],
            schema_by_title=FakeResponse(200, {"title": "Marketing Live", "type": "live"}),
        )
        tgt_post["/api/v1/elasticubes/live/Marketing Live/datasecurity/addMany"] = FakeResponse(200, {})
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post, tgt_put=tgt_put, capture_target=True)

        result = merge.migrate_datasecurity(datamodel_ids=["dm2"])

        assert result["succeeded"] == [{"title": "Marketing Live", "source_oid": "dm2", "rule_count": 1}]
        add_many_call = next(c for c in merge.target_client.calls if c[0] == "POST" and c[1] == "/api/v1/elasticubes/live/Marketing Live/datasecurity/addMany")
        assert add_many_call[2][0]["shares"] == [{"type": "group", "partyId": "tgt_group_1"}]


# ---------------------------------------------------------------------------
# migrate_all_datasecurity
# ---------------------------------------------------------------------------


class TestMigrateAllDatasecurity:
    def test_migrates_every_source_datamodel(self):
        src_get, src_post = _basic_source(
            [_SALES_EXTRACT],
            datasecurity_by_url={"/api/elasticubes/localhost/Sales Cube/datasecurity": FakeResponse(200, [_EXTRACT_RULE])},
        )
        tgt_get, tgt_post, tgt_put = _basic_target(
            existing_datamodels=[_SALES_EXTRACT],
            schema_by_title=FakeResponse(200, {"title": "Sales Cube", "type": "extract"}),
            tgt_post_extra={"/api/elasticubes/localhost/Sales Cube/datasecurity": FakeResponse(200, {})},
        )
        merge = _make_merge(src_get=src_get, src_post=src_post, tgt_get=tgt_get, tgt_post=tgt_post, tgt_put=tgt_put)
        result = merge.migrate_all_datasecurity()
        assert result["source_count"] == 1
        assert result["succeeded_count"] == 1

    def test_no_source_datamodels_is_noop(self):
        merge = _make_merge(src_post={"/api/v2/ecm/": _ecm_response([])})
        result = merge.migrate_all_datasecurity()
        assert result["ok"] is True
        assert result["status"] == "noop"

    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_post={"/api/v2/ecm/": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_all_datasecurity()
        assert result["ok"] is False
        assert result["status"] == "failed"
