"""Unit tests for pysisense.mergetool.custom_code.CustomCodeMergeMixin."""

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.mergetool import MergeTool

_NOTEBOOK_A = {"id": "nb1", "displayName": "ETL Pipeline"}
_NOTEBOOK_B = {"id": "nb2", "displayName": "Transform"}

_EXPORTED_A = {"id": "nb1", "displayName": "ETL Pipeline", "manifest": {"steps": []}}
_EXPORTED_B = {"id": "nb2", "displayName": "Transform", "manifest": {"steps": []}}


class CapturingFakeApiClient(FakeApiClient):
    """FakeApiClient that also records every POST/DELETE call for assertions."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls: list[tuple[str, str, object]] = []

    def post(self, url, data=None, **kwargs):
        self.calls.append(("POST", url, data))
        return super().post(url, data=data, **kwargs)

    def delete(self, url, **kwargs):
        self.calls.append(("DELETE", url, None))
        return super().delete(url, **kwargs)


def _make_merge(src_get=None, tgt_get=None, tgt_post=None, tgt_delete=None, capture_target=False):
    """Build a MergeTool instance backed by separate FakeApiClient source/target."""
    src = FakeApiClient(get_responses=src_get, logger=FakeLogger())
    tgt_cls = CapturingFakeApiClient if capture_target else FakeApiClient
    tgt = tgt_cls(get_responses=tgt_get, post_responses=tgt_post, delete_responses=tgt_delete, logger=FakeLogger())
    return MergeTool(source_client=src, target_client=tgt)


def _basic_source(notebooks, exported_by_id=None):
    src_get = {"/api/v1/notebooks": FakeResponse(200, notebooks)}
    if exported_by_id:
        for nb_id, exported in exported_by_id.items():
            src_get[f"/api/v1/notebooks/{nb_id}/export"] = FakeResponse(200, exported)
    return src_get


# ---------------------------------------------------------------------------
# migrate_notebooks — validation and fetch failures
# ---------------------------------------------------------------------------


class TestMigrateNotebooksValidation:
    def test_both_ids_and_names_raises(self):
        merge = _make_merge()
        with pytest.raises(ValueError, match="not both"):
            merge.migrate_notebooks(notebook_ids=["nb1"], notebook_names=["ETL Pipeline"])

    def test_neither_ids_nor_names_raises(self):
        merge = _make_merge()
        with pytest.raises(ValueError, match="Provide either"):
            merge.migrate_notebooks()

    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_get={"/api/v1/notebooks": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_notebooks(notebook_ids=["nb1"])
        assert result["ok"] is False
        assert result["status"] == "failed"


# ---------------------------------------------------------------------------
# migrate_notebooks — id/name filtering
# ---------------------------------------------------------------------------


class TestMigrateNotebooksFiltering:
    def test_missing_id_is_reported_as_failed(self):
        merge = _make_merge(
            src_get=_basic_source([_NOTEBOOK_A], exported_by_id={"nb1": _EXPORTED_A}),
            tgt_get={"/api/v1/notebooks": FakeResponse(200, [])},
            tgt_post={"/api/v1/notebooks": FakeResponse(201, {"id": "nb1"})},
        )
        result = merge.migrate_notebooks(notebook_ids=["nb1", "ghost"])
        failed_ids = {f["source_id"] for f in result["failed"]}
        assert "ghost" in failed_ids

    def test_missing_name_is_reported_as_failed(self):
        merge = _make_merge(
            src_get=_basic_source([_NOTEBOOK_A], exported_by_id={"nb1": _EXPORTED_A}),
            tgt_get={"/api/v1/notebooks": FakeResponse(200, [])},
            tgt_post={"/api/v1/notebooks": FakeResponse(201, {"id": "nb1"})},
        )
        result = merge.migrate_notebooks(notebook_names=["ETL Pipeline", "Ghost Notebook"])
        failed_names = {f["name"] for f in result["failed"]}
        assert "Ghost Notebook" in failed_names

    def test_no_matching_notebooks_is_noop(self):
        merge = _make_merge(src_get=_basic_source([_NOTEBOOK_A]))
        result = merge.migrate_notebooks(notebook_ids=["ghost"])
        assert result["ok"] is True
        assert result["status"] == "noop"


# ---------------------------------------------------------------------------
# migrate_notebooks — conflict handling
# ---------------------------------------------------------------------------


class TestMigrateNotebooksConflictHandling:
    def test_skip_leaves_existing_notebook_unchanged(self):
        merge = _make_merge(
            src_get=_basic_source([_NOTEBOOK_A]),
            tgt_get={"/api/v1/notebooks": FakeResponse(200, [_NOTEBOOK_A])},
        )
        result = merge.migrate_notebooks(notebook_ids=["nb1"], action="skip")
        assert result["skipped"] == [{"name": "ETL Pipeline", "source_id": "nb1", "reason": "Already exists on target."}]
        assert result["ok"] is True
        assert result["status"] == "success"

    def test_overwrite_deletes_existing_then_creates(self):
        merge = _make_merge(
            src_get=_basic_source([_NOTEBOOK_A], exported_by_id={"nb1": _EXPORTED_A}),
            tgt_get={"/api/v1/notebooks": FakeResponse(200, [_NOTEBOOK_A])},
            tgt_delete={"/api/v1/notebooks/nb1": FakeResponse(204, {})},
            tgt_post={"/api/v1/notebooks": FakeResponse(201, {"id": "nb1"})},
        )
        result = merge.migrate_notebooks(notebook_ids=["nb1"], action="overwrite")
        assert result["succeeded"] == [{"name": "ETL Pipeline", "source_id": "nb1"}]


# ---------------------------------------------------------------------------
# migrate_notebooks — export/create failure handling
# ---------------------------------------------------------------------------


class TestMigrateNotebooksExportCreateFailures:
    def test_export_failure_marks_notebook_failed(self):
        merge = _make_merge(
            src_get={"/api/v1/notebooks": FakeResponse(200, [_NOTEBOOK_A]), "/api/v1/notebooks/nb1/export": FakeResponse(500, {"error": "boom"})},
            tgt_get={"/api/v1/notebooks": FakeResponse(200, [])},
        )
        result = merge.migrate_notebooks(notebook_ids=["nb1"])
        assert result["failed"][0]["source_id"] == "nb1"
        assert "Export failed" in result["failed"][0]["reason"]

    def test_create_failure_marks_notebook_failed(self):
        merge = _make_merge(
            src_get=_basic_source([_NOTEBOOK_A], exported_by_id={"nb1": _EXPORTED_A}),
            tgt_get={"/api/v1/notebooks": FakeResponse(200, [])},
            tgt_post={"/api/v1/notebooks": FakeResponse(400, {"error": "bad request"})},
        )
        result = merge.migrate_notebooks(notebook_ids=["nb1"])
        assert result["failed"][0]["source_id"] == "nb1"
        assert "Create failed" in result["failed"][0]["reason"]


# ---------------------------------------------------------------------------
# migrate_notebooks — concurrency
# ---------------------------------------------------------------------------


class TestMigrateNotebooksConcurrency:
    def test_concurrent_notebooks_all_created(self):
        merge = _make_merge(
            src_get=_basic_source([_NOTEBOOK_A, _NOTEBOOK_B], exported_by_id={"nb1": _EXPORTED_A, "nb2": _EXPORTED_B}),
            tgt_get={"/api/v1/notebooks": FakeResponse(200, [])},
            tgt_post={"/api/v1/notebooks": FakeResponse(201, {"id": "new"})},
            capture_target=True,
        )
        result = merge.migrate_notebooks(notebook_ids=["nb1", "nb2"], concurrency=2)
        assert result["ok"] is True
        assert result["succeeded_count"] == 2
        create_calls = [c for c in merge.target_client.calls if c[0] == "POST" and c[1] == "/api/v1/notebooks"]
        assert len(create_calls) == 2


# ---------------------------------------------------------------------------
# migrate_all_notebooks
# ---------------------------------------------------------------------------


class TestMigrateAllNotebooks:
    def test_migrates_every_notebook_and_forwards_concurrency(self):
        merge = _make_merge(
            src_get=_basic_source([_NOTEBOOK_A, _NOTEBOOK_B], exported_by_id={"nb1": _EXPORTED_A, "nb2": _EXPORTED_B}),
            tgt_get={"/api/v1/notebooks": FakeResponse(200, [])},
            tgt_post={"/api/v1/notebooks": FakeResponse(201, {"id": "new"})},
        )
        result = merge.migrate_all_notebooks(concurrency=2)
        assert result["source_count"] == 2
        assert result["succeeded_count"] == 2

    def test_no_source_notebooks_is_noop(self):
        merge = _make_merge(src_get={"/api/v1/notebooks": FakeResponse(200, [])})
        result = merge.migrate_all_notebooks()
        assert result["ok"] is True
        assert result["status"] == "noop"
