"""Unit tests for pysisense.mergetool.folder.FolderMergeMixin."""

import asyncio

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.mergetool import MergeTool

_SRC_ROOT = {"oid": "src_root", "name": "Sales", "parentId": None}
_SRC_Q1 = {"oid": "src_q1", "name": "Q1", "parentId": "src_root"}
_SRC_Q2 = {"oid": "src_q2", "name": "Q2", "parentId": "src_root"}
_SRC_Q3 = {"oid": "src_q3", "name": "Q3", "parentId": "src_root"}


class NameAwareFakeApiClient(FakeApiClient):
    """FakeApiClient that resolves ``POST /api/v1/folders`` by the request's folder
    ``name`` instead of by URL, since every folder create hits the same URL."""

    def __init__(self, *args, folder_create_responses=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._folder_create_responses = folder_create_responses or {}
        self.create_calls: list[dict] = []

    def post(self, url, data=None, **kwargs):
        if url == "/api/v1/folders" and isinstance(data, dict) and "name" in data:
            self.create_calls.append(dict(data))
            return self._folder_create_responses.get(data["name"], FakeResponse(500, {"error": f"no fixture for folder '{data['name']}'"}))
        return super().post(url, data=data, **kwargs)


def _make_merge(source_folders, target_folders=None, create_responses=None, delete_responses=None):
    src = FakeApiClient(get_responses={"/api/v1/folders?structure=flat": FakeResponse(200, source_folders)}, logger=FakeLogger())
    tgt = NameAwareFakeApiClient(
        get_responses={"/api/v1/folders?structure=flat": FakeResponse(200, target_folders or [])},
        delete_responses=delete_responses,
        folder_create_responses=create_responses,
        logger=FakeLogger(),
    )
    return MergeTool(source_client=src, target_client=tgt)


# ---------------------------------------------------------------------------
# migrate_folders — validation and fetch failures
# ---------------------------------------------------------------------------


class TestMigrateFoldersValidation:
    def test_both_ids_and_names_raises(self):
        merge = _make_merge([])
        with pytest.raises(ValueError, match="not both"):
            merge.migrate_folders(folder_ids=["src_root"], folder_names=["Sales"])

    def test_neither_ids_nor_names_raises(self):
        merge = _make_merge([])
        with pytest.raises(ValueError, match="Provide either"):
            merge.migrate_folders()

    def test_source_fetch_failure_returns_failed_summary(self):
        src = FakeApiClient(get_responses={"/api/v1/folders?structure=flat": FakeResponse(500, {"error": "boom"})}, logger=FakeLogger())
        tgt = FakeApiClient(logger=FakeLogger())
        merge = MergeTool(source_client=src, target_client=tgt)
        result = merge.migrate_folders(folder_ids=["src_root"])
        assert result["ok"] is False
        assert result["status"] == "failed"

    def test_no_matching_folders_is_noop(self):
        merge = _make_merge([_SRC_ROOT])
        result = merge.migrate_folders(folder_ids=["ghost"])
        assert result["ok"] is True
        assert result["status"] == "noop"


# ---------------------------------------------------------------------------
# migrate_folders — sequential (default concurrency=1): parent/child chaining
# ---------------------------------------------------------------------------


class TestMigrateFoldersSequential:
    def test_parent_created_before_child_and_child_gets_new_parent_oid(self):
        merge = _make_merge(
            [_SRC_ROOT, _SRC_Q1],
            create_responses={
                "Sales": FakeResponse(201, {"oid": "tgt_root", "name": "Sales"}),
                "Q1": FakeResponse(201, {"oid": "tgt_q1", "name": "Q1"}),
            },
        )
        result = merge.migrate_folders(folder_ids=["src_root"])

        assert result["ok"] is True
        assert result["succeeded_count"] == 2
        names_to_oid = {c["name"]: c.get("parentId") for c in merge.target_client.create_calls}
        assert names_to_oid["Sales"] is None
        assert names_to_oid["Q1"] == "tgt_root"


# ---------------------------------------------------------------------------
# migrate_folders — conflict handling (skip / overwrite)
# ---------------------------------------------------------------------------


class TestMigrateFoldersConflictHandling:
    def test_skip_leaves_existing_folder_unchanged_and_still_links_children(self):
        merge = _make_merge(
            [_SRC_ROOT, _SRC_Q1],
            target_folders=[{"oid": "tgt_root", "name": "Sales", "parentId": None}],
            create_responses={"Q1": FakeResponse(201, {"oid": "tgt_q1", "name": "Q1"})},
        )
        result = merge.migrate_folders(folder_ids=["src_root"], action="skip")

        assert result["skipped"] == [{"name": "Sales", "path": "Sales", "source_oid": "src_root", "reason": "Already exists on target."}]
        assert result["succeeded"] == [{"name": "Q1", "path": "Sales/Q1", "source_oid": "src_q1"}]
        # Q1 must be created under the existing (skipped) target root, not at root level.
        assert merge.target_client.create_calls[0]["parentId"] == "tgt_root"

    def test_overwrite_deletes_existing_then_recreates(self):
        merge = _make_merge(
            [_SRC_ROOT],
            target_folders=[{"oid": "tgt_root_old", "name": "Sales", "parentId": None}],
            create_responses={"Sales": FakeResponse(201, {"oid": "tgt_root_new", "name": "Sales"})},
            delete_responses={"/api/v1/folders/tgt_root_old": FakeResponse(204, {})},
        )
        result = merge.migrate_folders(folder_ids=["src_root"], action="overwrite")
        assert result["succeeded"] == [{"name": "Sales", "path": "Sales", "source_oid": "src_root"}]


# ---------------------------------------------------------------------------
# migrate_folders — concurrency (siblings at the same depth run in parallel)
# ---------------------------------------------------------------------------


class TestMigrateFoldersConcurrency:
    def test_concurrent_siblings_all_link_to_new_parent_oid(self):
        merge = _make_merge(
            [_SRC_ROOT, _SRC_Q1, _SRC_Q2, _SRC_Q3],
            create_responses={
                "Sales": FakeResponse(201, {"oid": "tgt_root", "name": "Sales"}),
                "Q1": FakeResponse(201, {"oid": "tgt_q1", "name": "Q1"}),
                "Q2": FakeResponse(201, {"oid": "tgt_q2", "name": "Q2"}),
                "Q3": FakeResponse(201, {"oid": "tgt_q3", "name": "Q3"}),
            },
        )
        result = merge.migrate_folders(folder_ids=["src_root"], concurrency=3)

        assert result["ok"] is True
        assert result["succeeded_count"] == 4
        child_calls = [c for c in merge.target_client.create_calls if c["name"] != "Sales"]
        assert len(child_calls) == 3
        # Every sibling must see the parent's already-resolved target oid — the
        # depth barrier must complete before the concurrent batch starts.
        assert all(c["parentId"] == "tgt_root" for c in child_calls)

    def test_failure_in_concurrent_batch_is_recorded_without_aborting_siblings(self):
        merge = _make_merge(
            [_SRC_ROOT, _SRC_Q1, _SRC_Q2],
            create_responses={
                "Sales": FakeResponse(201, {"oid": "tgt_root", "name": "Sales"}),
                "Q1": FakeResponse(400, {"error": "bad request"}),
                "Q2": FakeResponse(201, {"oid": "tgt_q2", "name": "Q2"}),
            },
        )
        result = merge.migrate_folders(folder_ids=["src_root"], concurrency=2)

        assert result["succeeded_count"] == 2
        assert result["failed_count"] == 1
        assert result["failed"][0]["name"] == "Q1"

    def test_nested_event_loop_falls_back_to_sequential(self, monkeypatch):
        def _raise_runtime_error(coro):
            coro.close()
            raise RuntimeError("cannot run nested event loops")

        monkeypatch.setattr(asyncio, "run", _raise_runtime_error)

        merge = _make_merge(
            [_SRC_ROOT, _SRC_Q1, _SRC_Q2],
            create_responses={
                "Sales": FakeResponse(201, {"oid": "tgt_root", "name": "Sales"}),
                "Q1": FakeResponse(201, {"oid": "tgt_q1", "name": "Q1"}),
                "Q2": FakeResponse(201, {"oid": "tgt_q2", "name": "Q2"}),
            },
        )
        result = merge.migrate_folders(folder_ids=["src_root"], concurrency=2)

        assert result["ok"] is True
        assert result["succeeded_count"] == 3


# ---------------------------------------------------------------------------
# migrate_all_folders
# ---------------------------------------------------------------------------


class TestMigrateAllFolders:
    def test_migrates_every_source_folder_and_forwards_concurrency(self):
        merge = _make_merge(
            [_SRC_ROOT, _SRC_Q1, _SRC_Q2],
            create_responses={
                "Sales": FakeResponse(201, {"oid": "tgt_root", "name": "Sales"}),
                "Q1": FakeResponse(201, {"oid": "tgt_q1", "name": "Q1"}),
                "Q2": FakeResponse(201, {"oid": "tgt_q2", "name": "Q2"}),
            },
        )
        result = merge.migrate_all_folders(concurrency=2)
        assert result["source_count"] == 3
        assert result["succeeded_count"] == 3

    def test_no_source_folders_is_noop(self):
        merge = _make_merge([])
        result = merge.migrate_all_folders()
        assert result["ok"] is True
        assert result["status"] == "noop"
