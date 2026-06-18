"""Unit tests for pysisense.custom_code.CustomCode."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.custom_code import CustomCode

_NOTEBOOK = {
    "uuid": "nb-123",
    "displayName": "My Notebook",
    "notebookType": "CustomCodeTransformation",
}

_NOTEBOOK_LIST = [_NOTEBOOK]


def _make_custom_code(get_responses=None, post_responses=None, patch_responses=None, delete_responses=None):
    logger = FakeLogger()
    client = FakeApiClient(
        get_responses=get_responses,
        post_responses=post_responses,
        patch_responses=patch_responses,
        delete_responses=delete_responses,
        logger=logger,
    )
    return CustomCode(api_client=client)


class TestCustomCodeInit:
    def test_creates_with_fake_client(self):
        cc = _make_custom_code()
        assert cc is not None
        assert hasattr(cc, "api_client")


class TestGetNotebooks:
    def test_returns_list_on_success(self):
        cc = _make_custom_code(get_responses={"/api/v1/notebooks": FakeResponse(200, _NOTEBOOK_LIST)})
        result = cc.get_notebooks(notebook_type="CustomCodeTransformation")
        assert isinstance(result, list)
        assert result[0]["displayName"] == "My Notebook"


class TestExportNotebook:
    def test_returns_export_on_success(self):
        export = {"manifest": {}, "cells": []}
        cc = _make_custom_code(
            get_responses={"/api/v1/notebooks/nb-123/export": FakeResponse(200, export)},
        )
        result = cc.export_notebook("nb-123")
        assert "manifest" in result


class TestCreateNotebook:
    def test_returns_notebook_on_success(self):
        cc = _make_custom_code(post_responses={"/api/v1/notebooks": FakeResponse(201, _NOTEBOOK)})
        result = cc.create_notebook({"notebookType": "CustomCodeTransformation", "displayName": "New"})
        assert result["uuid"] == "nb-123"


class TestUpdateNotebook:
    def test_returns_notebook_on_success(self):
        updated = {**_NOTEBOOK, "displayName": "Renamed"}
        cc = _make_custom_code(
            patch_responses={"/api/v1/notebooks/nb-123": FakeResponse(200, updated)},
        )
        result = cc.update_notebook("nb-123", {"displayName": "Renamed"})
        assert result["displayName"] == "Renamed"


class TestDeleteNotebook:
    def test_returns_success_on_204(self):
        cc = _make_custom_code(
            delete_responses={"/api/v1/notebooks/nb-123": FakeResponse(204, None)},
        )
        result = cc.delete_notebook("nb-123")
        assert result == {"success": True}


class TestListNotebookFolderContents:
    def test_returns_contents_on_success(self):
        contents = {"files": ["a.ipynb"]}
        cc = _make_custom_code(
            get_responses={
                "/api/resources/notebooks/custom_code_notebooks/notebooks/folder1/": FakeResponse(200, contents),
            },
        )
        result = cc.list_notebook_folder_contents("folder1")
        assert result["files"] == ["a.ipynb"]


class TestRenameNotebookFile:
    def test_returns_response_on_success(self):
        cc = _make_custom_code(
            patch_responses={
                "/api/resources/notebooks/custom_code_notebooks/file.ipynb": FakeResponse(200, {"ok": True}),
            },
        )
        result = cc.rename_notebook_file(
            "notebooks/custom_code_notebooks/file.ipynb",
            {"name": "renamed.ipynb"},
        )
        assert result["ok"] is True


class TestRenameNotebookFolder:
    def test_returns_response_on_success(self):
        cc = _make_custom_code(
            patch_responses={
                "/api/resources/notebooks/custom_code_notebooks/notebooks/old-id/": FakeResponse(200, {"ok": True}),
            },
        )
        result = cc.rename_notebook_folder("old-id", {"name": "new-name"})
        assert result["ok"] is True
