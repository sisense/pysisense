"""Unit tests for pysisense.folder.Folder."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.folder import Folder

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------

_FOLDER = {
    "oid": "folder123456789012345678",
    "name": "Sales",
    "parentId": None,
    "owner": "owner12345678901234567890",
}

_FOLDER_TREE = [
    {
        "oid": "folder123456789012345678",
        "name": "Sales",
        "folders": [],
        "dashboards": [],
    }
]


def _make_folder(
    get_responses=None,
    post_responses=None,
    patch_responses=None,
    delete_responses=None,
):
    """Build a Folder instance backed by FakeApiClient."""
    logger = FakeLogger()
    client = FakeApiClient(
        get_responses=get_responses,
        post_responses=post_responses,
        patch_responses=patch_responses,
        delete_responses=delete_responses,
        logger=logger,
    )
    return Folder(api_client=client)


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------


class TestFolderInit:
    def test_creates_with_fake_client(self):
        folder = _make_folder()
        assert folder is not None
        assert hasattr(folder, "api_client")
        assert hasattr(folder, "logger")


# ---------------------------------------------------------------------------
# create_folder
# ---------------------------------------------------------------------------


class TestCreateFolder:
    def test_returns_folder_on_success(self):
        folder = _make_folder(
            post_responses={"/api/v1/folders": FakeResponse(201, _FOLDER)},
        )
        result = folder.create_folder("Sales")
        assert result["oid"] == "folder123456789012345678"
        assert result["name"] == "Sales"

    def test_returns_error_on_none_response(self):
        folder = _make_folder()
        result = folder.create_folder("Sales")
        assert "error" in result

    def test_returns_error_on_non_201(self):
        folder = _make_folder(
            post_responses={"/api/v1/folders": FakeResponse(400, {"message": "bad request"})},
        )
        result = folder.create_folder("Sales")
        assert "error" in result


# ---------------------------------------------------------------------------
# update_folder
# ---------------------------------------------------------------------------


class TestUpdateFolder:
    def test_returns_folder_on_success(self):
        updated = {**_FOLDER, "name": "Sales Updated"}
        folder = _make_folder(
            patch_responses={
                f"/api/v1/folders/{_FOLDER['oid']}": FakeResponse(200, updated),
            },
        )
        result = folder.update_folder(_FOLDER["oid"], name="Sales Updated")
        assert result["name"] == "Sales Updated"

    def test_returns_error_on_none_response(self):
        folder = _make_folder()
        result = folder.update_folder(_FOLDER["oid"], name="Sales Updated")
        assert "error" in result

    def test_returns_error_on_non_200(self):
        folder = _make_folder(
            patch_responses={
                f"/api/v1/folders/{_FOLDER['oid']}": FakeResponse(403, {"message": "forbidden"}),
            },
        )
        result = folder.update_folder(_FOLDER["oid"], owner="other_owner")
        assert "error" in result


# ---------------------------------------------------------------------------
# get_folder_id
# ---------------------------------------------------------------------------


class TestGetFolderId:
    def test_returns_folder_on_success(self):
        folder = _make_folder(
            get_responses={
                f"/api/v1/folders/{_FOLDER['oid']}": FakeResponse(200, _FOLDER),
            },
        )
        result = folder.get_folder_id(_FOLDER["oid"])
        assert result["name"] == "Sales"

    def test_returns_error_on_none_response(self):
        folder = _make_folder()
        result = folder.get_folder_id(_FOLDER["oid"])
        assert "error" in result

    def test_returns_error_on_non_200(self):
        folder = _make_folder(
            get_responses={
                f"/api/v1/folders/{_FOLDER['oid']}": FakeResponse(404, {"message": "not found"}),
            },
        )
        result = folder.get_folder_id(_FOLDER["oid"])
        assert "error" in result

    def test_returns_error_when_empty_result(self):
        folder = _make_folder(
            get_responses={
                f"/api/v1/folders/{_FOLDER['oid']}": FakeResponse(200, {}),
            },
        )
        result = folder.get_folder_id(_FOLDER["oid"])
        assert "error" in result


# ---------------------------------------------------------------------------
# get_all_folders
# ---------------------------------------------------------------------------


class TestGetAllFolders:
    def test_returns_list_on_success(self):
        folder = _make_folder(
            get_responses={"/api/v1/folders": FakeResponse(200, _FOLDER_TREE)},
        )
        result = folder.get_all_folders()
        assert isinstance(result, list)
        assert result[0]["name"] == "Sales"

    def test_returns_error_on_none_response(self):
        folder = _make_folder()
        result = folder.get_all_folders()
        assert "error" in result

    def test_returns_error_on_non_200(self):
        folder = _make_folder(
            get_responses={"/api/v1/folders": FakeResponse(500, {"message": "server error"})},
        )
        result = folder.get_all_folders()
        assert "error" in result


# ---------------------------------------------------------------------------
# delete_folder
# ---------------------------------------------------------------------------


class TestDeleteFolder:
    def test_returns_message_on_success(self):
        folder = _make_folder(
            delete_responses={
                f"/api/v1/folders/{_FOLDER['oid']}": FakeResponse(204, None),
            },
        )
        result = folder.delete_folder(_FOLDER["oid"])
        assert "message" in result
        assert _FOLDER["oid"] in result["message"]

    def test_returns_error_on_none_response(self):
        folder = _make_folder()
        result = folder.delete_folder(_FOLDER["oid"])
        assert "error" in result

    def test_returns_error_on_non_204(self):
        folder = _make_folder(
            delete_responses={
                f"/api/v1/folders/{_FOLDER['oid']}": FakeResponse(409, {"message": "not empty"}),
            },
        )
        result = folder.delete_folder(_FOLDER["oid"])
        assert "error" in result
