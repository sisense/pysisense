"""Unit tests for pysisense.git.Git (Sisense Git Integration)."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.git import Git

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------

_PROJECT_ID = "630581af4b046d0011cdec49"
_PROJECT_NAME = "Marketing_Project"

_PROJECT = {
    "name": _PROJECT_NAME,
    "remoteUrl": "https://github.com/acme/marketing-project.git",
    "oid": _PROJECT_ID,
    "owner": "someone@example.com",
    "created": "2024-01-01T00:00:00.000Z",
    "lastModified": "2024-01-02T00:00:00.000Z",
    "syncedAt": "2024-01-02T00:00:00.000Z",
    "shares": [],
    "userFavorites": [],
}

_OTHER_PROJECT = {
    "name": "Other_Project",
    "oid": "aaaaaaaaaaaaaaaaaaaaaaaa",
    "owner": "someone@example.com",
    "created": "2024-01-01T00:00:00.000Z",
    "lastModified": "2024-01-02T00:00:00.000Z",
    "syncedAt": "2024-01-02T00:00:00.000Z",
    "shares": [],
    "userFavorites": [],
}

_BRANCH = {"name": "main", "parents": ["abc123"], "head": "def456"}
_COMMIT = {
    "hash": "def456",
    "authorName": "Jane Doe",
    "authorEmail": "jane@example.com",
    "date": "2024-01-02T00:00:00.000Z",
    "parents": ["abc123"],
    "refs": [],
    "message": "Update dashboard",
}

_BASE = f"/api/v2/projects/{_PROJECT_ID}"
_ERROR_BODY = {"error": {"code": 95512, "status": 409, "httpMessage": "Conflict", "message": "Something went wrong."}}
_VALIDATION_ERROR_BODY = {"error": {"code": 95516, "status": 453, "httpMessage": "Asset Validation Error", "message": "Some assets failed validation."}}


class _RecordingApiClient(FakeApiClient):
    """FakeApiClient that records every call's url/params/data/extra_headers."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls: list[dict] = []

    def get(self, url, params=None, **kwargs):
        self.calls.append({"method": "GET", "url": url, "params": params, "extra_headers": kwargs.get("extra_headers")})
        return super().get(url, params=params, **kwargs)

    def post(self, url, data=None, **kwargs):
        self.calls.append({"method": "POST", "url": url, "data": data, "extra_headers": kwargs.get("extra_headers")})
        return super().post(url, data=data, **kwargs)

    def patch(self, url, data=None, **kwargs):
        self.calls.append({"method": "PATCH", "url": url, "data": data, "extra_headers": kwargs.get("extra_headers")})
        return super().patch(url, data=data, **kwargs)


def _make_git(
    get_responses=None,
    post_responses=None,
    patch_responses=None,
    operating_system="linux",
    client_cls=_RecordingApiClient,
):
    """Build a Git instance backed by a recording FakeApiClient."""
    logger = FakeLogger()
    client = client_cls(
        get_responses=get_responses,
        post_responses=post_responses,
        patch_responses=patch_responses,
        logger=logger,
        operating_system=operating_system,
    )
    return Git(api_client=client), client


def _with_project_get(extra=None):
    """GET responses that resolve _PROJECT_ID / _PROJECT_NAME via /projects[..]."""
    responses = {
        "/api/v2/projects": FakeResponse(200, [_PROJECT, _OTHER_PROJECT]),
        _BASE: FakeResponse(200, _PROJECT),
    }
    if extra:
        responses.update(extra)
    return responses


# ---------------------------------------------------------------------------
# Init / assembly
# ---------------------------------------------------------------------------


class TestGitInit:
    def test_creates_with_fake_client(self):
        git, _ = _make_git()
        assert git.api_client is not None
        assert git.logger is not None

    def test_is_assembled_from_all_mixins(self):
        names = {base.__name__ for base in Git.__mro__}
        assert {"GitCoreMixin", "GitBranchesMixin", "GitCommitsMixin", "GitRemoteMixin"} <= names


# ---------------------------------------------------------------------------
# Windows OS guard — sweep every public method
# ---------------------------------------------------------------------------


class TestWindowsGuard:
    def _windows_git(self):
        git, client = _make_git(operating_system="windows")
        return git, client

    def test_get_git_projects_blocked_on_windows(self):
        git, client = self._windows_git()
        result = git.get_git_projects()
        assert result == {"ok": False, "error": "get_git_projects is not supported on Windows deployments."}
        assert client.calls == []

    def test_get_git_project_blocked_on_windows(self):
        git, client = self._windows_git()
        result = git.get_git_project(_PROJECT_ID)
        assert result["ok"] is False
        assert client.calls == []

    def test_resolve_git_project_reference_blocked_on_windows(self):
        git, client = self._windows_git()
        result = git.resolve_git_project_reference(_PROJECT_ID)
        assert result == {
            "success": False,
            "ok": False,
            "status_code": None,
            "project_id": None,
            "project_name": None,
            "error": "resolve_git_project_reference is not supported on Windows deployments.",
        }
        assert client.calls == []

    def test_get_git_project_status_blocked_on_windows(self):
        git, client = self._windows_git()
        result = git.get_git_project_status(_PROJECT_ID)
        assert result["ok"] is False
        assert client.calls == []

    def test_unlock_git_project_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.unlock_git_project(_PROJECT_ID)
        assert result["ok"] is False

    def test_sync_git_project_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.sync_git_project(_PROJECT_ID, "checksum")
        assert result["ok"] is False

    def test_discard_git_project_changes_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.discard_git_project_changes(_PROJECT_ID, ["file.json"], "checksum")
        assert result["ok"] is False

    def test_get_git_branches_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.get_git_branches(_PROJECT_ID)
        assert result["ok"] is False

    def test_get_git_branch_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.get_git_branch(_PROJECT_ID, "main")
        assert result["ok"] is False

    def test_create_git_branch_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.create_git_branch(_PROJECT_ID, "feature", "checksum")
        assert result["ok"] is False

    def test_checkout_git_branch_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.checkout_git_branch(_PROJECT_ID, "main", "checksum")
        assert result["ok"] is False

    def test_get_git_commits_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.get_git_commits(_PROJECT_ID)
        assert result["ok"] is False

    def test_get_git_commit_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.get_git_commit(_PROJECT_ID, "abc123")
        assert result["ok"] is False

    def test_create_git_commit_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.create_git_commit(_PROJECT_ID, "msg", ["file.json"], "checksum")
        assert result["ok"] is False

    def test_checkout_git_commit_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.checkout_git_commit(_PROJECT_ID, "abc123", "checksum")
        assert result["ok"] is False

    def test_git_fetch_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.git_fetch(_PROJECT_ID)
        assert result["ok"] is False

    def test_git_pull_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.git_pull(_PROJECT_ID, "main", "checksum")
        assert result["ok"] is False

    def test_git_push_blocked_on_windows(self):
        git, _ = self._windows_git()
        result = git.git_push(_PROJECT_ID, "main", "main", "checksum")
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# core.py — get_git_projects / get_git_project
# ---------------------------------------------------------------------------


class TestGetGitProjects:
    def test_success_returns_list(self):
        git, _ = _make_git(get_responses={"/api/v2/projects": FakeResponse(200, [_PROJECT, _OTHER_PROJECT])})
        result = git.get_git_projects()
        assert result == [_PROJECT, _OTHER_PROJECT]

    def test_boolean_flags_only_sent_when_true(self):
        git, client = _make_git(get_responses={"/api/v2/projects": FakeResponse(200, [])})
        git.get_git_projects(current_branch_name=True, favorites_only=False)
        call = client.calls[-1]
        assert call["params"] == {"current_branch_name": "true"}

    def test_no_flags_sends_empty_params(self):
        git, client = _make_git(get_responses={"/api/v2/projects": FakeResponse(200, [])})
        git.get_git_projects()
        assert client.calls[-1]["params"] == {}

    def test_failure_returns_error_dict(self):
        git, _ = _make_git(get_responses={"/api/v2/projects": FakeResponse(500, {"error": "boom"})})
        result = git.get_git_projects()
        assert result["ok"] is False
        assert "error" in result

    def test_no_response_returns_error_dict(self):
        git, _ = _make_git(get_responses={"/api/v2/projects": None})
        result = git.get_git_projects()
        assert result["ok"] is False


class TestGetGitProject:
    def test_success_returns_project(self):
        git, _ = _make_git(get_responses={_BASE: FakeResponse(200, _PROJECT)})
        result = git.get_git_project(_PROJECT_ID)
        assert result == _PROJECT

    def test_query_flags_forwarded(self):
        git, client = _make_git(get_responses={_BASE: FakeResponse(200, _PROJECT)})
        git.get_git_project(_PROJECT_ID, branch_mappings=True, tag_names=True)
        assert client.calls[-1]["params"] == {"branch_mappings": "true", "tag_names": "true"}

    def test_not_found_returns_error_dict(self):
        git, _ = _make_git(get_responses={_BASE: FakeResponse(404, _ERROR_BODY)})
        result = git.get_git_project(_PROJECT_ID)
        assert result["ok"] is False
        assert result["status_code"] == 404


# ---------------------------------------------------------------------------
# core.py — resolve_git_project_reference
# ---------------------------------------------------------------------------


class TestResolveGitProjectReference:
    def test_resolves_by_id(self):
        git, _ = _make_git(get_responses=_with_project_get())
        result = git.resolve_git_project_reference(_PROJECT_ID)
        assert result == {
            "success": True,
            "status_code": 200,
            "project_id": _PROJECT_ID,
            "project_name": _PROJECT_NAME,
            "error": None,
        }

    def test_resolves_by_name(self):
        git, _ = _make_git(get_responses=_with_project_get())
        result = git.resolve_git_project_reference(_PROJECT_NAME)
        assert result["success"] is True
        assert result["project_id"] == _PROJECT_ID
        assert result["project_name"] == _PROJECT_NAME

    def test_id_shaped_but_unknown_falls_back_to_name_list_and_fails(self):
        unknown_id = "ffffffffffffffffffffffff"
        git, _ = _make_git(
            get_responses={
                "/api/v2/projects": FakeResponse(200, [_PROJECT, _OTHER_PROJECT]),
                f"/api/v2/projects/{unknown_id}": FakeResponse(404, _ERROR_BODY),
            }
        )
        result = git.resolve_git_project_reference(unknown_id)
        assert result["success"] is False
        assert result["ok"] is False
        assert result["status_code"] == 404

    def test_unknown_name_returns_not_found(self):
        git, _ = _make_git(get_responses=_with_project_get())
        result = git.resolve_git_project_reference("Nonexistent_Project")
        assert result["success"] is False
        assert result["ok"] is False
        assert result["status_code"] == 404
        assert result["project_id"] is None


# ---------------------------------------------------------------------------
# core.py — status / unlock / sync / discard
# ---------------------------------------------------------------------------


class TestGetGitProjectStatus:
    def test_success_returns_checksum(self):
        git, _ = _make_git(get_responses=_with_project_get({f"{_BASE}/status": FakeResponse(200, {"statusChecksum": "abc123"})}))
        result = git.get_git_project_status(_PROJECT_ID)
        assert result == {"statusChecksum": "abc123"}

    def test_unresolved_project_returns_error_dict(self):
        git, _ = _make_git(get_responses={"/api/v2/projects": FakeResponse(200, [])})
        result = git.get_git_project_status("Nonexistent_Project")
        assert result["ok"] is False
        assert "could not be resolved" in result["error"]


class TestUnlockGitProject:
    def test_success_returns_success_true(self):
        git, _ = _make_git(get_responses=_with_project_get(), patch_responses={f"{_BASE}/unlock": FakeResponse(204, {})})
        result = git.unlock_git_project(_PROJECT_ID)
        assert result == {"success": True}

    def test_failure_returns_error_dict(self):
        git, _ = _make_git(get_responses=_with_project_get(), patch_responses={f"{_BASE}/unlock": FakeResponse(403, {"error": {"message": "Admins only."}})})
        result = git.unlock_git_project(_PROJECT_ID)
        assert result["ok"] is False


class TestSyncGitProject:
    def test_success_forwards_checksum_header(self):
        git, client = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/sync": FakeResponse(204, {})})
        result = git.sync_git_project(_PROJECT_ID, "checksum-xyz")
        assert result == {"success": True}
        sync_call = next(c for c in client.calls if c["url"] == f"{_BASE}/sync")
        assert sync_call["extra_headers"] == {"x-git-status-checksum": "checksum-xyz"}

    def test_locked_project_returns_error_dict(self):
        git, _ = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/sync": FakeResponse(423, {"error": {"message": "Locked."}})})
        result = git.sync_git_project(_PROJECT_ID, "checksum")
        assert result["ok"] is False
        assert result["status_code"] == 423


class TestDiscardGitProjectChanges:
    def test_success_sends_changes_and_checksum(self):
        git, client = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/discard": FakeResponse(204, {})})
        result = git.discard_git_project_changes(_PROJECT_ID, ["dashboards/foo/widget.json"], "checksum-xyz")
        assert result == {"success": True}
        call = next(c for c in client.calls if c["url"] == f"{_BASE}/discard")
        assert call["data"] == {"changes": ["dashboards/foo/widget.json"]}
        assert call["extra_headers"] == {"x-git-status-checksum": "checksum-xyz"}

    def test_partial_validation_failure_returns_error_dict_with_status_453(self):
        git, _ = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/discard": FakeResponse(453, _VALIDATION_ERROR_BODY)})
        result = git.discard_git_project_changes(_PROJECT_ID, ["bad.json"], "checksum")
        assert result["ok"] is False
        assert result["status_code"] == 453


# ---------------------------------------------------------------------------
# branches.py
# ---------------------------------------------------------------------------


class TestGetGitBranches:
    def test_success_returns_list(self):
        git, _ = _make_git(get_responses=_with_project_get({f"{_BASE}/branches": FakeResponse(200, [_BRANCH])}))
        result = git.get_git_branches(_PROJECT_ID)
        assert result == [_BRANCH]

    def test_failure_returns_error_dict(self):
        git, _ = _make_git(get_responses=_with_project_get({f"{_BASE}/branches": FakeResponse(500, {"error": "boom"})}))
        result = git.get_git_branches(_PROJECT_ID)
        assert result["ok"] is False


class TestGetGitBranch:
    def test_success_returns_branch(self):
        git, _ = _make_git(get_responses=_with_project_get({f"{_BASE}/branches/main": FakeResponse(200, _BRANCH)}))
        result = git.get_git_branch(_PROJECT_ID, "main")
        assert result == _BRANCH

    def test_not_found_returns_error_dict(self):
        git, _ = _make_git(get_responses=_with_project_get({f"{_BASE}/branches/missing": FakeResponse(404, _ERROR_BODY)}))
        result = git.get_git_branch(_PROJECT_ID, "missing")
        assert result["ok"] is False


class TestCreateGitBranch:
    def test_sends_name_only_when_parent_omitted(self):
        git, client = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/branches": FakeResponse(200, _BRANCH)})
        result = git.create_git_branch(_PROJECT_ID, "feature", "checksum-xyz")
        assert result == _BRANCH
        call = next(c for c in client.calls if c["url"] == f"{_BASE}/branches")
        assert call["data"] == {"name": "feature"}
        assert call["extra_headers"] == {"x-git-status-checksum": "checksum-xyz"}

    def test_sends_parent_when_provided(self):
        git, client = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/branches": FakeResponse(200, _BRANCH)})
        git.create_git_branch(_PROJECT_ID, "feature", "checksum-xyz", parent="abc123")
        call = next(c for c in client.calls if c["url"] == f"{_BASE}/branches")
        assert call["data"] == {"name": "feature", "parent": "abc123"}

    def test_failure_returns_error_dict(self):
        git, _ = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/branches": FakeResponse(400, _ERROR_BODY)})
        result = git.create_git_branch(_PROJECT_ID, "feature", "checksum")
        assert result["ok"] is False


class TestCheckoutGitBranch:
    def test_success_forwards_checksum_header(self):
        git, client = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/branches/main/checkout": FakeResponse(204, {})})
        result = git.checkout_git_branch(_PROJECT_ID, "main", "checksum-xyz")
        assert result == {"success": True}
        call = next(c for c in client.calls if c["url"] == f"{_BASE}/branches/main/checkout")
        assert call["extra_headers"] == {"x-git-status-checksum": "checksum-xyz"}

    def test_partial_validation_failure_returns_error_dict_with_status_453(self):
        git, _ = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/branches/main/checkout": FakeResponse(453, _VALIDATION_ERROR_BODY)})
        result = git.checkout_git_branch(_PROJECT_ID, "main", "checksum")
        assert result["ok"] is False
        assert result["status_code"] == 453

    def test_locked_project_returns_error_dict(self):
        git, _ = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/branches/main/checkout": FakeResponse(423, {"error": {"message": "Locked."}})})
        result = git.checkout_git_branch(_PROJECT_ID, "main", "checksum")
        assert result["ok"] is False
        assert result["status_code"] == 423


# ---------------------------------------------------------------------------
# commits.py
# ---------------------------------------------------------------------------


class TestGetGitCommits:
    def test_success_returns_list(self):
        git, _ = _make_git(get_responses=_with_project_get({f"{_BASE}/commits": FakeResponse(200, [_COMMIT])}))
        result = git.get_git_commits(_PROJECT_ID)
        assert result == [_COMMIT]


class TestGetGitCommit:
    def test_success_returns_commit(self):
        git, _ = _make_git(get_responses=_with_project_get({f"{_BASE}/commits/def456": FakeResponse(200, _COMMIT)}))
        result = git.get_git_commit(_PROJECT_ID, "def456")
        assert result == _COMMIT

    def test_not_found_returns_error_dict(self):
        git, _ = _make_git(get_responses=_with_project_get({f"{_BASE}/commits/missing": FakeResponse(404, _ERROR_BODY)}))
        result = git.get_git_commit(_PROJECT_ID, "missing")
        assert result["ok"] is False


class TestCreateGitCommit:
    def test_sends_message_and_changes(self):
        git, client = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/commits": FakeResponse(200, _COMMIT)})
        result = git.create_git_commit(_PROJECT_ID, "Update dashboard", ["dashboards/foo/widget.json"], "checksum-xyz")
        assert result == _COMMIT
        call = next(c for c in client.calls if c["url"] == f"{_BASE}/commits")
        assert call["data"] == {"message": "Update dashboard", "changes": ["dashboards/foo/widget.json"]}
        assert call["extra_headers"] == {"x-git-status-checksum": "checksum-xyz"}

    def test_failure_returns_error_dict(self):
        git, _ = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/commits": FakeResponse(400, _ERROR_BODY)})
        result = git.create_git_commit(_PROJECT_ID, "msg", [], "checksum")
        assert result["ok"] is False


class TestCheckoutGitCommit:
    def test_success_forwards_checksum_header(self):
        git, client = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/commits/def456/checkout": FakeResponse(204, {})})
        result = git.checkout_git_commit(_PROJECT_ID, "def456", "checksum-xyz")
        assert result == {"success": True}
        call = next(c for c in client.calls if c["url"] == f"{_BASE}/commits/def456/checkout")
        assert call["extra_headers"] == {"x-git-status-checksum": "checksum-xyz"}

    def test_partial_validation_failure_returns_error_dict_with_status_453(self):
        git, _ = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/commits/def456/checkout": FakeResponse(453, _VALIDATION_ERROR_BODY)})
        result = git.checkout_git_commit(_PROJECT_ID, "def456", "checksum")
        assert result["ok"] is False
        assert result["status_code"] == 453


# ---------------------------------------------------------------------------
# remote.py
# ---------------------------------------------------------------------------


class TestGitFetch:
    def test_success_without_credentials_sends_empty_body(self):
        git, client = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/fetch": FakeResponse(204, {})})
        result = git.git_fetch(_PROJECT_ID)
        assert result == {"success": True}
        call = next(c for c in client.calls if c["url"] == f"{_BASE}/fetch")
        assert call["data"] == {}

    def test_success_with_credentials_sends_https_credentials(self):
        git, client = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/fetch": FakeResponse(204, {})})
        creds = {"username": "bot", "password": "token123", "save": True}
        git.git_fetch(_PROJECT_ID, https_credentials=creds)
        call = next(c for c in client.calls if c["url"] == f"{_BASE}/fetch")
        assert call["data"] == {"httpsCredentials": creds}

    def test_no_saved_credentials_returns_error_dict(self):
        git, _ = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/fetch": FakeResponse(400, _ERROR_BODY)})
        result = git.git_fetch(_PROJECT_ID)
        assert result["ok"] is False


class TestGitPull:
    def test_success_sends_remote_branch_and_checksum(self):
        git, client = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/pull": FakeResponse(204, {})})
        result = git.git_pull(_PROJECT_ID, "main", "checksum-xyz")
        assert result == {"success": True}
        call = next(c for c in client.calls if c["url"] == f"{_BASE}/pull")
        assert call["data"] == {"remoteBranch": "main"}
        assert call["extra_headers"] == {"x-git-status-checksum": "checksum-xyz"}

    def test_merge_conflict_returns_error_dict(self):
        git, _ = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/pull": FakeResponse(409, _ERROR_BODY)})
        result = git.git_pull(_PROJECT_ID, "main", "checksum")
        assert result["ok"] is False
        assert result["status_code"] == 409


class TestGitPush:
    def test_success_returns_response_body(self):
        push_body = {"remoteMessages": ["Everything up-to-date"], "remoteUrl": "https://github.com/acme/marketing-project.git"}
        git, client = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/push": FakeResponse(200, push_body)})
        result = git.git_push(_PROJECT_ID, "main", "main", "checksum-xyz")
        assert result == push_body
        call = next(c for c in client.calls if c["url"] == f"{_BASE}/push")
        assert call["data"] == {"localBranch": "main", "remoteBranch": "main"}
        assert call["extra_headers"] == {"x-git-status-checksum": "checksum-xyz"}

    def test_force_flag_included_only_when_true(self):
        push_body = {"remoteMessages": [], "remoteUrl": "https://github.com/acme/marketing-project.git"}
        git, client = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/push": FakeResponse(200, push_body)})
        git.git_push(_PROJECT_ID, "main", "main", "checksum-xyz", force=True)
        call = next(c for c in client.calls if c["url"] == f"{_BASE}/push")
        assert call["data"] == {"localBranch": "main", "remoteBranch": "main", "force": True}

    def test_failure_returns_error_dict(self):
        git, _ = _make_git(get_responses=_with_project_get(), post_responses={f"{_BASE}/push": FakeResponse(403, _ERROR_BODY)})
        result = git.git_push(_PROJECT_ID, "main", "main", "checksum")
        assert result["ok"] is False
