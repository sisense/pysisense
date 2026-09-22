Git Class Documentation
========================

The `Git` class wraps Sisense's **Git Integration** REST API
(`/api/v2/projects...`): listing and inspecting Git projects, working with
branches and commits, and syncing a project with its remote repository
(fetch/pull/push).

> **Optional feature.** Git Integration must be enabled by a Sisense admin
> (Admin > App Configuration > Feature Management) before any endpoint below
> returns project data. There is no API to check whether it is enabled — if
> it isn't, calls fail with a normal Sisense error response (surfaced the
> same way as any other failure below).

Linux-only: there is no Windows equivalent of this API surface. Every method
that takes a `project` parameter accepts either the Git project's
24-character OID or its name — see `resolve_git_project_reference`.

* * * * *

Class: `Git`
------------

### `__init__(self, api_client=None, debug=False)`

Initializes the `Git` class.

**Parameters:**

-   `api_client` (SisenseClient, optional): An existing authenticated client. When omitted, a new `SisenseClient` is created.

-   `debug` (bool): Enable debug logging on a newly created client. Default is `False`.

* * * * *

## Module: `core.py`

### `get_git_projects(current_branch_name=False, favorites_only=False, last_commit_message=False, last_commit_date=False, owner_id=False, detached_head_hash=False)`

Lists the Git projects accessible to the current user. Sends `GET /api/v2/projects`. Each boolean flag adds the matching field to every returned project only when `True`.

**Parameters:**

-   `current_branch_name` (bool, optional): Include each project's current branch name.
-   `favorites_only` (bool, optional): Return only projects favorited by the current user.
-   `last_commit_message` (bool, optional): Include each project's last commit message.
-   `last_commit_date` (bool, optional): Include each project's last commit timestamp.
-   `owner_id` (bool, optional): Include the ID of each project's creator.
-   `detached_head_hash` (bool, optional): Include the current commit hash for projects in a detached HEAD state.

**Returns:**

-   `list[dict]`: Git project objects on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `get_git_project(project_id, branch_mappings=False, commits=False, current_branch_name=False, owner_id=False, tag_names=False)`

Retrieves a single Git project by OID. Sends `GET /api/v2/projects/{project_id}`.

**Parameters:**

-   `project_id` (str): OID of the Git project.
-   `branch_mappings` (bool, optional): Include local/remote-tracking branch pairs.
-   `commits` (bool, optional): Include every commit in the project's local repository.
-   `current_branch_name` (bool, optional): Include the current branch name.
-   `owner_id` (bool, optional): Include the ID of the project's creator.
-   `tag_names` (bool, optional): Include every local Git tag name.

**Returns:**

-   `dict`: The Git project object on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `resolve_git_project_reference(project_ref)`

Resolves a Git project reference (OID or name) to a concrete project ID and name. An ID-shaped reference is looked up directly; anything else (or a failed ID lookup) falls back to matching `name` across every project from `get_git_projects`, since project lookup by name has no dedicated endpoint.

**Parameters:**

-   `project_ref` (str): A Git project OID or name.

**Returns:**

-   `dict`: `{"success": bool, "status_code": int, "project_id": str | None, "project_name": str | None, "error": str | None}`.

* * * * *

### `get_git_project_status(project)`

Retrieves a Git project's current status checksum. Sends `GET /api/v2/projects/{project_id}/status`. This checksum must be sent as the `x-git-status-checksum` header on any request that modifies the project (branch/commit checkout, discard, pull, push, sync); the server rejects the request if it's stale, so fetch a fresh checksum immediately before such a call.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.

**Returns:**

-   `dict`: `{"statusChecksum": "..."}` on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `unlock_git_project(project)`

Manually releases a stuck Git lock on a project. Sends `PATCH /api/v2/projects/{project_id}/unlock`. Requires an admin token — Sisense restricts this endpoint to admin users server-side, unlike every other method in this class.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.

**Returns:**

-   `dict`: `{"success": True}` on success (HTTP 204), or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `sync_git_project(project, status_checksum)`

Exports the current state of a project's tracked Sisense assets to files in its local Git repository — the opposite direction of a checkout or pull. Sends `POST /api/v2/projects/{project_id}/sync`.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.
-   `status_checksum` (str): Checksum from `get_git_project_status`, sent as the `x-git-status-checksum` header.

**Returns:**

-   `dict`: `{"success": True}` on success (HTTP 204), or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `discard_git_project_changes(project, changes, status_checksum)`

Discards uncommitted file changes in a project's Git working tree, then validates and imports the resulting asset files. Sends `POST /api/v2/projects/{project_id}/discard`.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.
-   `changes` (list[str]): File paths whose uncommitted changes should be discarded.
-   `status_checksum` (str): Checksum from `get_git_project_status`, sent as the `x-git-status-checksum` header.

**Returns:**

-   `dict`: `{"success": True}` on success (HTTP 204), or `{"ok": False, "error": "..."}` on failure. HTTP 453 means the discard succeeded but some assets failed validation on import — see [HTTP 453 note](#http-453--partial-asset-import) below.

* * * * *

## Module: `branches.py`

### `get_git_branches(project)`

Lists every local and remote-tracking Git branch of a project. Sends `GET /api/v2/projects/{project_id}/branches`. Remote-tracking branch names start with `origin/`.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.

**Returns:**

-   `list[dict]`: Branch objects (`name`, `parents`, `head`) on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `get_git_branch(project, branch)`

Retrieves a single Git branch of a project. Sends `GET /api/v2/projects/{project_id}/branches/{branch}`.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.
-   `branch` (str): Branch name.

**Returns:**

-   `dict`: The branch object on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `create_git_branch(project, name, status_checksum, parent=None)`

Creates a new Git branch in a project. Sends `POST /api/v2/projects/{project_id}/branches`.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.
-   `name` (str): Name of the new branch.
-   `status_checksum` (str): Checksum from `get_git_project_status`, sent as the `x-git-status-checksum` header.
-   `parent` (str, optional): Commit hash or commitish the new branch starts from. Defaults to HEAD.

**Returns:**

-   `dict`: The created branch object on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `checkout_git_branch(project, branch, status_checksum)`

Switches a project to a branch and imports its assets. Sends `POST /api/v2/projects/{project_id}/branches/{branch}/checkout`.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.
-   `branch` (str): Branch to check out.
-   `status_checksum` (str): Checksum from `get_git_project_status`, sent as the `x-git-status-checksum` header.

**Returns:**

-   `dict`: `{"success": True}` on success (HTTP 204), or `{"ok": False, "error": "..."}` on failure. HTTP 453 means the checkout succeeded but some assets failed validation on import — see [HTTP 453 note](#http-453--partial-asset-import) below.

* * * * *

## Module: `commits.py`

### `get_git_commits(project)`

Lists every Git commit of a project. Sends `GET /api/v2/projects/{project_id}/commits`.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.

**Returns:**

-   `list[dict]`: Commit objects (`hash`, `authorName`, `authorEmail`, `date`, `parents`, `refs`, `message`) on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `get_git_commit(project, commit)`

Retrieves a single Git commit of a project. Sends `GET /api/v2/projects/{project_id}/commits/{commit}`.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.
-   `commit` (str): Commit hash.

**Returns:**

-   `dict`: The commit object on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `create_git_commit(project, message, changes, status_checksum)`

Creates a new Git commit in a project. Sends `POST /api/v2/projects/{project_id}/commits`. Files are auto-staged by Sisense, so `changes` only needs to list which already-changed paths to include.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.
-   `message` (str): Subject line and optional body of the commit.
-   `changes` (list[str]): File paths whose changes should be included.
-   `status_checksum` (str): Checksum from `get_git_project_status`, sent as the `x-git-status-checksum` header.

**Returns:**

-   `dict`: The created commit object on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `checkout_git_commit(project, commit, status_checksum)`

Checks out a commit and imports its assets, detaching HEAD. Sends `POST /api/v2/projects/{project_id}/commits/{commit}/checkout`.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.
-   `commit` (str): Commit hash to check out.
-   `status_checksum` (str): Checksum from `get_git_project_status`, sent as the `x-git-status-checksum` header.

**Returns:**

-   `dict`: `{"success": True}` on success (HTTP 204), or `{"ok": False, "error": "..."}` on failure. HTTP 453 means the checkout succeeded but some assets failed validation on import — see [HTTP 453 note](#http-453--partial-asset-import) below.

* * * * *

## Module: `remote.py`

### `git_fetch(project, https_credentials=None)`

Downloads commits and tags from every branch of a project's remote repository, updating remote-tracking branches without merging anything. Sends `POST /api/v2/projects/{project_id}/fetch`.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.
-   `https_credentials` (dict, optional): `{"username": str, "password": str, "save": bool}` — see [`GitHttpsCredentialsPayload`](#gitHttpsCredentialsPayload). Omit to use credentials previously saved for the current user and this project; the request fails if none were saved.

**Returns:**

-   `dict`: `{"success": True}` on success (HTTP 204), or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `git_pull(project, remote_branch, status_checksum, https_credentials=None)`

Fetches commits from `remote_branch` on the connected remote and merges them into the project's current branch, then imports the resulting assets. If the merge has conflicts, it is aborted and nothing is imported. Sends `POST /api/v2/projects/{project_id}/pull`.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.
-   `remote_branch` (str): Remote branch to fetch and merge from.
-   `status_checksum` (str): Checksum from `get_git_project_status`, sent as the `x-git-status-checksum` header.
-   `https_credentials` (dict, optional): Same shape as `git_fetch`.

**Returns:**

-   `dict`: `{"success": True}` on success (HTTP 204), or `{"ok": False, "error": "..."}` on failure. HTTP 453 means the merge succeeded but some assets failed validation on import — see [HTTP 453 note](#http-453--partial-asset-import) below.

* * * * *

### `git_push(project, local_branch, remote_branch, status_checksum, force=False, https_credentials=None)`

Uploads commits from a local branch to a branch on the project's remote repository, creating it if it doesn't exist. Sends `POST /api/v2/projects/{project_id}/push`. This is the one write endpoint that returns a real response body instead of `{"success": True}`.

**Parameters:**

-   `project` (str): Git project reference — an OID or a name.
-   `local_branch` (str): Local branch whose commits should be pushed.
-   `remote_branch` (str): Remote branch to push to.
-   `status_checksum` (str): Checksum from `get_git_project_status`, sent as the `x-git-status-checksum` header.
-   `force` (bool, optional): When `True`, pushes with `--force-with-lease`, overriding the remote branch's history. Can discard commits pushed by others — use with care.
-   `https_credentials` (dict, optional): Same shape as `git_fetch`.

**Returns:**

-   `dict`: `{"remoteMessages": [...], "remoteUrl": "..."}` on success (HTTP 200), or `{"ok": False, "error": "..."}` on failure.

* * * * *

## `GitHttpsCredentialsPayload`

TypedDict for the `https_credentials` parameter on `git_fetch`, `git_pull`, and `git_push`.

-   `username` (str, required): HTTP(S) username for the remote repository.
-   `password` (str, required): HTTP(S) password or personal access token.
-   `save` (bool, optional): When `True`, saves the credentials server-side for the current user and this project, for automatic reuse on later remote operations.

* * * * *

## HTTP 453 — partial asset import

`checkout_git_branch`, `checkout_git_commit`, and `discard_git_project_changes` all validate and import Sisense asset files after the underlying Git operation succeeds. If one or more asset files fail JSON validation, the Git operation still completed (branch/commit checked out, or changes discarded), but the invalid assets were not imported — while the rest were. This is reported as an HTTP 453 failure; the redacted validation details travel in the failure dict's `raw_body` key.

## HTTP 423 — project locked

Any write operation can answer HTTP 423 when another in-flight request is currently modifying the same project's local Git repository. If a lock is left behind after an interrupted request (rather than released automatically), an admin can clear it with `unlock_git_project`.
