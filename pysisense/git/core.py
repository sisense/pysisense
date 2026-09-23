from __future__ import annotations

import re
from typing import Any

from ..utils import _extract_error_message

_PROJECT_ID_PATTERN = re.compile(r"^[0-9a-fA-F]{24}$")


class GitCoreMixin:
    def _require_linux(self, method_name: str) -> dict[str, Any] | None:
        """
        Guard a Git Integration call against a Windows deployment.

        Git Integration has no Windows API surface at all (Sisense's
        Windows REST API stops at v1; the ``projects`` endpoints are v2
        only), so this is checked the same way at the top of every public
        method in this class rather than being routed per-endpoint like
        Blox's partial Linux/Windows split.

        Parameters
        ----------
        method_name : str
            Name of the calling public method, used in the error message.

        Returns
        -------
        dict[str, Any] or None
            ``{"ok": False, "error": "..."}`` when running against a
            Windows deployment, otherwise ``None``.
        """
        if self.api_client.operating_system == "windows":
            msg = f"{method_name} is not supported on Windows deployments."
            self.logger.error(msg)
            return {"ok": False, "error": msg}
        return None

    def get_git_projects(
        self,
        current_branch_name: bool = False,
        favorites_only: bool = False,
        last_commit_message: bool = False,
        last_commit_date: bool = False,
        owner_id: bool = False,
        detached_head_hash: bool = False,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """List the Git Integration projects accessible to the current user.

        Git Integration is an optional Sisense feature that an admin must
        enable (Admin > App Configuration > Feature Management) before this
        endpoint returns project data. Sends ``GET /api/v2/projects``. Each
        boolean flag below adds the corresponding field to every returned
        project only when set to ``True``; the response otherwise omits it.

        Parameters
        ----------
        current_branch_name : bool, optional
            Include each project's currently checked-out branch name.
        favorites_only : bool, optional
            Return only projects the current user has favorited.
        last_commit_message : bool, optional
            Include the subject/body of each project's last commit.
        last_commit_date : bool, optional
            Include the timestamp of each project's last commit.
        owner_id : bool, optional
            Include the ID of the user who created each project.
        detached_head_hash : bool, optional
            Include the current commit hash for projects in a detached HEAD
            state.

        Returns
        -------
        list[dict[str, Any]] or dict[str, Any]
            A list of Git project objects (``name``, ``oid``, ``owner``,
            ``created``, ``lastModified``, ``syncedAt``, ``shares``,
            ``userFavorites``, plus any fields enabled above) on success, or
            ``{"ok": False, "error": "..."}`` on failure.
        """
        guard = self._require_linux("get_git_projects")
        if guard is not None:
            return guard

        # Sisense's v2 API expects the literal string "true" for boolean query
        # params — a bare Python True serializes to "True" (capitalized) in
        # the query string, which the server 500s on instead of parsing.
        params: dict[str, Any] = {}
        if current_branch_name:
            params["current_branch_name"] = "true"
        if favorites_only:
            params["favorites_only"] = "true"
        if last_commit_message:
            params["last_commit_message"] = "true"
        if last_commit_date:
            params["last_commit_date"] = "true"
        if owner_id:
            params["owner_id"] = "true"
        if detached_head_hash:
            params["detached_head_hash"] = "true"

        self.logger.debug(f"Fetching Git projects (os={self.api_client.operating_system})")
        response = self.api_client.get("/api/v2/projects", params=params)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, "Failed to get Git projects", self.api_client)
            self.logger.error(failure["error"])
            return failure

        projects = response.json()
        self.logger.info(f"Retrieved {len(projects)} Git project(s).")
        return projects

    def get_git_project(
        self,
        project_id: str,
        branch_mappings: bool = False,
        commits: bool = False,
        current_branch_name: bool = False,
        owner_id: bool = False,
        tag_names: bool = False,
    ) -> dict[str, Any]:
        """Retrieve a single Git Integration project by ID.

        Git Integration is an optional Sisense feature that an admin must
        enable (Admin > App Configuration > Feature Management) before this
        endpoint returns project data. Sends
        ``GET /api/v2/projects/{project_id}``. Each boolean flag below adds
        the corresponding field to the response only when set to ``True``.

        Parameters
        ----------
        project_id : str
            OID of the Git project.
        branch_mappings : bool, optional
            Include an array of all local branches and their remote-tracking
            branches.
        commits : bool, optional
            Include an array of all commits in the project's local Git
            repository.
        current_branch_name : bool, optional
            Include the name of the currently checked-out branch.
        owner_id : bool, optional
            Include the ID of the user who created the project.
        tag_names : bool, optional
            Include an array of all local Git tag names.

        Returns
        -------
        dict[str, Any]
            The Git project object on success, or ``{"ok": False, "error":
            "..."}`` on failure (including when ``project_id`` does not
            exist).
        """
        guard = self._require_linux("get_git_project")
        if guard is not None:
            return guard

        # Same "true" (not Python True) requirement as get_git_projects — see
        # the comment there.
        params: dict[str, Any] = {}
        if branch_mappings:
            params["branch_mappings"] = "true"
        if commits:
            params["commits"] = "true"
        if current_branch_name:
            params["current_branch_name"] = "true"
        if owner_id:
            params["owner_id"] = "true"
        if tag_names:
            params["tag_names"] = "true"

        self.logger.debug(f"Fetching Git project '{project_id}' (os={self.api_client.operating_system})")
        response = self.api_client.get(f"/api/v2/projects/{project_id}", params=params)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to get Git project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        return response.json()

    def resolve_git_project_reference(self, project_ref: str) -> dict[str, Any]:
        """Resolve a Git project reference (ID or name) to a concrete project ID and name.

        Accepts either a 24-character Git project OID or a project name. An
        ID-shaped reference is looked up with ``GET
        /api/v2/projects/{project_id}``; anything else (or a failed ID
        lookup) falls back to matching the ``name`` field of every project
        returned by ``GET /api/v2/projects``, since project lookup by name
        has no dedicated server-side endpoint.

        Parameters
        ----------
        project_ref : str
            Git project reference to resolve — an OID or a name.

        Returns
        -------
        dict[str, Any]
            ``{"success": bool, "status_code": int, "project_id": str or
            None, "project_name": str or None, "error": str or None}``.
            ``status_code`` is ``200`` when resolved, ``404`` when no
            project matches, or ``500`` on an unexpected error.
        """
        guard = self._require_linux("resolve_git_project_reference")
        if guard is not None:
            return {"success": False, "ok": False, "status_code": None, "project_id": None, "project_name": None, "error": guard["error"]}

        self.logger.debug(f"Resolving Git project reference: {project_ref}")

        is_id_candidate = bool(_PROJECT_ID_PATTERN.fullmatch(project_ref))

        if is_id_candidate:
            try:
                result_by_id = self.get_git_project(project_ref)
                if isinstance(result_by_id, dict) and result_by_id.get("ok") is not False and result_by_id.get("oid"):
                    project_id = result_by_id["oid"]
                    project_name = result_by_id.get("name")
                    self.logger.info(f"Resolved Git project reference '{project_ref}' as ID '{project_id}'.")
                    return {"success": True, "status_code": 200, "project_id": project_id, "project_name": project_name, "error": None}
            except Exception as exc:
                self.logger.exception(f"Unexpected error while resolving Git project reference '{project_ref}' as ID: {exc}")
                return {"success": False, "ok": False, "status_code": 500, "project_id": None, "project_name": None, "error": str(exc)}

        try:
            all_projects = self.get_git_projects()
            if isinstance(all_projects, list):
                match = next((p for p in all_projects if isinstance(p, dict) and p.get("name") == project_ref), None)
                if match and match.get("oid"):
                    project_id = match["oid"]
                    project_name = match.get("name")
                    self.logger.info(f"Resolved Git project reference '{project_ref}' as name to ID '{project_id}'.")
                    return {"success": True, "status_code": 200, "project_id": project_id, "project_name": project_name, "error": None}
        except Exception as exc:
            self.logger.exception(f"Unexpected error while resolving Git project reference '{project_ref}' as name: {exc}")
            return {"success": False, "ok": False, "status_code": 500, "project_id": None, "project_name": None, "error": str(exc)}

        error_msg = f"Git project reference '{project_ref}' could not be resolved as ID or name."
        self.logger.error(error_msg)
        return {"success": False, "ok": False, "status_code": 404, "project_id": None, "project_name": None, "error": error_msg}

    def get_git_project_status(self, project: str) -> dict[str, Any]:
        """Retrieve a Git project's current status checksum.

        Sends ``GET /api/v2/projects/{project_id}/status``. The returned
        checksum reflects the project's current working-directory file
        changes, current branch name, and HEAD commit. It must be sent as
        the ``x-git-status-checksum`` header on any request that modifies
        the project's local Git repository (branch/commit checkout,
        discard, pull, push, sync); the server rejects the request if the
        checksum is stale, so callers should fetch a fresh one immediately
        before such a call.

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.

        Returns
        -------
        dict[str, Any]
            ``{"statusChecksum": "..."}`` on success, or ``{"ok": False,
            "error": "..."}`` on failure (including an unresolved
            ``project``).
        """
        guard = self._require_linux("get_git_project_status")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        self.logger.debug(f"Fetching Git status for project '{project_id}'")
        response = self.api_client.get(f"/api/v2/projects/{project_id}/status")

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to get Git status for project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        return response.json()

    def unlock_git_project(self, project: str) -> dict[str, Any]:
        """Manually release a stuck Git lock on a project.

        Sends ``PATCH /api/v2/projects/{project_id}/unlock``. Sisense
        acquires a lock on a project while a request that modifies its
        local Git repository is in flight, and normally releases it when
        that request completes; this endpoint is the manual escape hatch
        when a lock was left behind (for example, after an interrupted
        request). Requires an admin token — Sisense restricts this endpoint
        to admin users server-side.

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` on success (HTTP 204), or ``{"ok": False,
            "error": "..."}`` on failure.
        """
        guard = self._require_linux("unlock_git_project")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        self.logger.debug(f"Unlocking Git project '{project_id}'")
        response = self.api_client.patch(f"/api/v2/projects/{project_id}/unlock")

        if response is None or response.status_code != 204:
            failure = _extract_error_message(response, f"Failed to unlock Git project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Unlocked Git project '{project_id}'.")
        return {"success": True}

    def sync_git_project(self, project: str, status_checksum: str) -> dict[str, Any]:
        """Synchronize a Git project's files with the current asset state.

        Sends ``POST /api/v2/projects/{project_id}/sync``. Exports the
        current state of the project's tracked Sisense assets to files in
        its local Git repository, so the Git files match the assets in the
        application — the opposite direction of a checkout or pull.

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.
        status_checksum : str
            Checksum from ``GET /api/v2/projects/{project_id}/status``,
            sent as the ``x-git-status-checksum`` header; the request fails
            if it does not match the project's current status.

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` on success (HTTP 204), or ``{"ok": False,
            "error": "..."}`` on failure (a locked project answers HTTP 423
            — see ``unlock_git_project``).
        """
        guard = self._require_linux("sync_git_project")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        self.logger.debug(f"Syncing Git project '{project_id}'")
        response = self.api_client.post(f"/api/v2/projects/{project_id}/sync", extra_headers={"x-git-status-checksum": status_checksum})

        if response is None or response.status_code != 204:
            failure = _extract_error_message(response, f"Failed to sync Git project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Synced Git project '{project_id}'.")
        return {"success": True}

    def discard_git_project_changes(self, project: str, changes: list[str], status_checksum: str) -> dict[str, Any]:
        """Discard uncommitted file changes in a Git project's working tree.

        Sends ``POST /api/v2/projects/{project_id}/discard``. Removes the
        listed uncommitted changes from the project's local Git working
        tree, then validates and imports the resulting asset files into the
        application; assets that fail validation are not imported, while
        the rest still are (see the HTTP 453 note below).

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.
        changes : list[str]
            File paths whose uncommitted changes should be discarded.
        status_checksum : str
            Checksum from ``GET /api/v2/projects/{project_id}/status``,
            sent as the ``x-git-status-checksum`` header; the request fails
            if it does not match the project's current status.

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` on success (HTTP 204), or ``{"ok": False,
            "error": "..."}`` on failure. HTTP 453 means the discard
            succeeded but one or more assets failed JSON validation on
            import — those assets were not imported, and validation details
            travel in the failure dict's ``raw_body``.
        """
        guard = self._require_linux("discard_git_project_changes")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        body = {"changes": changes}
        self.logger.debug(f"Discarding {len(changes)} change(s) in Git project '{project_id}'")
        response = self.api_client.post(f"/api/v2/projects/{project_id}/discard", data=body, extra_headers={"x-git-status-checksum": status_checksum})

        if response is None or response.status_code != 204:
            failure = _extract_error_message(response, f"Failed to discard changes in Git project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Discarded {len(changes)} change(s) in Git project '{project_id}'.")
        return {"success": True}
