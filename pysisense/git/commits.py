from __future__ import annotations

from typing import Any

from ..utils import _extract_error_message


class GitCommitsMixin:
    def get_git_commits(self, project: str) -> list[dict[str, Any]] | dict[str, Any]:
        """List all Git commits of a project.

        Sends ``GET /api/v2/projects/{project_id}/commits``. Returns
        metadata for every commit in the project's local Git repository.

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.

        Returns
        -------
        list[dict[str, Any]] or dict[str, Any]
            A list of commit objects (``hash``, ``authorName``,
            ``authorEmail``, ``date``, ``parents``, ``refs``, ``message``)
            on success, or ``{"ok": False, "error": "..."}`` on failure
            (including an unresolved ``project``).
        """
        guard = self._require_linux("get_git_commits")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        self.logger.debug(f"Fetching Git commits for project '{project_id}'")
        response = self.api_client.get(f"/api/v2/projects/{project_id}/commits")

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to get Git commits for project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        commits = response.json()
        self.logger.info(f"Retrieved {len(commits)} Git commit(s) for project '{project_id}'.")
        return commits

    def get_git_commit(self, project: str, commit: str) -> dict[str, Any]:
        """Retrieve a single Git commit of a project.

        Sends ``GET /api/v2/projects/{project_id}/commits/{commit}``.

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.
        commit : str
            Commit hash.

        Returns
        -------
        dict[str, Any]
            The commit object (``hash``, ``authorName``, ``authorEmail``,
            ``date``, ``parents``, ``refs``, ``message``) on success, or
            ``{"ok": False, "error": "..."}`` on failure (including an
            unresolved ``project`` or an unknown ``commit``).
        """
        guard = self._require_linux("get_git_commit")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        self.logger.debug(f"Fetching Git commit '{commit}' for project '{project_id}'")
        response = self.api_client.get(f"/api/v2/projects/{project_id}/commits/{commit}")

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to get Git commit '{commit}' for project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        return response.json()

    def create_git_commit(self, project: str, message: str, changes: list[str], status_checksum: str) -> dict[str, Any]:
        """Create a new Git commit in a project.

        Sends ``POST /api/v2/projects/{project_id}/commits``. Files are
        auto-staged by Sisense, so ``changes`` only needs to list which
        already-changed file paths to include — there is no separate stage
        step.

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.
        message : str
            Subject line and optional body of the new commit.
        changes : list[str]
            File paths whose changes should be included in the commit.
        status_checksum : str
            Checksum from ``GET /api/v2/projects/{project_id}/status``,
            sent as the ``x-git-status-checksum`` header; the request fails
            if it does not match the project's current status.

        Returns
        -------
        dict[str, Any]
            The created commit object on success, or ``{"ok": False,
            "error": "..."}`` on failure.
        """
        guard = self._require_linux("create_git_commit")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        body = {"message": message, "changes": changes}
        self.logger.debug(f"Creating Git commit in project '{project_id}' ({len(changes)} change(s))")
        response = self.api_client.post(f"/api/v2/projects/{project_id}/commits", data=body, extra_headers={"x-git-status-checksum": status_checksum})

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to create Git commit in project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Created Git commit in project '{project_id}'.")
        return response.json()

    def checkout_git_commit(self, project: str, commit: str, status_checksum: str) -> dict[str, Any]:
        """Check out a Git commit of a project and import its assets.

        Sends ``POST /api/v2/projects/{project_id}/commits/{commit}/checkout``.
        Updates the project's index and working tree to match the commit
        and detaches HEAD, then validates and imports the resulting asset
        files into the application; assets that fail validation are not
        imported, while the rest still are (see the HTTP 453 note below).

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.
        commit : str
            Commit hash to check out.
        status_checksum : str
            Checksum from ``GET /api/v2/projects/{project_id}/status``,
            sent as the ``x-git-status-checksum`` header; the request fails
            if it does not match the project's current status.

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` on success (HTTP 204), or ``{"ok": False,
            "error": "..."}`` on failure. HTTP 453 means the checkout
            succeeded but one or more assets failed JSON validation on
            import — those assets were not imported, and validation details
            travel in the failure dict's ``raw_body``.
        """
        guard = self._require_linux("checkout_git_commit")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        self.logger.debug(f"Checking out Git commit '{commit}' in project '{project_id}'")
        response = self.api_client.post(f"/api/v2/projects/{project_id}/commits/{commit}/checkout", extra_headers={"x-git-status-checksum": status_checksum})

        if response is None or response.status_code != 204:
            failure = _extract_error_message(response, f"Failed to checkout Git commit '{commit}' in project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Checked out Git commit '{commit}' in project '{project_id}'.")
        return {"success": True}
