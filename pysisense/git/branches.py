from __future__ import annotations

from typing import Any

from ..utils import _extract_error_message


class GitBranchesMixin:
    def get_git_branches(self, project: str) -> list[dict[str, Any]] | dict[str, Any]:
        """List all Git branches of a project.

        Sends ``GET /api/v2/projects/{project_id}/branches``. Returns
        metadata for every local and remote-tracking branch in the
        project's local Git repository; remote-tracking branch names start
        with ``origin/``.

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.

        Returns
        -------
        list[dict[str, Any]] or dict[str, Any]
            A list of branch objects (``name``, ``parents``, ``head``) on
            success, or ``{"ok": False, "error": "..."}`` on failure
            (including an unresolved ``project``).
        """
        guard = self._require_linux("get_git_branches")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        self.logger.debug(f"Fetching Git branches for project '{project_id}'")
        response = self.api_client.get(f"/api/v2/projects/{project_id}/branches")

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to get Git branches for project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        branches = response.json()
        self.logger.info(f"Retrieved {len(branches)} Git branch(es) for project '{project_id}'.")
        return branches

    def get_git_branch(self, project: str, branch: str) -> dict[str, Any]:
        """Retrieve a single Git branch of a project.

        Sends ``GET /api/v2/projects/{project_id}/branches/{branch}``.

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.
        branch : str
            Branch name.

        Returns
        -------
        dict[str, Any]
            The branch object (``name``, ``parents``, ``head``) on success,
            or ``{"ok": False, "error": "..."}`` on failure (including an
            unresolved ``project`` or an unknown ``branch``).
        """
        guard = self._require_linux("get_git_branch")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        self.logger.debug(f"Fetching Git branch '{branch}' for project '{project_id}'")
        response = self.api_client.get(f"/api/v2/projects/{project_id}/branches/{branch}")

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to get Git branch '{branch}' for project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        return response.json()

    def create_git_branch(self, project: str, name: str, status_checksum: str, parent: str | None = None) -> dict[str, Any]:
        """Create a new Git branch in a project.

        Sends ``POST /api/v2/projects/{project_id}/branches``.

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.
        name : str
            Name of the new branch.
        status_checksum : str
            Checksum from ``GET /api/v2/projects/{project_id}/status``,
            sent as the ``x-git-status-checksum`` header; the request fails
            if it does not match the project's current status.
        parent : str, optional
            Commit hash or commitish the new branch should start from.
            Defaults to HEAD when omitted.

        Returns
        -------
        dict[str, Any]
            The created branch object (``name``, ``parents``, ``head``) on
            success, or ``{"ok": False, "error": "..."}`` on failure.
        """
        guard = self._require_linux("create_git_branch")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        body: dict[str, Any] = {"name": name}
        if parent is not None:
            body["parent"] = parent

        self.logger.debug(f"Creating Git branch '{name}' in project '{project_id}'")
        response = self.api_client.post(f"/api/v2/projects/{project_id}/branches", data=body, extra_headers={"x-git-status-checksum": status_checksum})

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to create Git branch '{name}' in project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Created Git branch '{name}' in project '{project_id}'.")
        return response.json()

    def checkout_git_branch(self, project: str, branch: str, status_checksum: str) -> dict[str, Any]:
        """Switch a Git project to a branch and import its assets.

        Sends ``POST /api/v2/projects/{project_id}/branches/{branch}/checkout``.
        Updates the project's index and working tree to match the branch and
        points HEAD at it, then validates and imports the resulting asset
        files into the application; assets that fail validation are not
        imported, while the rest still are (see the HTTP 453 note below).

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.
        branch : str
            Name of the branch to check out.
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
        guard = self._require_linux("checkout_git_branch")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        self.logger.debug(f"Checking out Git branch '{branch}' in project '{project_id}'")
        response = self.api_client.post(f"/api/v2/projects/{project_id}/branches/{branch}/checkout", extra_headers={"x-git-status-checksum": status_checksum})

        if response is None or response.status_code != 204:
            failure = _extract_error_message(response, f"Failed to checkout Git branch '{branch}' in project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Checked out Git branch '{branch}' in project '{project_id}'.")
        return {"success": True}
