from __future__ import annotations

from typing import Any

from ..payloads import GitHttpsCredentialsPayload
from ..utils import _extract_error_message


class GitRemoteMixin:
    def git_fetch(self, project: str, https_credentials: GitHttpsCredentialsPayload | None = None) -> dict[str, Any]:
        """Download commits and tags from a Git project's remote repository.

        Sends ``POST /api/v2/projects/{project_id}/fetch``. Downloads
        commits from every branch and tag of the remote repository
        connected to the project, updating the local repository's
        remote-tracking branches. Nothing is merged into the current
        branch — see ``git_pull`` for that.

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.
        https_credentials : GitHttpsCredentialsPayload, optional
            HTTP(S) username/password (or personal access token) for the
            remote repository. When omitted, credentials previously saved
            for the current user and this project are used; the request
            fails if none were saved.

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` on success (HTTP 204), or ``{"ok": False,
            "error": "..."}`` on failure.
        """
        guard = self._require_linux("git_fetch")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        body: dict[str, Any] = {}
        if https_credentials is not None:
            body["httpsCredentials"] = dict(https_credentials)

        self.logger.debug(f"Running Git fetch for project '{project_id}'")
        response = self.api_client.post(f"/api/v2/projects/{project_id}/fetch", data=body)

        if response is None or response.status_code != 204:
            failure = _extract_error_message(response, f"Failed to run Git fetch for project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Ran Git fetch for project '{project_id}'.")
        return {"success": True}

    def git_pull(
        self,
        project: str,
        remote_branch: str,
        status_checksum: str,
        https_credentials: GitHttpsCredentialsPayload | None = None,
    ) -> dict[str, Any]:
        """Fetch and merge remote changes into a Git project's current branch.

        Sends ``POST /api/v2/projects/{project_id}/pull``. Downloads
        commits from ``remote_branch`` on the connected remote repository
        and merges them into the project's currently checked-out branch. If
        the merge has conflicts, it is aborted and nothing is imported.
        Otherwise the resulting asset files are validated and imported into
        the application; assets that fail validation are not imported,
        while the rest still are (see the HTTP 453 note below). New assets
        introduced by the merge are added to the project; when they are
        owned by users without prior project access, a pending share is
        created for each and requires admin approval.

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.
        remote_branch : str
            Name of the branch on the remote repository to fetch and merge
            from. The commits are merged into the project's current local
            branch.
        status_checksum : str
            Checksum from ``GET /api/v2/projects/{project_id}/status``,
            sent as the ``x-git-status-checksum`` header; the request fails
            if it does not match the project's current status.
        https_credentials : GitHttpsCredentialsPayload, optional
            HTTP(S) username/password (or personal access token) for the
            remote repository. When omitted, credentials previously saved
            for the current user and this project are used; the request
            fails if none were saved.

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` on success (HTTP 204), or ``{"ok": False,
            "error": "..."}`` on failure. HTTP 453 means the merge
            succeeded but one or more assets failed JSON validation on
            import — those assets were not imported, and validation details
            travel in the failure dict's ``raw_body``.
        """
        guard = self._require_linux("git_pull")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        body: dict[str, Any] = {"remoteBranch": remote_branch}
        if https_credentials is not None:
            body["httpsCredentials"] = dict(https_credentials)

        self.logger.debug(f"Running Git pull for project '{project_id}' from remote branch '{remote_branch}'")
        response = self.api_client.post(f"/api/v2/projects/{project_id}/pull", data=body, extra_headers={"x-git-status-checksum": status_checksum})

        if response is None or response.status_code != 204:
            failure = _extract_error_message(response, f"Failed to run Git pull for project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Ran Git pull for project '{project_id}' from remote branch '{remote_branch}'.")
        return {"success": True}

    def git_push(
        self,
        project: str,
        local_branch: str,
        remote_branch: str,
        status_checksum: str,
        force: bool = False,
        https_credentials: GitHttpsCredentialsPayload | None = None,
    ) -> dict[str, Any]:
        """Upload commits from a Git project's local branch to its remote repository.

        Sends ``POST /api/v2/projects/{project_id}/push``. If
        ``remote_branch`` does not yet exist on the remote, it is created.

        Parameters
        ----------
        project : str
            Git project reference — an OID or a name.
        local_branch : str
            Name of the local branch whose commits should be pushed.
        remote_branch : str
            Name of the branch on the remote repository to push to.
        status_checksum : str
            Checksum from ``GET /api/v2/projects/{project_id}/status``,
            sent as the ``x-git-status-checksum`` header; the request fails
            if it does not match the project's current status.
        force : bool, optional
            When ``True``, pushes with ``--force-with-lease``, overriding
            the remote branch's history with the local branch's history.
            This can discard commits pushed by others since the local
            branch's remote-tracking ref was last updated — use with care.
        https_credentials : GitHttpsCredentialsPayload, optional
            HTTP(S) username/password (or personal access token) for the
            remote repository. When omitted, credentials previously saved
            for the current user and this project are used; the request
            fails if none were saved.

        Returns
        -------
        dict[str, Any]
            ``{"remoteMessages": [...], "remoteUrl": "..."}`` on success
            (HTTP 200), or ``{"ok": False, "error": "..."}`` on failure.
        """
        guard = self._require_linux("git_push")
        if guard is not None:
            return guard

        ref = self.resolve_git_project_reference(project)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Git project '{project}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        project_id = ref["project_id"]

        body: dict[str, Any] = {"localBranch": local_branch, "remoteBranch": remote_branch}
        if force:
            body["force"] = True
        if https_credentials is not None:
            body["httpsCredentials"] = dict(https_credentials)

        self.logger.debug(f"Running Git push for project '{project_id}' ('{local_branch}' -> '{remote_branch}', force={force})")
        response = self.api_client.post(f"/api/v2/projects/{project_id}/push", data=body, extra_headers={"x-git-status-checksum": status_checksum})

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to run Git push for project '{project_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Ran Git push for project '{project_id}' ('{local_branch}' -> '{remote_branch}').")
        return response.json()
