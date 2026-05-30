from __future__ import annotations

from typing import Any


class FolderCoreMixin:
    def create_folder(self, name: str, parent_id: str | None = None) -> dict[str, Any]:
        """Create a new Sisense folder.

        Sends ``POST /api/v1/folders`` with the folder name and an optional
        parent folder ID. When ``parent_id`` is omitted, the folder is created
        at the root of the folder tree.

        Parameters
        ----------
        name : str
            Display name for the new folder.
        parent_id : str, optional
            OID of the parent folder. When omitted, the folder is created at
            the root level.

        Returns
        -------
        dict[str, Any]
            The created folder object from the API (includes ``oid``, ``name``,
            and related fields), or ``{"error": "..."}`` on failure.
        """
        endpoint = "/api/v1/folders"
        payload: dict[str, Any] = {"name": name}

        if parent_id is not None:
            payload["parentId"] = parent_id

        self.logger.debug(f"Creating folder — name={name!r}, parent_id={parent_id!r}")
        response = self.api_client.post(endpoint, data=payload)

        if response is None:
            self.logger.error("POST request to create folder failed: No response received.")
            return {"error": "No response received while creating folder."}

        if response.status_code != 201:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to create folder. Error: {error_message}")
            return {"error": f"Failed to create folder. {error_message}"}

        created_folder = response.json()

        self.logger.info(f"Successfully created folder with ID {created_folder.get('oid')}.")
        return created_folder

    def update_folder(
        self,
        folder_id: str,
        name: str | None = None,
        parent_id: str | None = None,
        owner: str | None = None,
    ) -> dict[str, Any]:
        """Update an existing folder.

        Sends ``PATCH /api/v1/folders/{folder_id}``. Only fields explicitly
        provided are included in the request body; omitted arguments are not
        modified on the server.

        Parameters
        ----------
        folder_id : str
            OID of the folder to update.
        name : str, optional
            New display name for the folder.
        parent_id : str, optional
            OID of the new parent folder (moves the folder in the tree).
        owner : str, optional
            User OID of the new folder owner.

        Returns
        -------
        dict[str, Any]
            The updated folder object from the API, or ``{"error": "..."}`` on
            failure.
        """
        endpoint = f"/api/v1/folders/{folder_id}"
        payload: dict[str, Any] = {}

        if name is not None:
            payload["name"] = name
        if parent_id is not None:
            payload["parentId"] = parent_id
        if owner is not None:
            payload["owner"] = owner

        self.logger.debug(f"Updating folder {folder_id} — fields: {list(payload.keys())}")
        response = self.api_client.patch(endpoint, data=payload)

        if response is None:
            self.logger.error(f"PATCH request to update folder {folder_id} failed: No response received.")
            return {"error": f"No response received while updating folder ID '{folder_id}'"}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to update folder {folder_id}. Error: {error_message}")
            return {"error": f"Failed to update folder '{folder_id}'. {error_message}"}

        updated_folder = response.json()

        self.logger.info(f"Successfully updated folder with ID {folder_id}.")
        return updated_folder

    def get_folder_id(self, folder_id: str) -> dict[str, Any]:
        """Retrieve a single folder by OID.

        Sends ``GET /api/v1/folders/{folder_id}`` and returns the folder
        metadata object.

        Parameters
        ----------
        folder_id : str
            OID of the folder to retrieve.

        Returns
        -------
        dict[str, Any]
            Folder metadata from the API, or ``{"error": "..."}`` if the
            request fails or no folder is found.
        """
        endpoint = f"/api/v1/folders/{folder_id}"
        self.logger.debug(f"Getting folder with ID: {folder_id}")
        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error(f"GET request to retrieve folder {folder_id} failed: No response received.")
            return {"error": f"No response received while retrieving folder ID '{folder_id}'"}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to retrieve folder {folder_id}. Error: {error_message}")
            return {"error": f"Failed to retrieve folder '{folder_id}'. {error_message}"}

        folder = response.json()

        if not folder:
            self.logger.warning(f"No folder found with ID {folder_id}.")
            return {"error": f"No folder found with ID '{folder_id}'"}

        self.logger.info(f"Successfully retrieved folder with ID {folder_id}.")
        return folder

    def get_folders(self, structure: str = "flat") -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve folders using a configurable ``structure`` query parameter.

        Sends ``GET /api/v1/folders?structure={structure}``. The default
        ``structure`` is ``"flat"``, which returns a flat folder list (used
        before migration in win2linux workflows).

        Parameters
        ----------
        structure : str, optional
            Sisense folder structure type (for example ``"flat"`` or ``"tree"``).
            Default is ``"flat"``.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            Folder data from the API (typically a list), or ``{"error": "..."}``
            on failure.
        """
        return self._get_folders_by_structure(structure)

    def get_folder_ancestors(self, structure: str) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve folder ancestor data for a given structure type.

        Sends ``GET /api/v1/folders?structure={structure}``. The ``structure``
        value is passed through to Sisense (for example an ancestors-specific
        structure string used by your environment).

        Parameters
        ----------
        structure : str
            Sisense folder structure type for the request.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            Folder data from the API, or ``{"error": "..."}`` on failure.
        """
        return self._get_folders_by_structure(structure)

    def get_navver(self) -> dict[str, Any]:
        """Retrieve the Sisense navigation tree (navver payload).

        Sends ``GET /api/v1/navver``. The response includes a ``folders`` key
        with the navigation folder hierarchy.

        Returns
        -------
        dict[str, Any]
            The navver response object on success, or ``{"error": "..."}`` on
            failure.
        """
        endpoint = "/api/v1/navver"
        self.logger.debug("Getting navver navigation payload.")
        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error("GET request to retrieve navver failed: No response received.")
            return {"error": "No response received while retrieving navver."}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to retrieve navver. Error: {error_message}")
            return {"error": f"Failed to retrieve navver. {error_message}"}

        navver = response.json()
        self.logger.info("Successfully retrieved navver navigation payload.")
        return navver

    def get_all_folders(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve the full folder tree.

        Sends ``GET /api/v1/folders?structure=tree`` and returns the nested
        folder hierarchy used by Sisense for organizing dashboards.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            A list of root-level folder nodes (each may contain nested
            ``folders`` and ``dashboards`` keys), or ``{"error": "..."}`` on
            failure.
        """
        return self._get_folders_by_structure("tree")

    def _get_folders_by_structure(self, structure: str) -> list[dict[str, Any]] | dict[str, Any]:
        endpoint = f"/api/v1/folders?structure={structure}"
        self.logger.debug(f"Getting folders with structure={structure!r}")
        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error(f"GET request to retrieve folders (structure={structure!r}) failed: No response received.")
            return {"error": "No response received while retrieving folders."}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to retrieve folders (structure={structure!r}). Error: {error_message}")
            return {"error": f"Failed to retrieve folders. {error_message}"}

        folders = response.json()

        count = len(folders) if isinstance(folders, list) else 1
        self.logger.info(f"Successfully retrieved folders (structure={structure!r}, count={count}).")
        return folders

    def delete_folder(self, folder_id: str) -> dict[str, Any]:
        """Delete a folder by OID.

        Sends ``DELETE /api/v1/folders/{folder_id}``. The folder must be empty
        or otherwise deletable per Sisense server rules.

        Parameters
        ----------
        folder_id : str
            OID of the folder to delete.

        Returns
        -------
        dict[str, Any]
            ``{"message": "..."}`` on success (HTTP 204), or ``{"error": "..."}``
            on failure.
        """
        endpoint = f"/api/v1/folders/{folder_id}"
        self.logger.debug(f"Deleting folder with ID: {folder_id}")
        response = self.api_client.delete(endpoint)

        if response is None:
            self.logger.error(f"DELETE request to delete folder {folder_id} failed: No response received.")
            return {"error": f"No response received while deleting folder ID '{folder_id}'"}

        if response.status_code != 204:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to delete folder {folder_id}. Error: {error_message}")
            return {"error": f"Failed to delete folder '{folder_id}'. {error_message}"}

        self.logger.info(f"Successfully deleted folder with ID {folder_id}.")
        return {"message": f"Folder with ID '{folder_id}' deleted successfully."}
