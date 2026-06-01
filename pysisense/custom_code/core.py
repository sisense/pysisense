from __future__ import annotations

from typing import Any

# Sisense notebook create/update require the Internal header (win2linux migration).
_INTERNAL_HEADER = {"Internal": "true"}


class CustomCodeCoreMixin:
    def get_notebooks(
        self,
        notebook_type: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve notebooks from Sisense.

        Sends ``GET /api/v1/notebooks`` with optional query parameters.

        Parameters
        ----------
        notebook_type : str, optional
            When provided, sets ``notebookType`` query parameter (for example
            ``CustomCodeTransformation``).
        params : dict[str, Any], optional
            Additional query parameters merged with ``notebookType`` when set.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            Notebook list from the API, or ``{"error": "..."}`` on failure.
        """
        query: dict[str, Any] = dict(params or {})
        if notebook_type is not None:
            query["notebookType"] = notebook_type

        endpoint = "/api/v1/notebooks"
        self.logger.debug(f"GET {endpoint}")
        response = self.api_client.get(endpoint, params=query or None)

        if response is None:
            return {"error": "No response received while fetching notebooks."}

        if not response.ok:
            try:
                detail = response.json()
            except Exception:
                detail = response.text if response else "No response text available."
            self.logger.error(f"Failed to fetch notebooks. Error: {detail}")
            return {"error": f"Failed to fetch notebooks. {detail}"}

        try:
            result = response.json()
        except Exception:
            result = {}

        self.logger.info("Successfully fetched notebooks.")
        return result

    def export_notebook(self, notebook_id: str) -> dict[str, Any]:
        """Export a notebook definition.

        Sends ``GET /api/v1/notebooks/{notebook_id}/export``.

        Parameters
        ----------
        notebook_id : str
            Notebook UUID or identifier.

        Returns
        -------
        dict[str, Any]
            Exported notebook payload on success, or ``{"error": "..."}`` on
            failure.
        """
        endpoint = f"/api/v1/notebooks/{notebook_id}/export"
        self.logger.debug(f"GET {endpoint}")
        response = self.api_client.get(endpoint)

        if response is None:
            return {"error": f"No response received while fetching notebook export {notebook_id}."}

        if not response.ok:
            try:
                detail = response.json()
            except Exception:
                detail = response.text if response else "No response text available."
            self.logger.error(f"Failed to fetch notebook export {notebook_id}. Error: {detail}")
            return {"error": f"Failed to fetch notebook export {notebook_id}. {detail}"}

        try:
            result = response.json()
        except Exception:
            result = {}

        self.logger.info(f"Successfully fetched notebook export {notebook_id}.")
        return result

    def create_notebook(
        self,
        notebook_data: dict[str, Any],
        *,
        use_internal_header: bool = True,
    ) -> dict[str, Any]:
        """Create a new notebook.

        Sends ``POST /api/v1/notebooks``. By default includes the ``Internal``
        header required for programmatic notebook creation.

        Parameters
        ----------
        notebook_data : dict[str, Any]
            Notebook creation payload (for example ``notebookType``,
            ``displayName``, manifest fields).
        use_internal_header : bool, optional
            When ``True`` (default), send ``Internal: true`` header.

        Returns
        -------
        dict[str, Any]
            Created notebook object on success, or ``{"error": "..."}`` on
            failure.
        """
        if not isinstance(notebook_data, dict):
            return {"error": "notebook_data must be a dictionary."}

        endpoint = "/api/v1/notebooks"
        extra_headers = _INTERNAL_HEADER if use_internal_header else None
        self.logger.debug(f"POST {endpoint}")
        response = self.api_client.post(endpoint, data=notebook_data, extra_headers=extra_headers)

        if response is None:
            return {"error": "No response received while create notebook."}

        if response.status_code not in (200, 201):
            try:
                detail = response.json()
            except Exception:
                detail = response.text if response else "No response text available."
            self.logger.error(f"Failed to create notebook. Error: {detail}")
            return {"error": f"Failed to create notebook. {detail}"}

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info("Successfully completed create notebook.")
        return result

    def update_notebook(
        self,
        notebook_id: str,
        notebook_data: dict[str, Any],
        *,
        use_internal_header: bool = True,
    ) -> dict[str, Any]:
        """Update an existing notebook.

        Sends ``PATCH /api/v1/notebooks/{notebook_id}``. Only fields present in
        ``notebook_data`` are sent. By default includes the ``Internal`` header.

        Parameters
        ----------
        notebook_id : str
            Notebook UUID or identifier.
        notebook_data : dict[str, Any]
            Fields to update.
        use_internal_header : bool, optional
            When ``True`` (default), send ``Internal: true`` header.

        Returns
        -------
        dict[str, Any]
            Updated notebook object on success, or ``{"error": "..."}`` on
            failure.
        """
        if not notebook_data:
            return {"error": "notebook_data must contain at least one field to update."}

        endpoint = f"/api/v1/notebooks/{notebook_id}"
        extra_headers = _INTERNAL_HEADER if use_internal_header else None
        self.logger.debug(f"PATCH {endpoint}")
        response = self.api_client.patch(endpoint, data=notebook_data, extra_headers=extra_headers)

        if response is None:
            return {"error": f"No response received while update notebook {notebook_id}."}

        if response.status_code != 200:
            try:
                detail = response.json()
            except Exception:
                detail = response.text if response else "No response text available."
            self.logger.error(f"Failed to update notebook {notebook_id}. Error: {detail}")
            return {"error": f"Failed to update notebook {notebook_id}. {detail}"}

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info(f"Successfully completed update notebook {notebook_id}.")
        return result

    def delete_notebook(self, notebook_id: str) -> dict[str, Any]:
        """Delete a notebook by ID.

        Sends ``DELETE /api/v1/notebooks/{notebook_id}``.

        Parameters
        ----------
        notebook_id : str
            Notebook UUID or identifier.

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` on success (HTTP 204), or ``{"error": "..."}``
            on failure.
        """
        endpoint = f"/api/v1/notebooks/{notebook_id}"
        self.logger.debug(f"Deleting notebook {notebook_id}")
        response = self.api_client.delete(endpoint)

        if response is None:
            return {"error": f"No response received while deleting notebook '{notebook_id}'."}

        if response.status_code == 204:
            self.logger.info(f"Successfully deleted notebook {notebook_id}.")
            return {"success": True}

        if response.ok:
            try:
                return response.json()
            except Exception:
                return {"success": True}

        try:
            detail = response.json()
        except Exception:
            detail = response.text if response else "No response text available."
        self.logger.error(f"Failed to delete notebook '{notebook_id}'. Error: {detail}")
        return {"error": f"Failed to delete notebook '{notebook_id}'. {detail}"}

    def list_notebook_folder_contents(self, folder_id: str) -> dict[str, Any] | list[Any]:
        """List contents of a custom-code notebook folder.

        Sends ``GET /api/resources/notebooks/custom_code_notebooks/notebooks/{folder_id}/``.

        Parameters
        ----------
        folder_id : str
            Folder identifier.

        Returns
        -------
        dict[str, Any] | list[Any]
            Folder contents from the API, or ``{"error": "..."}`` on failure.
        """
        endpoint = f"/api/resources/notebooks/custom_code_notebooks/notebooks/{folder_id}/"
        self.logger.debug(f"GET {endpoint}")
        response = self.api_client.get(endpoint)

        if response is None:
            return {"error": f"No response received while fetching folder contents {folder_id}."}

        if not response.ok:
            try:
                detail = response.json()
            except Exception:
                detail = response.text if response else "No response text available."
            self.logger.error(f"Failed to fetch folder contents {folder_id}. Error: {detail}")
            return {"error": f"Failed to fetch folder contents {folder_id}. {detail}"}

        try:
            result = response.json()
        except Exception:
            result = {}

        self.logger.info(f"Successfully fetched folder contents {folder_id}.")
        return result

    def rename_notebook_file(self, resource_path: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Rename or update a notebook resource file.

        Sends ``PATCH /api/resources/{resource_path}``.

        Parameters
        ----------
        resource_path : str
            Resource path relative to ``/api/resources/`` (for example
            ``notebooks/custom_code_notebooks/.../file.ipynb``).
        payload : dict[str, Any]
            PATCH body with fields to update (for example new name/path).

        Returns
        -------
        dict[str, Any]
            API response on success, or ``{"error": "..."}`` on failure.
        """
        if not payload:
            return {"error": "payload must contain at least one field to update."}

        path = resource_path.lstrip("/")
        endpoint = f"/api/resources/{path}"
        self.logger.debug(f"PATCH {endpoint}")
        response = self.api_client.patch(endpoint, data=payload)

        if response is None:
            return {"error": f"No response received while rename resource {path}."}

        if response.status_code != 200:
            try:
                detail = response.json()
            except Exception:
                detail = response.text if response else "No response text available."
            self.logger.error(f"Failed to rename resource {path}. Error: {detail}")
            return {"error": f"Failed to rename resource {path}. {detail}"}

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info(f"Successfully completed rename resource {path}.")
        return result

    def rename_notebook_folder(self, old_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Rename a custom-code notebook folder.

        Sends ``PATCH /api/resources/notebooks/custom_code_notebooks/notebooks/{old_id}/``.

        Parameters
        ----------
        old_id : str
            Current folder identifier.
        payload : dict[str, Any]
            PATCH body (for example new folder name or target id).

        Returns
        -------
        dict[str, Any]
            API response on success, or ``{"error": "..."}`` on failure.
        """
        if not payload:
            return {"error": "payload must contain at least one field to update."}

        endpoint = f"/api/resources/notebooks/custom_code_notebooks/notebooks/{old_id}/"
        self.logger.debug(f"PATCH {endpoint}")
        response = self.api_client.patch(endpoint, data=payload)

        if response is None:
            return {"error": f"No response received while rename notebook folder {old_id}."}

        if response.status_code != 200:
            try:
                detail = response.json()
            except Exception:
                detail = response.text if response else "No response text available."
            self.logger.error(f"Failed to rename notebook folder {old_id}. Error: {detail}")
            return {"error": f"Failed to rename notebook folder {old_id}. {detail}"}

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info(f"Successfully completed rename notebook folder {old_id}.")
        return result
