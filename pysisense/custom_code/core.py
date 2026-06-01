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
        return self._notebook_get("/api/v1/notebooks", params=query or None, context="notebooks")

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
        return self._notebook_get(endpoint, params=None, context=f"notebook export {notebook_id}")

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

        extra_headers = _INTERNAL_HEADER if use_internal_header else None
        return self._notebook_write(
            "POST",
            "/api/v1/notebooks",
            notebook_data,
            extra_headers=extra_headers,
            context="create notebook",
            success_codes=(200, 201),
        )

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

        extra_headers = _INTERNAL_HEADER if use_internal_header else None
        return self._notebook_write(
            "PATCH",
            f"/api/v1/notebooks/{notebook_id}",
            notebook_data,
            extra_headers=extra_headers,
            context=f"update notebook {notebook_id}",
            success_codes=(200,),
        )

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
            return self._response_json(response, default={"success": True})

        return self._error_from_response(response, f"Failed to delete notebook '{notebook_id}'")

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
        return self._notebook_get(endpoint, params=None, context=f"folder contents {folder_id}")

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
        return self._notebook_write(
            "PATCH",
            f"/api/resources/{path}",
            payload,
            extra_headers=None,
            context=f"rename resource {path}",
            success_codes=(200,),
        )

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
        return self._notebook_write(
            "PATCH",
            endpoint,
            payload,
            extra_headers=None,
            context=f"rename notebook folder {old_id}",
            success_codes=(200,),
        )

    def _notebook_get(
        self,
        endpoint: str,
        params: dict[str, Any] | None,
        *,
        context: str,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        self.logger.debug(f"GET {endpoint} — context={context!r}")
        response = self.api_client.get(endpoint, params=params)

        if response is None:
            return {"error": f"No response received while fetching {context}."}

        if not response.ok:
            return self._error_from_response(response, f"Failed to fetch {context}")

        result = self._response_json(response)
        self.logger.info(f"Successfully fetched {context}.")
        return result

    def _notebook_write(
        self,
        method: str,
        endpoint: str,
        payload: dict[str, Any],
        *,
        extra_headers: dict[str, str] | None,
        context: str,
        success_codes: tuple[int, ...],
    ) -> dict[str, Any]:
        self.logger.debug(f"{method} {endpoint} — context={context!r}")

        if method == "POST":
            response = self.api_client.post(endpoint, data=payload, extra_headers=extra_headers)
        elif method == "PATCH":
            response = self.api_client.patch(endpoint, data=payload, extra_headers=extra_headers)
        else:
            return {"error": f"Unsupported method '{method}'."}

        if response is None:
            return {"error": f"No response received while {context}."}

        if response.status_code not in success_codes:
            return self._error_from_response(response, f"Failed to {context}")

        result = self._response_json(response, default={"success": True})
        self.logger.info(f"Successfully completed {context}.")
        return result

    def _response_json(self, response: Any, default: dict[str, Any] | None = None) -> Any:
        try:
            return response.json()
        except Exception:
            return default if default is not None else {}

    def _error_from_response(self, response: Any, message: str) -> dict[str, Any]:
        try:
            detail = response.json()
        except Exception:
            detail = response.text if response else "No response text available."
        self.logger.error(f"{message}. Error: {detail}")
        return {"error": f"{message}. {detail}"}
