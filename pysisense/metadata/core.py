from __future__ import annotations

from typing import Any


class MetadataCoreMixin:
    def get_datasource_measures(
        self,
        datasource: str | None = None,
        ds_full_name: str | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve saved formula measures for a datasource.

        Sends ``GET /api/metadata/measures`` with optional ``datasource`` and
        ``dsFullName`` query parameters.

        Parameters
        ----------
        datasource : str, optional
            Datasource identifier (for example datamodel title).
        ds_full_name : str, optional
            Full datasource name (for example ``localhost/MyModel``).

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            Measures payload from the API (typically a list), or
            ``{"error": "..."}`` on failure.
        """
        params = self._datasource_query_params(datasource, ds_full_name)
        return self._metadata_get("/api/metadata/measures", params=params, context="measures")

    def get_datasource_dimensions(
        self,
        datasource: str | None = None,
        ds_full_name: str | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve saved filter dimensions for a datasource.

        Sends ``GET /api/metadata/dimensions`` with optional ``datasource`` and
        ``dsFullName`` query parameters.

        Parameters
        ----------
        datasource : str, optional
            Datasource identifier (for example datamodel title).
        ds_full_name : str, optional
            Full datasource name (for example ``localhost/MyModel``).

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            Dimensions payload from the API (typically a list), or
            ``{"error": "..."}`` on failure.
        """
        params = self._datasource_query_params(datasource, ds_full_name)
        return self._metadata_get("/api/metadata/dimensions", params=params, context="dimensions")

    def get_datasources(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve all datasources visible to the authenticated user.

        Sends ``GET /api/datasources``.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            Datasource list from the API, or ``{"error": "..."}`` on failure.
        """
        return self._metadata_get("/api/datasources", params=None, context="datasources")

    def add_datasource_measure(self, measure: dict[str, Any]) -> dict[str, Any]:
        """Create a saved formula measure in Sisense metadata.

        Sends ``POST /api/metadata/`` with the measure definition payload.

        Parameters
        ----------
        measure : dict[str, Any]
            Measure object in Sisense metadata format (for example datasource,
            table, column, expression, and related fields).

        Returns
        -------
        dict[str, Any]
            Created measure object on success, or ``{"error": "..."}`` on
            failure.
        """
        if not isinstance(measure, dict):
            self.logger.error("add_datasource_measure requires measure to be a dict.")
            return {"error": "measure must be a dictionary."}

        return self._metadata_post("/api/metadata/", measure, context="add measure")

    def post_metadata_query(self, query_payload: dict[str, Any]) -> dict[str, Any]:
        """Execute a metadata query against Sisense.

        Sends ``POST /api/metadata`` with the query payload.

        Parameters
        ----------
        query_payload : dict[str, Any]
            Metadata query body as required by the Sisense API.

        Returns
        -------
        dict[str, Any]
            Query result from the API, or ``{"error": "..."}`` on failure.
        """
        if not isinstance(query_payload, dict):
            self.logger.error("post_metadata_query requires query_payload to be a dict.")
            return {"error": "query_payload must be a dictionary."}

        return self._metadata_post("/api/metadata", query_payload, context="metadata query")

    def _datasource_query_params(
        self,
        datasource: str | None,
        ds_full_name: str | None,
    ) -> dict[str, str] | None:
        params: dict[str, str] = {}
        if datasource is not None:
            params["datasource"] = datasource
        if ds_full_name is not None:
            params["dsFullName"] = ds_full_name
        return params or None

    def _metadata_get(
        self,
        endpoint: str,
        params: dict[str, str] | None,
        *,
        context: str,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        self.logger.debug(f"GET {endpoint} — context={context!r}, params={params}")
        response = self.api_client.get(endpoint, params=params)

        if response is None:
            self.logger.error(f"GET {endpoint} failed: No response received.")
            return {"error": f"No response received while fetching {context}."}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"GET {endpoint} failed. Error: {error_message}")
            return {"error": f"Failed to fetch {context}. {error_message}"}

        result = response.json()
        count = len(result) if isinstance(result, list) else 1
        self.logger.info(f"Successfully fetched {context} (count={count}).")
        return result

    def _metadata_post(
        self,
        endpoint: str,
        payload: dict[str, Any],
        *,
        context: str,
    ) -> dict[str, Any]:
        self.logger.debug(f"POST {endpoint} — context={context!r}")
        response = self.api_client.post(endpoint, data=payload)

        if response is None:
            self.logger.error(f"POST {endpoint} failed: No response received.")
            return {"error": f"No response received while posting {context}."}

        if response.status_code not in (200, 201):
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text if response else "No response text available."
            self.logger.error(f"POST {endpoint} failed. Error: {error_message}")
            return {"error": f"Failed to post {context}. {error_message}"}

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info(f"Successfully posted {context}.")
        return result
