from __future__ import annotations

from typing import Any

from ..payloads import MeasurePayload
from ..utils import _extract_error_message


class MetadataCoreMixin:
    def _fetch_metadata_list(
        self,
        kind: str,
        datasource: str | None,
        ds_full_name: str | None,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Fetch a metadata list (measures or dimensions) for a datasource.

        Shared by ``get_datasource_measures`` and ``get_datasource_dimensions``,
        which differ only in the endpoint suffix.

        Parameters
        ----------
        kind : str
            ``"measures"`` or ``"dimensions"`` — selects
            ``GET /api/metadata/{kind}`` and is used in log/error messages.
        datasource : str, optional
            Datasource identifier (for example datamodel title).
        ds_full_name : str, optional
            Full datasource name (for example ``localhost/MyModel``).

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            Payload from the API (typically a list), or ``{"error": "..."}``
            on failure.
        """
        params: dict[str, str] = {}
        if datasource is not None:
            params["datasource"] = datasource
        if ds_full_name is not None:
            params["dsFullName"] = ds_full_name

        endpoint = f"/api/metadata/{kind}"
        self.logger.debug(f"GET {endpoint} — params={params or None}")
        response = self.api_client.get(endpoint, params=params or None)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to fetch {kind}", self.api_client)
            self.logger.error(failure["error"])
            return failure

        result = response.json()
        count = len(result) if isinstance(result, list) else 1
        self.logger.info(f"Successfully fetched {kind} (count={count}).")
        return result

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
        return self._fetch_metadata_list("measures", datasource, ds_full_name)

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
        return self._fetch_metadata_list("dimensions", datasource, ds_full_name)

    def get_datasources(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve all datasources visible to the authenticated user.

        Sends ``GET /api/datasources``.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            Datasource list from the API, or ``{"error": "..."}`` on failure.
        """
        endpoint = "/api/datasources"
        self.logger.debug(f"GET {endpoint}")
        response = self.api_client.get(endpoint)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, "Failed to fetch datasources", self.api_client)
            self.logger.error(failure["error"])
            return failure

        result = response.json()
        count = len(result) if isinstance(result, list) else 1
        self.logger.info(f"Successfully fetched datasources (count={count}).")
        return result

    def add_datasource_measure(self, measure: MeasurePayload) -> dict[str, Any]:
        """Create a saved formula measure in Sisense metadata.

        Sends ``POST /api/metadata/`` with the measure definition payload.

        Parameters
        ----------
        measure : MeasurePayload
            Measure object in Sisense metadata format. ``title`` and
            ``datasource`` (``{"title": ..., "fullname": ...}``) are required;
            additional Sisense metadata fields (expression, context,
            table/column references) may be included and are passed through
            unchanged.

        Returns
        -------
        dict[str, Any]
            Created measure object on success, or ``{"error": "..."}`` on
            failure.
        """
        if not isinstance(measure, dict):
            self.logger.error("add_datasource_measure requires measure to be a dict.")
            return {"ok": False, "error": "measure must be a dictionary."}

        endpoint = "/api/metadata/"
        self.logger.debug(f"POST {endpoint}")
        response = self.api_client.post(endpoint, data=measure)

        if response is None or response.status_code not in (200, 201):
            failure = _extract_error_message(response, "Failed to post add measure", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info("Successfully posted add measure.")
        return result

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
            return {"ok": False, "error": "query_payload must be a dictionary."}

        endpoint = "/api/metadata"
        self.logger.debug(f"POST {endpoint}")
        response = self.api_client.post(endpoint, data=query_payload)

        if response is None or response.status_code not in (200, 201):
            failure = _extract_error_message(response, "Failed to post metadata query", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info("Successfully posted metadata query.")
        return result
