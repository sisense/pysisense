from __future__ import annotations

from typing import Any

from ..utils import _extract_error_message


class QueriesCoreMixin:
    def elasticube_run_jaql_query(
        self,
        datasource_name: str,
        jaql_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Run a JAQL query against a datasource (elasticube).

        Sends ``POST /api/datasources/{datasource_name}/jaql``. Use for
        validation-tab style queries in Sisense.

        Parameters
        ----------
        datasource_name : str
            Datasource / elasticube name.
        jaql_payload : dict[str, Any]
            JAQL query body (for example ``metadata``, ``datasource``, and
            related fields as required by the API).

        Returns
        -------
        dict[str, Any]
            Query result from the API, or ``{"error": "..."}`` on failure.
        """
        endpoint = f"/api/datasources/{datasource_name}/jaql"
        context = f"JAQL query on '{datasource_name}'"
        self.logger.debug(f"POST {endpoint} — context={context!r}")
        response = self.api_client.post(endpoint, data=jaql_payload)

        if response is None or not response.ok:
            failure = _extract_error_message(response, f"Failed to run {context}", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info(f"Successfully completed {context}.")
        return result

    def elasticubes_run_jaql_csv(
        self,
        datasource_name: str,
        jaql_payload: dict[str, Any],
    ) -> dict[str, Any] | str:
        """Run a JAQL query and return CSV output.

        Sends ``POST /api/datasources/{datasource_name}/jaql/csv``.

        Parameters
        ----------
        datasource_name : str
            Datasource / elasticube name.
        jaql_payload : dict[str, Any]
            JAQL query body.

        Returns
        -------
        dict[str, Any] | str
            Parsed JSON if the response is JSON, raw CSV text if the response
            is not JSON, or ``{"error": "..."}`` on failure.
        """
        endpoint = f"/api/datasources/{datasource_name}/jaql/csv"
        context = f"JAQL CSV query on '{datasource_name}'"
        self.logger.debug(f"POST {endpoint} — context={context!r}")
        response = self.api_client.post(endpoint, data=jaql_payload)

        if response is None or not response.ok:
            failure = _extract_error_message(response, f"Failed to run {context}", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            result = response.json()
        except Exception:
            result = None

        if result is None or not isinstance(result, dict | list):
            text = response.text if response else ""
            if text or result is not None:
                self.logger.info(f"Successfully completed {context} (text/csv response).")
                return text if text else str(result)

        self.logger.info(f"Successfully completed {context}.")
        return result
