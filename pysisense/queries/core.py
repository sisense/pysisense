from __future__ import annotations

from typing import Any


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
        if not isinstance(jaql_payload, dict):
            return {"error": "jaql_payload must be a dictionary."}

        endpoint = f"/api/datasources/{datasource_name}/jaql"
        return self._queries_post(endpoint, jaql_payload, context=f"JAQL query on '{datasource_name}'")

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
        if not isinstance(jaql_payload, dict):
            return {"error": "jaql_payload must be a dictionary."}

        endpoint = f"/api/datasources/{datasource_name}/jaql/csv"
        return self._queries_post(
            endpoint,
            jaql_payload,
            context=f"JAQL CSV query on '{datasource_name}'",
            allow_text=True,
        )

    def elasticube_run_sql_query(
        self,
        elasticube_name: str,
        sql_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Run a SQL query against an elasticube.

        Sends ``POST /api/elasticubes/{elasticube_name}/Sql``.

        Parameters
        ----------
        elasticube_name : str
            Elasticube / datamodel name.
        sql_payload : dict[str, Any]
            SQL request body (for example ``{"query": "SELECT ..."}`` or fields
            required by your Sisense version).

        Returns
        -------
        dict[str, Any]
            Query result from the API, or ``{"error": "..."}`` on failure.
        """
        if not isinstance(sql_payload, dict):
            return {"error": "sql_payload must be a dictionary."}

        endpoint = f"/api/elasticubes/{elasticube_name}/Sql"
        return self._queries_post(endpoint, sql_payload, context=f"SQL query on '{elasticube_name}'")

    def _queries_post(
        self,
        endpoint: str,
        payload: dict[str, Any],
        *,
        context: str,
        allow_text: bool = False,
    ) -> dict[str, Any] | str:
        self.logger.debug(f"POST {endpoint} — context={context!r}")
        response = self.api_client.post(endpoint, data=payload)

        if response is None:
            self.logger.error(f"POST {endpoint} failed: No response received.")
            return {"error": f"No response received while running {context}."}

        if not response.ok:
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text if response else "No response text available."
            self.logger.error(f"POST {endpoint} failed. Error: {error_message}")
            return {"error": f"Failed to run {context}. {error_message}"}

        try:
            result = response.json()
        except Exception:
            result = None

        if allow_text and (result is None or not isinstance(result, dict | list)):
            text = response.text if response else ""
            if text or result is not None:
                self.logger.info(f"Successfully completed {context} (text/csv response).")
                return text if text else str(result)

        if result is None:
            result = {"success": True}

        self.logger.info(f"Successfully completed {context}.")
        return result
