from __future__ import annotations

from ..sisenseclient import SisenseClient
from .core import QueriesCoreMixin


class Queries(QueriesCoreMixin):
    """Execute custom JAQL and SQL queries against Sisense datasources.

    Sends caller-supplied query payloads directly to the elasticube or live
    datasource engine. Requires the caller to already know the internal table
    and column names — not for discovering schema, listing measures, or
    retrieving row counts. Use DataModel for row counts per table, and
    Metadata for browsing the semantic layer (measures, dimensions).

    Modules
    -------
    core :
        Query execution — run a JAQL query against an elasticube
        (``elasticube_run_jaql_query``), run a JAQL query and return
        results as CSV (``elasticubes_run_jaql_csv``).
    """

    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """Initialize the Queries client for Sisense JAQL and SQL query APIs.

        Parameters
        ----------
        api_client : SisenseClient, optional
            An existing Sisense client. When omitted, a new client is created.
        debug : bool, optional
            Enable debug logging on a newly created client. Default is False.
        """
        self.api_client = api_client if api_client else SisenseClient(debug=debug)
        self.logger = self.api_client.logger
        self.logger.debug("Queries class initialized.")
