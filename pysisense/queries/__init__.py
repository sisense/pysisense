from __future__ import annotations

from ..sisenseclient import SisenseClient
from .core import QueriesCoreMixin


class Queries(QueriesCoreMixin):
    """Execute JAQL and SQL queries against Sisense datasources.

    Provides direct query execution against elasticubes and live datasources.
    Use for ad-hoc data retrieval and validation queries; for browsing the
    semantic layer (measures, dimensions) use the Metadata class instead.

    Modules
    -------
    core :
        Query execution — run a JAQL query against an elasticube, run a
        JAQL query and return results as CSV, run a SQL query against a
        datasource.
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
