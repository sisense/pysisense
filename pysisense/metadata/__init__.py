from __future__ import annotations

from ..sisenseclient import SisenseClient
from .core import MetadataCoreMixin


class Metadata(MetadataCoreMixin):
    """Retrieve datasource metadata — measures, dimensions, and field definitions.

    Provides read-only access to the semantic layer of a Sisense datasource:
    saved formula measures, dimension definitions, the list of available
    datasources, and the ability to run raw metadata queries. Does not
    build or modify data model schemas — use the DataModel class for that.

    Modules
    -------
    core :
        Datasource metadata — retrieve saved measures, retrieve dimensions,
        list available datasources, add a measure, run a metadata query.
    """

    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """Initialize the Metadata client for Sisense metadata API operations.

        Parameters
        ----------
        api_client : SisenseClient, optional
            An existing Sisense client. When omitted, a new client is created.
        debug : bool, optional
            Enable debug logging on a newly created client. Default is False.
        """
        self.api_client = api_client if api_client else SisenseClient(debug=debug)
        self.logger = self.api_client.logger
        self.logger.debug("Metadata class initialized.")
