from ..sisenseclient import SisenseClient
from .build import BuildMixin
from .connections import ConnectionsMixin
from .core import DataModelCoreMixin
from .data import DataMixin
from .security import SecurityMixin
from .shares import SharesMixin


class DataModel(
    DataModelCoreMixin,
    ConnectionsMixin,
    BuildMixin,
    SecurityMixin,
    SharesMixin,
    DataMixin,
):
    """Manage Sisense data models, connections, builds, security, and data.

    Covers the full data model lifecycle — schema retrieval, provisioning
    new models and tables, connection management, row-level security rules,
    user/group share assignments, and direct table-level data access via SQL.

    Modules
    -------
    core :
        Data model schema — get, list, describe, load, and delete data
        models; resolve by ID or title; list elasticubes.
    connections :
        Connection management — get, update, and create data source
        connections; generate connection payloads for supported providers.
    build :
        Schema provisioning — create data models, datasets, and tables;
        deploy a data model with configurable build behavior.
    security :
        Row-level security — get, update, and bulk-add datasecurity rules
        that restrict data access per user or group.
    shares :
        Data model access — get and add share entries (users and groups)
        with READ, USE, or EDIT permission levels.
    data :
        Data retrieval — query table contents via SQL; get row counts for
        a table within a data model.
    """

    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """
        Initializes the DataModel class.

        If no Sisense client is provided, a new SisenseClient is created.

        Parameters:
            api_client (SisenseClient, optional): An existing SisenseClient instance.
                If None, a new SisenseClient is created.
            debug (bool, optional): Enables debug logging if True. Default is False.
        """
        # Use provided API client or create a new one
        self.api_client = api_client if api_client else SisenseClient(debug=debug)

        # Use the logger from the SisenseClient instance
        self.logger = self.api_client.logger
        self.logger.debug("DataModel class initialized.")
