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
