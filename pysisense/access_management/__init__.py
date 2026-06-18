from ..datamodel import DataModel
from ..sisenseclient import SisenseClient
from .admin import AdminMixin
from .columns import ColumnsMixin
from .groups import GroupsMixin
from .ownership import OwnershipMixin
from .users import UsersMixin


class AccessManagement(
    UsersMixin,
    GroupsMixin,
    ColumnsMixin,
    OwnershipMixin,
    AdminMixin,
):
    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """
        Initializes the AccessManagement class.
        If no Sisense client is provided, creates a SisenseClient internally.

        Parameters:
            api_client (SisenseClient, optional):
            Existing SisenseClient or None.
            debug (bool, optional): Enables debug logging if True.
        """
        # Use provided API client or create a new one
        if api_client is not None:
            self.api_client = api_client
        else:
            self.api_client = SisenseClient(debug=debug)

        self.datamodel = DataModel(api_client=self.api_client, debug=debug)

        # Use the logger from the APIClient instance
        self.logger = self.api_client.logger
        self.logger.debug("AccessManagement class initialized.")
