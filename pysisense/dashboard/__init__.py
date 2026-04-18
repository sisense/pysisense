from ..access_management import AccessManagement
from ..sisenseclient import SisenseClient
from .columns import ColumnsMixin
from .core import DashboardCoreMixin
from .scripts import ScriptsMixin
from .shares import SharesMixin


class Dashboard(DashboardCoreMixin, SharesMixin, ColumnsMixin, ScriptsMixin):
    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """
        Initializes the Dashboard class, managing API interactions for dashboards.

        If no Sisense client is provided, a new SisenseClient is created.

        Parameters:
            api_client (SisenseClient, optional): An existing SisenseClient instance.
                If None, a new SisenseClient is created.
            debug (bool, optional): Enables debug logging if True. Default is False.
        """
        # Use provided Sisense client or create a new one
        self.api_client = api_client if api_client else SisenseClient(debug=debug)

        # Initialize AccessManagement for user and group management
        self.access_mgmt = AccessManagement(self.api_client, debug=debug)

        # Use the logger from the SisenseClient instance
        self.logger = self.api_client.logger
        self.logger.debug("Dashboard class initialized.")
