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
    """Manage Sisense users, groups, permissions, and access control.

    Covers user lifecycle (create, update, deactivate), group membership,
    role assignment, folder and dashboard ownership transfer, column-level
    security restrictions, and administrative reporting on dashboard shares
    and scheduled builds.

    Modules
    -------
    users :
        User CRUD — get, create, update, and deactivate users; resolve by
        email or ID; map role and group names to internal IDs.
    groups :
        Group membership — list groups by name, list all members per group.
    columns :
        Column-level data model security — list all columns in a data model,
        identify unused columns across one or more data models.
    ownership :
        Transfer folder and dashboard ownership — reassign an entire folder
        tree (including subfolders and dashboards) to a new owner.
    admin :
        Administrative reporting — retrieve share entries for every dashboard
        on the instance; schedule elasticube builds.
    """

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
