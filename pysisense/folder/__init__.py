from __future__ import annotations

from ..sisenseclient import SisenseClient
from .core import FolderCoreMixin


class Folder(FolderCoreMixin):
    """Manage the Sisense folder hierarchy and individual folder records.

    Covers folder CRUD (create, update, delete), lookup by name or ID,
    navigation of the full folder tree, and retrieval of a folder's
    ancestor chain. Does not manage dashboard content inside folders —
    use the Dashboard class for that.

    Modules
    -------
    core :
        Folder operations — create, update, and delete folders; look up a
        folder ID by name; retrieve the flat or tree folder structure;
        get a folder's ancestor chain.
    """

    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """Initialize the Folder client for Sisense folder API operations.

        Parameters
        ----------
        api_client : SisenseClient, optional
            An existing Sisense client. When omitted, a new client is created.
        debug : bool, optional
            Enable debug logging on a newly created client. Default is False.
        """
        self.api_client = api_client if api_client else SisenseClient(debug=debug)
        self.logger = self.api_client.logger
        self.logger.debug("Folder class initialized.")
