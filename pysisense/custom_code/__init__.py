from __future__ import annotations

from ..sisenseclient import SisenseClient
from .core import CustomCodeCoreMixin


class CustomCode(CustomCodeCoreMixin):
    """Manage Sisense custom-code notebooks and their file system layout.

    Covers the full notebook lifecycle — create, retrieve, update, export,
    and delete notebooks — as well as renaming notebook files and folders
    within the Sisense custom-code directory structure.

    Modules
    -------
    core :
        Notebook lifecycle — get, create, update, delete, and export
        notebooks; list folder contents; rename notebook files and folders.
    """

    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """Initialize the CustomCode client for Sisense notebook API operations.

        Parameters
        ----------
        api_client : SisenseClient, optional
            An existing Sisense client. When omitted, a new client is created.
        debug : bool, optional
            Enable debug logging on a newly created client. Default is False.
        """
        self.api_client = api_client if api_client else SisenseClient(debug=debug)
        self.logger = self.api_client.logger
        self.logger.debug("CustomCode class initialized.")
