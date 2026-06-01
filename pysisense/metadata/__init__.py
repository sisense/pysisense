from __future__ import annotations

from ..sisenseclient import SisenseClient
from .core import MetadataCoreMixin


class Metadata(MetadataCoreMixin):
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
