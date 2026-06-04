from __future__ import annotations

from ..sisenseclient import SisenseClient
from .core import EncryptionCoreMixin


class Encryption(EncryptionCoreMixin):
    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """Initialize the Encryption client for Sisense encryption API operations.

        Parameters
        ----------
        api_client : SisenseClient, optional
            An existing Sisense client. When omitted, a new client is created.
        debug : bool, optional
            Enable debug logging on a newly created client. Default is False.
        """
        self.api_client = api_client if api_client else SisenseClient(debug=debug)
        self.logger = self.api_client.logger
        self.logger.debug("Encryption class initialized.")
