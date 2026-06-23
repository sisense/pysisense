from __future__ import annotations

from ..sisenseclient import SisenseClient
from .core import EncryptionCoreMixin


class Encryption(EncryptionCoreMixin):
    """Encrypt and decrypt Sisense connection parameter values.

    Wraps the Sisense encryption service for preparing or recovering
    connection credentials (such as passwords and keys) used during
    cross-server data model migration.

    Modules
    -------
    core :
        Encryption operations — encrypt a plaintext value via the Sisense
        encryption API; decrypt a previously encrypted value.
    """

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
