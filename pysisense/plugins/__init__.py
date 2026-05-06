from ..sisenseclient import SisenseClient
from .core import PluginsCoreMixin
from .snapshots import PluginsSnapshotsMixin


class Plugins(PluginsCoreMixin, PluginsSnapshotsMixin):
    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """Initialize the Plugins class for managing Sisense plugin states.

        If no Sisense client is provided, a new SisenseClient is created using
        the default ``config.yaml``.

        Parameters
        ----------
        api_client : SisenseClient, optional
            An existing SisenseClient instance. If ``None``, a new client is
            created.
        debug : bool, optional
            Enables debug-level logging when ``True``. Default is ``False``.
        """
        self.api_client = api_client if api_client else SisenseClient(debug=debug)
        self.logger = self.api_client.logger
        self.logger.debug("Plugins class initialized.")
