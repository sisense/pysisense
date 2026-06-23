from ..sisenseclient import SisenseClient
from .core import PluginsCoreMixin
from .snapshots import PluginsSnapshotsMixin


class Plugins(PluginsCoreMixin, PluginsSnapshotsMixin):
    """Manage Sisense plugin installation state and enable/disable lifecycle.

    Covers listing installed plugins, toggling individual plugins or batches
    of plugins, and capturing or restoring a full plugin state snapshot.
    Does not install or remove plugin packages from the filesystem.

    Modules
    -------
    core :
        Plugin lifecycle — list all plugins, get a single plugin by name,
        enable or disable a plugin individually, enable or disable in bulk.
    snapshots :
        Plugin state snapshots — capture the current enabled/disabled state
        of all plugins; restore a previously captured snapshot.
    """

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
