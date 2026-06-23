from ..sisenseclient import SisenseClient
from .core import BloxCoreMixin


class Blox(BloxCoreMixin):
    """Manage custom Blox actions on a Sisense instance.

    Provides read, write, and delete access to Blox custom actions. Fetch
    operations are supported on both Linux and Windows deployments via
    OS-routed endpoints; save and delete are Linux-only.

    Modules
    -------
    core :
        Custom Blox actions — retrieve all actions (OS-routed), save a new
        action (Linux), and delete an existing action (Linux).
    """

    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """Initialize the Blox class for managing custom Blox actions.

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
        self.logger.debug("Blox class initialized.")
