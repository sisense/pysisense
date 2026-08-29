from ..sisenseclient import SisenseClient
from .core import ReportManagerCoreMixin


class ReportManager(ReportManagerCoreMixin):
    """Manage scheduled reports through the Report Manager plugin.

    Report Manager is an on-demand Sisense plugin and is not guaranteed to
    be installed or enabled on every instance. All methods return
    ``{"error": "..."}`` rather than raising when the plugin's endpoints are
    unavailable, so callers can detect and handle that case directly.

    Modules
    -------
    core :
        CRUD operations on scheduled reports (list, get, create, update,
        delete) and triggering an on-demand run of a report.
    """

    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """Initialize the ReportManager class for managing scheduled reports.

        If no Sisense client is provided, a new SisenseClient is created
        using the default ``config.yaml``.

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
        self.logger.debug("ReportManager class initialized.")
