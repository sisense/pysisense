from ..access_management import AccessManagement
from ..sisenseclient import SisenseClient
from .columns import ColumnsMixin
from .core import DashboardCoreMixin
from .scripts import ScriptsMixin
from .shares import SharesMixin
from .widgets import DashboardWidgetsMixin


class Dashboard(DashboardCoreMixin, SharesMixin, ColumnsMixin, ScriptsMixin, DashboardWidgetsMixin):
    """Manage Sisense dashboards, their shares, columns, scripts, and widgets.

    Covers dashboard CRUD (get, export, rename, move, publish), share
    management for users and groups, inspection of all table/column
    references used within a dashboard, reading or writing JavaScript
    scripts attached to dashboards and individual widgets, and direct
    widget read/write operations.

    Modules
    -------
    core :
        Dashboard CRUD and lifecycle — get all dashboards, find by ID or title,
        export, import, rename, move to folder, publish, delete (id and exact
        title must both match); resolve dashboard references; find every
        dashboard that uses a data model, including through a single widget;
        duplicate a dashboard with a marker in the copy's title; change the datasource a
        dashboard queries, e.g. from a data model to a perspective built over it.
    shares :
        Dashboard share management — add or update share entries for users
        and groups; retrieve current shares; change dashboard owner.
    columns :
        Column inspection — extract every distinct table/column reference a
        dashboard uses: dashboard and default filters, drill hierarchies,
        widget panels (including nested formulas, conditional formatting and
        drill chains), drill history, widget query metadata and table headers.
    scripts :
        Dashboard and widget scripts — read and write JavaScript attached
        to a dashboard or to a specific widget within it.
    widgets :
        Widget operations — fetch a single widget by ID, write widget data
        back to Sisense, and search for widgets by type across dashboards.
    """

    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """
        Initializes the Dashboard class, managing API interactions for dashboards.

        If no Sisense client is provided, a new SisenseClient is created.

        Parameters:
            api_client (SisenseClient, optional): An existing SisenseClient instance.
                If None, a new SisenseClient is created.
            debug (bool, optional): Enables debug logging if True. Default is False.
        """
        # Use provided Sisense client or create a new one
        self.api_client = api_client if api_client else SisenseClient(debug=debug)

        # Initialize AccessManagement for user and group management
        self.access_mgmt = AccessManagement(self.api_client, debug=debug)

        # Use the logger from the SisenseClient instance
        self.logger = self.api_client.logger
        self.logger.debug("Dashboard class initialized.")
