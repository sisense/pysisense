__version__ = "1.1.0"

# Core classes
from .access_management import AccessManagement
from .blox import Blox
from .custom_code import CustomCode
from .dashboard import Dashboard
from .datamodel import DataModel
from .encryption import Encryption
from .folder import Folder
from .mergetool import MergeTool
from .metadata import Metadata
from .migration import Migration

# Payload contracts (TypedDicts for dict parameters)
from .payloads import (
    AthenaConnectionParams,
    BigQueryConnectionParams,
    ConnectionPayload,
    ConnectionUpdatePayload,
    CreateUserPayload,
    DataBricksConnectionParams,
    DatasourceRef,
    MeasurePayload,
    NotebookCreatePayload,
    NotebookUpdatePayload,
    PluginSnapshot,
    RedShiftConnectionParams,
    UpdateUserPayload,
)
from .plugins import Plugins
from .queries import Queries
from .report_manager import ReportManager
from .sisenseclient import SisenseClient

# Utilities
from .utils import convert_to_dataframe, convert_utc_to_local, export_to_csv
from .wellcheck import WellCheck

# Tool-bearing facade classes — the classes whose public methods form the
# SDK's operational surface. Downstream tool-schema generators should iterate
# this registry (not __all__, which also carries TypedDict payload contracts
# and utility functions) when discovering methods to expose.
# SisenseClient is intentionally excluded: it is the shared HTTP/auth client,
# not an operation facade.
FACADES: tuple[type, ...] = (
    AccessManagement,
    Blox,
    CustomCode,
    Dashboard,
    DataModel,
    Encryption,
    Folder,
    MergeTool,
    Metadata,
    Migration,
    Plugins,
    Queries,
    ReportManager,
    WellCheck,
)

__all__ = [
    "__version__",
    "FACADES",
    "SisenseClient",
    "AccessManagement",
    "Blox",
    "CustomCode",
    "DataModel",
    "Encryption",
    "Dashboard",
    "Folder",
    "Metadata",
    "MergeTool",
    "Migration",
    "Plugins",
    "Queries",
    "ReportManager",
    "WellCheck",
    "convert_to_dataframe",
    "export_to_csv",
    "convert_utc_to_local",
    "AthenaConnectionParams",
    "BigQueryConnectionParams",
    "ConnectionPayload",
    "ConnectionUpdatePayload",
    "CreateUserPayload",
    "DataBricksConnectionParams",
    "DatasourceRef",
    "MeasurePayload",
    "NotebookCreatePayload",
    "NotebookUpdatePayload",
    "PluginSnapshot",
    "RedShiftConnectionParams",
    "UpdateUserPayload",
]
