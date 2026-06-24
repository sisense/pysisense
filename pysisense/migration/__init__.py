from __future__ import annotations

from ..sisenseclient import SisenseClient
from .base import MigrationBaseMixin
from .custom_code import CustomCodeMigrationMixin
from .dashboards import DashboardsMigrationMixin
from .datamodels import DatamodelsMigrationMixin
from .groups import GroupsMigrationMixin
from .users import UsersMigrationMixin


class Migration(
    MigrationBaseMixin,
    GroupsMigrationMixin,
    UsersMigrationMixin,
    DashboardsMigrationMixin,
    DatamodelsMigrationMixin,
    CustomCodeMigrationMixin,
):
    """Copy Sisense content between two separate Sisense environments.

    Connects to a source and a target Sisense instance (via YAML config files
    or injected clients) and migrates groups, users, dashboards, and data
    models from one to the other. Does not operate on a single instance —
    use AccessManagement or Dashboard for single-environment changes.

    Modules
    -------
    base :
        Progress emission — internal helper for reporting migration events
        to a caller-provided callback.
    groups :
        Group migration — copy specific groups or all groups from source to
        target using the bulk group endpoint.
    users :
        User migration — copy specific users or all users from source to
        target, preserving role and group assignments.
    dashboards :
        Dashboard migration — copy dashboards and their share entries across
        environments; migrate all dashboards in bulk.
    datamodels :
        Data model migration — copy data model schemas and connection
        definitions from source to target; supports bulk migration.
    custom_code :
        Notebook migration — copy custom-code notebooks from source to
        target with skip, overwrite, or duplicate conflict handling.
    """

    def __init__(
        self,
        source_yaml: str | None = None,
        target_yaml: str | None = None,
        debug: bool = False,
        *,
        source_client: SisenseClient | None = None,
        target_client: SisenseClient | None = None,
    ):
        """
        Initializes the Migration class with API clients for both source and
        target environments.

        Supported patterns:

        1) YAML-based (existing behavior, fully supported):
            migration = Migration(
                source_yaml="source.yaml",
                target_yaml="target.yaml",
                debug=False,
            )

        2) Client-based (for agent app / inline connections):
            src_client = SisenseClient.from_connection(
                domain="https://source.sisense.com",
                token="SRC_TOKEN",
                is_ssl=True,
                debug=True,
            )
            tgt_client = SisenseClient.from_connection(
                domain="https://target.sisense.com",
                token="TGT_TOKEN",
                is_ssl=True,
                debug=True,
            )
            migration = Migration(
                source_client=src_client,
                target_client=tgt_client,
                debug=True,
            )

        Exactly one mode must be provided:
        - Either both source_client and target_client
        - Or both source_yaml and target_yaml
        """
        # Prefer explicit clients if provided (agent / runtime connections)
        if source_client is not None and target_client is not None:
            self.source_client = source_client
            self.target_client = target_client

        # Otherwise fall back to YAML-based configuration (legacy / scripts)
        elif source_yaml is not None and target_yaml is not None:
            self.source_client = SisenseClient(
                config_file=source_yaml,
                debug=debug,
            )
            self.target_client = SisenseClient(
                config_file=target_yaml,
                debug=debug,
            )

        else:
            raise ValueError("Migration requires either (source_client and target_client) OR (source_yaml and target_yaml).")

        # Use the logger from the source client for consistency
        self.logger = self.source_client.logger
