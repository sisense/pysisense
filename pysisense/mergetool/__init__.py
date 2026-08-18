from __future__ import annotations

from ..migration.base import MigrationBaseMixin
from ..sisenseclient import SisenseClient
from .base import MergeToolConcurrencyMixin
from .blox import BloxMergeMixin
from .custom_code import CustomCodeMergeMixin
from .dashboards import DashboardMergeMixin
from .datamodels import DatamodelsMergeMixin
from .datasecurity import DatasecurityMergeMixin
from .folder import FolderMergeMixin
from .groups import GroupsMergeMixin
from .users import UsersMergeMixin


class MergeTool(
    MigrationBaseMixin,
    MergeToolConcurrencyMixin,
    CustomCodeMergeMixin,
    FolderMergeMixin,
    BloxMergeMixin,
    GroupsMergeMixin,
    UsersMergeMixin,
    DatamodelsMergeMixin,
    DatasecurityMergeMixin,
    DashboardMergeMixin,
):
    """Copy Sisense content between two separate Sisense environments.

    Connects to a source and a target Sisense instance (via YAML config files
    or injected clients) and merges custom-code notebooks, folders, Blox
    actions, groups, users, data models, and dashboards from one to the
    other. Does not operate on a single instance — use CustomCode, Folder,
    Blox, AccessManagement, DataModel, or Dashboard for single-environment
    changes.

    Modules
    -------
    custom_code :
        Notebook migration — copy custom-code notebooks from source to
        target with skip, overwrite, or duplicate conflict handling.
    folder :
        Folder migration — recreate the folder hierarchy from source on the
        target with skip, overwrite, or duplicate conflict handling.
    blox :
        Blox action migration — copy custom Blox actions from source to
        target with skip, overwrite, or duplicate conflict handling.
        Target must be a Linux deployment.
    groups :
        Group migration — copy groups from source to target with skip,
        overwrite, or duplicate conflict handling. Migrate groups before
        users, since user payloads reference target group IDs.
    users :
        User migration — copy users from source to target with skip,
        overwrite, or duplicate conflict handling, resolving role and group
        assignments to target IDs.
    datamodels :
        Data model migration — export data model schemas from source and
        import them into target with skip, overwrite, or duplicate conflict
        handling, remapping connection credentials via a provider map and
        optionally migrating shares.
    datasecurity :
        Data security (row-level security) migration — copy datasecurity
        rules for data models that already exist on the target, remapping
        rule shares to target users and groups by email/name. Migrate data
        models before data security.
    dashboards :
        Dashboard migration — export dashboards from source and import them
        into target with skip, overwrite, or duplicate conflict handling,
        remapping owner/shares to target users and groups and placing each
        dashboard in its corresponding target folder. Migrate groups, users,
        folders, and data models first, since dashboards reference all four.
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
        """Initialize MergeTool with API clients for both source and target environments.

        Parameters
        ----------
        source_yaml : str, optional
            Path to the YAML config file for the source Sisense environment.
        target_yaml : str, optional
            Path to the YAML config file for the target Sisense environment.
        debug : bool, optional
            Enable debug logging on a newly created client. Default is False.
        source_client : SisenseClient, optional
            Pre-built client for the source environment. Takes precedence over
            ``source_yaml``.
        target_client : SisenseClient, optional
            Pre-built client for the target environment. Takes precedence over
            ``target_yaml``.

        Raises
        ------
        ValueError
            If neither ``(source_client, target_client)`` nor
            ``(source_yaml, target_yaml)`` are provided.

        Notes
        -----
        Supported init patterns:

        YAML-based::

            merge = MergeTool(source_yaml="source.yaml", target_yaml="target.yaml")

        Client-based::

            src = SisenseClient(config_file="source.yaml", debug=True)
            tgt = SisenseClient(config_file="target.yaml", debug=True)
            merge = MergeTool(source_client=src, target_client=tgt)
        """
        if source_client is not None and target_client is not None:
            self.source_client = source_client
            self.target_client = target_client
        elif source_yaml is not None and target_yaml is not None:
            self.source_client = SisenseClient(config_file=source_yaml, debug=debug)
            self.target_client = SisenseClient(config_file=target_yaml, debug=debug)
        else:
            raise ValueError("MergeTool requires either (source_client and target_client) OR (source_yaml and target_yaml).")

        self.logger = self.source_client.logger
