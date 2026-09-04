from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from ..migration.base import MigrationBaseMixin
from ..sisenseclient import SisenseClient
from .base import MergeToolConcurrencyMixin
from .blox import BloxMergeMixin
from .custom_code import CustomCodeMergeMixin
from .dashboards import DashboardMergeMixin
from .datamodels import DatamodelsMergeMixin
from .datasecurity import DatasecurityMergeMixin
from .filters import FiltersMergeMixin
from .folder import FolderMergeMixin
from .formulas import FormulasMergeMixin
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
    FormulasMergeMixin,
    FiltersMergeMixin,
    DashboardMergeMixin,
):
    """Copy Sisense content between two separate Sisense environments.

    Connects to a source and a target Sisense instance (via config files,
    config dicts, or injected clients) and merges custom-code notebooks, folders, Blox
    actions, groups, users, data models, data security rules, saved
    formulas, saved filters, and dashboards from one to the other. Does not
    operate on a single instance — use CustomCode, Folder, Blox,
    AccessManagement, DataModel, or Metadata for single-environment changes.

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
    formulas :
        Saved formula migration — copy saved formula measures for data
        models that already exist on the target, with skip or
        duplicate-on-create conflict handling.
    filters :
        Saved filter migration — copy saved filter dimensions for data
        models that already exist on the target, with skip or
        duplicate-on-create conflict handling.
    dashboards :
        Dashboard migration — export dashboards from source and import them
        into target with skip, overwrite, or duplicate conflict handling,
        remapping owner/shares to target users and groups and placing each
        dashboard in its corresponding target folder. Migrate groups, users,
        folders, and data models first, since dashboards reference all four.
    """

    def __init__(
        self,
        source_yaml: str | os.PathLike[str] | Mapping[str, Any] | None = None,
        target_yaml: str | os.PathLike[str] | Mapping[str, Any] | None = None,
        debug: bool = False,
        *,
        source_client: SisenseClient | None = None,
        target_client: SisenseClient | None = None,
        source_config: str | os.PathLike[str] | Mapping[str, Any] | None = None,
        target_config: str | os.PathLike[str] | Mapping[str, Any] | None = None,
    ):
        """Initialize MergeTool with API clients for both source and target environments.

        Provide either two pre-built clients, or two configs from which the
        clients are built. A config is anything ``SisenseClient`` accepts as
        ``config_file``: a YAML file path, a JSON file path, or a plain dict.

        Parameters
        ----------
        source_yaml : str | os.PathLike | Mapping, optional
            Alias for ``source_config`` kept for backward compatibility.
        target_yaml : str | os.PathLike | Mapping, optional
            Alias for ``target_config`` kept for backward compatibility.
        debug : bool, optional
            Enable debug logging on a newly created client. Default is False.
        source_client : SisenseClient, optional
            Pre-built client for the source environment. Takes precedence over
            ``source_config``.
        target_client : SisenseClient, optional
            Pre-built client for the target environment. Takes precedence over
            ``target_config``.
        source_config : str | os.PathLike | Mapping, optional
            Config for the source environment: a ``.yaml``/``.yml`` or
            ``.json`` file path, or a dict with the same keys.
        target_config : str | os.PathLike | Mapping, optional
            Config for the target environment, in the same forms.

        Raises
        ------
        ValueError
            If neither ``(source_client, target_client)`` nor
            ``(source_config, target_config)`` are provided.

        Notes
        -----
        Supported init patterns:

        Config-based (YAML or JSON file, or a dict)::

            merge = MergeTool(source_config="source.yaml", target_config="target.json")
            merge = MergeTool(
                source_config={"domain": "src.example.com", "token": "SRC_TOKEN"},
                target_config={"domain": "tgt.example.com", "token": "TGT_TOKEN"},
            )

        Client-based::

            src = SisenseClient(config_file="source.yaml", debug=True)
            tgt = SisenseClient(config_file="target.yaml", debug=True)
            merge = MergeTool(source_client=src, target_client=tgt)
        """
        source_config = source_config if source_config is not None else source_yaml
        target_config = target_config if target_config is not None else target_yaml

        if source_client is not None and target_client is not None:
            self.source_client = source_client
            self.target_client = target_client
        elif source_config is not None and target_config is not None:
            self.source_client = SisenseClient(config_file=source_config, debug=debug)
            self.target_client = SisenseClient(config_file=target_config, debug=debug)
        else:
            raise ValueError("MergeTool requires either (source_client and target_client) OR (source_config and target_config).")

        self.logger = self.source_client.logger
