from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from ..sisenseclient import SisenseClient
from .base import MigrationBaseMixin
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
):
    """Copy Sisense content between two separate Sisense environments.

    Connects to a source and a target Sisense instance (via config files,
    config dicts, or injected clients) and migrates groups, users, dashboards, and data
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
        """Initialize Migration with API clients for both source and target environments.

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
            Enable debug logging on newly created clients. Default is False.
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

            migration = Migration(source_config="source.yaml", target_config="target.json")
            migration = Migration(
                source_config={"domain": "src.example.com", "token": "SRC_TOKEN"},
                target_config={"domain": "tgt.example.com", "token": "TGT_TOKEN"},
            )

        Client-based (for inline / runtime connections)::

            src = SisenseClient.from_connection(domain="src.example.com", token="SRC_TOKEN")
            tgt = SisenseClient.from_connection(domain="tgt.example.com", token="TGT_TOKEN")
            migration = Migration(source_client=src, target_client=tgt)
        """
        source_config = source_config if source_config is not None else source_yaml
        target_config = target_config if target_config is not None else target_yaml

        # Prefer explicit clients if provided (inline / runtime connections)
        if source_client is not None and target_client is not None:
            self.source_client = source_client
            self.target_client = target_client

        # Otherwise build the clients from the configs (files or dicts)
        elif source_config is not None and target_config is not None:
            self.source_client = SisenseClient(config_file=source_config, debug=debug)
            self.target_client = SisenseClient(config_file=target_config, debug=debug)

        else:
            raise ValueError("Migration requires either (source_client and target_client) OR (source_config and target_config).")

        # Use the logger from the source client for consistency
        self.logger = self.source_client.logger
