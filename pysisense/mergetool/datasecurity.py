from __future__ import annotations

from collections.abc import Callable
from typing import Any

from typing_extensions import deprecated


# Deprecated: each method runs the matching Migration method (pysisense/migration/datasecurity.py)
# through a Migration built on the same clients, with the same signature. A test fails if a
# signature here drifts from its Migration counterpart.
class DatasecurityMergeMixin:
    @deprecated("use Migration.migrate_datasecurity")
    def migrate_datasecurity(
        self,
        datamodel_ids: list[str] | None = None,
        datamodel_names: list[str] | None = None,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate row-level datasecurity rules for specific data models from source to target.

        Deprecated: use ``Migration.migrate_datasecurity``, which runs the same code
        with the same parameters and result. MergeTool is removed in the next major
        release.

        Fetches each data model's raw datasecurity rules from the source,
        remaps the user/group shares on every rule to their target-environment
        ids (matched by email for users and by name for groups), and writes
        the resolved rules onto the already-existing target data model.
        Requires the target data model to already exist — run
        ``migrate_datamodels_v2``/``migrate_all_datamodels_v2`` first.

        Parameters
        ----------
        datamodel_ids : list[str] or None, default None
            Data model OIDs to migrate. Provide either this or
            ``datamodel_names``.
        datamodel_names : list[str] or None, default None
            Data model titles to migrate. Provide either this or
            ``datamodel_ids``.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``title``, ``source_oid``,
              and ``rule_count``
            - ``skipped`` : list[dict] — each has ``title``, ``source_oid``,
              and ``reason``
            - ``failed`` : list[dict] — each has ``title``, ``source_oid``,
              and ``reason``
            - ``source_count`` : int
            - ``succeeded_count`` : int
            - ``skipped_count`` : int
            - ``failed_count`` : int

        Raises
        ------
        ValueError
            If both or neither of ``datamodel_ids`` and ``datamodel_names``
            are provided.
        """
        return self._as_migration().migrate_datasecurity(datamodel_ids=datamodel_ids, datamodel_names=datamodel_names, emit=emit)

    @deprecated("use Migration.migrate_all_datasecurity")
    def migrate_all_datasecurity(
        self,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate datasecurity rules for all data models from source to target.

        Deprecated: use ``Migration.migrate_all_datasecurity``, which runs the same
        code with the same parameters and result. MergeTool is removed in the next
        major release.

        Fetches every data model from the source environment and delegates
        to ``migrate_datasecurity``. Data models not yet present on the
        target are skipped — run ``migrate_all_datamodels_v2`` first.

        Parameters
        ----------
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_datasecurity``.
        """
        return self._as_migration().migrate_all_datasecurity(emit=emit)
