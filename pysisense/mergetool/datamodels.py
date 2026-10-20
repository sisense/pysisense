from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from typing_extensions import deprecated


# Deprecated: each method runs the matching Migration method (pysisense/migration/datamodels_v2.py)
# through a Migration built on the same clients, with the same signature. A test fails if a
# signature here drifts from its Migration counterpart.
class DatamodelsMergeMixin:
    @deprecated("use Migration.migrate_datamodels_v2")
    def migrate_datamodels(
        self,
        datamodel_ids: list[str] | None = None,
        datamodel_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        dependencies: list[str] | str | None = None,
        provider_connection_map: dict[str, str] | None = None,
        shares: bool = False,
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
        new_title: str | None = None,
    ) -> dict[str, Any]:
        """Migrate specific data models from source to target.

        Deprecated: use ``Migration.migrate_datamodels_v2``, which runs the same
        code with the same parameters and result. MergeTool is removed in the next
        major release.

        Exports each data model's schema from the source environment and
        imports it into the target via the schema import endpoint. Conflict
        detection is based on the data model's ``title``. Embedded connection
        credentials are repointed using ``provider_connection_map`` when a
        matching provider entry is supplied, and stripped otherwise — they
        must be re-entered (or reconnected) on the target after migration.

        Parameters
        ----------
        datamodel_ids : list[str] or None, default None
            Data model OIDs to migrate. Provide either this or
            ``datamodel_names``.
        datamodel_names : list[str] or None, default None
            Data model titles to migrate. Provide either this or
            ``datamodel_ids``.
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy when a data model with the same ``title``
            already exists on the target.

            - ``"skip"`` — leave the existing data model unchanged.
            - ``"overwrite"`` — replace the existing data model's schema with
              the source version, matched by the target model's own OID.
            - ``"duplicate"`` — always create a new data model titled
              ``"<title> (Duplicate)"``, regardless of conflicts.
        dependencies : list[str] or str or None, default None
            Dependencies to include in the export. One or more of
            ``"dataSecurity"``, ``"formulas"``, ``"hierarchies"``,
            ``"perspectives"``. Defaults to all of them when ``None`` or
            ``"all"``. Windows: has no effect when the source is a Windows
            deployment — its export endpoint accepts no dependencies parameter.
        provider_connection_map : dict[str, str] or None, default None
            Maps a connection provider name (for example ``"Athena"``) to a
            target-environment connection OID. Datasets whose provider is not
            present in this map have their connection parameters cleared on
            import.
        shares : bool, default False
            Whether to also migrate each data model's shares after a
            successful import, remapping users and groups by email/name.
        concurrency : int, default 1
            Maximum number of data models to migrate concurrently, run via a
            background thread pool (``asyncio.to_thread``) since the
            underlying HTTP client is synchronous. Data models are
            independent of each other, so any value is safe. Values <= 1
            (the default) process data models one at a time.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys. When
            ``concurrency`` is greater than 1, this callback may be invoked
            from multiple worker threads concurrently.
        new_title : str or None, default None
            Title for the new data model when ``action="duplicate"``, instead
            of ``"<title> (Duplicate)"``. Ignored for other actions. Every
            selected model gets this title, so use it with one model.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``title``, ``source_oid``,
              and ``target_id``
            - ``skipped`` : list[dict] — each has ``title``, ``source_oid``,
              and ``reason``
            - ``failed`` : list[dict] — each has ``title``, ``source_oid``,
              and ``reason``
            - ``source_count`` : int
            - ``succeeded_count`` : int
            - ``skipped_count`` : int
            - ``failed_count`` : int
            - ``error`` : str — only when the run could not start
            - ``meta`` : dict — ``dependencies``/``api_dependencies`` (what
              was exported), ``shares_requested``,
              ``export_requested``/``export_succeeded``/``export_failed``,
              ``import_succeeded``/``import_failed``,
              ``share_success_count``/``share_fail_count`` (one per data
              model), and ``warnings`` (list[str]) for share problems that
              did not fail the data model

        Raises
        ------
        ValueError
            If both or neither of ``datamodel_ids`` and ``datamodel_names``
            are provided.

        Notes
        -----
        If called from code that is already running an asyncio event loop,
        ``concurrency`` greater than 1 falls back to sequential processing (a
        nested event loop cannot be started) and logs a warning.
        """
        return self._as_migration().migrate_datamodels_v2(
            datamodel_ids=datamodel_ids,
            datamodel_names=datamodel_names,
            action=action,
            dependencies=dependencies,
            provider_connection_map=provider_connection_map,
            shares=shares,
            concurrency=concurrency,
            emit=emit,
            new_title=new_title,
        )

    @deprecated("use Migration.migrate_all_datamodels_v2")
    def migrate_all_datamodels(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        dependencies: list[str] | str | None = None,
        provider_connection_map: dict[str, str] | None = None,
        shares: bool = False,
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all data models from source to target.

        Deprecated: use ``Migration.migrate_all_datamodels_v2``, which runs the same
        code with the same parameters and result. MergeTool is removed in the next
        major release.

        Fetches every data model from the source environment and delegates
        to ``migrate_datamodels``.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy applied to every data model.
        dependencies : list[str] or str or None, default None
            Same as in ``migrate_datamodels``.
        provider_connection_map : dict[str, str] or None, default None
            Same as in ``migrate_datamodels``.
        shares : bool, default False
            Same as in ``migrate_datamodels``.
        concurrency : int, default 1
            Same as in ``migrate_datamodels``.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_datamodels``.
        """
        return self._as_migration().migrate_all_datamodels_v2(
            action=action, dependencies=dependencies, provider_connection_map=provider_connection_map, shares=shares, concurrency=concurrency, emit=emit
        )
