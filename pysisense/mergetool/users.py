from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from typing_extensions import deprecated


# Deprecated: each method runs the matching Migration method (pysisense/migration/users_v2.py)
# through a Migration built on the same clients, with the same signature. A test fails if a
# signature here drifts from its Migration counterpart.
class UsersMergeMixin:
    @deprecated("use Migration.migrate_users_v2")
    def migrate_users(
        self,
        user_emails: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        ignore_custom_roles: bool = False,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate specific users from source to target.

        Deprecated: use ``Migration.migrate_users_v2``, which runs the same code
        with the same parameters and result. MergeTool is removed in the next major
        release.

        Fetches the requested users from the source environment, resolves
        their role and group assignments to target environment IDs, and
        creates (or replaces) them on the target via the bulk user endpoint.
        Conflict detection is based on the user's ``email`` field.

        Role names are adapted to the target before matching: when the
        target environment is multi-tenant (its role list includes
        ``tenantAdmin``), source ``super``/``admin`` roles are mapped to
        ``tenantAdmin``; when the target is a Windows deployment, source
        ``tenantAdmin`` is mapped back to ``admin``.

        Parameters
        ----------
        user_emails : list[str] or None, default None
            Email addresses of the users to migrate. If ``None``, every user
            on the source is migrated. (format: email)
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy when a user with the same ``email`` already
            exists on the target.

            - ``"skip"`` — leave the existing user unchanged.
            - ``"overwrite"`` — delete the existing user then recreate from
              source.
            - ``"duplicate"`` — always create, regardless of conflicts.
        ignore_custom_roles : bool, default False
            When ``True``, strips a ``custom_`` prefix from source role names
            before matching them against target roles (and matches target
            roles with the same prefix stripped).
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``email``
            - ``skipped`` : list[dict] — each has ``email`` and ``reason``
            - ``failed`` : list[dict] — each has ``email`` and ``reason``
            - ``source_count`` : int
            - ``succeeded_count`` : int
            - ``skipped_count`` : int
            - ``failed_count`` : int
            - ``error`` : str — only when the run could not start
            - ``meta`` : dict — ``existing_on_target_count``,
              ``missing_role_mappings_count`` (users not created because
              their role has no target match), ``missing_group_mappings_count``
              (group memberships dropped because the group is not on the
              target), ``unmapped_role_names`` (list[str]) and ``warnings``
              (list[str])
        """
        return self._as_migration().migrate_users_v2(user_emails=user_emails, action=action, ignore_custom_roles=ignore_custom_roles, emit=emit)

    @deprecated("use Migration.migrate_all_users_v2")
    def migrate_all_users(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        ignore_custom_roles: bool = False,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all eligible users from source to target.

        Deprecated: use ``Migration.migrate_all_users_v2``, which runs the same code
        with the same parameters and result. MergeTool is removed in the next major
        release.

        Fetches every user from the source environment, excludes users with
        the built-in ``super`` role (the source and target super admin
        accounts are expected to already exist independently on each
        environment), and — when the source environment exposes tenant
        information — restricts migration to users belonging to the system
        tenant, then delegates to ``migrate_users``.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy applied to every user.
        ignore_custom_roles : bool, default False
            When ``True``, strips a ``custom_`` prefix from source role names
            before matching them against target roles.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_users``. ``meta`` also carries
            ``source_total_count`` (every user on the source),
            ``eligible_count``, ``excluded_count`` (users without an email),
            ``skipped_super_count`` and ``skipped_multi_tenant_count``.

        Notes
        -----
        If the source environment does not expose tenant information (single
        tenant on-premises deployments), tenant-based filtering is skipped and
        every non-``super`` user is treated as eligible.
        """
        return self._as_migration().migrate_all_users_v2(action=action, ignore_custom_roles=ignore_custom_roles, emit=emit)
