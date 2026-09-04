"""Enforcement tests for the SDK's introspection contracts.

Downstream schema generators (FES Assistant, sisense-admin-mcp) introspect
this package; these tests lock in the conventions they rely on:

- ``pysisense.FACADES`` is the explicit registry of tool-bearing facade
  classes (never ``__all__``).
- Every deprecated alias is machine-readable (``__deprecated__`` via PEP 702)
  and points at an existing, non-deprecated counterpart.
- Every dict-typed parameter on a facade method either references a TypedDict
  contract from ``pysisense/payloads.py`` or is on the documented
  free-form/grandfathered list below.
"""

from __future__ import annotations

import inspect
import re
import types
import typing

import pysisense
from pysisense import FACADES

_EXPECTED_FACADE_NAMES = {
    "AccessManagement",
    "Blox",
    "CustomCode",
    "Dashboard",
    "DataModel",
    "Encryption",
    "Folder",
    "MergeTool",
    "Metadata",
    "Migration",
    "Plugins",
    "Queries",
    "ReportManager",
    "WellCheck",
}

# Dict-typed params without a payloads.py TypedDict contract, as
# "Facade.method.param". Two kinds live here:
# - genuinely free-form payloads whose shape is a language, not a field list
#   (JAQL, metadata queries, Blox JSON, encryption bodies, raw schema/widget
#   DTOs round-tripped from the API);
# - grandfathered pre-contract params that are candidates for future
#   TypedDicts.
# A NEW dict-typed param must either get a TypedDict in payloads.py or be
# added here with a docstring that says the payload is free-form.
_FREE_FORM_OR_GRANDFATHERED = {
    "AccessManagement.create_groups_bulk.groups",
    "AccessManagement.create_users_bulk.users",
    "Blox.save_blox_action.action",
    "Blox.update_blox_widget_style.current_card",
    "Blox.update_blox_widget_style.current_config",
    "CustomCode.get_notebooks.params",
    "CustomCode.rename_notebook_file.payload",
    "CustomCode.rename_notebook_folder.payload",
    "Dashboard.add_dashboard_script.script",
    "Dashboard.add_dashboard_shares.shares",
    "Dashboard.add_widget_script.script",
    "Dashboard.import_dashboards_bulk.dashboards",
    "Dashboard.update_widget.widget_data",
    "DataModel.add_datamodel_shares.shares",
    "DataModel.create_table.build_behavior_config",
    "DataModel.import_datamodel_schema.schema",
    "DataModel.set_live_datasecurity_add_many.rules",
    "DataModel.setup_datamodel.tables",
    "DataModel.update_datamodel_permissions_extract.shares",
    "DataModel.update_datamodel_permissions_live.shares",
    "DataModel.update_datasecurity.datasecurity",
    "Encryption.decrypt.payload",
    "Encryption.encrypt.payload",
    "MergeTool.migrate_all_datamodels.provider_connection_map",
    "MergeTool.migrate_datamodels.provider_connection_map",
    "Metadata.post_metadata_query.query_payload",
    "Migration.migrate_datamodels.provider_connection_map",
    "Queries.elasticube_run_jaql_query.jaql_payload",
    "Queries.elasticubes_run_jaql_csv.jaql_payload",
    "ReportManager.create_report.report",
    "ReportManager.update_report.report",
}


def _union_members(annotation):
    if typing.get_origin(annotation) in (typing.Union, types.UnionType):
        return list(typing.get_args(annotation))
    return [annotation]


def _iter_facade_methods():
    for cls in FACADES:
        for name, member in inspect.getmembers(cls, inspect.isfunction):
            if name.startswith("_") or not member.__module__.startswith("pysisense"):
                continue
            yield cls, name, member


class TestFacadeRegistry:
    def test_facades_matches_expected_classes(self):
        assert {cls.__name__ for cls in FACADES} == _EXPECTED_FACADE_NAMES
        assert isinstance(FACADES, tuple)
        for cls in FACADES:
            assert inspect.isclass(cls)
            # every facade is also part of the public export surface
            assert cls.__name__ in pysisense.__all__
            assert getattr(pysisense, cls.__name__) is cls

    def test_facades_contains_no_payload_types(self):
        for cls in FACADES:
            assert not hasattr(cls, "__required_keys__"), f"{cls.__name__} looks like a TypedDict, not a facade"


class TestDeprecatedAliases:
    def _deprecated_methods(self):
        found = []
        for cls, name, member in _iter_facade_methods():
            message = getattr(member, "__deprecated__", None)
            if message is not None:
                found.append((cls, name, message))
        return found

    def test_every_deprecated_alias_names_a_live_counterpart(self):
        deprecated = self._deprecated_methods()
        assert deprecated, "expected at least one deprecated alias (e.g. get_group)"
        for cls, name, message in deprecated:
            match = re.search(r"use (\w+)", message)
            assert match, f"{cls.__name__}.{name} deprecation message must say 'use <new_name>', got: {message!r}"
            counterpart = getattr(cls, match.group(1), None)
            assert counterpart is not None, f"{cls.__name__}.{name} points at missing counterpart {match.group(1)!r}"
            assert getattr(counterpart, "__deprecated__", None) is None, f"counterpart {match.group(1)!r} is itself deprecated"

    def test_a_deprecated_alias_is_machine_readable(self):
        from pysisense import AccessManagement

        assert getattr(AccessManagement.get_group, "__deprecated__", None) == "use get_groups"

    def test_get_connections_removed_after_its_deprecation_window(self):
        # Deprecated in 1.1.0, removed in 2.0 — must not come back.
        from pysisense import DataModel

        assert not hasattr(DataModel, "get_connections")


class TestDictParamsHaveContracts:
    def _uncontracted_dict_params(self):
        found = set()
        for cls, name, member in _iter_facade_methods():
            sig = inspect.signature(member, eval_str=True)
            for param_name, param in sig.parameters.items():
                for candidate in _union_members(param.annotation):
                    if getattr(candidate, "__required_keys__", None) is not None:
                        break  # has a TypedDict contract
                    if typing.get_origin(candidate) is dict:
                        found.add(f"{cls.__name__}.{name}.{param_name}")
                        break
                    args = typing.get_args(candidate)
                    if typing.get_origin(candidate) is list and args and typing.get_origin(args[0]) is dict:
                        found.add(f"{cls.__name__}.{name}.{param_name}")
                        break
        return found

    def test_dict_params_are_contracted_or_documented(self):
        found = self._uncontracted_dict_params()
        new_uncontracted = found - _FREE_FORM_OR_GRANDFATHERED
        assert not new_uncontracted, f"new dict-typed params need a TypedDict in payloads.py or an entry in the free-form list: {sorted(new_uncontracted)}"
        stale_entries = _FREE_FORM_OR_GRANDFATHERED - found
        assert not stale_entries, f"free-form list entries no longer match a dict-typed param (gained a contract? renamed?) — remove them: {sorted(stale_entries)}"


# ---------------------------------------------------------------------------
# Facade "Modules" docstring section — one entry per mixin file
# ---------------------------------------------------------------------------
#
# Downstream tooling builds its hierarchical tool routing from each facade's
# docstring: the ``Modules`` section names every mixin file in the package and
# says what its methods are for. A mixin file with no entry is invisible to
# that routing, so this test fails the build when one is added without its
# entry (or an entry is left behind after a file is removed).


def _modules_entries(cls) -> dict[str, str]:
    doc = inspect.getdoc(cls) or ""
    match = re.search(r"^Modules\n-+\n(.*?)(?:\n\n[A-Z][A-Za-z ]+\n-+\n|\Z)", doc, re.S | re.M)
    if not match:
        return {}
    entries: dict[str, str] = {}
    current = None
    for line in match.group(1).splitlines():
        head = re.match(r"^([a-z_][a-z0-9_]*) :\s*$", line)
        if head:
            current = head.group(1)
            entries[current] = ""
        elif current and line.strip():
            entries[current] = (entries[current] + " " + line.strip()).strip()
    return entries


def _mixin_modules(cls, public_only: bool = True) -> set[str]:
    """Mixin files of a facade; with ``public_only`` only those exposing a live public method (private helpers and deprecated aliases do not count)."""
    package = cls.__module__  # e.g. "pysisense.dashboard"
    names = set()
    for base in cls.__mro__[1:]:
        module = getattr(base, "__module__", "")
        if not (module.startswith(package + ".") and base.__name__.endswith("Mixin")):
            continue
        live_public = (name for name, member in vars(base).items() if not name.startswith("_") and callable(member) and getattr(member, "__deprecated__", None) is None)
        if not public_only or any(live_public):
            names.add(module.rsplit(".", 1)[-1])
    return names


class TestFacadeModulesSectionCoversEveryMixin:
    def test_every_mixin_file_has_a_modules_entry(self):
        missing = []
        for cls in FACADES:
            mixins = _mixin_modules(cls)
            if not mixins:
                continue  # single-file facade
            entries = _modules_entries(cls)
            for module in sorted(mixins - set(entries)):
                missing.append(f"{cls.__name__}: mixin file '{module}.py' has no entry in the Modules section of its facade docstring")
        assert not missing, "\n".join(missing)

    def test_no_stale_modules_entries(self):
        stale = []
        for cls in FACADES:
            mixins = _mixin_modules(cls, public_only=False)
            if not mixins:
                continue
            for entry in sorted(set(_modules_entries(cls)) - mixins):
                stale.append(f"{cls.__name__}: Modules entry '{entry}' has no mixin file behind it")
        assert not stale, "\n".join(stale)

    def test_every_modules_entry_has_a_description(self):
        empty = [f"{cls.__name__}: '{name}'" for cls in FACADES for name, text in _modules_entries(cls).items() if len(text) < 20]
        assert not empty, "Modules entries need a real description: " + ", ".join(empty)
