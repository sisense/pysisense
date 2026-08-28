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
        assert deprecated, "expected at least one deprecated alias (get_connections)"
        for cls, name, message in deprecated:
            match = re.search(r"use (\w+)", message)
            assert match, f"{cls.__name__}.{name} deprecation message must say 'use <new_name>', got: {message!r}"
            counterpart = getattr(cls, match.group(1), None)
            assert counterpart is not None, f"{cls.__name__}.{name} points at missing counterpart {match.group(1)!r}"
            assert getattr(counterpart, "__deprecated__", None) is None, f"counterpart {match.group(1)!r} is itself deprecated"

    def test_get_connections_is_machine_readable_deprecated(self):
        from pysisense import DataModel

        assert getattr(DataModel.get_connections, "__deprecated__", None) == "use get_connections_all"


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
