"""Unit tests for the machine-readable payload contracts.

Covers what downstream schema generators rely on: TypedDict required/optional
key introspection, runtime-resolvable annotations on method signatures, and
Literal-typed enum parameters — plus the validate-early and pre-check
behaviors added alongside the contracts.
"""

from __future__ import annotations

import inspect
import typing

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense import payloads
from pysisense.access_management import AccessManagement
from pysisense.datamodel import DataModel
from pysisense.payloads import CreateUserPayload, PluginSnapshot, UpdateUserPayload

# ---------------------------------------------------------------------------
# TypedDict introspection contract
# ---------------------------------------------------------------------------


class TestPayloadIntrospection:
    def test_create_user_payload_required_and_optional_keys(self):
        assert CreateUserPayload.__required_keys__ == frozenset({"email", "role"})
        assert CreateUserPayload.__optional_keys__ == frozenset({"userName", "firstName", "lastName", "groups", "password", "preferences"})

    def test_update_user_payload_all_keys_optional(self):
        assert UpdateUserPayload.__required_keys__ == frozenset()
        assert "role" in UpdateUserPayload.__optional_keys__

    def test_plugin_snapshot_requires_plugins_only(self):
        assert PluginSnapshot.__required_keys__ == frozenset({"plugins"})
        assert PluginSnapshot.__optional_keys__ == frozenset({"created"})

    def test_all_exported_payload_types_resolve_type_hints(self):
        import pysisense

        payload_types = [
            getattr(payloads, name) for name in dir(payloads) if not name.startswith("_") and isinstance(getattr(payloads, name), type) and hasattr(getattr(payloads, name), "__required_keys__")
        ]
        assert payload_types, "no payload TypedDicts found"
        for payload_type in payload_types:
            hints = typing.get_type_hints(payload_type)
            assert hints, f"{payload_type.__name__} has no resolvable hints"
            # each contract is importable from the package root
            assert getattr(pysisense, payload_type.__name__) is payload_type

    def test_method_signatures_resolve_at_runtime(self):
        # Downstream generators introspect installed-package signatures; the
        # annotations must eval-resolve despite `from __future__ import annotations`.
        sig = inspect.signature(AccessManagement.create_user, eval_str=True)
        assert sig.parameters["user_data"].annotation is CreateUserPayload

        sig = inspect.signature(DataModel.deploy_datamodel, eval_str=True)
        assert set(typing.get_args(sig.parameters["build_type"].annotation)) == {"full", "by_table", "schema_changes"}
        assert set(typing.get_args(sig.parameters["schema_origin"].annotation)) == {"latest", "running"}

        sig = inspect.signature(DataModel.create_datamodel, eval_str=True)
        assert set(typing.get_args(sig.parameters["datamodel_type"].annotation)) == {"extract", "live"}


# ---------------------------------------------------------------------------
# Validate-early: create_user
# ---------------------------------------------------------------------------


class TestCreateUserValidateEarly:
    def _access_mgmt(self):
        return AccessManagement(api_client=FakeApiClient(logger=FakeLogger()))

    def test_missing_role_rejected_before_any_api_call(self):
        result = self._access_mgmt().create_user({"firstName": "Himanshu", "lastName": "Negi"})
        assert "error" in result
        assert "'role'" in result["error"] and "'email'" in result["error"]
        assert "firstName" in result["error"]

    def test_missing_role_only(self):
        result = self._access_mgmt().create_user({"email": "a@b.com", "firstName": "A"})
        assert result["error"].startswith("create_user requires 'role' in user_data")

    def test_non_dict_rejected(self):
        result = self._access_mgmt().create_user("not-a-dict")
        assert result == {"ok": False, "error": "user_data must be a dictionary."}


# ---------------------------------------------------------------------------
# create_datamodel duplicate-name pre-check
# ---------------------------------------------------------------------------


class TestCreateDatamodelPrecheck:
    def test_duplicate_title_rejected_with_clear_error(self):
        client = FakeApiClient(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, {"oid": "dm-1", "title": "Sales"})},
            logger=FakeLogger(),
        )
        result = DataModel(api_client=client).create_datamodel("Sales", "extract")
        assert "already exists" in result["error"]
        assert "dm-1" in result["error"]

    def test_title_free_proceeds_to_create(self):
        client = FakeApiClient(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, [])},
            post_responses={"/api/v2/datamodels": FakeResponse(201, {"oid": "dm-new"})},
            logger=FakeLogger(),
        )
        result = DataModel(api_client=client).create_datamodel("Fresh", "extract")
        assert result == {"datamodel_id": "dm-new"}


# ---------------------------------------------------------------------------
# get_datamodel error detail (no longer truncated to a status code)
# ---------------------------------------------------------------------------


class TestGetDatamodelErrorDetail:
    def test_404_detail_is_returned_not_just_logged(self):
        client = FakeApiClient(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(404, {"detail": "ElasticubeNotFound: model does not exist"})},
            logger=FakeLogger(),
        )
        result = DataModel(api_client=client).get_datamodel("Ghost")
        assert "ElasticubeNotFound: model does not exist" in result["error"]
        assert result["status_code"] == 404


# ---------------------------------------------------------------------------
# get_connections_all rename (get_connections kept as alias)
# ---------------------------------------------------------------------------


class TestGetConnectionsAllRename:
    def test_alias_and_new_name_return_same_result(self):
        connections = [{"oid": "c1", "name": "conn"}]
        client = FakeApiClient(get_responses={"/api/v2/connections": FakeResponse(200, connections)}, logger=FakeLogger())
        dm = DataModel(api_client=client)
        assert dm.get_connections_all() == connections
        with pytest.warns(DeprecationWarning, match="use get_connections_all"):
            assert dm.get_connections() == connections
