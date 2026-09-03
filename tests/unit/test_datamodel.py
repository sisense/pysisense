"""Unit tests for pysisense.datamodel.DataModel."""

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.datamodel import DataModel

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------
_DATAMODEL_EXTRACT = {
    "oid": "dm123",
    "title": "SalesModel",
    "type": "EXTRACT",
    "lastBuildTime": "2025-01-01T00:00:00.000Z",
    "datasets": [],
    "shares": [],
}

_DATAMODEL_LIVE = {
    "oid": "dm456",
    "title": "LiveModel",
    "type": "LIVE",
    "lastPublishTime": "2025-01-01T00:00:00.000Z",
    "datasets": [],
    "shares": [],
}

_CONNECTION = {"oid": "conn1", "name": "MyConnection", "provider": "athena"}


def _make_dm(get_responses=None, post_responses=None, put_responses=None, patch_responses=None, delete_responses=None):
    """Build a DataModel backed by FakeApiClient."""
    logger = FakeLogger()
    client = FakeApiClient(
        get_responses=get_responses,
        post_responses=post_responses,
        put_responses=put_responses,
        patch_responses=patch_responses,
        delete_responses=delete_responses,
        logger=logger,
    )
    return DataModel(api_client=client)


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------


class TestDataModelInit:
    def test_creates_with_fake_client(self):
        dm = _make_dm()
        assert dm is not None
        assert hasattr(dm, "api_client")
        assert hasattr(dm, "logger")


# ---------------------------------------------------------------------------
# get_datamodel
# ---------------------------------------------------------------------------


class TestGetDatamodel:
    def test_returns_datamodel_on_success(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT)})
        result = dm.get_datamodel("SalesModel")
        assert result["oid"] == "dm123"

    def test_returns_error_when_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.get_datamodel("NoSuchModel")
        assert "error" in result

    def test_returns_error_on_api_failure(self):
        dm = _make_dm()
        result = dm.get_datamodel("SalesModel")
        assert "error" in result


# ---------------------------------------------------------------------------
# get_all_datamodel
# ---------------------------------------------------------------------------


class TestGetAllDatamodel:
    def test_returns_datamodel_list_on_success(self):
        graphql_response = {"data": {"elasticubesMetadata": [{"oid": "dm123", "title": "SalesModel", "type": "EXTRACT", "status": ["ready"], "sizeInMb": 100}]}}
        dm = _make_dm(post_responses={"/api/v2/ecm/": FakeResponse(200, graphql_response)})
        result = dm.get_all_datamodel()
        assert isinstance(result, list)
        assert result[0]["oid"] == "dm123"

    def test_returns_error_on_api_failure(self):
        dm = _make_dm()
        result = dm.get_all_datamodel()
        assert "error" in result


# ---------------------------------------------------------------------------
# get_connection
# ---------------------------------------------------------------------------


class TestGetConnection:
    def test_returns_connection_list_on_success(self):
        dm = _make_dm(get_responses={"/api/v2/connections": FakeResponse(200, [_CONNECTION])})
        result = dm.get_connection("MyConnection")
        assert isinstance(result, list)
        assert result[0]["name"] == "MyConnection"

    def test_returns_error_when_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/connections": FakeResponse(200, [])})
        result = dm.get_connection("NoSuchConnection")
        assert "error" in result

    def test_returns_error_on_api_failure(self):
        dm = _make_dm()
        result = dm.get_connection("MyConnection")
        assert "error" in result


# ---------------------------------------------------------------------------
# get_table_schema
# ---------------------------------------------------------------------------


class TestGetTableSchema:
    def test_returns_schema_on_success(self):
        schema = {"tableName": "orders", "columns": [{"name": "id"}]}
        dm = _make_dm(get_responses={"/api/v2/connections/": FakeResponse(200, schema)})
        result = dm.get_table_schema("conn1", "mydb", "public", "orders")
        assert result is not None


# ---------------------------------------------------------------------------
# create_datamodel
# ---------------------------------------------------------------------------


class TestCreateDatamodel:
    def test_returns_datamodel_id_dict_on_success(self):
        # create_datamodel returns {"datamodel_id": oid}, not the full response
        created = {"oid": "dmnew", "title": "NewModel", "type": "EXTRACT"}
        dm = _make_dm(post_responses={"/api/v2/datamodels": FakeResponse(200, created)})
        result = dm.create_datamodel("NewModel", "extract")
        assert result.get("datamodel_id") == "dmnew"

    def test_returns_error_on_failure(self):
        dm = _make_dm()
        result = dm.create_datamodel("NewModel", "extract")
        assert "error" in result


# ---------------------------------------------------------------------------
# generate_connections_payload
# ---------------------------------------------------------------------------


class TestGenerateConnectionsPayload:
    def test_generates_athena_payload(self):
        dm = _make_dm()
        params = {
            "name": "AthenaConn",
            "region": "us-east-1",
            "s3_output_location": "s3://bucket/output",
            "aws_access_key": "AKID",
            "aws_secret_key": "secret",
        }
        payload = dm.generate_connections_payload("Athena", params)
        assert payload["provider"] == "athena"
        assert payload["name"] == "AthenaConn"

    def test_generates_bigquery_payload(self):
        dm = _make_dm()
        params = {"name": "BQConn", "service_account_key_path": "/path/to/key.json"}
        payload = dm.generate_connections_payload("BigQuery", params)
        assert payload["provider"] == "GoogleBigQuery"

    def test_generates_redshift_payload(self):
        dm = _make_dm()
        params = {"server": "rs.example.com", "username": "admin", "password": "pw"}
        payload = dm.generate_connections_payload("Redshift", params)
        assert payload["provider"] == "RedShift"

    def test_generates_databricks_payload(self):
        dm = _make_dm()
        params = {"name": "DBConn", "connection_string": "jdbc://...", "token": "dapi123"}
        payload = dm.generate_connections_payload("DataBricks", params)
        assert payload["provider"] == "Databricks"

    def test_raises_value_error_for_unsupported_type(self):
        dm = _make_dm()
        with pytest.raises(ValueError, match="Unsupported"):
            dm.generate_connections_payload("Oracle", {})


# ---------------------------------------------------------------------------
# create_connections
# ---------------------------------------------------------------------------


class TestCreateConnections:
    def test_returns_connection_dict_on_201(self):
        created = {"oid": "conn1", "name": "NewConn"}
        dm = _make_dm(post_responses={"/api/v2/connections": FakeResponse(201, created)})
        result = dm.create_connections({"name": "NewConn"})
        assert result["oid"] == "conn1"

    def test_returns_error_dict_on_failure(self):
        dm = _make_dm()
        result = dm.create_connections({"name": "NewConn"})
        assert result["ok"] is False
        assert "connection failed" in result["error"]


# ---------------------------------------------------------------------------
# get_connections
# ---------------------------------------------------------------------------


class TestGetConnections:
    def test_returns_list_on_success(self):
        dm = _make_dm(get_responses={"/api/v2/connections": FakeResponse(200, [_CONNECTION])})
        result = dm.get_connections_all()
        assert isinstance(result, list)
        assert result[0]["name"] == "MyConnection"

    def test_returns_error_on_failure(self):
        dm = _make_dm(get_responses={"/api/v2/connections": FakeResponse(500, {"message": "error"})})
        result = dm.get_connections_all()
        assert "error" in result


# ---------------------------------------------------------------------------
# update_connection
# ---------------------------------------------------------------------------


class TestUpdateConnection:
    def test_returns_updated_connection_on_success(self):
        updated = {**_CONNECTION, "name": "RenamedConnection"}
        dm = _make_dm(
            patch_responses={
                "/api/v2/connections/conn1": FakeResponse(200, updated),
            },
        )
        result = dm.update_connection("conn1", {"name": "RenamedConnection"})
        assert result["name"] == "RenamedConnection"

    def test_returns_error_when_empty_payload(self):
        dm = _make_dm()
        result = dm.update_connection("conn1", {})
        assert "error" in result

    def test_returns_error_on_patch_failure(self):
        dm = _make_dm(
            patch_responses={
                "/api/v2/connections/conn1": FakeResponse(400, {"error": "invalid"}),
            },
        )
        result = dm.update_connection("conn1", {"name": "Bad"})
        assert "error" in result


# ---------------------------------------------------------------------------
# create_dataset
# ---------------------------------------------------------------------------


class TestCreateDataset:
    def test_returns_dataset_dict_on_success(self):
        created_ds = {"oid": "ds1", "name": "public"}
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/v2/connections": FakeResponse(200, [_CONNECTION]),
            },
            post_responses={"/api/v2/datamodels/dm123/schema/datasets": FakeResponse(201, created_ds)},
        )
        result = dm.create_dataset("SalesModel", "MyConnection", "mydb", "public")
        assert result.get("oid") == "ds1"

    def test_returns_error_when_datamodel_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.create_dataset("NoSuchModel", "conn", "db", "schema")
        assert "error" in result


# ---------------------------------------------------------------------------
# create_table
# ---------------------------------------------------------------------------


class TestCreateTable:
    def test_returns_error_when_datamodel_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.create_table("NoSuchModel", "orders")
        assert "error" in result


# ---------------------------------------------------------------------------
# setup_datamodel
# ---------------------------------------------------------------------------


class TestSetupDatamodel:
    def test_returns_error_when_create_datamodel_fails(self):
        dm = _make_dm()  # no POST → None → error from create_datamodel
        result = dm.setup_datamodel("NewModel", "extract", "conn", "db", "schema", ["table1"])
        assert "error" in result


# ---------------------------------------------------------------------------
# deploy_datamodel
# ---------------------------------------------------------------------------


class TestDeployDatamodel:
    def test_deploys_extract_model_on_success(self):
        build_result = {"oid": "build1", "status": "building"}
        dm = _make_dm(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT)},
            post_responses={"/api/v2/builds": FakeResponse(201, build_result)},
        )
        result = dm.deploy_datamodel("SalesModel")
        assert result.get("oid") == "build1"

    def test_returns_error_when_model_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.deploy_datamodel("NoSuchModel")
        assert "error" in result

    def test_deploys_live_model_on_success(self):
        build_result = {"oid": "build2", "status": "building"}
        dm = _make_dm(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_LIVE)},
            post_responses={"/api/v2/builds": FakeResponse(201, build_result)},
        )
        result = dm.deploy_datamodel("LiveModel")
        assert result.get("oid") == "build2"


# ---------------------------------------------------------------------------
# describe_datamodel_raw
# ---------------------------------------------------------------------------


class TestDescribeDatamodelRaw:
    def test_returns_error_when_model_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.describe_datamodel_raw("NoSuchModel")
        assert "error" in result

    def test_returns_description_dict_on_success(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT)})
        result = dm.describe_datamodel_raw("SalesModel")
        # Returns a structured dict even with no shares/datasets
        assert isinstance(result, dict)
        assert "error" not in result


# ---------------------------------------------------------------------------
# describe_datamodel
# ---------------------------------------------------------------------------


class TestDescribeDatamodel:
    def test_returns_empty_list_when_model_not_found(self):
        # describe_datamodel returns [] (not error dict) when model not found
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.describe_datamodel("NoSuchModel")
        assert result == []


# ---------------------------------------------------------------------------
# get_datamodel_shares
# ---------------------------------------------------------------------------


class TestGetDatamodelShares:
    def test_returns_shares_list_on_success(self):
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/v1/users": FakeResponse(200, []),
                "/api/v1/groups": FakeResponse(200, []),
            }
        )
        result = dm.get_datamodel_shares("SalesModel")
        assert isinstance(result, list)

    def test_resolves_user_and_group_shares_via_shared_fetch_helper(self):
        # Regression: get_datamodel_shares and add_datamodel_shares now share
        # _fetch_users_and_groups_detail_lists() instead of each duplicating
        # the /api/v1/users + /api/v1/groups fetch — confirms resolution
        # still works for known and unknown parties on both types.
        model = {
            **_DATAMODEL_LIVE,
            "shares": [{"partyId": "u1", "type": "user", "permission": "w"}, {"partyId": "u_missing", "type": "user", "permission": "r"}, {"partyId": "g1", "type": "group", "permission": "a"}],
        }
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, model),
                "/api/v1/users": FakeResponse(200, [{"_id": "u1", "email": "alice@example.com"}]),
                "/api/v1/groups": FakeResponse(200, [{"_id": "g1", "name": "Engineers"}]),
            }
        )
        result = dm.get_datamodel_shares("LiveModel")
        assert result == [
            {"datamodel_name": "LiveModel", "datamodel_id": "dm456", "party_name": "alice@example.com", "party_type": "user", "permission": "EDIT"},
            {"datamodel_name": "LiveModel", "datamodel_id": "dm456", "party_name": "[Unknown user: u_missing]", "party_type": "user", "permission": "USE"},
            {"datamodel_name": "LiveModel", "datamodel_id": "dm456", "party_name": "Engineers", "party_type": "group", "permission": "READ"},
        ]

    def test_returns_error_dict_when_model_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.get_datamodel_shares("NoSuchModel")
        assert result["ok"] is False
        assert "error" in result


# ---------------------------------------------------------------------------
# get_datasecurity
# ---------------------------------------------------------------------------


class TestGetDatasecurity:
    def test_returns_empty_list_when_no_security_rules(self):
        # A model with zero rules must return [] — a placeholder row reads as
        # "one rule" to any consumer that counts results.
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/elasticubes/localhost/SalesModel/datasecurity": FakeResponse(200, []),
            }
        )
        assert dm.get_datasecurity("SalesModel") == []

    def test_returns_error_dict_when_model_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.get_datasecurity("NoSuchModel")
        assert result["ok"] is False
        assert "NoSuchModel" in result["error"]

    def test_returns_security_rules_when_present(self):
        datasecurity = [{"table": "orders", "column": "amount", "datatype": "numeric"}]
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/elasticubes/localhost/SalesModel/datasecurity": FakeResponse(200, datasecurity),
            }
        )
        result = dm.get_datasecurity("SalesModel")
        assert len(result) == 1
        assert result[0]["table_name"] == "orders"


# ---------------------------------------------------------------------------
# get_datasecurity_detail
# ---------------------------------------------------------------------------


class TestGetDatasecurityDetail:
    def test_returns_empty_list_when_no_rules(self):
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/elasticubes/localhost/SalesModel/datasecurity": FakeResponse(200, []),
            }
        )
        assert dm.get_datasecurity_detail("SalesModel") == []

    def test_returns_error_dict_when_model_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.get_datasecurity_detail("NoSuchModel")
        assert result["ok"] is False
        assert "NoSuchModel" in result["error"]


# ---------------------------------------------------------------------------
# get_model_schema
# ---------------------------------------------------------------------------


class TestGetModelSchema:
    def test_returns_schema_on_success(self):
        schema_detail = {"oid": "dm123", "title": "SalesModel", "datasets": []}
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/v2/datamodels/dm123/schema": FakeResponse(200, schema_detail),
            }
        )
        result = dm.get_model_schema("SalesModel")
        assert result is not None

    def test_returns_error_when_model_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.get_model_schema("NoSuchModel")
        assert "error" in result


# ---------------------------------------------------------------------------
# add_datamodel_shares
# ---------------------------------------------------------------------------


class TestAddDatamodelShares:
    def test_returns_error_when_model_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.add_datamodel_shares("NoSuchModel", [{"type": "user", "shareId": "u1", "rule": "EDIT"}])
        assert "error" in result

    def test_adds_shares_to_live_model(self):
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_LIVE),
                "/api/v1/users": FakeResponse(200, [{"_id": "u1", "email": "alice@example.com"}]),
                "/api/v1/groups": FakeResponse(200, []),
            },
            patch_responses={"/api/v1/elasticubes/live/dm456/permissions": FakeResponse(200, {"success": True})},
        )
        result = dm.add_datamodel_shares("LiveModel", [{"name": "alice@example.com", "type": "user", "permission": "EDIT"}])
        assert result["success"] is True
        assert result["new_shares"] == 1
        assert result["skipped"] == []

    def test_returns_error_when_no_share_resolves(self):
        # Nothing resolvable must fail loudly, not write existing shares back
        # unchanged and report success — and the failure names each skip.
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/v1/users": FakeResponse(200, []),
                "/api/v1/groups": FakeResponse(200, []),
            },
        )
        result = dm.add_datamodel_shares("SalesModel", [{"name": "alice@example.com", "type": "user", "permission": "EDIT"}])
        assert result["ok"] is False
        assert "could be resolved" in result["error"]
        assert result["skipped"] == [{"name": "alice@example.com", "type": "user", "reason": "User not found."}]

    def test_adds_shares_to_extract_model_via_put_by_title(self):
        # Live-verified (2026-08 sandbox): the EXTRACT permissions endpoint
        # keys entries by "partyId" (same as LIVE) — a "party"-keyed entry is
        # silently dropped by the PUT. New entries merge with the existing
        # raw share list.
        put_payloads = []

        class _RecordingClient(FakeApiClient):
            def put(self, url, data=None, **kwargs):
                put_payloads.append((url, data))
                return super().put(url, data=data, **kwargs)

        client = _RecordingClient(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/v1/users": FakeResponse(200, [{"_id": "u1", "email": "alice@example.com"}]),
                "/api/v1/groups": FakeResponse(200, []),
                "/api/elasticubes/localhost/SalesModel/permissions": FakeResponse(200, {"shares": [{"partyId": "u0", "type": "user", "permission": "r"}]}),
            },
            put_responses={"/api/elasticubes/localhost/SalesModel/permissions": FakeResponse(200, {"success": True})},
            logger=FakeLogger(),
        )
        dm = DataModel(api_client=client)
        result = dm.add_datamodel_shares("SalesModel", [{"name": "alice@example.com", "type": "user", "permission": "EDIT"}])
        assert result["success"] is True
        assert result["new_shares"] == 1
        assert result["updated_shares"] == 0
        assert result["skipped"] == []

        url, payload = put_payloads[0]
        assert url == "/api/elasticubes/localhost/SalesModel/permissions"
        assert payload == [
            {"partyId": "u0", "type": "user", "permission": "r"},
            {"partyId": "u1", "type": "user", "permission": "w"},
        ]

    def test_extract_share_for_existing_party_updates_permission_in_place(self):
        put_payloads = []

        class _RecordingClient(FakeApiClient):
            def put(self, url, data=None, **kwargs):
                put_payloads.append((url, data))
                return super().put(url, data=data, **kwargs)

        client = _RecordingClient(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/v1/users": FakeResponse(200, [{"_id": "u1", "email": "alice@example.com"}]),
                "/api/v1/groups": FakeResponse(200, []),
                "/api/elasticubes/localhost/SalesModel/permissions": FakeResponse(200, {"shares": [{"partyId": "u1", "type": "user", "permission": "r"}]}),
            },
            put_responses={"/api/elasticubes/localhost/SalesModel/permissions": FakeResponse(200, {"success": True})},
            logger=FakeLogger(),
        )
        dm = DataModel(api_client=client)
        result = dm.add_datamodel_shares("SalesModel", [{"name": "alice@example.com", "type": "user", "permission": "EDIT"}])
        assert result["success"] is True
        assert result["new_shares"] == 0
        assert result["updated_shares"] == 1

        _, payload = put_payloads[0]
        assert payload == [{"partyId": "u1", "type": "user", "permission": "w"}]

    def test_share_for_inactive_user_is_skipped_not_submitted(self):
        # Live-verified: Sisense accepts the write but silently drops entries
        # for inactive users — the SDK must not submit them and pretend the
        # share landed. With only an inactive candidate, nothing resolves.
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/v1/users": FakeResponse(200, [{"_id": "u1", "email": "alice@example.com", "active": False}]),
                "/api/v1/groups": FakeResponse(200, []),
            },
        )
        result = dm.add_datamodel_shares("SalesModel", [{"name": "alice@example.com", "type": "user", "permission": "EDIT"}])
        assert result["ok"] is False
        assert "could be resolved" in result["error"]
        assert result["skipped"][0]["name"] == "alice@example.com"
        assert "inactive" in result["skipped"][0]["reason"]

    def test_partial_skip_is_reported_in_the_success_dict(self):
        # One resolvable share + one unknown user: the write succeeds, and the
        # unknown user is reported in "skipped" instead of a log-only warning.
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/v1/users": FakeResponse(200, [{"_id": "u1", "email": "alice@example.com", "active": True}]),
                "/api/v1/groups": FakeResponse(200, []),
                "/api/elasticubes/localhost/SalesModel/permissions": FakeResponse(200, {"shares": []}),
            },
            put_responses={"/api/elasticubes/localhost/SalesModel/permissions": FakeResponse(200, {"success": True})},
        )
        result = dm.add_datamodel_shares(
            "SalesModel",
            [
                {"name": "alice@example.com", "type": "user", "permission": "EDIT"},
                {"name": "ghost@example.com", "type": "user", "permission": "USE"},
            ],
        )
        assert result["success"] is True
        assert result["new_shares"] == 1
        assert result["skipped"] == [{"name": "ghost@example.com", "type": "user", "reason": "User not found."}]

    def test_extract_returns_error_when_permissions_fetch_fails(self):
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/v1/users": FakeResponse(200, [{"_id": "u1", "email": "alice@example.com"}]),
                "/api/v1/groups": FakeResponse(200, []),
                # No /permissions endpoint → None → connection-failure dict
            },
        )
        result = dm.add_datamodel_shares("SalesModel", [{"name": "alice@example.com", "type": "user", "permission": "EDIT"}])
        assert result["ok"] is False
        assert "connection failed" in result["error"]


# ---------------------------------------------------------------------------
# get_data
# ---------------------------------------------------------------------------


class TestGetData:
    def test_returns_row_list_on_success(self):
        # get_data calls /api/datasources/{name}/sql directly (not get_datamodel)
        sql_result = {"headers": ["id", "name"], "values": [[1, "Alice"], [2, "Bob"]]}
        dm = _make_dm(
            get_responses={"/api/datasources/": FakeResponse(200, sql_result)},
        )
        result = dm.get_data("SalesModel", "orders")
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["id"] == 1

    def test_returns_error_dict_on_api_failure(self):
        dm = _make_dm()
        result = dm.get_data("SalesModel", "orders")
        assert result["ok"] is False
        assert "connection failed" in result["error"]


# ---------------------------------------------------------------------------
# get_row_count
# ---------------------------------------------------------------------------


class TestGetRowCount:
    def test_returns_list_with_total_row_when_no_tables(self):
        # Model exists but has no datasets/tables → returns list with just total
        dm = _make_dm(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT)},
        )
        result = dm.get_row_count("SalesModel")
        assert isinstance(result, list)

    def test_returns_error_dict_when_model_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.get_row_count("NoSuchModel")
        assert result["ok"] is False
        assert "error" in result


# ---------------------------------------------------------------------------
# resolve_datamodel_reference
# ---------------------------------------------------------------------------


class TestResolveDatamodelReference:
    def test_resolves_by_title_on_success(self):
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                # ID-based attempt will also hit /api/v2/datamodels/{ref}/schema
                "/api/v2/datamodels/SalesModel/schema": FakeResponse(404, {}),
            }
        )
        result = dm.resolve_datamodel_reference("SalesModel")
        assert result["success"] is True
        assert result["datamodel_id"] == "dm123"

    def test_returns_failure_when_not_found(self):
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(404, {}),
                "/api/v2/datamodels/NoSuchModel/schema": FakeResponse(404, {}),
            }
        )
        result = dm.resolve_datamodel_reference("NoSuchModel")
        assert result["success"] is False
        assert result["datamodel_id"] is None

    def test_resolves_by_id_when_id_lookup_succeeds(self):
        dm_by_id = {"oid": "dm123", "title": "SalesModel"}
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/dm123/schema": FakeResponse(200, dm_by_id),
            }
        )
        result = dm.resolve_datamodel_reference("dm123")
        assert result["success"] is True
        assert result["datamodel_id"] == "dm123"


# ---------------------------------------------------------------------------
# get_elasticubes
# ---------------------------------------------------------------------------

_ELASTICUBES = [
    {"title": "SalesCube", "address": "LocalHost", "fullname": "LocalHost/SalesCube"},
    {"title": "FinanceCube", "address": "LocalHost", "fullname": "LocalHost/FinanceCube"},
]


class TestGetElasticubes:
    def test_returns_list_on_success(self):
        dm = _make_dm(get_responses={"/api/v1/elasticubes/getElasticubes": FakeResponse(200, _ELASTICUBES)})
        result = dm.get_elasticubes()
        assert isinstance(result, list)
        assert len(result) == 2

    def test_returns_error_on_none_response(self):
        dm = _make_dm()
        result = dm.get_elasticubes()
        assert "error" in result

    def test_returns_error_on_non_200(self):
        dm = _make_dm(get_responses={"/api/v1/elasticubes/getElasticubes": FakeResponse(500, {})})
        result = dm.get_elasticubes()
        assert "error" in result


# ---------------------------------------------------------------------------
# load_datamodel
# ---------------------------------------------------------------------------

_LOAD_DM_RESPONSE = {"data": {"elasticubeByTitle": {"oid": "dm_oid_abc123", "__typename": "ElasticubeMetadata"}}}


class TestLoadDatamodel:
    def test_returns_oid_on_success(self):
        dm = _make_dm(post_responses={"/api/v2/ecm/": FakeResponse(200, _LOAD_DM_RESPONSE)})
        result = dm.load_datamodel("SalesCube")
        assert result.get("oid") == "dm_oid_abc123"

    def test_returns_error_when_model_not_in_response(self):
        empty = {"data": {"elasticubeByTitle": None}}
        dm = _make_dm(post_responses={"/api/v2/ecm/": FakeResponse(200, empty)})
        result = dm.load_datamodel("MissingCube")
        assert "error" in result

    def test_returns_error_on_none_response(self):
        dm = _make_dm()
        result = dm.load_datamodel("SalesCube")
        assert "error" in result

    def test_returns_error_on_non_200(self):
        dm = _make_dm(post_responses={"/api/v2/ecm/": FakeResponse(403, {})})
        result = dm.load_datamodel("SalesCube")
        assert "error" in result

    def test_error_includes_title(self):
        dm = _make_dm()
        result = dm.load_datamodel("MySpecialCube")
        assert "MySpecialCube" in result["error"]

    def test_returns_error_on_graphql_errors_in_200_response(self):
        graphql_error = {"data": None, "errors": [{"message": "Cube not found in ECM"}]}
        dm = _make_dm(post_responses={"/api/v2/ecm/": FakeResponse(200, graphql_error)})
        result = dm.load_datamodel("MissingCube")
        assert "error" in result
        assert "Cube not found in ECM" in result["error"]


# ---------------------------------------------------------------------------
# delete_datamodel
# ---------------------------------------------------------------------------


class TestDeleteDatamodel:
    def test_returns_success_on_200(self):
        dm = _make_dm(post_responses={"/api/v2/ecm/": FakeResponse(200, {"removeElasticube": True})})
        result = dm.delete_datamodel("SalesCube", "LocalHost")
        assert result == {"success": True}

    def test_returns_success_on_201(self):
        dm = _make_dm(post_responses={"/api/v2/ecm/": FakeResponse(201, {})})
        result = dm.delete_datamodel("SalesCube", "LocalHost")
        assert result == {"success": True}

    def test_returns_error_on_none_response(self):
        dm = _make_dm()
        result = dm.delete_datamodel("SalesCube", "LocalHost")
        assert "error" in result

    def test_error_includes_title(self):
        dm = _make_dm()
        result = dm.delete_datamodel("ImportantCube", "LocalHost")
        assert "ImportantCube" in result["error"]


# ---------------------------------------------------------------------------
# update_datasecurity
# ---------------------------------------------------------------------------

_DS_RULES = [{"table": "Orders", "column": "Region", "members": ["West"], "shares": [], "exclusionary": False}]

_EXTRACT_MODEL = {"oid": "dm_extract", "title": "SalesCube", "type": "extract"}
_LIVE_MODEL = {"oid": "dm_live", "title": "LiveModel", "type": "live"}


class TestUpdateDatasecurity:
    def test_returns_response_on_200(self):
        dm = _make_dm(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _EXTRACT_MODEL)},
            post_responses={"/api/elasticubes/localhost/SalesCube/datasecurity": FakeResponse(200, _DS_RULES)},
        )
        result = dm.update_datasecurity("SalesCube", _DS_RULES)
        assert "error" not in result

    def test_returns_error_when_model_not_found(self):
        dm = _make_dm()
        result = dm.update_datasecurity("SalesCube", _DS_RULES)
        assert "error" in result

    def test_returns_error_on_put_failure(self):
        dm = _make_dm(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _EXTRACT_MODEL)},
            put_responses={"/api/elasticubes/localhost/SalesCube/datasecurity": FakeResponse(403, {})},
        )
        result = dm.update_datasecurity("SalesCube", _DS_RULES)
        assert "error" in result

    def test_returns_error_on_wrong_model_type(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _LIVE_MODEL)})
        result = dm.update_datasecurity("LiveModel", _DS_RULES)
        assert "error" in result

    def test_returns_error_when_rules_not_a_list(self):
        dm = _make_dm()
        result = dm.update_datasecurity("SalesCube", {"bad": "input"})
        assert "error" in result

    def test_strips_server_managed_fields_before_posting(self):
        # Rules read back via get_datasecurity_raw carry server-managed fields
        # that the write API rejects — they must be stripped automatically.
        class RecordingClient(FakeApiClient):
            def post(self, url, data=None, **kwargs):
                self.last_post = (url, data)
                return super().post(url, data=data, **kwargs)

        client = RecordingClient(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _EXTRACT_MODEL)},
            post_responses={"/api/elasticubes/localhost/SalesCube/datasecurity": FakeResponse(200, {})},
            logger=FakeLogger(),
        )
        dm = DataModel(api_client=client)
        dirty_rule = {**_DS_RULES[0], "_id": "abc", "created": "2025-01-01", "lastModified": "2026-01-01", "importedIdIdentifier": "xyz"}
        result = dm.update_datasecurity("SalesCube", [dirty_rule])
        assert "error" not in result
        sent = client.last_post[1][0]
        assert not ({"_id", "created", "lastModified", "importedIdIdentifier"} & sent.keys())
        assert sent["table"] == "Orders"


# ---------------------------------------------------------------------------
# set_live_datasecurity_add_many
# ---------------------------------------------------------------------------


class TestSetLiveDatasecurityAddMany:
    def test_returns_response_on_200(self):
        dm = _make_dm(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _LIVE_MODEL)},
            post_responses={"/api/v1/elasticubes/live/LiveModel/datasecurity/addMany": FakeResponse(200, {"ok": True})},
        )
        result = dm.set_live_datasecurity_add_many("LiveModel", _DS_RULES)
        assert "error" not in result

    def test_returns_error_when_model_not_found(self):
        dm = _make_dm()
        result = dm.set_live_datasecurity_add_many("LiveModel", _DS_RULES)
        assert "error" in result

    def test_returns_error_on_wrong_model_type(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _EXTRACT_MODEL)})
        result = dm.set_live_datasecurity_add_many("SalesCube", _DS_RULES)
        assert "error" in result

    def test_returns_error_when_rules_not_a_list(self):
        dm = _make_dm()
        result = dm.set_live_datasecurity_add_many("LiveModel", {"bad": "input"})
        assert "error" in result

    def test_autofills_live_and_fullname(self):
        class RecordingClient(FakeApiClient):
            def post(self, url, data=None, **kwargs):
                self.last_post = (url, data)
                return super().post(url, data=data, **kwargs)

        client = RecordingClient(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _LIVE_MODEL)},
            post_responses={"/api/v1/elasticubes/live/LiveModel/datasecurity/addMany": FakeResponse(201, [{}])},
            logger=FakeLogger(),
        )
        dm = DataModel(api_client=client)
        result = dm.set_live_datasecurity_add_many("LiveModel", _DS_RULES)
        assert "error" not in result
        sent = client.last_post[1][0]
        assert sent["live"] is True
        assert sent["fullname"] == "live:LiveModel"

    def test_draft_model_failure_carries_published_hint(self):
        dm = _make_dm(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _LIVE_MODEL)},
            post_responses={"/api/v1/elasticubes/live/LiveModel/datasecurity/addMany": FakeResponse(500, {"status": "error", "message": "Elasticube has not been found"})},
        )
        result = dm.set_live_datasecurity_add_many("LiveModel", _DS_RULES)
        assert "must be published" in result["error"]


# ---------------------------------------------------------------------------
# delete_datasecurity
# ---------------------------------------------------------------------------


class TestDeleteDatasecurity:
    def test_deletes_extract_rule(self):
        dm = _make_dm(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _EXTRACT_MODEL)},
            delete_responses={"/api/elasticubes/localhost/SalesCube/datasecurity/Orders/Region": FakeResponse(200, {})},
        )
        assert dm.delete_datasecurity("SalesCube", "Orders", "Region") == {"success": True}

    def test_deletes_live_rule_on_204(self):
        dm = _make_dm(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _LIVE_MODEL)},
            delete_responses={"/api/v1/elasticubes/live/LiveModel/datasecurity/trips/zip": FakeResponse(204, None, text="")},
        )
        assert dm.delete_datasecurity("LiveModel", "trips", "zip") == {"success": True}

    def test_returns_error_on_failure(self):
        dm = _make_dm(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _EXTRACT_MODEL)},
            delete_responses={"/api/elasticubes/localhost/SalesCube/datasecurity/Orders/Region": FakeResponse(404, {"detail": "no rules"})},
        )
        result = dm.delete_datasecurity("SalesCube", "Orders", "Region")
        assert "no rules" in result["error"]
        assert result["status_code"] == 404


# ---------------------------------------------------------------------------
# get_datasecurity_raw
# ---------------------------------------------------------------------------


class TestGetDatasecurityRaw:
    def test_returns_raw_rules_with_explicit_type(self):
        dm = _make_dm(get_responses={"/api/elasticubes/localhost/SalesCube/datasecurity": FakeResponse(200, _DS_RULES)})
        result = dm.get_datasecurity_raw("SalesCube", datamodel_type="extract")
        assert result == _DS_RULES

    def test_explicit_type_skips_resolve_call(self):
        # No /api/v2/datamodels/schema fixture registered — would error if the resolve step ran.
        dm = _make_dm(get_responses={"/api/v1/elasticubes/live/LiveModel/datasecurity": FakeResponse(200, _DS_RULES)})
        result = dm.get_datasecurity_raw("LiveModel", datamodel_type="live")
        assert result == _DS_RULES

    def test_unsupported_type_returns_error(self):
        dm = _make_dm()
        result = dm.get_datasecurity_raw("SalesCube", datamodel_type="bogus")
        assert "error" in result

    def test_resolves_by_name_when_type_omitted(self):
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _EXTRACT_MODEL),
                "/api/elasticubes/localhost/SalesCube/datasecurity": FakeResponse(200, _DS_RULES),
            }
        )
        result = dm.get_datasecurity_raw("SalesCube")
        assert result == _DS_RULES

    def test_returns_error_when_model_not_found(self):
        dm = _make_dm()
        result = dm.get_datasecurity_raw("Ghost")
        assert "error" in result

    def test_returns_error_on_fetch_failure(self):
        dm = _make_dm(get_responses={"/api/elasticubes/localhost/SalesCube/datasecurity": FakeResponse(500, {"error": "boom"})})
        result = dm.get_datasecurity_raw("SalesCube", datamodel_type="extract")
        assert "error" in result


# ---------------------------------------------------------------------------
# get_datamodel_permissions_extract / get_datamodel_permissions_live
# ---------------------------------------------------------------------------


class TestGetDatamodelPermissionsExtract:
    def test_returns_raw_shares_list(self):
        dm = _make_dm(get_responses={"/api/elasticubes/localhost/SalesCube/permissions": FakeResponse(200, {"shares": [{"type": "user", "partyId": "u1", "permission": "a"}]})})
        result = dm.get_datamodel_permissions_extract("SalesCube")
        assert result == [{"type": "user", "partyId": "u1", "permission": "a"}]

    def test_returns_error_on_failure(self):
        dm = _make_dm(get_responses={"/api/elasticubes/localhost/SalesCube/permissions": FakeResponse(500, {"error": "boom"})})
        result = dm.get_datamodel_permissions_extract("SalesCube")
        assert "error" in result

    def test_no_response_returns_error(self):
        dm = _make_dm()
        result = dm.get_datamodel_permissions_extract("SalesCube")
        assert "error" in result


class TestGetDatamodelPermissionsLive:
    def test_returns_raw_shares_list(self):
        dm = _make_dm(get_responses={"/api/v1/elasticubes/live/dm_live/permissions": FakeResponse(200, [{"type": "group", "partyId": "g1", "permission": "a"}])})
        result = dm.get_datamodel_permissions_live("dm_live")
        assert result == [{"type": "group", "partyId": "g1", "permission": "a"}]

    def test_returns_error_on_failure(self):
        dm = _make_dm(get_responses={"/api/v1/elasticubes/live/dm_live/permissions": FakeResponse(500, {"error": "boom"})})
        result = dm.get_datamodel_permissions_live("dm_live")
        assert "error" in result


# ---------------------------------------------------------------------------
# update_datamodel_permissions_extract / update_datamodel_permissions_live
# ---------------------------------------------------------------------------


class TestUpdateDatamodelPermissionsExtract:
    def test_returns_response_on_success(self):
        dm = _make_dm(put_responses={"/api/elasticubes/localhost/SalesCube/permissions": FakeResponse(200, {"ok": True})})
        result = dm.update_datamodel_permissions_extract("SalesCube", [{"partyId": "u1", "type": "user", "permission": "a"}])
        assert "error" not in result

    def test_returns_error_on_failure(self):
        dm = _make_dm(put_responses={"/api/elasticubes/localhost/SalesCube/permissions": FakeResponse(403, {})})
        result = dm.update_datamodel_permissions_extract("SalesCube", [])
        assert "error" in result

    def test_returns_error_when_shares_not_a_list(self):
        dm = _make_dm()
        result = dm.update_datamodel_permissions_extract("SalesCube", {"bad": "input"})
        assert "error" in result


class TestUpdateDatamodelPermissionsLive:
    def test_returns_response_on_success(self):
        dm = _make_dm(patch_responses={"/api/v1/elasticubes/live/dm_live/permissions": FakeResponse(200, {"ok": True})})
        result = dm.update_datamodel_permissions_live("dm_live", [{"partyId": "g1", "type": "group", "permission": "a"}])
        assert "error" not in result

    def test_returns_error_on_failure(self):
        dm = _make_dm(patch_responses={"/api/v1/elasticubes/live/dm_live/permissions": FakeResponse(403, {})})
        result = dm.update_datamodel_permissions_live("dm_live", [])
        assert "error" in result

    def test_returns_error_when_shares_not_a_list(self):
        dm = _make_dm()
        result = dm.update_datamodel_permissions_live("dm_live", {"bad": "input"})
        assert "error" in result


# ---------------------------------------------------------------------------
# export_datamodel_schema
# ---------------------------------------------------------------------------


class TestExportDatamodelSchema:
    def test_returns_schema_on_success(self):
        dm = _make_dm(get_responses={"/api/v2/datamodel-exports/schema": FakeResponse(200, {"oid": "dm1", "title": "SalesCube"})})
        result = dm.export_datamodel_schema("dm1")
        assert result == {"oid": "dm1", "title": "SalesCube"}

    def test_returns_error_on_failure(self):
        dm = _make_dm(get_responses={"/api/v2/datamodel-exports/schema": FakeResponse(500, {"error": "boom"})})
        result = dm.export_datamodel_schema("dm1")
        assert "error" in result

    def test_windows_uses_stream_endpoint(self):
        dm = _make_dm(get_responses={"/api/v1/elasticubes/dm1/datamodel-exports/stream/schema": FakeResponse(200, {"oid": "dm1", "title": "SalesCube"})})
        dm.api_client.operating_system = "windows"
        result = dm.export_datamodel_schema("dm1", dependencies=["dataContext"])
        assert result == {"oid": "dm1", "title": "SalesCube"}

    def test_returns_error_on_non_dict_json(self):
        dm = _make_dm(get_responses={"/api/v2/datamodel-exports/schema": FakeResponse(200, ["not", "a", "dict"])})
        result = dm.export_datamodel_schema("dm1")
        assert "error" in result


# ---------------------------------------------------------------------------
# import_datamodel_schema
# ---------------------------------------------------------------------------


class TestImportDatamodelSchema:
    def test_plain_create_returns_datamodel_id(self):
        dm = _make_dm(post_responses={"/api/v2/datamodel-imports/schema": FakeResponse(201, {"oid": "new-oid"})})
        result = dm.import_datamodel_schema({"title": "SalesCube"})
        assert result == {"datamodel_id": "new-oid", "already_exists": False}

    def test_overwrite_targets_existing_id(self):
        dm = _make_dm(post_responses={"/api/v2/datamodel-imports/schema?datamodelId=dm1": FakeResponse(201, {"oid": "dm1"})})
        result = dm.import_datamodel_schema({"title": "SalesCube"}, action="overwrite", target_datamodel_id="dm1")
        assert result == {"datamodel_id": "dm1", "already_exists": False}

    def test_duplicate_uses_new_title_query(self):
        dm = _make_dm(post_responses={"/api/v2/datamodel-imports/schema?newTitle=SalesCube (Duplicate)": FakeResponse(201, {"oid": "dm2"})})
        result = dm.import_datamodel_schema({"title": "SalesCube"}, action="duplicate")
        assert result == {"datamodel_id": "dm2", "already_exists": False}

    def test_already_exists_conflict_is_flagged(self):
        dm = _make_dm(post_responses={"/api/v2/datamodel-imports/schema": FakeResponse(400, {"title": "ElasticubeAlreadyExists"})})
        result = dm.import_datamodel_schema({"title": "SalesCube"})
        assert result["already_exists"] is True
        assert "error" in result

    def test_other_failure_is_not_flagged_as_already_exists(self):
        dm = _make_dm(post_responses={"/api/v2/datamodel-imports/schema": FakeResponse(400, {"error": "bad request"})})
        result = dm.import_datamodel_schema({"title": "SalesCube"})
        assert result["already_exists"] is False
        assert "error" in result


# ---------------------------------------------------------------------------
# get_perspectives
# ---------------------------------------------------------------------------

_P_DEFAULT_A = {"oid": "p-def-a", "name": "Default", "isDefault": True, "parentOid": None, "datamodelOid": "dm-a", "tables": []}
_P_SALES = {"oid": "p-sales", "name": "Company Sales", "parentOid": "dm-a", "datamodelOid": "dm-a", "tables": [{"oid": "t1", "diffType": "include", "columnsDiff": [{"oid": "c1", "enabled": True}]}]}
_P_OPS = {"oid": "p-ops", "name": "Ops", "parentOid": "dm-b", "datamodelOid": "dm-b", "tables": []}
_PERSPECTIVES = [_P_DEFAULT_A, _P_SALES, None, _P_OPS, "junk"]


def _make_dm_with_perspectives(perspectives=_PERSPECTIVES, **extra_get):
    return _make_dm(
        get_responses={
            "/api/v2/perspectives": FakeResponse(200, perspectives),
            "/api/v2/datamodels/dm-a/schema": FakeResponse(200, {"oid": "dm-a", "title": "Model A"}),
            **extra_get,
        }
    )


class TestGetPerspectives:
    def test_no_arguments_lists_real_perspectives_only(self):
        result = _make_dm_with_perspectives().get_perspectives()
        assert [p["oid"] for p in result] == ["p-sales", "p-ops"]

    def test_include_default(self):
        result = _make_dm_with_perspectives().get_perspectives(include_default=True)
        assert [p["oid"] for p in result] == ["p-def-a", "p-sales", "p-ops"]

    def test_filter_by_datamodel_id(self):
        result = _make_dm_with_perspectives().get_perspectives(datamodel="dm-a")
        assert [p["oid"] for p in result] == ["p-sales"]

    def test_filter_by_datamodel_with_default(self):
        result = _make_dm_with_perspectives().get_perspectives(datamodel="dm-a", include_default=True)
        assert [p["oid"] for p in result] == ["p-def-a", "p-sales"]

    def test_unresolvable_datamodel_is_an_error(self):
        dm = _make_dm_with_perspectives(**{"/api/v2/datamodels/nope/schema": FakeResponse(404, {}), "/api/v2/datamodels/schema": FakeResponse(404, {})})
        result = dm.get_perspectives(datamodel="nope")
        assert result["ok"] is False and "nope" in result["error"]

    def test_lookup_by_name_is_case_insensitive_and_returns_the_object(self):
        result = _make_dm_with_perspectives().get_perspectives("company sales")
        assert result == [_P_SALES]

    def test_lookup_by_oid(self):
        assert _make_dm_with_perspectives().get_perspectives("p-ops") == [_P_OPS]

    def test_lookup_list_mixed_and_deduplicated(self):
        result = _make_dm_with_perspectives().get_perspectives(["Company Sales", "p-sales", "Ops"])
        assert [p["oid"] for p in result] == ["p-sales", "p-ops"]

    def test_explicit_request_can_name_a_default_perspective(self):
        assert _make_dm_with_perspectives().get_perspectives("p-def-a") == [_P_DEFAULT_A]

    def test_lookup_scoped_by_datamodel(self):
        result = _make_dm_with_perspectives().get_perspectives("Ops", datamodel="dm-a")
        assert result["ok"] is False and result["missing"] == ["Ops"] and result["results"] == []

    def test_missing_reference_fails_loudly_but_returns_what_was_found(self):
        result = _make_dm_with_perspectives().get_perspectives(["Company Sales", "no-such"])
        assert result["ok"] is False
        assert "no-such" in result["error"]
        assert result["missing"] == ["no-such"]
        assert result["results"] == [_P_SALES]

    def test_empty_reference_input_is_an_error(self):
        assert _make_dm_with_perspectives().get_perspectives("")["ok"] is False
        assert _make_dm_with_perspectives().get_perspectives([])["ok"] is False

    def test_api_failure_returns_error_dict(self):
        dm = _make_dm(get_responses={"/api/v2/perspectives": FakeResponse(500, {"message": "boom"})})
        result = dm.get_perspectives()
        assert result["ok"] is False and result["status_code"] == 500

    def test_no_response_returns_error_dict(self):
        assert _make_dm(get_responses={"/api/v2/perspectives": None}).get_perspectives()["ok"] is False

    def test_unexpected_structure_returns_error_dict(self):
        assert _make_dm(get_responses={"/api/v2/perspectives": FakeResponse(200, {"not": "a list"})}).get_perspectives()["ok"] is False

    def test_empty_instance_returns_empty_list(self):
        assert _make_dm(get_responses={"/api/v2/perspectives": FakeResponse(200, [])}).get_perspectives() == []
