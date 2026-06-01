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

    def test_returns_none_on_failure(self):
        dm = _make_dm()
        result = dm.create_connections({"name": "NewConn"})
        assert result is None


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

    def test_returns_empty_list_when_model_not_found(self):
        # get_datamodel_shares returns [] when model not found
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.get_datamodel_shares("NoSuchModel")
        assert result == []


# ---------------------------------------------------------------------------
# get_datasecurity
# ---------------------------------------------------------------------------


class TestGetDatasecurity:
    def test_returns_default_row_when_no_security_rules(self):
        datasecurity = []
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/elasticubes/localhost/SalesModel/datasecurity": FakeResponse(200, datasecurity),
            }
        )
        result = dm.get_datasecurity("SalesModel")
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["table_name"] == ""

    def test_returns_empty_list_when_model_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.get_datasecurity("NoSuchModel")
        assert result == []

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
    def test_returns_default_row_when_no_rules(self):
        dm = _make_dm(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT),
                "/api/elasticubes/localhost/SalesModel/datasecurity": FakeResponse(200, []),
            }
        )
        result = dm.get_datasecurity_detail("SalesModel")
        assert isinstance(result, list)
        assert len(result) == 1

    def test_returns_empty_list_when_model_not_found(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.get_datasecurity_detail("NoSuchModel")
        assert result == []


# ---------------------------------------------------------------------------
# update_datasecurity
# ---------------------------------------------------------------------------


class TestUpdateDatasecurity:
    def test_returns_response_on_success(self):
        rules = [{"table": "orders", "column": "region", "datatype": "text", "members": [], "shares": []}]
        dm = _make_dm(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT)},
            put_responses={
                "/api/elasticubes/localhost/SalesModel/datasecurity": FakeResponse(200, {"updated": True}),
            },
        )
        result = dm.update_datasecurity("SalesModel", rules)
        assert result["updated"] is True

    def test_returns_error_when_not_extract(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_LIVE)})
        result = dm.update_datasecurity("LiveModel", [])
        assert "error" in result

    def test_returns_error_when_payload_not_list(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT)})
        result = dm.update_datasecurity("SalesModel", {})
        assert "error" in result


# ---------------------------------------------------------------------------
# set_live_datasecurity_add_many
# ---------------------------------------------------------------------------


class TestSetLiveDatasecurityAddMany:
    def test_returns_response_on_success(self):
        rules = [{"table": "orders", "column": "region", "datatype": "text", "members": [], "shares": []}]
        dm = _make_dm(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_LIVE)},
            post_responses={
                "/api/v1/elasticubes/live/LiveModel/datasecurity/addMany": FakeResponse(200, {"added": 1}),
            },
        )
        result = dm.set_live_datasecurity_add_many("LiveModel", rules)
        assert result["added"] == 1

    def test_returns_error_when_not_live(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_EXTRACT)})
        result = dm.set_live_datasecurity_add_many("SalesModel", [])
        assert "error" in result

    def test_returns_error_when_payload_not_list(self):
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, _DATAMODEL_LIVE)})
        result = dm.set_live_datasecurity_add_many("LiveModel", {})
        assert "error" in result


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

    def test_returns_empty_list_on_api_failure(self):
        # get_data returns [] (not error dict) on failure
        dm = _make_dm()
        result = dm.get_data("SalesModel", "orders")
        assert result == []


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

    def test_returns_empty_list_when_model_not_found(self):
        # get_row_count returns [] (not error dict) when model not found
        dm = _make_dm(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, None)})
        result = dm.get_row_count("NoSuchModel")
        assert result == []


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
