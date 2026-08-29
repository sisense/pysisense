"""Unit tests for the shared error-extraction contract.

Covers the `_extract_error_message` helper directly and, through a sample of
migrated methods, the failure-dict contract: "error" carries a human-readable
message ending with the HTTP status, "status_code" is added when a status is
available, connection failures use network wording with no invented status,
and secrets in error bodies are redacted.
"""

from __future__ import annotations

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.dashboard import Dashboard
from pysisense.datamodel import DataModel
from pysisense.encryption import Encryption
from pysisense.folder import Folder
from pysisense.utils import _extract_error_message


class _NonJsonResponse:
    """Response stand-in whose body is not JSON."""

    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text

    def json(self):
        raise ValueError("not JSON")


class _ClientWithDomain:
    domain = "sisense.example.com"


# ---------------------------------------------------------------------------
# Helper contract
# ---------------------------------------------------------------------------


class TestExtractErrorMessage:
    def test_http_error_with_detail_body(self):
        response = FakeResponse(403, {"detail": "Access denied: admin role required"})
        failure = _extract_error_message(response, "Failed to retrieve dashboards")
        assert failure["error"] == "Failed to retrieve dashboards: Access denied: admin role required (HTTP 403)"
        assert failure["status_code"] == 403

    def test_body_key_preference_order(self):
        assert "from-detail" in _extract_error_message(FakeResponse(400, {"detail": "from-detail", "message": "from-message"}), "x")["error"]
        assert "from-message" in _extract_error_message(FakeResponse(400, {"message": "from-message", "title": "from-title"}), "x")["error"]
        assert "from-title" in _extract_error_message(FakeResponse(400, {"title": "from-title", "error": "from-error"}), "x")["error"]
        assert "from-error" in _extract_error_message(FakeResponse(400, {"error": "from-error"}), "x")["error"]

    def test_no_response_uses_network_wording_without_status(self):
        failure = _extract_error_message(None, "Failed to retrieve dashboards")
        assert failure["error"] == "Failed to retrieve dashboards: no response from the Sisense server — connection failed"
        assert "status_code" not in failure

    def test_no_response_names_the_domain_when_available(self):
        failure = _extract_error_message(None, "Failed to encrypt", _ClientWithDomain())
        assert "no response from sisense.example.com" in failure["error"]
        assert "status_code" not in failure

    def test_empty_error_body_is_said_explicitly(self):
        response = FakeResponse(502, None, text="")
        failure = _extract_error_message(response, "Failed to deploy")
        assert failure["error"] == "Failed to deploy: the server returned an empty error body (HTTP 502)"
        assert failure["status_code"] == 502

    def test_non_json_body_is_relayed_as_text(self):
        failure = _extract_error_message(_NonJsonResponse(500, "Internal Server Error"), "Failed to create DataModel 'X'")
        assert failure["error"] == "Failed to create DataModel 'X': Internal Server Error (HTTP 500)"
        assert failure["status_code"] == 500

    def test_long_raw_body_is_truncated(self):
        failure = _extract_error_message(_NonJsonResponse(500, "x" * 1000), "context")
        assert "x" * 300 + "…" in failure["error"]
        assert "x" * 301 not in failure["error"]

    def test_secrets_in_error_body_are_redacted(self):
        response = FakeResponse(400, {"code": 1, "password": "hunter2", "token": "sekret-token"})
        failure = _extract_error_message(response, "Failed to create user")
        assert "hunter2" not in failure["error"]
        assert "sekret-token" not in failure["error"]
        assert "***REDACTED***" in failure["error"]


# ---------------------------------------------------------------------------
# Migrated methods honor the contract
# ---------------------------------------------------------------------------


class TestMigratedMethodsContract:
    def test_get_all_dashboards_relays_sisense_reason_and_status(self):
        client = FakeApiClient(get_responses={"/api/v1/dashboards/admin": FakeResponse(403, {"detail": "Access denied: admin role required"})}, logger=FakeLogger())
        result = Dashboard(api_client=client).get_all_dashboards()
        assert "Access denied: admin role required" in result["error"]
        assert "(HTTP 403)" in result["error"]
        assert result["status_code"] == 403

    def test_get_all_dashboards_connection_failure_has_no_status(self):
        client = FakeApiClient(get_responses={"/api/v1/dashboards/admin": None}, logger=FakeLogger())
        result = Dashboard(api_client=client).get_all_dashboards()
        assert "connection failed" in result["error"]
        assert "status_code" not in result

    def test_get_folders_empty_error_body(self):
        client = FakeApiClient(get_responses={"/api/v1/folders": FakeResponse(500, None, text="")}, logger=FakeLogger())
        result = Folder(api_client=client).get_folders()
        assert "empty error body" in result["error"]
        assert result["status_code"] == 500

    def test_encrypt_error_body_secrets_are_redacted(self):
        client = FakeApiClient(post_responses={"/api/v1/encryption/encrypt": FakeResponse(400, {"reason": "bad", "password": "hunter2"})}, logger=FakeLogger())
        result = Encryption(api_client=client).encrypt({"value": "x"})
        assert "hunter2" not in result["error"]
        assert result["status_code"] == 400

    def test_setup_datamodel_failure_carries_partial_state(self):
        dm = DataModel(api_client=FakeApiClient(logger=FakeLogger()))
        dm.create_datamodel = lambda **kwargs: {"datamodel_id": "dm1"}
        dm.create_dataset = lambda **kwargs: {"oid": "ds1"}

        def fake_create_table(**kwargs):
            if kwargs["table_name"] == "t2":
                return {"error": "Failed to create table 't2' in DataModel 'M': boom (HTTP 400)", "status_code": 400}
            return {"oid": f"tbl-{kwargs['table_name']}"}

        dm.create_table = fake_create_table
        result = dm.setup_datamodel(
            datamodel_name="M",
            datamodel_type="extract",
            connection_name="c",
            database_name="db",
            schema_name="s",
            tables=[{"table_name": "t1"}, {"table_name": "t2"}],
        )
        assert "error" in result
        assert result["status_code"] == 400
        assert result["datamodel_id"] == "dm1"
        assert result["dataset_id"] == "ds1"
        assert result["created_tables"] == ["t1"]
