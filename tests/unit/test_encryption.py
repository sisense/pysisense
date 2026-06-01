"""Unit tests for pysisense.encryption.Encryption."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.encryption import Encryption


def _make_encryption(get_responses=None, post_responses=None):
    logger = FakeLogger()
    client = FakeApiClient(
        get_responses=get_responses,
        post_responses=post_responses,
        logger=logger,
    )
    return Encryption(api_client=client)


class TestEncryptionInit:
    def test_creates_with_fake_client(self):
        enc = _make_encryption()
        assert enc is not None
        assert hasattr(enc, "api_client")
        assert hasattr(enc, "logger")


class TestEncrypt:
    def test_returns_result_on_success(self):
        enc = _make_encryption(
            post_responses={
                "/api/v1/encryption/encrypt": FakeResponse(200, {"value": "encrypted_blob"}),
            },
        )
        result = enc.encrypt({"value": "plaintext"})
        assert result["value"] == "encrypted_blob"

    def test_returns_error_when_not_dict(self):
        enc = _make_encryption()
        result = enc.encrypt([])
        assert "error" in result

    def test_returns_error_on_failure(self):
        enc = _make_encryption(
            post_responses={
                "/api/v1/encryption/encrypt": FakeResponse(500, {"message": "error"}),
            },
        )
        result = enc.encrypt({"value": "plaintext"})
        assert "error" in result


class TestDecrypt:
    def test_returns_result_on_success(self):
        enc = _make_encryption(
            post_responses={
                "/api/v1/encryption/decrypt": FakeResponse(200, {"value": "decrypted_text"}),
            },
        )
        result = enc.decrypt({"value": "encrypted_blob"})
        assert result["value"] == "decrypted_text"

    def test_returns_error_on_failure(self):
        enc = _make_encryption(
            post_responses={
                "/api/v1/encryption/decrypt": FakeResponse(400, {"error": "invalid"}),
            },
        )
        result = enc.decrypt({"value": "bad"})
        assert "error" in result
