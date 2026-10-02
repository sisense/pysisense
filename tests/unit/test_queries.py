"""Unit tests for pysisense.queries.Queries."""

import json

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.queries import Queries

_JAQL_PAYLOAD = {
    "metadata": [{"jaql": {"dim": "[Orders].[Amount]", "agg": "sum"}}],
    "datasource": {"title": "SalesModel"},
}

_JAQL_RESULT = {"headers": ["Amount"], "values": [[100]]}


def _make_queries(post_responses=None):
    logger = FakeLogger()
    client = FakeApiClient(post_responses=post_responses, logger=logger)
    return Queries(api_client=client)


class TestQueriesInit:
    def test_creates_with_fake_client(self):
        q = _make_queries()
        assert q is not None
        assert hasattr(q, "api_client")


class TestElasticubeRunJaqlQuery:
    def test_returns_result_on_success(self):
        q = _make_queries(
            post_responses={
                "/api/datasources/SalesModel/jaql": FakeResponse(200, _JAQL_RESULT),
            },
        )
        result = q.elasticube_run_jaql_query("SalesModel", _JAQL_PAYLOAD)
        assert result["headers"] == ["Amount"]

    def test_returns_error_when_payload_not_dict(self):
        q = _make_queries()
        result = q.elasticube_run_jaql_query("SalesModel", [])
        assert "error" in result


class _RecordingApiClient(FakeApiClient):
    """FakeApiClient that also remembers the keyword arguments of each post()."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.post_calls: list[tuple[str, dict]] = []

    def post(self, url, data=None, **kwargs):
        self.post_calls.append((url, {"data": data, **kwargs}))
        return super().post(url, data=data, **kwargs)


class TestElasticubesRunJaqlCsv:
    def test_sends_jaql_as_the_data_form_field_not_a_json_body(self):
        # The CSV endpoint reads its JAQL from a URL-encoded form field named
        # ``data`` (what the Sisense UI's export sends). A JSON body is answered
        # with HTTP 400 '"undefined" is not valid JSON'.
        logger = FakeLogger()
        client = _RecordingApiClient(
            post_responses={"/api/datasources/SalesModel/jaql/csv": FakeResponse(200, "a,b\n1,2")},
            logger=logger,
        )
        Queries(api_client=client).elasticubes_run_jaql_csv("SalesModel", _JAQL_PAYLOAD)

        assert len(client.post_calls) == 1
        url, kwargs = client.post_calls[0]
        assert url == "/api/datasources/SalesModel/jaql/csv"
        assert kwargs["data"] is None, "must not send a JSON body"
        assert set(kwargs["form"]) == {"data"}
        assert json.loads(kwargs["form"]["data"]) == _JAQL_PAYLOAD

    def test_returns_csv_text_on_non_json_response(self):
        q = _make_queries(
            post_responses={
                "/api/datasources/SalesModel/jaql/csv": FakeResponse(200, "a,b\n1,2"),
            },
        )
        result = q.elasticubes_run_jaql_csv("SalesModel", _JAQL_PAYLOAD)
        assert result == "a,b\n1,2"

    def test_returns_json_when_parseable(self):
        q = _make_queries(
            post_responses={
                "/api/datasources/SalesModel/jaql/csv": FakeResponse(200, {"csv": "data"}),
            },
        )
        result = q.elasticubes_run_jaql_csv("SalesModel", _JAQL_PAYLOAD)
        assert result["csv"] == "data"
