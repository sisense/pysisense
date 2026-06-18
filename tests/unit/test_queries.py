"""Unit tests for pysisense.queries.Queries."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.queries import Queries

_JAQL_PAYLOAD = {
    "metadata": [{"jaql": {"dim": "[Orders].[Amount]", "agg": "sum"}}],
    "datasource": {"title": "SalesModel"},
}

_JAQL_RESULT = {"headers": ["Amount"], "values": [[100]]}

_SQL_PAYLOAD = {"query": "SELECT 1"}


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


class TestElasticubesRunJaqlCsv:
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


class TestElasticubeRunSqlQuery:
    def test_returns_result_on_success(self):
        sql_result = {"headers": ["col"], "values": [[1]]}
        q = _make_queries(
            post_responses={
                "/api/elasticubes/SalesModel/Sql": FakeResponse(200, sql_result),
            },
        )
        result = q.elasticube_run_sql_query("SalesModel", _SQL_PAYLOAD)
        assert result["headers"] == ["col"]

    def test_returns_error_on_failure(self):
        q = _make_queries(
            post_responses={
                "/api/elasticubes/SalesModel/Sql": FakeResponse(500, {"message": "error"}),
            },
        )
        result = q.elasticube_run_sql_query("SalesModel", _SQL_PAYLOAD)
        assert "error" in result
