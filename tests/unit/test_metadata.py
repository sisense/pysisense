"""Unit tests for pysisense.metadata.Metadata."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.metadata import Metadata

_MEASURE = {
    "title": "Revenue Sum",
    "datasource": {"title": "SalesModel", "fullname": "localhost/SalesModel"},
}

_MEASURES_LIST = [_MEASURE]

_DIMENSIONS_LIST = [{"title": "Active Only", "datasource": {"title": "SalesModel"}}]

_DATASOURCES = [{"title": "SalesModel", "fullname": "localhost/SalesModel"}]


def _make_metadata(get_responses=None, post_responses=None):
    logger = FakeLogger()
    client = FakeApiClient(
        get_responses=get_responses,
        post_responses=post_responses,
        logger=logger,
    )
    return Metadata(api_client=client)


class TestMetadataInit:
    def test_creates_with_fake_client(self):
        meta = _make_metadata()
        assert meta is not None
        assert hasattr(meta, "api_client")
        assert hasattr(meta, "logger")


class TestGetDatasourceMeasures:
    def test_returns_measures_on_success(self):
        meta = _make_metadata(
            get_responses={"/api/metadata/measures": FakeResponse(200, _MEASURES_LIST)},
        )
        result = meta.get_datasource_measures(datasource="SalesModel", ds_full_name="localhost/SalesModel")
        assert isinstance(result, list)
        assert result[0]["title"] == "Revenue Sum"

    def test_returns_error_on_failure(self):
        meta = _make_metadata(
            get_responses={"/api/metadata/measures": FakeResponse(500, {"message": "error"})},
        )
        result = meta.get_datasource_measures()
        assert "error" in result


class TestGetDatasourceDimensions:
    def test_returns_dimensions_on_success(self):
        meta = _make_metadata(
            get_responses={"/api/metadata/dimensions": FakeResponse(200, _DIMENSIONS_LIST)},
        )
        result = meta.get_datasource_dimensions(datasource="SalesModel")
        assert isinstance(result, list)

    def test_returns_error_on_none_response(self):
        meta = _make_metadata()
        result = meta.get_datasource_dimensions()
        assert "error" in result


class TestGetDatasources:
    def test_returns_list_on_success(self):
        meta = _make_metadata(
            get_responses={"/api/datasources": FakeResponse(200, _DATASOURCES)},
        )
        result = meta.get_datasources()
        assert isinstance(result, list)
        assert result[0]["title"] == "SalesModel"

    def test_returns_error_on_failure(self):
        meta = _make_metadata(
            get_responses={"/api/datasources": FakeResponse(403, {"message": "forbidden"})},
        )
        result = meta.get_datasources()
        assert "error" in result


class TestAddDatasourceMeasure:
    def test_returns_measure_on_success(self):
        meta = _make_metadata(
            post_responses={"/api/metadata/": FakeResponse(201, _MEASURE)},
        )
        result = meta.add_datasource_measure(_MEASURE)
        assert result["title"] == "Revenue Sum"

    def test_returns_error_when_not_dict(self):
        meta = _make_metadata()
        result = meta.add_datasource_measure([])
        assert "error" in result

    def test_returns_error_on_post_failure(self):
        meta = _make_metadata(
            post_responses={"/api/metadata/": FakeResponse(400, {"error": "invalid"})},
        )
        result = meta.add_datasource_measure(_MEASURE)
        assert "error" in result


class TestPostMetadataQuery:
    def test_returns_result_on_success(self):
        query_result = {"values": [[1, 2]], "headers": ["a", "b"]}
        meta = _make_metadata(
            post_responses={"/api/metadata": FakeResponse(200, query_result)},
        )
        result = meta.post_metadata_query({"metadata": [{"jaql": {}}]})
        assert result["headers"] == ["a", "b"]

    def test_returns_error_when_not_dict(self):
        meta = _make_metadata()
        result = meta.post_metadata_query([])
        assert "error" in result

    def test_uses_metadata_endpoint_without_trailing_slash(self):
        meta = _make_metadata(
            post_responses={"/api/metadata": FakeResponse(200, {"ok": True})},
        )
        result = meta.post_metadata_query({"query": "test"})
        assert result["ok"] is True
