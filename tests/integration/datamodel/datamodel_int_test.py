import os

import pytest

from pysisense.datamodel import DataModel
from pysisense.sisenseclient import SisenseClient

CONFIG_PATH = os.environ.get("PYSISENSE_CONFIG_PATH", "config.yaml")


def _make_client() -> SisenseClient:
    if not os.path.exists(CONFIG_PATH):
        pytest.skip(f"Config file '{CONFIG_PATH}' not found; skipping integration test.")
    return SisenseClient(config_file=CONFIG_PATH, debug=False)


@pytest.mark.integration
def test_get_all_datamodel_returns_list() -> None:
    """get_all_datamodel should return a non-empty list of data model dicts."""
    datamodel = DataModel(api_client=_make_client())
    result = datamodel.get_all_datamodel()

    assert isinstance(result, list), f"Expected list, got: {result}"
    assert len(result) > 0, "Expected at least one data model."

    first = result[0]
    assert "oid" in first or "_id" in first
    assert "title" in first


@pytest.mark.integration
def test_get_datamodel_returns_model() -> None:
    """get_datamodel should return the correct model dict for a known title."""
    client = _make_client()
    datamodel = DataModel(api_client=client)

    all_models = datamodel.get_all_datamodel()
    if not isinstance(all_models, list) or not all_models:
        pytest.skip("No data models available.")

    title = all_models[0].get("title", "")
    if not title:
        pytest.skip("First data model has no title.")

    result = datamodel.get_datamodel(title)

    assert isinstance(result, dict)
    assert result.get("title") == title


@pytest.mark.integration
def test_get_datamodel_shares_returns_data() -> None:
    """get_datamodel_shares should return share data for a known model."""
    client = _make_client()
    datamodel = DataModel(api_client=client)

    all_models = datamodel.get_all_datamodel()
    if not isinstance(all_models, list) or not all_models:
        pytest.skip("No data models available.")

    title = all_models[0].get("title", "")
    if not title:
        pytest.skip("First data model has no title.")

    result = datamodel.get_datamodel_shares(title)

    assert isinstance(result, (dict, list))


@pytest.mark.integration
def test_get_datasecurity_returns_data() -> None:
    """get_datasecurity should return a non-empty list (at minimum the no-rules sentinel row)."""
    client = _make_client()
    datamodel = DataModel(api_client=client)

    all_models = datamodel.get_all_datamodel()
    if not isinstance(all_models, list) or not all_models:
        pytest.skip("No data models available.")

    title = all_models[0].get("title", "")
    if not title:
        pytest.skip("First data model has no title.")

    result = datamodel.get_datasecurity(title)

    # get_datasecurity always returns at least one row (sentinel when no RLS rules exist)
    assert isinstance(result, list)
    assert len(result) >= 1
