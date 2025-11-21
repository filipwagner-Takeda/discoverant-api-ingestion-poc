import types
import json
import pytest
import time as _time

import api_ingestion.utils as api_utils
import api_ingestion.constants as constants
from api_ingestion.connector import RestDataSource, RestDataSourceReader
from api_ingestion.json_utils import JsonUtils
from pyspark.sql.types import StructType, StructField, StringType


class FakeResponse:
    def __init__(self, status_code=200, json_obj=None, text="OK"):
        self.status_code = status_code
        self._json = json_obj if json_obj is not None else {}
        self.text = text

    def json(self):
        return self._json


class FakeSession:
    """A fake requests.Session that yields responses from a queue."""
    def __init__(self, queue):
        self.headers = {}
        self.queue = queue[:]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, auth=None, params=None, timeout=10):
        if not self.queue:
            return FakeResponse(200, {})
        return self.queue.pop(0)


@pytest.fixture
def stub_constants(monkeypatch):
    """Provide small constants so tests are fast and predictable."""
    monkeypatch.setattr(constants, "RETRIES", 2, raising=False)
    monkeypatch.setattr(constants, "BACKOFF_FACTOR", 0, raising=False)
    monkeypatch.setattr(constants, "THROTTLE", 0, raising=False)
    monkeypatch.setattr(constants, "MAX_PAGES", 5, raising=False)
    monkeypatch.setattr(constants, "CHUNK_SIZE", 2, raising=False)
    return constants


@pytest.fixture
def stub_apiutils(monkeypatch):
    """Used to make pagination probing predictable -> set it to 3 pages."""
    def fake_find_valid_pages(url, start, max_pages, page_param):
        return 3
    monkeypatch.setattr(api_utils.ApiUtils, "find_valid_pages", staticmethod(fake_find_valid_pages))
    return api_utils


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(_time, "sleep", lambda *_: None)


def test_fetch_fields_from_api_with_json_path(monkeypatch):
    payload = {"data": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]}
    monkeypatch.setattr(
        "api_ingestion.connector.requests.Session",
        lambda: FakeSession([FakeResponse(200, payload)])
    )
    ds = RestDataSource(options={})
    fields = ds.fetch_fields_from_api(
        base_url="https://api.example.com",
        endpoint="/items",
        json_path="data",
        pagination=False,
    )
    names = {f.name for f in fields}
    assert {"id", "name"} <= names


def test_schema_infer_true_calls_fetch_fields(monkeypatch):
    returned_fields = [StructField("id", StringType()), StructField("name", StringType())]
    def fake_fetch_fields_from_api(self, **kwargs):
        return returned_fields
    monkeypatch.setattr(RestDataSource, "fetch_fields_from_api", fake_fetch_fields_from_api, raising=False)
    ds = RestDataSource(
        options={
            "infer_schema": "true",
            "base_url": "https://api.example.com",
            "endpoint": "/items",
        }
    )
    schema = ds.schema()
    assert isinstance(schema, StructType)
    assert [f.name for f in schema.fields] == ["id", "name"]


def test_schema_static_path(monkeypatch):
    def fake_load_schema(path):
        return StructType([StructField("a", StringType()), StructField("b", StringType())])
    monkeypatch.setattr(JsonUtils, "load_spark_schema_from_json", staticmethod(fake_load_schema))
    ds = RestDataSource(
        options={
            "infer_schema": "false",
            "schema_path": "/tmp/schema.json",
        }
    )
    schema = ds.schema()
    assert [f.name for f in schema.fields] == ["a", "b"]


def test_reader_page_strategy_reads(monkeypatch, stub_constants, stub_apiutils, no_sleep):
    payloads = [
        [{"id": "1", "name": "p1"}],
        [{"id": "2", "name": "p2"}],
        [{"id": "3", "name": "p3"}],
    ]
    responses = [FakeResponse(200, p) for p in payloads]
    monkeypatch.setattr(
        "api_ingestion.connector.requests.Session",
        lambda: FakeSession(responses)
    )
    monkeypatch.setattr(JsonUtils, "flatten_json", staticmethod(lambda d: d))
    monkeypatch.setattr(JsonUtils, "convert_value_to_type", staticmethod(lambda v, t: v))
    schema = StructType([StructField("id", StringType()), StructField("name", StringType())])
    rdr = RestDataSourceReader(
        schema=schema,
        options={
            "base_url": "https://api.example.com",
            "endpoint": "/items",
            "pagination": "true",
            "partitioning_strategy": "page",
            "chunk_size": 2,
        },
    )
    part = types.SimpleNamespace(value=(1, 3))
    rows = list(rdr.read(part))
    assert rows == [("1", "p1"), ("2", "p2"), ("3", "p3")]


def test_reader_param_strategy_reads(monkeypatch, stub_constants, no_sleep):
    responses = [FakeResponse(200, [{"id": "x", "v": "ok"}])]
    monkeypatch.setattr(
        "api_ingestion.connector.requests.Session",
        lambda: FakeSession(responses)
    )
    monkeypatch.setattr(JsonUtils, "flatten_json", staticmethod(lambda d: d))
    monkeypatch.setattr(JsonUtils, "convert_value_to_type", staticmethod(lambda v, t: v))
    schema = StructType([StructField("id", StringType()), StructField("v", StringType())])
    rdr = RestDataSourceReader(
        schema=schema,
        options={
            "base_url": "https://api.example.com",
            "endpoint": "/item",
            "pagination": "false",
            "partitioning_strategy": "param",
            "param_key": "item_id",
            "params": json.dumps({"size": "S"}),
        },
    )
    partition = types.SimpleNamespace(value=["A42"])
    rows = list(rdr.read(partition))
    assert rows == [("x", "ok")]


def test_read_retries_on_error_then_succeeds(monkeypatch, stub_constants, no_sleep):
    responses = [
        FakeResponse(500, None, "boom"),
        FakeResponse(200, [{"id": "1"}]),
    ]
    monkeypatch.setattr(
        "api_ingestion.connector.requests.Session",
        lambda: FakeSession(responses)
    )
    monkeypatch.setattr(JsonUtils, "flatten_json", staticmethod(lambda d: d))
    monkeypatch.setattr(JsonUtils, "convert_value_to_type", staticmethod(lambda v, t: v))
    schema = StructType([StructField("id", StringType())])
    rdr = RestDataSourceReader(
        schema=schema,
        options={
            "base_url": "https://api.example.com",
            "endpoint": "/retry",
            "pagination": "false",
        },
    )
    part = types.SimpleNamespace(value=0)
    rows = list(rdr.read(part))
    assert rows == [("1",)]


def test_partitions_page(stub_apiutils):
    rdr = RestDataSourceReader(
        schema=StructType([]),
        options={
            "base_url": "x",
            "endpoint": "y",
            "pagination": "true",
            "partitioning_strategy": "page",
            "chunk_size": 2
        },
    )
    parts = rdr.partitions()
    values = [p.value for p in parts]
    assert values == [(1, 2), (3, 3)]


def test_partitions_param_chunking():
    rdr = RestDataSourceReader(
        schema=StructType([]),
        options={
            "base_url": "x",
            "endpoint": "y",
            "partitioning_strategy": "param",
            "param_list": "a,b,c,d,e",
            "chunk_size": 2,
        },
    )
    parts = rdr.partitions()
    assert [p.value for p in parts] == [["a", "b"], ["c", "d"], ["e"]]


def test_chunk_list_staticmethod():
    data = ["a", "b", "c", "d", "e"]
    out = list(RestDataSourceReader._chunk_list(data, 2))
    assert out == [["a", "b"], ["c", "d"], ["e"]]
