from typing import Any, List
import json
import datetime
import os
import pytest

from api_ingestion.json_utils import JsonUtils

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    LongType,
    DoubleType,
    TimestampType,
)
@pytest.fixture
def sample_json() -> dict:
    return {
        "a": {
            "b": [
                {"c": 1},
                {"c": 2},
                {"d": 3},
            ]
        },
        "x": [
            [ {"y": 10}, {"y": 20} ],
            [ {"y": 30} ],
        ],
        "list": [1, 2, 3],
        "mixed": {
            "s": "hello",
            "n": 42,
            "f": 3.14,
            "b": True,
            "t": "2023-07-01T12:34:56",
            "obj": {"k": "v"},
            "arr": [1, 2, 3],
        },
    }


class TestJsonUtils:
    """
    Test case suite for JsonUtils
    Tests for spark type inferrence skipped since I couldn't get the mechanism to work
    - everything is inferred as StringType
    """

    def test_flatten_json_dict_and_lists(self, sample_json):
        flat = JsonUtils.flatten_json(sample_json)
        # dict keys are dotted
        assert "a.b" in flat
        assert "x" in flat
        assert "list" in flat
        assert "mixed.s" in flat
        assert "mixed.obj.k" in flat

        for key in ("a.b", "x", "list", "mixed.arr"):
            assert isinstance(flat[key], str)
            json.loads(flat[key])

        assert flat["mixed.s"] == "hello"
        assert flat["mixed.n"] == 42
        assert flat["mixed.f"] == 3.14
        assert flat["mixed.b"] is True

    def test_flatten_json_list_root(self):
        nested = [1, {"a": 2}, [3, 4]]
        flat = JsonUtils.flatten_json(nested, parent_key="root")

        assert "root" in flat
        assert isinstance(flat["root"], str)
        assert json.loads(flat["root"]) == nested


    def test_iter_jsonpath_expands_lists(self, sample_json):
        values = list(JsonUtils.iter_jsonpath(sample_json, "a.b.c"))
        assert values == [1, 2]

        values = list(JsonUtils.iter_jsonpath(sample_json, "x.y"))
        assert values == [10, 20, 30]

    def test_iter_jsonpath_terminal_list_yields_each_element(self, sample_json):
        values = list(JsonUtils.iter_jsonpath(sample_json, "list"))
        assert values == [1, 2, 3]

    def test_iter_jsonpath_none_or_empty_path_returns_whole_object(self, sample_json):
        values_none = list(JsonUtils.iter_jsonpath(sample_json, None))
        values_empty = list(JsonUtils.iter_jsonpath(sample_json, ""))

        assert len(values_none) == 1 and values_none[0] is sample_json
        assert len(values_empty) == 1 and values_empty[0] is sample_json

    def test_iter_jsonpath_path_not_found(self, sample_json):
        values = list(JsonUtils.iter_jsonpath(sample_json, "a.z.w"))
        assert values == []

        first = JsonUtils.first_jsonpath(sample_json, "a.z.w")
        assert first is None

    def test_first_jsonpath_returns_first_match(self, sample_json):
        first = JsonUtils.first_jsonpath(sample_json, "a.b.c")
        assert first == 1

    def test_load_serialized_json_roundtrip(self):
        data = {"x": [1, 2], "y": {"z": True}}
        s = json.dumps(data)
        loaded = JsonUtils.load_serialized_json(s)
        assert loaded == data

    def test_load_spark_schema_from_json(self, tmp_path):
        schema_dict = {
            "fields": [
                {"name": "id", "type": "long", "nullable": False},
                {"name": "name", "type": "string", "nullable": True},
                {"name": "score", "type": "double", "nullable": True},
                {"name": "flag", "type": "boolean", "nullable": True},
                {"name": "ts", "type": "timestamp", "nullable": True},
                {"name": "unknown", "type": "whatever", "nullable": True},  # should default to StringType
            ]
        }
        p = tmp_path / "schema.json"
        p.write_text(json.dumps(schema_dict))

        schema: StructType = JsonUtils.load_spark_schema_from_json(str(p))
        assert isinstance(schema, StructType)
        fields_by_name = {f.name: f for f in schema.fields}

        assert isinstance(fields_by_name["id"].dataType, LongType)
        assert fields_by_name["id"].nullable is False

        assert isinstance(fields_by_name["name"].dataType, StringType)
        assert isinstance(fields_by_name["score"].dataType, DoubleType)
        assert isinstance(fields_by_name["flag"].dataType, BooleanType)
        assert isinstance(fields_by_name["ts"].dataType, TimestampType)

        assert isinstance(fields_by_name["unknown"].dataType, StringType)

    @pytest.mark.parametrize(
        "spark_type,value,expected",
        [
            (LongType(), "123", 123),
            (LongType(), 456, 456),
            (LongType(), "bad", None),

            (DoubleType(), "3.14", 3.14),
            (DoubleType(), 2.5, 2.5),
            (DoubleType(), "bad", None),

            (BooleanType(), True, True),
            (BooleanType(), "true", True),
            (BooleanType(), "Yes", True),
            (BooleanType(), "1", True),
            (BooleanType(), "false", False),
            (BooleanType(), "0", False),

            (TimestampType(), "2023-01-02T03:04:05", datetime.datetime(2023, 1, 2, 3, 4, 5)),
            (TimestampType(), datetime.datetime(2020, 1, 1, 0, 0, 0), datetime.datetime(2020, 1, 1, 0, 0, 0)),
            (TimestampType(), "not-a-time", None),

            (StringType(), 789, "789"),
            (StringType(), None, None),
        ]
    )
    def test_convert_value_to_type(self, spark_type, value, expected):
        out = JsonUtils.convert_value_to_type(value, spark_type)
        assert out == expected

    def test_convert_value_to_type_boolean_edge_cases(self):
        assert JsonUtils.convert_value_to_type("T", BooleanType()) is True
        assert JsonUtils.convert_value_to_type("F", BooleanType()) is False
        assert JsonUtils.convert_value_to_type("TrUe", BooleanType()) is True
        assert JsonUtils.convert_value_to_type("No", BooleanType()) is False
