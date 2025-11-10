import pytest
import datetime
from pyspark.sql.types import StringType, BooleanType, LongType, DoubleType, TimestampType
from api_ingestion.json_utils import JsonUtils

class TestJsonUtils:

    def test_flatten_json_simple_dict(self):
        nested = {"a": {"b": 123, "c": 456}}
        result = JsonUtils.flatten_json(nested)
        assert result == {"a.b": 123, "a.c": 456}

    def test_flatten_json_with_list(self):
        nested = {"a": [1, 2, 3], "b": {"c": "x"}}
        result = JsonUtils.flatten_json(nested)
        assert "a" in result
        assert isinstance(result["a"], str)
        assert result["b.c"] == "x"

    def test_get_nested_value_valid_path(self):
        data = {"a": {"b": {"c": 42}}}
        assert JsonUtils.get_nested_value(data, "a.b.c") == 42

    def test_get_nested_value_invalid_path(self):
        data = {"a": {"b": {"c": 42}}}
        assert JsonUtils.get_nested_value(data, "a.x.c") is None

    def test_get_nested_value_empty_path(self):
        data = {"a": 1}
        assert JsonUtils.get_nested_value(data, "") == data

    def test_infer_spark_type_boolean(self):
        assert isinstance(JsonUtils.infer_spark_type(True), BooleanType)

    def test_infer_spark_type_integer(self):
        assert isinstance(JsonUtils.infer_spark_type(123), LongType)

    def test_infer_spark_type_float(self):
        assert isinstance(JsonUtils.infer_spark_type(12.34), DoubleType)

    def test_infer_spark_type_timestamp(self):
        ts = datetime.datetime.now().isoformat()
        assert isinstance(JsonUtils.infer_spark_type(ts), TimestampType)

    def test_infer_spark_type_string(self):
        assert isinstance(JsonUtils.infer_spark_type("hello"), StringType)

    def test_convert_value_to_type_long(self):
        assert JsonUtils.convert_value_to_type("123", LongType()) == 123

    def test_convert_value_to_type_double(self):
        assert JsonUtils.convert_value_to_type("12.34", DoubleType()) == 12.34

    def test_convert_value_to_type_boolean_true(self):
        assert JsonUtils.convert_value_to_type("true", BooleanType()) is True

    def test_convert_value_to_type_boolean_false(self):
        assert JsonUtils.convert_value_to_type("no", BooleanType()) is False

    def test_convert_value_to_type_timestamp(self):
        ts = datetime.datetime.now().isoformat()
        result = JsonUtils.convert_value_to_type(ts, TimestampType())
        assert isinstance(result, datetime.datetime)

    def test_convert_value_to_type_invalid(self):
        assert JsonUtils.convert_value_to_type("abc", LongType()) is None