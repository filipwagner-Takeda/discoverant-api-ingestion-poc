import pytest
import datetime
import json
import tempfile
import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, TimestampType
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

    @pytest.fixture
    def create_json_file(self):
        def _create_json_file(content: dict):
            tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
            with open(tmp_file.name, "w") as f:
                json.dump(content, f)
            return tmp_file.name
        return _create_json_file

    def test_valid_schema(self,create_json_file):
        schema_content = {
            "fields": [
                {"name": "id", "type": "long", "nullable": False},
                {"name": "name", "type": "string", "nullable": True},
                {"name": "price", "type": "double"}
            ]
        }
        json_path = create_json_file(schema_content)
        schema = JsonUtils.load_spark_schema_from_json(json_path)

        expected = StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("price", DoubleType(), True)
        ])
        assert schema == expected

    def test_missing_fields_key(self,create_json_file):
        schema_content = {}
        json_path = create_json_file(schema_content)
        schema = JsonUtils.load_spark_schema_from_json(json_path)
        assert schema == StructType([])

    def test_unknown_type_defaults_to_string(self,create_json_file):
        schema_content = {
            "fields": [{"name": "unknown_field", "type": "unknown"}]
        }
        json_path = create_json_file(schema_content)
        schema = JsonUtils.load_spark_schema_from_json(json_path)
        assert schema.fields[0].dataType == StringType()

    def test_nullable_flag(self,create_json_file):
        schema_content = {
            "fields": [{"name": "flag", "type": "boolean", "nullable": False}]
        }
        json_path = create_json_file(schema_content)
        schema = JsonUtils.load_spark_schema_from_json(json_path)
        assert schema.fields[0].nullable is False