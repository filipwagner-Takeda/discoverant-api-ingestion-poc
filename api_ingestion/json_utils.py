import json
import datetime
from typing import Any, Dict, Optional
from pyspark.sql.types import (
    StructType,StructField, StringType, BooleanType, LongType, DoubleType, TimestampType, DataType
)
import json

class JsonUtils:
    """
    Stateless utility class for flattening JSON, extracting nested values,
    inferring Spark types, and converting values to Spark-compatible types.
    Basically infer schema on initial load in connector
    """

    @staticmethod
    def flatten_json(nested: Any, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        """
        Recursively flattens a nested JSON-like structure (dict or list) into a single-level dict.
        Example:
            {"a": {"b": 123, "c": 456}} -> {"a.b": 123, "a.c": 456}
        Lists are converted to JSON strings.
        """
        items = []

        if isinstance(nested, dict):
            for k, v in nested.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(JsonUtils.flatten_json(v, new_key, sep).items())
                elif isinstance(v, list):
                    items.append((new_key, json.dumps(v)))
                else:
                    items.append((new_key, v))
        elif isinstance(nested, list):
            items.append((parent_key, json.dumps(nested)))
        else:
            items.append((parent_key, nested))

        return dict(items)

    @staticmethod
    def get_nested_value(data: Dict[str, Any], json_path: str) -> Optional[Any]:
        """
        Extracts a nested value from a dict using a dot-separated path.
        Example:
            data = {"a": {"b": 123}}, json_path = "a.b" -> 123
        Returns None if any level is missing.
        """
        if not json_path:
            return data

        keys = json_path.split(".")
        for key in keys:
            if not isinstance(data, dict):
                return None
            data = data.get(key)
            if data is None:
                return None
        return data

    @staticmethod
    def infer_spark_type(value: Any) -> DataType:
        """
        Infer Spark DataType from a Python value.
        """
        if value is None:
            return StringType()
        if isinstance(value, bool):
            return BooleanType()
        if isinstance(value, int):
            return LongType()
        if isinstance(value, float):
            return DoubleType()
        if isinstance(value, str):
            try:
                datetime.datetime.fromisoformat(value)
                return TimestampType()
            except ValueError:
                return StringType()
        return StringType()

    @staticmethod
    def convert_value_to_type(value: Any, spark_type: DataType) -> Optional[Any]:
        """
        Converts a value to a Python type compatible with the given Spark DataType.
        Returns None if conversion fails.
        """
        if value is None:
            return None

        try:
            if isinstance(spark_type, LongType):
                return int(value)
            if isinstance(spark_type, DoubleType):
                return float(value)
            if isinstance(spark_type, BooleanType):
                if isinstance(value, bool):
                    return value
                return str(value).lower() in ["true", "1", "yes", "t"]
            if isinstance(spark_type, TimestampType):
                if isinstance(value, str):
                    return datetime.datetime.fromisoformat(value)
                if isinstance(value, datetime.datetime):
                    return value
                return None
        except Exception:
            return None

        # Fallback: convert to string
    @staticmethod
    def load_spark_schema_from_json(json_schema_path: str) -> StructType:
        """
        Loads a JSON schema file and converts it into a Spark StructType.

        :param json_schema_path: Path to the JSON schema file
        :return: Spark StructType
        """
        with open(json_schema_path, "r") as f:
            schema_dict = json.load(f)

        type_mapping = {
            "string": StringType(),
            "long": LongType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
            "timestamp": TimestampType()
        }

        fields = []
        for field in schema_dict.get("fields", []):
            spark_type = type_mapping.get(field["type"].lower(), StringType())
            fields.append(StructField(field["name"], spark_type, field.get("nullable", True)))

        return StructType(fields)
