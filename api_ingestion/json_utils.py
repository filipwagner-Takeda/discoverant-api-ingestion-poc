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
    Basically infer schema on initial load in connector.
    Also supporting manual schema entry and load from json
    """

    @staticmethod
    def flatten_json(nested, parent_key="", sep="."):
        """
        Basic recursive flatten of a JSON object (dict) into a one-level dict.
        E.g. {"a": {"b": 123, "c": 456}} -> {"a.b": 123, "a.c": 456}
        Arrays (lists) remain as raw JSON strings.
        """
        items = []
        if isinstance(nested, dict):
            for k, v in nested.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(JsonUtils.flatten_json(v, new_key, sep=sep).items())
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
    def get_nested_value(data, json_path):
        """
        Extracts a nested value from data following a path like "data.items".
        If any level is missing, returns None.
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
    def infer_spark_type(value):
        """
        Infer Spark DataType from a given Python value.
        For strings, we try to detect an ISO-formatted datetime.
        For lists or dicts, we fallback to StringType since these are flattened to JSON strings.
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
                # Attempt to parse ISO formatted datetime
                datetime.datetime.fromisoformat(value)
                return TimestampType()
            except ValueError:
                return StringType()
        return StringType()

    @staticmethod
    def convert_value_to_type(value, spark_type):
        """
        Convert a value to the corresponding Python type matching the Spark DataType.
        """
        if value is None:
            return None

        if isinstance(spark_type, LongType):
            try:
                return int(value)
            except Exception:
                return None

        if isinstance(spark_type, DoubleType):
            try:
                return float(value)
            except Exception:
                return None

        if isinstance(spark_type, BooleanType):
            try:
                if isinstance(value, bool):
                    return value
                value_lower = str(value).lower()
                return value_lower in ["true", "1", "yes", "t"]
            except Exception:
                return None

        if isinstance(spark_type, TimestampType):
            try:
                if isinstance(value, str):
                    return datetime.datetime.fromisoformat(value)
                elif isinstance(value, datetime.datetime):
                    return value
                else:
                    return None
            except Exception:
                return None

        # Fallback to string conversion
        return str(value)

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
