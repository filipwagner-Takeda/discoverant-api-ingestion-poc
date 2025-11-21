import json
import datetime
from typing import Any, Dict, Iterator, List, Optional

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    LongType,
    DoubleType,
    TimestampType,
)


class JsonUtils:
    """
    Stateless class used in path probing of json API response and schema loading
    Currently works well only for strict string column schema ingestion
    JSON helpers for:
      - flattening dicts to dotted keys
      - iterating deep JSON with a simplified path syntax (dot only)
      - simple schema I/O and type conversions

    Path behavior:
      - Use dot notation only: e.g. "aglist.valueList.valuesList.replId"(discoverant example)
      - When a token meets a list, the iterator implicitly expands over elements
        and continues matching the next token on each element.
      - Final nodes that are lists are expanded (yield each element).
    """
    @staticmethod
    def flatten_json(nested: Any, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        items: List[tuple[str, Any]] = []
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
    def iter_jsonpath(obj: Any, path: Optional[str]) -> Iterator[Any]:
        """
        Traverse using dot-only paths. If a token hits a list, implicitly expand over
        its elements and continue matching the next token on each element.
        The final yielded objects are:
          - Each element if the terminal node is a list
          - The node itself otherwise
        """
        if not path:
            yield obj
            return

        tokens = [t for t in path.split('.') if t]
        nodes: List[Any] = [obj]

        for tok in tokens:
            next_nodes: List[Any] = []
            for n in nodes:
                if isinstance(n, dict):
                    if tok in n:
                        next_nodes.append(n[tok])
                elif isinstance(n, list):
                    for el in n:
                        if isinstance(el, dict) and tok in el:
                            next_nodes.append(el[tok])
                        elif isinstance(el, list):
                            for sub in el:
                                if isinstance(sub, dict) and tok in sub:
                                    next_nodes.append(sub[tok])
            nodes = next_nodes
            if not nodes:
                return

        for n in nodes:
            if isinstance(n, list):
                for el in n:
                    yield el
            else:
                yield n

    @staticmethod
    def first_jsonpath(obj: Any, path: Optional[str]) -> Any:
        for n in JsonUtils.iter_jsonpath(obj, path):
            return n
        return None

    @staticmethod
    def load_serialized_json(serialized_params: str):
        return json.loads(serialized_params)

    @staticmethod
    def load_spark_schema_from_json(json_schema_path: str) -> StructType:
        with open(json_schema_path, "r") as f:
            schema_dict = json.load(f)
        type_mapping = {
            "string": StringType(),
            "long": LongType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
            "timestamp": TimestampType(),
        }
        fields: List[StructField] = []
        for field in schema_dict.get("fields", []):
            spark_type = type_mapping.get(field["type"].lower(), StringType())
            fields.append(StructField(field["name"], spark_type, field.get("nullable", True)))
        return StructType(fields)

    @staticmethod
    def infer_spark_type(value: Any):
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
    def convert_value_to_type(value: Any, spark_type):
        if value is None:
            return None
        if isinstance(spark_type, LongType):
            try: return int(value)
            except Exception: return None
        if isinstance(spark_type, DoubleType):
            try: return float(value)
            except Exception: return None
        if isinstance(spark_type, BooleanType):
            try:
                if isinstance(value, bool):
                    return value
                return str(value).lower() in ["true", "1", "yes", "t"]
            except Exception:
                return None
        if isinstance(spark_type, TimestampType):
            try:
                if isinstance(value, str):
                    return datetime.datetime.fromisoformat(value)
                if isinstance(value, datetime.datetime):
                    return value
                return None
            except Exception:
                return None
        return str(value)
