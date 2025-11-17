import json
from typing import Any
from pyspark.sql import DataFrame
from api_ingestion.app_context import AppContext
from api_ingestion.config_loader import ConfigLoader
from api_ingestion.connector import MyRestDataSource
import api_ingestion.constants as constants
class ApiIngestionBuilder:
    def __init__(self, spark: Any):
        self.spark = spark
        self.options = {}

    # --- Builder methods for overriding ---
    def set_base_url(self, url: str):
        self.options["base_url"] = url
        return self

    def set_endpoint(self, endpoint: str):
        self.options["endpoint"] = endpoint
        return self

    def set_json_path(self, path: str):
        self.options["json_path"] = path
        return self

    def set_schema_path(self, path: str):
        self.options["schema_path"] = path
        return self

    def set_credentials(self, username: str, password: str):
        self.options["username"] = username
        self.options["password"] = password
        return self

    def set_auth_token(self, token: str):
        self.options["auth_token"] = token
        return self

    def set_throttle(self, value: int):
        self.options["throttle"] = str(value)
        return self

    def set_retries(self, value: int):
        self.options["retries"] = str(value)
        return self

    def set_max_pages(self, value: int):
        self.options["max_pages"] = str(value)
        return self

    def set_partition_strategy(self, strategy: str):
        self.options["partition_strategy"] = strategy
        return self

    def set_pagination(self, enabled: bool):
        self.options["pagination"] = str(enabled).lower()
        return self

    def set_infer_schema(self, enabled: bool):
        self.options["infer_schema"] = str(enabled).lower()
        return self

    def build(self) -> DataFrame:
        reader = self.spark.read.format("api-ingestion")
        for key, value in self.options.items():
            reader = reader.option(key, value)
        return reader.load()


class ApiIngestion:
    def __init__(self, spark: Any, config_path: str):
        self.spark = spark
        config_loader = ConfigLoader(config_path)
        self.spark.dataSource.register(MyRestDataSource)
        self.app_context = config_loader.load_app_context()
        self.builder_instance = None

    def builder(self) -> ApiIngestionBuilder:
        # Initialize builder with defaults from config
        self.builder_instance = ApiIngestionBuilder(self.spark)
        self.builder_instance.options.update({
            "base_url": self.app_context.url,
            "endpoint": self.app_context.endpoint.endpoint_name,
            "pagination": str(self.app_context.pages.enabled).lower(),
            "json_path": self.app_context.json_path,
            "schema_path": self.app_context.schema_path,
            "partition_strategy": "page",
            "infer_schema": str("true").lower()
        })
        return self.builder_instance