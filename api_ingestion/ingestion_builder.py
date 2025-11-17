import uuid
from typing import Any
from pyspark.sql import DataFrame
from api_ingestion.app_context import AppContext
from api_ingestion.config_loader import ConfigLoader
from api_ingestion.connector import MyRestDataSource

class ApiIngestionBuilder:
    def __init__(self, spark: Any):
        self.spark = spark
        self.options = {}

    def set_credentials(self, username: str, password: str):
        self.options["username"] = username
        self.options["password"] = password
        return self
    def set_auth_token(self,auth_token: str):
        self.options["auth_token"] = auth_token
        return self

    def set_throttle(self, value: int):
        self.options["throttle"] = value
        return self

    def set_retries(self, value: int):
        self.options["retries"] = value
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
        # Initialize builder with base options from config
        self.builder_instance = ApiIngestionBuilder(self.spark)
        self.builder_instance.options.update({
            "base_url": self.app_context.url,
            "endpoint": self.app_context.endpoint.endpoint_name,
            "pagination": self.app_context.pages.enabled,
            "json_path": self.app_context.json_path
        })
        return self.builder_instance
