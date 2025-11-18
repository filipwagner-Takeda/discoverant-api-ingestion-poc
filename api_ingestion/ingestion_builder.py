from typing import Any
from pyspark.sql import DataFrame
from api_ingestion.config_loader import ConfigLoader
from api_ingestion.connector import MyRestDataSource
import api_ingestion.constants as constants

class ApiIngestion:
    def __init__(self, spark: Any, config_path: str):
        self.spark = spark
        self.spark.dataSource.register(MyRestDataSource)

        config_loader = ConfigLoader(config_path)
        self.app_context = config_loader.load_app_context()

        self.options = {
            "base_url": self.app_context.url,
            "endpoint": self.app_context.endpoint.endpoint_name,
            "pagination": str(self.app_context.pages.enabled).lower(),
            "json_path": self.app_context.json_path,
            "schema_path": self.app_context.schema_path,
            "partition_strategy": self.app_context.partitioning_strategy,
            "infer_schema": str(self.app_context.infer_schema).lower(),
            "throttle": str(constants.THROTTLE),
            "max_pages": str(constants.MAX_PAGES),
            "retries": str(constants.RETRIES),
        }


    def set_auth_token(self,token: str):
            self.options["auth_token"] =token

    def set_auth_credentials(self,username: str,password: str):
        self.options["username"] = username
        self.options["password"] = password

    def run_ingestion(self) -> DataFrame:
        reader = self.spark.read.format("api-ingestion")
        for key, value in self.options.items():
            reader = reader.option(key, value)
        return reader.load()