from typing import Any

import api_ingestion.constants as constants
from api_ingestion.app_context import AppContext
from pyspark.sql import DataFrame


def fetch_from_rest(spark:Any,configuration:AppContext) -> DataFrame:
    reader = (spark.read
          .format("api-ingestion")
          .option("base_url", configuration.url)
          .option("endpoint", configuration.endpoint.endpoint_name)
          .option("pagination","true")
          .option("json_path", configuration.json_path)
          .option("username", constants.USERNAME)
          .option("password", constants.PASSWORD)
          .option("throttle", constants.THROTTLE)
          .option("max_pages",configuration.pages.max_pages)
          .option("retries", constants.RETRIES)
          )

    return reader.load()
