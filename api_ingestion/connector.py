import requests
import json
import datetime
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
import api_ingestion.api_utils as api_utils
from typing import Iterator
from api_ingestion.json_utils import JsonUtils

class MyRestDataSource(DataSource):
    """
    Spark Data Source V2 in Python to read any REST API.
    This data source attempts to infer schema dynamically.
    It supports the following options:
        .option("auth_token", "Bearer XYZ")
        .option("pagination", "true")
        .option("page_param", "page")
        .option("start_page", "1")
        .option("max_pages", "10")
        .option("json_path", "data.items")
        .option("base_url", "...")
        .option("endpoint", "...")
        .option("infer_types", "true")  # Optional: if set to true, infer types from the first record
    """

    @classmethod
    def name(cls):
        # Name used in spark.read.format("myrestdatasource")
        return "api-datasource"

    def schema(self):
        """
        Spark calls this method to get a schema (StructType)
        for the DataFrame.

        We perform a quick API call to infer the columns by examining the first JSON object.
        Each field is flattened and its type is inferred (if enabled) or set as a string.
        """
        base_url = self.options.get("base_url", "")
        endpoint = self.options.get("endpoint", "")
        url = f"{base_url}/{endpoint}".rstrip("/")

        auth_token = self.options.get("auth_token")
        pagination = self.options.get("pagination", "false").lower() == "true"
        page_param = self.options.get("page_param", "page")
        start_page = int(self.options.get("start_page", 1))
        infer_types_flag = self.options.get("infer_types", "false").lower() == "true"

        params = {}
        if pagination:
            params[page_param] = start_page

        # Use a requests.Session for improved performance and connection reuse
        with requests.Session() as session:
            if auth_token:
                session.headers.update({"Authorization": auth_token})
            # Set a timeout to avoid hanging indefinitely
            resp = session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

        # Apply json_path if present
        json_path = self.options.get("json_path")
        data = JsonUtils.get_nested_value(data, json_path)
        if data is None:
            # No data returns an empty schema
            return StructType([])

        # If the root is a single object, wrap it in a list
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list) or len(data) == 0:
            return StructType([])

        # Infer columns based on the first element
        first_elem = data[0]
        if not isinstance(first_elem, dict):
            return StructType([])

        flattened = JsonUtils.flatten_json(first_elem)
        fields = []
        for key, value in flattened.items():
            if infer_types_flag:
                spark_type = JsonUtils.infer_spark_type(value)
            else:
                spark_type = StringType()
            fields.append(StructField(key, spark_type, True))

        return StructType(fields)

    def reader(self, schema):
        """
        Creates and returns a DataSourceReader that uses the schema
        determined in the schema() method.
        """
        return MyRestDataSourceReader(schema, self.options)


class MyRestDataSourceReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options

    def read(self, partition) -> Iterator[tuple]:
        """
        Spark calls this on each partition (in this case, only one partition).
        We loop to handle pagination, retrieving and flattening each JSON object
        based on the inferred schema and converting each value to its proper type.
        """
        base_url = self.options.get("base_url", "")
        endpoint = self.options.get("endpoint", "")
        url = f"{base_url}/{endpoint}".rstrip("/")

        auth_token = self.options.get("auth_token")
        pagination = self.options.get("pagination", "false").lower() == "true"
        page_param = self.options.get("page_param", "page")
        start_page = int(self.options.get("start_page", 1))
        max_pages = int(self.options.get("max_pages", 10))
        json_path = self.options.get("json_path")

        # Retrieve column names and their corresponding Spark types from the schema
        col_details = [(field.name, field.dataType) for field in self.schema.fields]

        page = start_page

        # Use a requests.Session for connection reuse and improved performance

        start_page, end_page = partition.value  # Unpack the tuple

        with requests.Session() as session:
            if auth_token:
                session.headers.update({"Authorization": auth_token})
            for page_num in range(start_page, end_page + 1):
                while True:
                    params = {}
                    if pagination:
                        params[page_param] = page_num

                    resp = session.get(url, headers={}, params=params, timeout=10)
                    resp.raise_for_status()
                    data = resp.json()

                    data = JsonUtils.get_nested_value(data, json_path)
                    if data is None:
                        break


                    if isinstance(data, dict):
                        data = [data]
                    if not isinstance(data, list) or len(data) == 0:
                        break

                    for elem in data:

                        flattened = JsonUtils.flatten_json(elem)
                        row = []

                        for col, spark_type in col_details:
                            val = flattened.get(col)
                            converted_val = JsonUtils.convert_value_to_type(val, spark_type)
                            row.append(converted_val)
                        yield tuple(row)

                    # Exit the loop if pagination is not enabled
                    if not pagination:
                        break

                    if page >= max_pages:
                        break
                    page += 1

    def partitions(self):
        url_with_endpoint = f"{self.options.get('base_url', '')}/{self.options.get('endpoint', '')}"
        max_number = api_utils.find_valid_pages(url_with_endpoint,1,50,page_param="page")
        chunk_size = 10
        partitions = []
        for start in range(1, max_number + 1, chunk_size):
            end = min(start + chunk_size - 1, max_number)
            partitions.append(InputPartition((start, end)))

        return partitions

        #return [InputPartition(0)]