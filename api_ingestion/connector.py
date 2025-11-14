import requests
import json
import datetime
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
import api_ingestion.api_utils as api_utils
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    TimestampType,
)
from typing import Iterator, List
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
        return "api-ingestion"

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
        Spark calls this on each partition.
        """
        start_page, end_page = partition.value
        base_url = self.options.get("base_url", "")
        endpoint = self.options.get("endpoint", "")
        url = f"{base_url}/{endpoint}".rstrip("/")

        auth_token = self.options.get("auth_token")
        json_path = self.options.get("json_path")
        page_param = self.options.get("page_param", "page")

        # Retrieve column names and their corresponding Spark types from the schema
        col_details = [(field.name, field.dataType) for field in self.schema.fields]

        with requests.Session() as session:
            if auth_token:
                session.headers.update({"Authorization": auth_token})

            for page_num in range(start_page, end_page + 1):
                for row in self._fetch_page_data(session, url, page_num, page_param, json_path, col_details):
                    yield row

    def _fetch_page_data(self,session, url, page_num, page_param, json_path, col_details):
        """
        Fetch and process data for a single page.

        :param session: requests.Session object with headers already set
        :param url: API endpoint
        :param page_num: Current page number
        :param page_param: Name of the pagination parameter
        :param pagination: Boolean indicating if pagination is enabled
        :param json_path: Path to extract data from JSON response
        :param col_details: List of (column_name, spark_type) tuples
        :return: Generator yielding tuples of processed rows
        """
        params = {page_param: page_num}

        resp = session.get(url, headers={}, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        data = JsonUtils.get_nested_value(data, json_path)
        if data is None:
            return  # No data for this page

        # Normalize to list
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list) or len(data) == 0:
            return

        for elem in data:
            flattened = JsonUtils.flatten_json(elem)
            row = []
            for col, spark_type in col_details:
                val = flattened.get(col)
                converted_val = JsonUtils.convert_value_to_type(val, spark_type)
                row.append(converted_val)
            yield tuple(row)

    def partitions(self) -> List[InputPartition]:
        """
        Method used to partition data to send to executors
        pagination enabled -> split pages to chunks
        :return: List of input partitions for executors
        """
        # Return a single partition since this example handles one partition only.
        base_url = self.options.get("base_url", "")
        endpoint = self.options.get("endpoint", "")
        url = f"{base_url}/{endpoint}".rstrip("/")
        max_number = api_utils.find_valid_pages(url, 1, 50, page_param="page")
        chunk_size = 10
        partitions = []
        for start in range(1, max_number + 1, chunk_size):
            end = min(start + chunk_size - 1, max_number)
            partitions.append(InputPartition((start, end)))

        return partitions
        # return [InputPartition(0)]