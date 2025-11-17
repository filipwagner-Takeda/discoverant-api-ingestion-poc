import requests
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
import api_ingestion.api_utils as api_utils
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)
from typing import Iterator, List
from api_ingestion.json_utils import JsonUtils
import api_ingestion.constants as constants

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

    from typing import List, Optional
    from pyspark.sql.types import StructField, StringType
    import requests

    def fetch_fields_from_api(
            self,
            base_url: str,
            endpoint: str,
            auth_token: Optional[str] = None,
            pagination: bool = False,
            page_param: str = "page",
            start_page: int = 1,
            json_path: Optional[str] = None,
            infer_types_flag: bool = False
    ) -> List[StructField]:
        """
        Fetch JSON data from an API and return a list of Spark StructField objects.

        :param base_url: Base URL of the API
        :param endpoint: API endpoint
        :param auth_token: Optional authorization token
        :param pagination: Whether to include pagination params
        :param page_param: Pagination parameter name
        :param start_page: Starting page number
        :param json_path: Optional JSON path to extract nested data
        :param infer_types_flag: Whether to infer Spark types from values
        :return: List of StructField objects
        """
        url = f"{base_url}/{endpoint}".rstrip("/")
        params = {page_param: start_page} if pagination else {}
        with requests.Session() as session:
            if auth_token:
                session.headers.update({"Authorization": auth_token})
            session.headers.update({"Content-Type": "application/json"})
            resp = session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

        # Apply json_path if present
        if json_path:
            data = JsonUtils.get_nested_value(data, json_path)

        if data is None:
            return []

        # Normalize data to list of dicts
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list) or len(data) == 0:
            return []

        first_elem = data[0]
        if not isinstance(first_elem, dict):
            return []

        # Flatten and infer fields
        flattened = JsonUtils.flatten_json(first_elem)
        fields = []
        for key, value in flattened.items():
            spark_type = JsonUtils.infer_spark_type(value) if infer_types_flag else StringType()
            fields.append(StructField(key, spark_type, True))

        return fields
    def schema(self) -> StructType:
        """
        Spark calls this method to get a schema (StructType)
        for the DataFrame.
        Either we:
        - perform a quick API call to infer the columns by examining the first JSON object.
        Each field is flattened and its type is inferred (if enabled) or set as a string.
        else:
        - load schema provided from config.json
        """
        if self.options.get("infer_schema"):
            fields = self.fetch_fields_from_api(
                base_url=self.options.get("base_url", ""),
                endpoint=self.options.get("endpoint", ""),
                auth_token=self.options.get("auth_token"),
                pagination=self.options.get("pagination", "false").lower() == "true",
                page_param=self.options.get("page_param", "page"),
                start_page=int(self.options.get("start_page", 1)),
                json_path=self.options.get("json_path"),
                infer_types_flag=self.options.get("infer_types", "false").lower() == "true"
            )
            return StructType(fields)
        return JsonUtils.load_spark_schema_from_json(self.options.get("schema_path"))


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
        #TODO HANDLE
        #start_page, end_page = partition.value
        # url - related options
        base_url = self.options.get("base_url", "")
        endpoint = self.options.get("endpoint", "")
        url = f"{base_url}/{endpoint}".rstrip("/")
        #auth - options
        auth_token = self.options.get("auth_token")
        username = self.options.get("username")
        password = self.options.get("password")
        #parser - options
        json_path = self.options.get("json_path")
        page_param = self.options.get("page_param", "page")
        pagination = self.options.get("pagination", "false").lower() == "true"
        pagination_max_pages = self.options.get("max_pages",constants.MAX_PAGES)

        # Retrieve column names and their corresponding Spark types from the schema
        col_details = [(field.name, field.dataType) for field in self.schema.fields]

        with requests.Session() as session:
            if auth_token:
                session.headers.update({"Authorization": auth_token})
            session.headers.update({"Content-Type": "application/json"})
            strategy = self.options.get("partitioning_strategy", "")
            if strategy != "":
                if strategy == "page":
                    start_page, end_page = partition.value
                    for page_num in range(start_page, end_page + 1):
                        for row in self._fetch_page_data(session, url, page_num, page_param, json_path, col_details):
                            yield row
                if strategy == "param" and pagination:
                    id_list = partition.value
                    for param_id in id_list:
                        for page_num in range(pagination_max_pages):
                            for row in self._fetch_page_data(session, url, page_num, page_param, json_path,
                                                             col_details):
                                yield row
            else:
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
        :param col_details: List of (column_name, spark_type) - schema to fit onto df row
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
        #max_number = api_utils.find_valid_pages(url, 1, 50, page_param="page")
        return self._partition_strategy()
        # return [InputPartition(0)]
    def _page_partitioning(self) ->List[InputPartition]:
        """
        This method splits pages into chunks to be executed by executors
        :return: List of input partitions for executors
        """
        max_pages = self.options.get("max_pages", 100)
        chunk_size = self.options.get("chunk_size", 100)
        partitions = []
        for start in range(1, max_pages + 1, chunk_size):
            end = min(start + chunk_size - 1, max_pages)
            partitions.append(InputPartition((start, end)))
        return partitions

    def _chunk_list(self,input_list,chunk_size):
        for i in range(0,len(input_list),chunk_size):
            yield input_list[i:i+chunk_size]

    def _param_partitioning(self) -> List[InputPartition]:
        params = self.options.get("param_list", "")
        chunk_size = self.options.get("chunk_size", 100)
        params_split = params.split(",")
        return [InputPartition(chunk) for chunk in self._chunk_list(params_split, chunk_size)]


    def _partition_strategy(self):
        strategy = self.options.get("partitioning_strategy", "")
        match strategy:
            case "page":
                return self._page_partitioning()
            case "param":
                return self._param_partitioning()
            case _:
                return [InputPartition(0)]
