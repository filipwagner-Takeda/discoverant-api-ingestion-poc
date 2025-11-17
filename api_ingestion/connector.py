import json

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
    This data source attempts to infer schema dynamically
    Or we are able to provide schema.
    Currently only field type that works for ingestion is StringType()
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
            params: Optional[dict] = None,
            pagination: bool = False,
            page_param: str = "page",
            start_page: int = 1,
            json_path: Optional[str] = None,
    ) -> List[StructField] | None:
        """

        :param base_url: base url
        :param endpoint: api endpoint
        :param auth_token: Bearer XYZ
        :param params: parameter for api call
        :param pagination: boolean pagination flag
        :param page_param: just a page string
        :param start_page: start bage but not useful here-fetch only first row
        :param json_path: path if we dont want outer-level json schema
        :return: returns fields that are used in building StructType() schema
        """
        url = f"{base_url}/{endpoint}".rstrip("/")
        params = {page_param: start_page} if pagination else {}
        with requests.Session() as session:
            if auth_token:
                session.headers.update({"Authorization": auth_token})
            session.headers.update({"Content-Type": "application/json"})
            resp = session.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return
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
            spark_type = StringType()
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
        param_str = self.options.get("params")
        params = json.loads(param_str) if param_str else {}
        if self.options.get("infer_schema"):
            fields = self.fetch_fields_from_api(
                base_url=self.options.get("base_url", ""),
                endpoint=self.options.get("endpoint", ""),
                auth_token=self.options.get("auth_token"),
                params=params,
                pagination=self.options.get("pagination", "false").lower() == "true",
                page_param=self.options.get("page_param", "page"),
                start_page=int(self.options.get("start_page", 1)),
                json_path=self.options.get("json_path")
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
        we have two partitioning mechanisms
        -we run partitions by pages
        -we run partitions by provided parameter list
        """
        url = f"{self.options.get('base_url', '')}/{self.options.get('endpoint', '')}".rstrip("/")
        auth_token = self.options.get("auth_token")
        json_path = self.options.get("json_path")
        page_param = self.options.get("page_param", "page")
        pagination = self.options.get("pagination", "false").lower() == "true"
        max_pages = int(self.options.get("max_pages", constants.MAX_PAGES))

        col_details = [(field.name, field.dataType) for field in self.schema.fields]

        with requests.Session() as session:
            if auth_token:
                session.headers.update({"Authorization": auth_token})
            session.headers.update({"Content-Type": "application/json"})

            strategy = self.options.get("partitioning_strategy", "")

            if strategy == "page":
                # Partition contains (start_page, end_page)
                start_page, end_page = partition.value
                for page_num in range(start_page, end_page + 1):
                    yield from self._fetch_page_data(session, url, page_num, page_param, json_path, col_details)

            elif strategy == "param":
                # Partition contains list of IDs
                id_list = partition.value
                for param_id in id_list:
                    for page_num in range(1, max_pages + 1):
                        yield from self._fetch_page_data(session, url, page_num, page_param, json_path, col_details,extra_param={"id": param_id})
            else:
                # Default: pagination or single call
                if pagination:
                    for page_num in range(1, max_pages + 1):
                        yield from self._fetch_page_data(session, url, page_num, page_param, json_path, col_details)
                else:
                    yield from self._fetch_page_data(session, url, None, page_param, json_path, col_details)

    def _fetch_page_data(self, session, url, page_num, page_param, json_path, col_details, extra_param=None):
        params = {}
        if page_num:
            params[page_param] = page_num
        if extra_param:
            params.update(extra_param)

        resp = session.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            return
        data = resp.json()

        data = JsonUtils.get_nested_value(data, json_path)
        if not data:
            return

        if isinstance(data, dict):
            data = [data]

        for elem in data:
            row = []
            for col, spark_type in col_details:
                val = elem.get(col)
                converted_val = JsonUtils.convert_value_to_type(val, spark_type)
                row.append(converted_val)
            yield tuple(row)

    def partitions(self) -> list[InputPartition]:
        strategy = self.options.get("partitioning_strategy", "")
        max_pages = int(self.options.get("max_pages", constants.MAX_PAGES))
        chunk_size = int(self.options.get("chunk_size", constants.CHUNK_SIZE))

        if strategy == "page":
            return [InputPartition((start, min(start + chunk_size - 1, max_pages)))
                    for start in range(1, max_pages + 1, chunk_size)]
        elif strategy == "param":
            params = self.options.get("param_list", "").split(",")
            return [InputPartition(chunk) for chunk in self._chunk_list(params, chunk_size)]
        return [InputPartition(0)]

    def _chunk_list(self, input_list, chunk_size):
        for i in range(0, len(input_list), chunk_size):
            yield input_list[i:i + chunk_size]