from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType
from .json_utils import JsonUtils
import requests
import time
import logging
from typing import Iterator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class MyRestDataSource(DataSource):
    """
    DataSource class used for reading
    name: format which is set up when reading data
    schema: returns schema used in read method(DataSourceReader)
    reader: returns the datasource reader implementation
    """
    @classmethod
    def name(cls):
        return "custom-discoverant-connector"

    def _infer_schema(self, response, json_path):
        data = JsonUtils.get_nested_value(response, json_path)
        if data is None:
            return StructType([])

        if isinstance(data, dict):
            data = [data]

        if not isinstance(data, list) or len(data) == 0:
            return StructType([])

        first_elem = data[0]
        if not isinstance(first_elem, dict):
            return StructType([])

        flattened = JsonUtils.flatten_json(first_elem)
        fields = []
        infer_types_flag = True
        for key, value in flattened.items():
            if infer_types_flag:
                spark_type = JsonUtils.infer_spark_type(value)
            else:
                spark_type = StringType()
            fields.append(StructField(key, spark_type, True))
        return StructType(fields)

    def schema(self):
        base_url = self.options.get("base_url", "")
        endpoint = self.options.get("endpoint", "")
        url = f"{base_url}/{endpoint}".rstrip("/")
        auth_token = self.options.get("auth_token")
        json_path = self.options.get("json_path")
        username = self.options.get("username")
        password = self.options.get("password")
        infer_types_flag = self.options.get("infer_types", "false").lower() == "true"

        headers = {"Accept": "application/json"}
        agid = self.options.get("agid")
        params = {}
        if agid:
            params = {
                "Parameters": f"AGId={agid}",
                "AGId": f"{agid}"
            }
        # params = {}
        with requests.Session() as session:
            resp = session.get(url, headers=headers, params=params, auth=(username, password))
            resp.raise_for_status()
            data = resp.json()

        return self._infer_schema(data, json_path)

    def reader(self, schema):
        return MyRestDataSourceReader(schema, self.options)


class MyRestDataSourceReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options
    """
    def partitions(self):
        # For now just comma separated id-list to partition for parallel requests
        ids = self.options.get("id_list", "")
        if not ids:
            raise ValueError("id_list option is required for ID-based partitioning")

        id_list = [id.strip() for id in ids.split(",")]

        ids_per_partition = int(self.options.get("ids_per_partition", 1))
        partitions = [
            InputPartition(id_list[i:i + ids_per_partition])
            for i in range(0, len(id_list), ids_per_partition)
        ]

        logger.info(f"Created {len(partitions)} partitions for {len(id_list)} IDs.")
        return partitions
    """

    def partitions(self):
        max_pages = int(self.options.get("max_pages", 1))
        pages_per_partition = int(self.options.get("pages_per_partition", 1))
        partitions = [InputPartition((start, min(start + pages_per_partition - 1, max_pages)))
                      for start in range(1, max_pages + 1, pages_per_partition)]
        logger.info(f"Created {len(partitions)} partitions for API pagination.")
        return [InputPartition(0)]


    def read(self, partition) -> Iterator[tuple]:
        # start_page, end_page = partition.value
        base_url = self.options.get("base_url", "")
        endpoint = self.options.get("endpoint", "")
        url = f"{base_url}/{endpoint}".rstrip("/")
        auth_token = self.options.get("auth_token")
        username = self.options.get("username")
        password = self.options.get("password")
        json_path = self.options.get("json_path")
        throttle = float(self.options.get("throttle", 0))  # seconds between requests
        retries = int(self.options.get("retries", 3))

        agid = self.options.get("agid")

        headers = {"Accept": "application/json"}
        col_details = [(field.name, field.dataType) for field in self.schema.fields]

        with requests.Session() as session:
            if auth_token:
                session.headers.update({"Authorization": auth_token})

            params = {}
            """
            ids = partition.value
            for id in ids:
                if id:
                    params = {
                        "Parameters": f"AGId={id}",
                        "AGId": f"{id}"
                    }
            """
            attempt = 0
            while attempt < retries:
                try:
                    resp = session.get(url, headers=headers, auth=(username, password), params=params, timeout=15)
                    resp.raise_for_status()
                    data = resp.json()
                    data = JsonUtils.get_nested_value(data, json_path)
                    if not data:
                        logger.error(f"No data loaded by api call.")
                        break

                    if isinstance(data, dict):
                        data = [data]

                    for elem in data:
                        flattened = JsonUtils.flatten_json(elem)
                        row = []
                        for col, spark_type in col_details:
                            val = flattened.get(col)
                            converted_val = JsonUtils.convert_value_to_type(val, spark_type)
                            row.append(converted_val if converted_val is not None else val)
                        yield tuple(row)
                    break
                except Exception as e:
                    attempt += 1
                    time.sleep(2)

                if throttle > 0:
                    time.sleep(throttle)