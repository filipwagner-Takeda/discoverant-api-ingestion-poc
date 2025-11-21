import logging
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple
import requests
from requests.exceptions import RequestException
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType

from api_ingestion.utils import ApiUtils
from api_ingestion.json_utils import JsonUtils
import api_ingestion.constants as constants


def _normalize_key(name: str) -> str:
    """
    Lowercase and keep only [a-z0-9] to allow matching schema names like 'LocationName'
    to flattened keys like 'location.name' -> both become 'locationname'.
    """
    return "".join(ch for ch in name.lower() if ch.isalnum())


class RestDataSource(DataSource):
    """
    Spark Data Source V2 (Python) for generic REST APIs.
    Features:
      - json_path to extract inner path of responses
      - Paging strategies (page/param),
      - retries, throttle implemented to retry/ slow down ingestion when failure occurs
    """

    @classmethod
    def name(cls):
        return "api-ingestion"

    def fetch_fields_from_api(
            self,
            base_url: str,
            endpoint: str,
            auth_token: Optional[str] = None,
            auth: Optional[Tuple[str, str]] = None,
            params: Optional[dict] = None,
            pagination: bool = False,
            page_param: str = "page",
            start_page: int = 1,
            json_path: Optional[str] = None,
    ) -> List[StructField]:
        if not base_url:
            raise ValueError("base_url must be provided")

        url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}".rstrip("/")
        query_params = dict(params or {})
        if pagination and page_param:
            query_params[page_param] = start_page

        if auth and (not auth[0] and not auth[1]):
            auth = None

        with requests.Session() as session:
            headers = {"Accept": "application/json"}
            if auth_token:
                headers["Authorization"] = auth_token
            session.headers.update(headers)

            resp = session.get(url, auth=auth, params=query_params, timeout=10)
            if resp.status_code != 200:
                raise RuntimeError(f"Schema probe failed: HTTP {resp.status_code}: {resp.text}")
            data = resp.json()

        nodes = list(JsonUtils.iter_jsonpath(data, json_path)) if json_path else (
            data if isinstance(data, list) else [data]
        )
        if not nodes:
            return []

        head = nodes[0]
        if isinstance(head, dict):
            flat = JsonUtils.flatten_json(head)
        else:
            flat = {"value": head}
        return [StructField(k, StringType(), True) for k in flat.keys()]

    def schema(self) -> StructType:
        raw_params = self.options.get("params")
        params = JsonUtils.load_serialized_json(raw_params) if raw_params else {}

        infer_schema_opt = self.options.get("infer_schema")
        infer_schema_flag = False
        if isinstance(infer_schema_opt, bool):
            infer_schema_flag = infer_schema_opt
        elif isinstance(infer_schema_opt, str):
            infer_schema_flag = infer_schema_opt.lower() == "true"

        if infer_schema_flag:
            fields = self.fetch_fields_from_api(
                base_url=self.options.get("base_url", ""),
                endpoint=self.options.get("endpoint", ""),
                auth_token=self.options.get("auth_token"),
                auth=(self.options.get("username", ""), self.options.get("password", "")),
                params=params,
                pagination=(self.options.get("pagination", "false").lower() == "true"),
                page_param=self.options.get("page_param", "page"),
                start_page=int(self.options.get("start_page", 1)),
                json_path=self.options.get("json_path"),
            )
            return StructType(fields or [])

        return JsonUtils.load_spark_schema_from_json(self.options.get("schema_path"))

    def reader(self, schema):
        return RestDataSourceReader(schema, self.options)


class RestDataSourceReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options

        raw_params = self.options.get("params")
        self.params = JsonUtils.load_serialized_json(raw_params) if raw_params else {}

        base_url = self.options.get("base_url", "").rstrip("/")
        endpoint = self.options.get("endpoint", "").lstrip("/")
        self.url = f"{base_url}/{endpoint}".rstrip("/")

        self.auth_token = self.options.get("auth_token")
        self.json_path = self.options.get("json_path")
        self.page_param = self.options.get("page_param", "page")
        self.pagination = self.options.get("pagination", "false").lower() == "true"
        self.strategy = (self.options.get("partitioning_strategy", "") or "").strip()
        self.chunk_size = int(self.options.get("chunk_size", constants.CHUNK_SIZE))

        username = self.options.get("username")
        password = self.options.get("password")
        self.auth: Optional[Tuple[str, str]] = (username, password) if (username or password) else None

        if self.pagination:
            self.max_pages = ApiUtils.find_valid_pages(self.url, 1, constants.MAX_PAGES, self.page_param)
        else:
            self.max_pages = 1

    def read(self, partition) -> Iterator[tuple]:
        col_details = [(field.name, field.dataType) for field in self.schema.fields]

        with requests.Session() as session:
            headers = {"Accept": "application/json"}
            if self.auth_token:
                headers["Authorization"] = self.auth_token
            session.headers.update(headers)

            strategy = self.strategy
            if strategy == "page":
                start_page, end_page = partition.value
                for page_num in range(start_page, end_page + 1):
                    yield from self._fetch_page_data(session, self.url, page_num, col_details)

            elif strategy == "param":
                id_list = partition.value
                param_key = self.options.get("param_key")
                for param_id in id_list:
                    per_call_params = dict(self.params or {})
                    if param_key:
                        per_call_params[param_key] = param_id
                    if self.pagination:
                        for page_num in range(1, self.max_pages + 1):
                            yield from self._fetch_page_data(session, self.url, page_num, col_details, per_call_params)
                    else:
                        yield from self._fetch_page_data(session, self.url, None, col_details, per_call_params)

            else:
                if self.pagination:
                    for page_num in range(1, self.max_pages + 1):
                        yield from self._fetch_page_data(session, self.url, page_num, col_details)
                else:
                    yield from self._fetch_page_data(session, self.url, None, col_details)

    def _fetch_page_data(
            self,
            session: requests.Session,
            url: str,
            page_num: Optional[int],
            col_details: List[Tuple[str, object]],
            override_params: Optional[dict] = None,
    ) -> Iterator[tuple]:

        query_params = dict(self.params or {})
        if override_params:
            query_params.update(override_params)
        if page_num is not None and self.page_param:
            query_params[self.page_param] = page_num

        attempt = 0
        consecutive_empty = 0

        while attempt <= constants.RETRIES:
            try:
                if attempt > 0:
                    sleep_time = constants.BACKOFF_FACTOR ** attempt
                    logging.warning(f"Retrying in {sleep_time:.2f}s (attempt {attempt}/{constants.RETRIES})…")
                    time.sleep(sleep_time)
                else:
                    time.sleep(constants.THROTTLE)

                resp = session.get(url, auth=self.auth, params=query_params, timeout=10)
                if resp.status_code != 200:
                    logging.error(f"HTTP {resp.status_code} for {url} params={query_params}: {resp.text}")
                    attempt += 1
                    continue

                data = resp.json()
                row_nodes = list(JsonUtils.iter_jsonpath(data, self.json_path)) if self.json_path else (
                    data if isinstance(data, list) else [data]
                )

                if not row_nodes:
                    consecutive_empty += 1
                    if consecutive_empty >= 5:
                        logging.info("Stopping after multiple empty payloads.")
                        return
                    return

                for elem in row_nodes:
                    if not isinstance(elem, dict):
                        elem = {"value": elem}
                    flat = JsonUtils.flatten_json(elem)
                    norm_lookup = {_normalize_key(k): v for k, v in flat.items()}

                    row: List[object] = []
                    for col, spark_type in col_details:
                        # 1) exact flattened key match
                        val = flat.get(col)
                        if val is None:
                            # 2) normalized-name match (e.g., LocationName -> location.name)
                            val = norm_lookup.get(_normalize_key(col))
                            if val is None:
                                # 3) as a last resort, try interpreting the schema name as a json path
                                val = JsonUtils.first_jsonpath(elem, col)
                        row.append(JsonUtils.convert_value_to_type(val, spark_type))
                    yield tuple(row)
                return

            except RequestException as e:
                logging.error(f"Request error: {e}")
                attempt += 1

    def partitions(self) -> List[InputPartition]:
        """
        Two partitioning strategies:
        - page: if pagination is supported, use chunks of pages per executor to speed up ingestion,
        - param: if using many metadata values as parameters, split into partitions based on chunk settings and send partitions to executors
        """
        if self.strategy == "page":
            parts: List[InputPartition] = []
            for start in range(1, self.max_pages + 1, self.chunk_size):
                end = min(start + self.chunk_size - 1, self.max_pages)
                parts.append(InputPartition((start, end)))
            return parts
        if self.strategy == "param":
            raw_param_list = (self.options.get("param_list", "") or "").strip()
            ids = [p.strip() for p in raw_param_list.split(",") if p.strip()]
            return [InputPartition(chunk) for chunk in self._chunk_list(ids, self.chunk_size)]
        return [InputPartition(0)]

    @staticmethod
    def _chunk_list(input_list: List[str], chunk_size: int) -> Iterator[List[str]]:
        for i in range(0, len(input_list), chunk_size):
            yield input_list[i: i + chunk_size]
