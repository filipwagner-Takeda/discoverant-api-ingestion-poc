import json
from .app_context import AppContext, Endpoint
import api_ingestion.constants as constants
import re

class ConfigValidator:
    def _to_bool(self, value):
        """Convert various string representations to boolean."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            v = value.strip().lower()
            if v in ("true", "1", "yes", "y"):
                return True
            elif v in ("false", "0", "no", "n", ""):
                return False
        raise ValueError(f"Invalid boolean value: {value!r}")

    @staticmethod
    def validate(config:dict) -> None:
        for key in constants.REQUIRED_KEYS_CONFIG:
            if key not in config:
                raise ValueError(f"Required key: {key} is missing")
            if config[key] == "":
                raise ValueError(f"Required key: {key} is empty/has no value")
        if not ConfigValidator.is_valid_url(config["base_url"]):
            raise ValueError(f"Invalid url: {config['base_url']}")
        ConfigValidator.validate_partition_strategy(config.get("partitioning_strategy"))
        pass
    @staticmethod
    def is_valid_url(url):
        pattern = re.compile(
            r'^(https?|ftp)://[^\s/$.?#].[^\s]*$',
            re.IGNORECASE
        )
        return re.match(pattern, url) is not None

    @staticmethod
    def validate_partition_strategy(partition_strategy:str) -> None:
        if partition_strategy not in constants.SUPPORTED_PARTITION_STRATEGIES:
            raise ValueError(f"Invalid partition strategy: {partition_strategy}")

class ConfigLoader:
    def __init__(self,path: str):
        self.path = path
    """
    Loads the config JSON into structured objects (AppContext, etc.)
    Supports multiple endpoints under one base URL.
    """
    def parse_params(self):
        with open(self.path, "r") as f:
            cfg = json.load(f)["ingest-source-config"]
        params = cfg.get("endpoint").get("params")
        return json.dumps(params)

    def load_app_context(self) -> AppContext:
        with open(self.path, "r") as f:
            cfg = json.load(f)["ingest-source-config"]
        ConfigValidator.validate(cfg)
        endpoint = Endpoint(
                endpoint_name=cfg.get("endpoint").get("endpoint-name"),
                params=cfg.get("endpoint").get("params"),
                headers=cfg.get("endpoint").get("headers"))

        return AppContext(
            app_name=cfg.get("app-name"),
            url=cfg.get("base-url"),
            endpoint=endpoint,
            json_path=cfg.get("json_path"),
            schema_path=cfg.get("schema_path"),
            partitioning_strategy =  cfg.get("partitioning_strategy")
        )
