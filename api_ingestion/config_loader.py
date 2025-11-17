import json
from .app_context import AppContext, AuthConfig, Endpoint


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
        required_values_for_keys = ['app-name', 'base-url', 'endpoint']
        for key in required_values_for_keys:
            if key not in config:
                raise ValueError(f"Required key: {key} is missing")
            if config[key] == "":
                raise ValueError(f"Required key: {key} is empty/has no value")
        values = config.values()

        print(values)
        pass

class ConfigLoader:
    def __init__(self,path: str):
        self.path = path
    """
    Loads the config JSON into structured objects (AppContext, etc.)
    Supports multiple endpoints under one base URL.
    """
    def load_app_context(self) -> AppContext:
        with open(self.path, "r") as f:
            cfg = json.load(f)["ingest-souce-config"]
        ConfigValidator.validate(cfg)
        endpoint = Endpoint(
                endpoint_name=cfg.get("endpoint").get("endpoint-name"),
                params=cfg.get("endpoint").get("params"),
                headers=cfg.get("endpoint").get("headers"))

        return AppContext(
            app_name=cfg.get("app-name"),
            url=cfg.get("base-url"),
            endpoint=endpoint,
            json_path=cfg.get("result").get("json_path"),
            schema_path=cfg.get("schema_path")
        )
