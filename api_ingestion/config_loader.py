import json
from .app_context import AppContext, AuthConfig, HistoryLoadConfig, Endpoint


class ConfigValidator:
    def validate_history_load(self, history_cfg: dict):
        """Validate history load configuration block."""
        enabled_value = self._to_bool(history_cfg.get("enabled"))

        if enabled_value:
            required_keys = ["start-date"]
            invalid = [k for k in required_keys if not history_cfg.get(k)]
            if invalid:
                raise ValueError(
                    f"Invalid configuration: history load enabled but missing fields: {invalid}"
                )

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
            cfg.get("endpoint")
        endpoint = Endpoint(
                endpoint_name=cfg.get("endpoint").get("endpoint-name"),
                params=cfg.get("endpoint").get("params"),
                headers=cfg.get("endpoint").get("headers"))

        return AppContext(
            app_name=cfg["app-name"],
            url=cfg["base-url"],
            history_load=HistoryLoadConfig(**cfg["history-load"]),
            endpoint=endpoint
        )
