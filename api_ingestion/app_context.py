from dataclasses import dataclass, field
from typing import Optional, List, Dict


@dataclass
class AuthConfig:
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass
class HistoryLoadConfig:
    enabled: bool
    start_date: Optional[str] = None


@dataclass
class Endpoint:
    endpoint_name: str
    params: Optional[Dict] = field(default_factory=dict)
    headers: Optional[Dict[str, str]] = field(default_factory=dict)


@dataclass
class AppContext:
    app_name: str
    url: str
    history_load: HistoryLoadConfig
    endpoint: Endpoint
    json_path: Optional[str] = None


@dataclass
class NotificationContext:
    base_url: str
    username: str
    password: str
