from dataclasses import dataclass, field
from typing import Optional, List, Dict


@dataclass
class AuthConfig:
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None

@dataclass
class Pagination:
    enabled: Optional[bool] = False
    max_pages: Optional[int] = None

@dataclass
class Endpoint:
    endpoint_name: str
    params: Optional[Dict] = field(default_factory=dict)
    headers: Optional[Dict[str, str]] = field(default_factory=dict)

@dataclass
class AppContext:
    app_name: str
    url: str
    endpoint: Endpoint
    pages: Optional[Pagination] = None
    json_path: Optional[str] = None
    schema_path: Optional[str] = None
    partitioning_strategy: Optional[str] = None

@dataclass
class NotificationContext:
    base_url: str
    username: str
    password: str
