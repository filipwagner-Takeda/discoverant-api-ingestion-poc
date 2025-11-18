import pytest
import json
import tempfile
from pathlib import Path
from api_ingestion.config_loader import ConfigValidator, ConfigLoader  # adjust import path
import api_ingestion.constants as constants

# Constants mock
constants.REQUIRED_KEYS_CONFIG = [
    "app-name", "base_url", "endpoint", "json_path", "schema_path", "partitioning_strategy"
]

def test_to_bool_valid_cases():
    cv = ConfigValidator()
    assert cv._to_bool(True) is True
    assert cv._to_bool(False) is False
    assert cv._to_bool("true") is True
    assert cv._to_bool("yes") is True
    assert cv._to_bool("1") is True
    assert cv._to_bool("false") is False
    assert cv._to_bool("no") is False
    assert cv._to_bool("") is False

def test_to_bool_invalid_case():
    cv = ConfigValidator()
    with pytest.raises(ValueError):
        cv._to_bool("maybe")

def test_is_valid_url():
    assert ConfigValidator.is_valid_url("http://example.com")
    assert ConfigValidator.is_valid_url("https://example.com")
    assert not ConfigValidator.is_valid_url("example.com")
    assert not ConfigValidator.is_valid_url("http:/example.com")

def test_validate_missing_key():
    config = {"app-name": "test"}
    with pytest.raises(ValueError) as exc:
        ConfigValidator.validate(config)
    assert "Required key" in str(exc.value)

def test_validate_empty_value():
    config = {
        "app-name": "",
        "base_url": "http://example.com",
        "endpoint": {},
        "json_path": "path",
        "schema_path": "path",
        "partitioning_strategy": "daily"
    }
    with pytest.raises(ValueError) as exc:
        ConfigValidator.validate(config)
    assert "empty/has no value" in str(exc.value)

def test_validate_invalid_url():
    config = {
        "app-name": "test",
        "base_url": "invalid-url",
        "endpoint": {},
        "json_path": "path",
        "schema_path": "path",
        "partitioning_strategy": "daily"
    }
    with pytest.raises(ValueError) as exc:
        ConfigValidator.validate(config)
    assert "Invalid url" in str(exc.value)

def test_load_app_context(tmp_path):
    # Valid config
    valid_config = {
        "ingest-source-config": {
            "app-name": "test-app",
            "base_url": "http://example.com",
            "endpoint": {
                "endpoint-name": "test-endpoint",
                "params": {"param1": "value1"},
                "headers": {"Authorization": "Bearer token"}
            },
            "json_path": "/data/json",
            "schema_path": "/data/schema",
            "partitioning_strategy": "daily"
        }
    }
    file_path = tmp_path / "config.json"
    file_path.write_text(json.dumps(valid_config))

    loader = ConfigLoader(str(file_path))
    app_context = loader.load_app_context()

    # Assertions for valid config
    assert app_context.app_name == "test-app"
    assert app_context.url == "http://example.com"
    assert app_context.endpoint.endpoint_name == "test-endpoint"
    assert app_context.endpoint.params["param1"] == "value1"
    assert app_context.json_path == "/data/json"

def test_invalid_partition_strategy(tmp_path):
    # Invalid partition strategy
    invalid_config = {
        "ingest-source-config": {
            "app-name": "test-app",
            "base_url": "http://example.com",
            "endpoint": {
                "endpoint-name": "test-endpoint",
                "params": {"param1": "value1"},
                "headers": {"Authorization": "Bearer token"}
            },
            "json_path": "/data/json",
            "schema_path": "/data/schema",
            "partitioning_strategy": "invalid-strategy"
        }
    }
    file_path = tmp_path / "config.json"
    file_path.write_text(json.dumps(invalid_config))

    loader = ConfigLoader(str(file_path))

    # Expect ValueError for invalid partition strategy
    with pytest.raises(ValueError) as exc:
        loader.load_app_context()
    assert "Invalid partition strategy" in str(exc.value)


def test_parse_params(tmp_path):
    # Create a temporary JSON file
    config_data = {
        "ingest-source-config": {
            "app-name": "test-app",
            "base_url": "http://example.com",
            "endpoint": {
                "endpoint-name": "test-endpoint",
                "params": {"param1": "value1", "param2": "value2"},
                "headers": {"Authorization": "Bearer token"}
            },
            "json_path": "/data/json",
            "schema_path": "/data/schema",
            "partitioning_strategy": "daily"
        }
    }
    file_path = tmp_path / "config.json"
    file_path.write_text(json.dumps(config_data))

    loader = ConfigLoader(str(file_path))
    params_json = loader.parse_params()

    # Validate that parse_params returns JSON string of params
    assert isinstance(params_json, str)
    parsed = json.loads(params_json)
    assert parsed["param1"] == "value1"
    assert parsed["param2"] == "value2"