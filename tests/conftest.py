import pytest
import tempfile
import yaml
import os
from unittest.mock import Mock

from controller import DataSource, DataTarget, Transformation
from typing import Dict, Any 

@pytest.fixture # TODO: kill this
def sample_config():
    return {
        "source": {
            "name": "test",
            "params": {
                "auth_url": "https://auth.example.com",
                "client_id": "test_client",
                "client_secret": "test_secret",
                "get_asset_ids_url": "https://assets.example.com/ids",
                "get_asset_data_url": "https://assets.example.com/data"
            }
        },
        "target": {
            "name": "test",
            "params": {
                "sepio_url": "https://sepio.example.com/api",
                "batch_size": 100,
                "source_name": "test_source",
                "max_retries": 3
            }
        },
        "transformation": {
            "name": "map",
            "params": {
                "mapping": {
                    "target_id": "source_id",
                    "target_name": "hostname",
                    "target_mac": "mac_address",
                    "target_type": {
                        "value": "device"
                    }
                }
            }
        }
    }

@pytest.fixture
def sample_config_dict():
    return {
        "polling_frequency_seconds": -1,
        "source": {
            "name": "test",
            "params": {
                "url": "https://example.com/api",
                "timeout": 30,
            }
        },
        "target": {
            "name": "test",
            "params": {
                "url": "https://target.example.com/api",
                "batch_size": 100,
            }
        },
        "transformation": {
            "name": "map",
            "params": {
                "mapping": {
                    "target_id": "source_id",
                    "target_name": "name",
                    "target_type": {
                        "FIXED_CUSTOM_VALUE": "device",
                    },
                },
            },
        }
    }


@pytest.fixture
def mock_response():
    """Create a mock HTTP response."""
    mock = Mock()
    mock.status_code = 200
    mock.json.return_value = {}
    mock.raise_for_status = Mock()
    return mock


class MockSource(DataSource):
    """Mock source for testing."""
    def __init__(self, entries=None):
        self.entries = entries or []
        self.initialized = False
        self.closed = False
        self.init_params = None
    
    def initialize(self, params):
        self.initialized = True
        self.init_params = params
    
    def get_entries(self):
        return self.entries
    
    def close(self):
        self.closed = True


class MockTarget(DataTarget):
    """Mock target for testing."""
    def __init__(self):
        self.entries = []
        self.initialized = False
        self.closed = False
        self.init_params = None
    
    def initialize(self, params):
        self.initialized = True
        self.init_params = params
    
    def create_entries(self, entries):
        """Create entries in the target."""
        if isinstance(entries, (list, tuple)):
            self.entries.extend(entries)
        else:
            self.entries.append(entries)
    
    def close(self):
        self.closed = True



class MockTransformation(Transformation):
    """Abstract base class for all data transformations."""
    
    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the transformation with the provided parameters."""
        pass
    
    def transform(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single entry according to the transformation rules."""
        return entry


@pytest.fixture
def mock_source():
    """Provide a mock source with sample entries."""
    return MockSource([
        {"source_id": "001", "hostname": "Device 1", "mac_address": "001122334455", "ip": "192.168.1.1"},
        {"source_id": "002", "hostname": "Device 2", "mac_address": "AABBCCDDEEFF", "ip": "192.168.1.2"}
    ])


@pytest.fixture
def mock_target():
    return MockTarget()

@pytest.fixture
def mock_transformation():
    return MockTransformation()


@pytest.fixture # TODO: kill this
def etl_config():
    """Create a test ETL configuration."""
    return {
        'source': {
            'type': 'test',
            'config': {
                'test_key': 'test_value'
            }
        },
        'target': {
            'type': 'test',
            'config': {
                'test_key': 'test_value'
            }
        },
        'transform': {
            'type': 'test_transform',
            'config': {
                'test_key': 'test_value'
            }
        }
    }


# @pytest.fixture
# def etl_controller(sample_config_dict):
#     """Create a test ETL controller."""
#     from controller import ETLController

#     return ETLController(sample_config_dict) 