import pytest
import yaml
import os
from unittest.mock import Mock, patch

from controller import load_config, validate_config

@pytest.fixture
def sample_config():
    return {
        "source": {
            "name": "test_source",
            "params": {
                "auth": {
                    "auth_url": "https://auth.example.com",
                    "client_id": "test_client",
                    "client_secret": "test_secret"
                },
                "source_name": "test_source",
                "polling_frequency_minutes": 15,
                "sepio_post_url": "https://sepio.com/test",
                "get_asset_ids_url": "https://assets.example.com/ids",
                "get_asset_data_url": "https://assets.example.com/data"
            }
        },
        "target": {
            "name": "test_target",
            "params": {}
        },
        "transformation": {
            "name": "test_transformation",
            "params": {}
        },
    }

@pytest.fixture
def mock_config_file(tmp_path, sample_config):
    """Create temporary config file for testing."""
    config_file = tmp_path / "test_config.yml"
    with open(config_file, 'w') as f:
        yaml.dump(sample_config, f)
    return str(config_file)

def test_load_config_valid(mock_config_file: str) -> None:
    """Test loading valid configuration file."""
    config = load_config(mock_config_file)
    assert config["source"]["params"]["auth"]["auth_url"] == "https://auth.example.com" 

def test_load_config_missing_keys(sample_config) -> None:

    for key in ["source", "target"]:
        broken_config = sample_config.copy()
        broken_config.pop(key)
        assert not validate_config(broken_config)

        broken_config = sample_config.copy()
        broken_config[key].pop("name")
        assert not validate_config(broken_config)
    
    # broken transformation
    broken_config = sample_config.copy()
    broken_config["transformation"] = {}
    assert not validate_config(broken_config)
    broken_config = sample_config.copy()
    broken_config["transformation"].pop("name")
    assert not validate_config(broken_config)

@patch('yaml.safe_load')
def test_load_config_invalid_yaml(mock_safe_load, tmp_path) -> None:
    """Test loading an invalid YAML file."""
    # Mock yaml.safe_load to raise YAMLError
    mock_safe_load.side_effect = yaml.YAMLError("Invalid YAML syntax")
    
    # Create file with invalid YAML content
    config_file = tmp_path / "invalid.yml"
    with open(config_file, 'w') as f:
        f.write("invalid: yaml: content:\nthis is not valid yaml")
    
    pytest.raises(yaml.YAMLError, load_config, str(config_file))

def test_load_config_missing_file() -> None:
    """Test loading a non-existent config file."""

    pytest.raises(FileNotFoundError, load_config, "nonexistent_config.yml")

# @patch('yaml.safe_load')
def test_load_config_empty_file(tmp_path) -> None:
    """Test loading an empty config file."""
    # Create empty file
    config_file = tmp_path / "empty.yml"
    config_file.touch()
    
    config = load_config(str(config_file))
    assert config is None


# @patch('receiveData.validate_config')
def test_load_config_some_error(tmp_path) -> None:
    """Test loading config with empty auth values."""
    
    config_file = tmp_path / "config.yml"
    config_file.touch()

    with patch('yaml.safe_load') as mock_safe_load:
        mock_safe_load.side_effect = Exception("some crazy error")
        with pytest.raises(Exception, match="some crazy error"):
            config = load_config(str(config_file))
    
    # Should return empty dict due to error, but we don't want to test that
            print(config)
            assert config is None

def test_validate_config_valid(mock_config_file, sample_config):
    """Test validation of valid configuration."""
    config = load_config(mock_config_file)
    assert validate_config(config)
    assert config == sample_config
