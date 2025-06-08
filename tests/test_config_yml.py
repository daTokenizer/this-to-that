import pytest
import yaml
import os
import tempfile
import json
from unittest.mock import patch, mock_open

from controller import ETLController, MISSING_DATA_DEFAULT_VALUE

@pytest.fixture
def minimal_valid_config():
    """Minimal valid configuration."""
    return {
        "source": {
            "name": "test",
            "params": {}
        },
        "target": {
            "name": "test",
            "params": {}
        },
        "transformation": {
            "name": "map",
            "params": {
                "mapping": {
                    "target_field": "source_field",
                }
            },
        }
    }


def test_load_valid_yaml_config(sample_config):
    """Test loading a valid YAML config file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(sample_config, config_file)
        config_path = config_file.name
    
        controller = ETLController(config_path)
        assert controller.config == sample_config
    
    # Cleanup
    if os.path.exists(config_path):
        os.remove(config_path)


def test_load_valid_json_config(sample_config):
    """Test loading a valid JSON config file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as config_file:
        json.dump(sample_config, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        assert controller.config == sample_config
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_minimal_valid_config(minimal_valid_config):
    """Test that a minimal valid config is accepted."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(minimal_valid_config, config_file)
        config_path = config_file.name
    
        try:
            controller = ETLController(config_path)
            assert controller.config == minimal_valid_config
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)


def test_missing_source_config(minimal_valid_config):
    """Test config validation with missing source section."""
    config = minimal_valid_config.copy()
    config.pop("source")

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config, config_file)
        config_path = config_file.name
    
    try:
        with pytest.raises(ValueError, match="Empty or Invalid Configuration Found, halting."):
            controller = ETLController(config_path)
            controller._load_source()
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_missing_target_config(minimal_valid_config):
    """Test config validation with missing target section."""
    config = minimal_valid_config.copy()
    config.pop("target")

    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config, config_file)
        config_path = config_file.name
    
    try:
        with pytest.raises(ValueError, match="Empty or Invalid Configuration Found, halting."):
            controller = ETLController(config_path)
            controller._load_target()
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_missing_mapping_config(minimal_valid_config):
    """Test config validation with missing mapping section."""
    config = minimal_valid_config.copy()
    config.pop("transformation")
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        # No mapping means no fields will be mapped, but it should not raise an error
        # Just test that we can create a controller
        assert controller.config == config
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_invalid_yaml_format():
    """Test loading config with invalid YAML format."""
    invalid_yaml = """
    source:
      name: test_source
      params: {
    target:
      name: test_target
      params: {}
    """

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        config_file.write(invalid_yaml)
        config_path = config_file.name
        with pytest.raises(ValueError):
            ETLController(config_path)


def test_empty_config_file():
    """Test loading an empty config file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        config_path = config_file.name
    
    try:
        with pytest.raises(ValueError):
            ETLController(config_path)
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_missing_source_name(sample_config_dict):
    """Test config with missing source name."""
    config = sample_config_dict.copy()
    config["source"].pop("name")
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config, config_file)
        config_path = config_file.name
    
    try:
        with pytest.raises(ValueError, match="Empty or Invalid Configuration Found, halting."):
            controller = ETLController(config_path)
            controller._load_source()
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_missing_target_name(sample_config_dict):
    """Test config with missing target name."""
    config = sample_config_dict.copy()
    config["target"].pop("name")
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config, config_file)
        config_path = config_file.name
    
    try:
        with pytest.raises(ValueError, match="Empty or Invalid Configuration Found, halting."):
            controller = ETLController(config_path)
            controller._load_target()
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_complex_mapping(sample_config_dict):
    """Test config with complex mapping including transformations."""
    config = sample_config_dict.copy()
    config["transformation"]["params"]["mapping"] = {
        "target_field1": "source_field1",
        "target_field2": {"FIXED_CUSTOM_VALUE": "constant_value"},
        "target_field3": "some_non_existent_source_field",
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        controller._load_transformation()
        
        # Test mapping with direct field
        source_entry = {"source_field1": "value1", "extra": "not_mapped"}
        target_entry = controller.transformation.transform(source_entry)
        
        assert "target_field1" in target_entry
        assert target_entry["target_field1"] == "value1"
        
        # Test mapping with constant value
        assert "target_field2" in target_entry
        assert target_entry["target_field2"] == "constant_value"
        
        # Transform function is not implemented yet, so field should not be in result
        assert "target_field3" in target_entry
        assert target_entry["target_field3"] == MISSING_DATA_DEFAULT_VALUE
        assert "target_field4" not in target_entry
    finally:
        if os.path.exists(config_path):
            os.remove(config_path) 