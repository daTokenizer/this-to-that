import pytest
import os
from unittest.mock import Mock, patch
import tempfile
import yaml
from controller import ETLController, DataSource, DataTarget, Transformation

def test_etl_controller_initialization(sample_config_dict):
    print(f"etl_config: {sample_config_dict}")
    """Test ETL controller initialization."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(sample_config_dict, config_file)
        config_path = config_file.name
        controller = ETLController(config_path)
        assert controller.config == sample_config_dict
        assert controller.source is None
        assert controller.target is None
        assert controller.transformation is None


def test_etl_controller_setup(sample_config_dict):
    """Test ETL controller setup."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(sample_config_dict, config_file)
        config_path = config_file.name
        try:
            controller = ETLController(config_path)
            controller._load_source()
            controller._load_target()
            controller._load_transformation()

            assert controller.source is not None
            assert isinstance(controller.source, DataSource)
            assert controller.target is not None
            assert isinstance(controller.target, DataTarget)
            assert controller.transformation is not None
            assert isinstance(controller.transformation, Transformation)
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)
    
        
def test_etl_controller_run(sample_config_dict):
    """Test ETL controller run method."""
    test_data = [{'id': 1, 'name': 'test'}]
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(sample_config_dict, config_file)
        controller = ETLController(config_path=config_file.name)
        controller._load_all_modules()
        
        mock_get_entries = Mock(return_value=test_data)
        controller.source.get_entries = mock_get_entries
        
        mock_transform = Mock(return_value=test_data[0])
        controller.transformation.transform = mock_transform
        
        mock_create_entries = Mock(return_value=1)
        controller.target.create_entries = mock_create_entries

        controller.run()
        
        mock_get_entries.assert_called_once()
        mock_transform.assert_called_once_with(test_data[0])
        mock_create_entries.assert_called_once_with(test_data)
