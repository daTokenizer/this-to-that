import pytest
import os
from unittest.mock import Mock, patch
import tempfile
import yaml
from controller import ETLController, DataSource, DataTarget, Transformation, CONF_FILE_PATH, LOG_FILE_PATH
import argparse
import logging

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

def test_command_line_args_default():
    """Test ETL controller with default command line arguments."""
    with patch('sys.argv', ['etl_controller.py']):
        with patch('argparse.ArgumentParser.parse_args') as mock_parse:
            mock_parse.return_value = argparse.Namespace(
                config=CONF_FILE_PATH,
                log_file=LOG_FILE_PATH
            )
            with patch('controller.ETLController') as mock_controller:
                mock_controller.return_value.run.return_value = 10
                
                # Run the main block directly
                with patch('sys.exit') as mock_exit:
                    from controller import main
                    main()
                    
                    # Verify the controller was initialized with default config
                    mock_controller.assert_called_once_with(CONF_FILE_PATH)
                    mock_controller.return_value.run.assert_called_once()
                    mock_exit.assert_called_once_with(0)

def test_command_line_args_custom():
    """Test ETL controller with custom command line arguments."""
    custom_config = "/path/to/custom/config.yml"
    custom_log = "/path/to/custom/log.log"
    
    with patch('sys.argv', ['etl_controller.py', '-c', custom_config, '-l', custom_log]):
        with patch('argparse.ArgumentParser.parse_args') as mock_parse:
            mock_parse.return_value = argparse.Namespace(
                config=custom_config,
                log_file=custom_log
            )
            with patch('controller.ETLController') as mock_controller:
                mock_controller.return_value.run.return_value = 10
                
                # Run the main block directly
                with patch('sys.exit') as mock_exit:
                    from controller import main
                    main()
                    
                    # Verify the controller was initialized with custom config
                    mock_controller.assert_called_once_with(custom_config)
                    mock_controller.return_value.run.assert_called_once()
                    mock_exit.assert_called_once_with(0)

def test_command_line_args_invalid_config():
    """Test ETL controller with invalid config file path."""
    invalid_config = "/nonexistent/config.yml"
    
    with patch('sys.argv', ['etl_controller.py', '-c', invalid_config]):
        with patch('argparse.ArgumentParser.parse_args') as mock_parse:
            mock_parse.return_value = argparse.Namespace(
                config=invalid_config,
                log_file=LOG_FILE_PATH
            )
            with patch('controller.ETLController') as mock_controller:
                mock_controller.side_effect = FileNotFoundError("Config file not found")
                
                # Run the main block directly
                from controller import main
                with pytest.raises(FileNotFoundError):
                    main()

def test_command_line_args_logging_setup():
    """Test ETL controller logging setup with custom log file."""
    custom_log = "/path/to/custom/log.log"
    
    with patch('sys.argv', ['etl_controller.py', '-l', custom_log]):
        with patch('argparse.ArgumentParser.parse_args') as mock_parse:
            mock_parse.return_value = argparse.Namespace(
                config=CONF_FILE_PATH,
                log_file=custom_log
            )
            with patch('logging.basicConfig') as mock_logging:
                with patch('controller.ETLController') as mock_controller:
                    mock_controller.return_value.run.return_value = 10
                    
                    # Run the main block directly
                    with patch('sys.exit') as mock_exit:
                        from controller import main
                        main()
                        
                        # Verify logging was configured with custom log file
                        mock_logging.assert_called_with(
                            filename=custom_log,
                            level=logging.INFO,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                        )
                        mock_exit.assert_called_once_with(0)
