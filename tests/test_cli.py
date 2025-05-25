import pytest
import sys
import os
from unittest.mock import patch, Mock, call

from controller import ETLController, CONF_FILE_PATH


@pytest.fixture
def mock_controller():
    """Mock ETLController for testing."""
    mock = Mock(spec=ETLController)
    mock.run.return_value = 5  # Mock 5 processed entries
    return mock


@patch('sys.exit')
@patch('controller.ETLController')
def test_main_with_args(mock_controller_class, mock_exit, mock_controller, capsys):
    """Test main function with command line arguments."""
    # Setup mock controller
    mock_controller_class.return_value = mock_controller
    
    # Create a temporary config file path for testing
    test_config_path = "test_config.yml"
    
    # Mock command line arguments
    with patch('sys.argv', ['controller.py', '--config', test_config_path]):
        # Call main() directly
        from controller import main
        main()
    
    # Verify ETLController was instantiated with the command line argument
    mock_controller_class.assert_called_once_with(test_config_path)
    
    # Verify run() was called
    mock_controller.run.assert_called_once()
    
    # Verify output
    captured = capsys.readouterr()
    assert "Successfully processed 5 entries" in captured.out
    
    # Verify sys.exit was called with 0 (success)
    mock_exit.assert_called_once_with(0)


@patch('sys.exit')
@patch('controller.ETLController')
def test_main_with_default_config(mock_controller_class, mock_exit, mock_controller, capsys):
    """Test main function without command line arguments."""
    # Setup mock controller
    mock_controller_class.return_value = mock_controller
    
    # Mock command line arguments - no config path provided
    with patch('sys.argv', ['controller.py']):
        # Call main() directly
        from controller import main
        main()
    
    # Verify ETLController was instantiated with the default config path
    mock_controller_class.assert_called_once_with(CONF_FILE_PATH)
    
    # Verify run() was called
    mock_controller.run.assert_called_once()
    
    # Verify output
    captured = capsys.readouterr()
    assert "Successfully processed 5 entries" in captured.out
    
    # Verify sys.exit was called with 0 (success)
    mock_exit.assert_called_once_with(0)


@patch('sys.exit')
@patch('controller.ETLController')
def test_main_with_error(mock_controller_class, mock_exit, capsys):
    """Test main function when an error occurs."""
    # Make the controller raise an exception
    mock_controller_class.return_value.run.side_effect = Exception("Test error")
    
    # Mock command line arguments
    with patch('sys.argv', ['controller.py', '--config', 'config.yml']):
        # Call main() directly
        from controller import main
        main()
    
    # Verify output contains error message
    captured = capsys.readouterr()
    assert "ETL process failed: Test error" in captured.out
    
    # Verify sys.exit was called with 1 (error)
    mock_exit.assert_called_once_with(1)


@patch('sys.exit')
def test_main_usage_message(mock_exit, capsys):
    """Test usage message is displayed when run with --help."""
    # Mock command line arguments
    with patch('sys.argv', ['controller.py', '--help']):
        # Call main() directly
        from controller import main
        main()
    
    # Verify output contains usage message
    captured = capsys.readouterr()
    assert "usage:" in captured.out
    
    # Should exit after displaying help
    assert mock_exit.call_count >= 1
    assert mock_exit.call_args_list[-1] == call(1) 