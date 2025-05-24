import pytest
from unittest.mock import patch, MagicMock
import importlib
import sys

from controller import DataSource, DataTarget


# Test DataSource abstract class
def test_data_source_is_abstract():
    """Test that DataSource cannot be instantiated directly."""
    with pytest.raises(TypeError):
        DataSource()


# Test DataTarget abstract class
def test_data_target_is_abstract():
    """Test that DataTarget cannot be instantiated directly."""
    with pytest.raises(TypeError):
        DataTarget()


# Minimal concrete implementations for testing
class MinimalDataSource(DataSource):
    def initialize(self, params):
        self.initialized = True
        self.params = params
    
    def get_entries(self):
        return [{"id": 1, "name": "test"}]
    
    def close(self):
        self.closed = True


class MinimalDataTarget(DataTarget):
    def __init__(self):
        self.entries = []
    
    def initialize(self, params):
        self.initialized = True
        self.params = params
    
    def create_entries(self, entries):
        self.entries.extend(entries)
    
    def close(self):
        self.closed = True


# Test concrete implementation of DataSource
def test_minimal_data_source():
    """Test a minimal concrete implementation of DataSource."""
    source = MinimalDataSource()
    
    # Test initialize
    source.initialize({"test_param": "value"})
    assert source.initialized
    assert source.params["test_param"] == "value"
    
    # Test get_entries
    entries = source.get_entries()
    assert len(entries) == 1
    assert entries[0]["id"] == 1
    
    # Test close
    source.close()
    assert source.closed


# Test concrete implementation of DataTarget
def test_minimal_data_target():
    """Test a minimal concrete implementation of DataTarget."""
    target = MinimalDataTarget()
    
    # Test initialize
    target.initialize({"test_param": "value"})
    assert target.initialized
    assert target.params["test_param"] == "value"
    
    # Test create_entries
    target.create_entries([{"id": 1, "name": "test"}])
    assert len(target.entries) == 1
    assert target.entries[0]["id"] == 1
    
    # Test close
    target.close()
    assert target.closed


# Test loading an actual source implementation
@patch('importlib.import_module')
def test_load_source_module(mock_import):
    """Test loading a source module."""
    # Create mock module with a source class
    mock_module = MagicMock()
    mock_module.CrowdstrikeSource = MinimalDataSource
    mock_import.return_value = mock_module
    
    # Load the module
    module = importlib.import_module("sources.crowdstrike")
    
    # Get the source class
    source_class = getattr(module, "CrowdstrikeSource")
    
    # Instantiate and test
    source = source_class()
    source.initialize({"test": "value"})
    entries = source.get_entries()
    
    assert source.initialized
    assert len(entries) == 1


# Test loading an actual target implementation
@patch('importlib.import_module')
def test_load_target_module(mock_import):
    """Test loading a target module."""
    # Create mock module with a target class
    mock_module = MagicMock()
    mock_module.SepioTarget = MinimalDataTarget
    mock_import.return_value = mock_module
    
    # Load the module
    module = importlib.import_module("targets.sepio")
    
    # Get the target class
    target_class = getattr(module, "SepioTarget")
    
    # Instantiate and test
    target = target_class()
    target.initialize({"test": "value"})
    target.create_entries([{"id": 1}])
    
    assert target.initialized
    assert len(target.entries) == 1


# Test error handling for missing abstract methods
def test_missing_abstract_methods():
    """Test that missing abstract methods are caught."""
    # Define a class that doesn't implement all abstract methods
    class IncompleteSource(DataSource):
        def initialize(self, params):
            pass
        
        # Missing get_entries and close methods
    
    # Should raise TypeError when instantiated
    with pytest.raises(TypeError):
        IncompleteSource() 