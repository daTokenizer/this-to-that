import pytest
import yaml
import os
from unittest.mock import Mock, patch 
import tempfile
import uuid
import random

from controller import ETLController, MISSING_DATA_DEFAULT_VALUE
from transformers.identity import IdentityTransformation
from transformers.map import MapTransformation
from sources.test import TestSource as MockSource
import time


def sample_source_entry_generator(source_id: str = None, name: str = None, ip: str = None, score: float = None):
    source_id = str(source_id if source_id else uuid.uuid4())
    name = name if name else f"entry_{uuid.uuid4()}"
    ip = ip if ip else f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
    score = score if score else random.random()
    return {
            "source_id": source_id,
            "name": name,
            "ip": ip,
            "score": score,
        }

def populate_source_entries(config_dict: dict, count: int) -> tuple[dict, list]:
    sample_entries = [sample_source_entry_generator() for _ in range(count)]
    config_dict = config_dict.copy()
    config_dict["source"]["params"]["entries"] = sample_entries
    return config_dict, sample_entries


@pytest.fixture
def mock_source():
    return MockSource([
        {"source_id": "001", "name": "Device 1", "mac": "00:11:22:33:44:55"},
        {"source_id": "002", "name": "Device 2", "mac": "AA:BB:CC:DD:EE:FF"}
    ])


def test_init_loads_config(sample_config_dict):
    """Test ETLController initialization loads config properly."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(sample_config_dict, config_file)
        config_path = config_file.name
    
        controller = ETLController(config_path)
        assert controller.config == sample_config_dict
    
    # Cleanup
    if os.path.exists(config_path):
        os.remove(config_path)


def test_invalid_config_path():
    """Test ETLController handles invalid config path."""
    with pytest.raises(FileNotFoundError):
        ETLController("nonexistent_file.yml")


def test_unsupported_config_format():
    """Test ETLController handles unsupported config format."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as config_file:
        config_path = config_file.name
    
    try:
        with pytest.raises(ValueError, match="Unsupported config file format"):
            ETLController(config_path)
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_load_source(sample_config_dict):
    """Test loading data source."""
    
    # Create controller with test config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(sample_config_dict, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        controller._load_all_modules()
        
        # Verify module was imported correctly
        # TODO: assert_called_with("sources.test")
        
        # Verify source was initialized with correct params
        assert controller.source.initialized
        assert controller.source.init_params == sample_config_dict["source"]["params"]
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_load_target(sample_config_dict):
    """Test loading data target."""

    # Create controller with test config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(sample_config_dict, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        controller._load_all_modules()
        
        # Verify module was imported correctly
        # TODO: assert_called_with("targets.test")
        
        # Verify target was initialized with correct params
        assert controller.target.initialized
        assert controller.target.init_params == sample_config_dict["target"]["params"]
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_map_entry(sample_config_dict):
    """Test mapping from source entry to target entry."""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        config_dict, _ = populate_source_entries(sample_config_dict, 10)
        yaml.dump(config_dict, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        controller._load_all_modules()
        source_entry = {
            "source_id": "123",
            "name": "Test Device",
            "extra_field": "should not be mapped"
        }
        
        target_entry = controller.transformation.transform(source_entry)
        
        assert target_entry == {
            "target_id": "123",
            "target_name": "Test Device",
            "target_type": "device"
        }
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_run_successful(sample_config_dict):
    """Test successful ETL process execution."""    
    # Create controller with test config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        config_dict, sample_entries = populate_source_entries(sample_config_dict, 2)
        yaml.dump(config_dict, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        
        # Run the ETL process
        controller.run()
        
        # Verify results
        source = controller.source
        target = controller.target
        assert len(target.entries) == 2  # Two entries were processed
        assert target.entries[0]["target_id"] == sample_entries[0]["source_id"]
        assert target.entries[1]["target_id"] == sample_entries[1]["source_id"]
        assert source.closed  # Source was closed properly
        assert target.closed  # Target was closed properly
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


@patch('importlib.import_module')
def test_run_handles_import_error(mock_import, sample_config_dict):
    """Test ETL process handles source loading error."""
    mock_import.side_effect = ImportError("Module not found")
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(sample_config_dict, config_file)
        config_path = config_file.name
        
        try:
            with pytest.raises(ValueError):
                controller = ETLController(config_path)
                controller.run()

                assert controller.source is None
                assert controller.transformation is None
                assert controller.target is None
        
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)


def test_run_handles_source_error(sample_config_dict):
    """Test ETL process handles source loading error."""
    entries_to_process = 10
    entries = [sample_source_entry_generator() for _ in range(entries_to_process)]
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(sample_config_dict, config_file)
        config_path = config_file.name
        try:
            controller = ETLController(config_path)
            controller._load_all_modules()
            controller.source.get_entries = Mock(side_effect=Exception("some error"))

            controller.run()
            
            # Verify source was closed even though there was an error
            assert controller.source is not None
            assert controller.transformation is not None
            assert controller.target is not None

            assert len(controller.target.entries) == 0
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)

def test_run_handles_transformation_error(sample_config_dict):
    """Test ETL process handles transformation operation error."""
    entries_to_process = 10
    entries = [sample_source_entry_generator() for _ in range(entries_to_process)]
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(sample_config_dict, config_file)
        config_path = config_file.name
        try:
            controller = ETLController(config_path)
            controller._load_all_modules()
            controller.source.get_entries = Mock(return_value=entries)
            controller.transformation.transform = Mock(side_effect=Exception("some error"))

            controller.run()
            
            # Verify source was closed even though there was an error
            assert controller.source is not None
            assert controller.transformation is not None
            assert controller.target is not None
            assert len(controller.target.entries) == 0

        finally:
            if os.path.exists(config_path):
                os.remove(config_path)


def test_run_handles_target_error(sample_config_dict):
    """Test ETL process handles target loading error."""

    entries_to_process = 10
    entries = [sample_source_entry_generator() for _ in range(entries_to_process)]
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(sample_config_dict, config_file)
        config_path = config_file.name
        try:
            controller = ETLController(config_path)
            controller._load_all_modules()
            controller.source.get_entries = Mock(return_value=entries)
            controller.target.create_entries = Mock(side_effect=Exception("some error"))

            controller.run()
            
            # Verify source was closed even though there was an error
            assert controller.source is not None
            assert controller.transformation is not None
            assert controller.target is not None
    
            assert len(controller.target.entries) == 0
        finally:
            if os.path.exists(config_path):
                os.remove(config_path)


def test_run_handles_processing_error(sample_config_dict):
    """Test ETL process handles errors during processing."""
    total_entries = 10
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        config_dict, sample_entries = populate_source_entries(sample_config_dict, total_entries)
        yaml.dump(config_dict, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)

        
        # Make target.create_entries fail on the second entry
        controller._load_all_modules() # force load target before manipulating it
        original_create_entries = controller.target.create_entries
        call_count = 0
        
        def failing_create_entries(entries):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise ValueError("Test error")
            original_create_entries(entries)
        
        controller.target.create_entries = failing_create_entries
        
        # Run should proceed despite the error
        controller.run()
        
        # Verify results
        assert len(controller.target.entries) == total_entries - 1
        assert controller.source.closed  # Source was closed properly
        assert controller.target.closed  # Target was closed properly
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_map_entry_missing_source_field(sample_config_dict):
    """Test mapping when source field is missing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(sample_config_dict, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        controller._load_all_modules()
        
        source_entry = {
            "source_id": "123",
            # name is missing
        }
        
        target_entry = controller.transformation.transform(source_entry)
        
        # Target entry should have target_id but not target_name
        assert target_entry == {
            "target_id": "123",
            "target_type": "device",
            "target_name": MISSING_DATA_DEFAULT_VALUE,
        }
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_load_transformation_identity(sample_config_dict):
    """Test loading identity transformation."""
    config_dict = sample_config_dict.copy()
    config_dict["transformation"] = {
        "name": "identity",
        "params": {}
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config_dict, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        controller._load_all_modules()
        
        assert isinstance(controller.transformation, IdentityTransformation)
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_load_transformation_map(sample_config_dict):
    """Test loading map transformation."""
    config_dict = sample_config_dict.copy()
    config_dict["transformation"] = {
        "name": "map",
        "params": {
            "mapping": {
                "target_field": "source_field"
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config_dict, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        controller._load_all_modules()
        
        assert isinstance(controller.transformation, MapTransformation)
        assert controller.transformation.mapping == config_dict["transformation"]["params"]["mapping"]
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_load_transformation_default(sample_config_dict):
    """Test loading default transformation when none specified."""
    config_dict = sample_config_dict.copy()
    # No transformation specified
    config_dict.pop("transformation")
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config_dict, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        controller._load_all_modules()
        
        assert isinstance(controller.transformation, IdentityTransformation)
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


@patch('importlib.import_module')
def test_load_transformation_error(mock_import, sample_config_dict):
    """Test handling of transformation loading error."""
    mock_import.side_effect = ImportError("Module not found")
    
    config_dict = sample_config_dict.copy()
    config_dict["transformation"] = {
        "name": "nonexistent",
        "params": {}
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config_dict, config_file)
        config_path = config_file.name
    
    try:
        with pytest.raises(ValueError):
            ETLController(config_path)
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_run_with_identity_transformation(sample_config_dict):
    """Test ETL process with identity transformation."""
    config_dict = sample_config_dict.copy()
    config_dict["transformation"] = {
        "name": "identity",
        "params": {}
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        config_dict, sample_entries = populate_source_entries(config_dict, 2)
        yaml.dump(config_dict, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        controller.run()

        actual_entries = controller.target.entries
        assert len(actual_entries) == 2
        # Verify entries were passed through unchanged
        assert actual_entries[0] == sample_entries[0]
        assert actual_entries[1] == sample_entries[1]
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_run_with_map_transformation(sample_config_dict):
    """Test ETL process with map transformation."""
    config_dict = sample_config_dict.copy()
    config_dict["transformation"] = {
        "name": "map",
        "params": {
            "mapping": {
                "target_id": "source_id",
                "target_name": "name",
                "target_score": "score"
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        config_dict, sample_entries = populate_source_entries(config_dict, 2)
        yaml.dump(config_dict, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        controller.run()

        actual_entries = controller.target.entries
        assert len(actual_entries) == 2
        # Verify entries were mapped correctly
        assert actual_entries[0]["target_id"] == sample_entries[0]["source_id"]
        assert actual_entries[0]["target_name"] == sample_entries[0]["name"]
        assert actual_entries[0]["target_score"] == sample_entries[0]["score"]
        assert actual_entries[1]["target_id"] == sample_entries[1]["source_id"]
        assert actual_entries[1]["target_name"] == sample_entries[1]["name"]
        assert actual_entries[1]["target_score"] == sample_entries[1]["score"]
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_run_with_transformation_error(sample_config_dict):
    """Test ETL process handles transformation errors."""
    config_dict = sample_config_dict.copy()
    config_dict["transformation"] = {
        "name": "map",
        "params": {
            "mapping": {
                "target_field": "source_field"
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        total_entries = 10
        config_dict, sample_entries = populate_source_entries(config_dict, total_entries)
        yaml.dump(config_dict, config_file)
        config_path = config_file.name
    
    try:
        controller = ETLController(config_path)
        controller._load_all_modules()
        
        # Make transformation.transform fail
        original_transform = controller.transformation.transform
        call_count = 0
        def failing_transform(entry):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise ValueError("Test transformation error")
            original_transform(entry)
        
        controller.transformation.transform = failing_transform
        
        # Run should handle the error and continue
        controller.run()
        
        actual_entries = controller.target.entries
        assert len(actual_entries) == total_entries - 1
        assert controller.source.closed  # Source was closed properly
        assert controller.target.closed  # Target was closed properly
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)


def test_run_halts_on_zero_polling(sample_config_dict):
    """Test ETLController halts after one cycle if polling_frequency is 0."""
    config_dict, sample_entries = populate_source_entries(sample_config_dict, 2)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config_dict, config_file)
        config_path = config_file.name

    try:
        controller = ETLController(config_path, polling_frequency=0)
        with patch.object(controller, 'process_cycle', wraps=controller.process_cycle) as mock_cycle, \
             patch('time.sleep') as mock_sleep:
            controller.run()
            assert mock_cycle.call_count == 1
            mock_sleep.assert_not_called()
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)

def test_run_halts_on_negative_polling(sample_config_dict):
    """Test ETLController halts after one cycle if polling_frequency is negative."""
    config_dict, sample_entries = populate_source_entries(sample_config_dict, 2)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config_dict, config_file)
        config_path = config_file.name

    try:
        controller = ETLController(config_path, polling_frequency=-5)
        with patch.object(controller, 'process_cycle', wraps=controller.process_cycle) as mock_cycle, \
             patch('time.sleep') as mock_sleep:
            controller.run()
            assert mock_cycle.call_count == 1
            mock_sleep.assert_not_called()
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)

def test_run_repeats_on_positive_polling(sample_config_dict):
    """Test ETLController repeats run if polling_frequency is positive."""
    config_dict, sample_entries = populate_source_entries(sample_config_dict, 2)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config_dict, config_file)
        config_path = config_file.name

    try:
        controller = ETLController(config_path, polling_frequency=1)
        # Patch process_cycle to break after 2 cycles
        call_counter = {'count': 0}
        orig_process_cycle = controller.process_cycle
        def limited_cycle():
            call_counter['count'] += 1
            if call_counter['count'] >= 2:
                # Set polling_frequency to 0 to break loop after 2nd cycle
                controller.polling_frequency = 0
            return orig_process_cycle()
        with patch.object(controller, 'process_cycle', side_effect=limited_cycle) as mock_cycle, \
             patch('time.sleep') as mock_sleep:
            controller.run()
            assert mock_cycle.call_count == 2
            mock_sleep.assert_called_once_with(1)
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)

def test_run_repeats_longer_on_positive_polling(sample_config_dict):
    """Test ETLController repeats run if polling_frequency is positive."""
    config_dict, sample_entries = populate_source_entries(sample_config_dict, 2)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as config_file:
        yaml.dump(config_dict, config_file)
        config_path = config_file.name

    try:
        expected_cycles = 10
        controller = ETLController(config_path, polling_frequency=1)
        # Patch process_cycle to break after 2 cycles
        call_counter = {'count': 0}
        orig_process_cycle = controller.process_cycle
        def limited_cycle():
            call_counter['count'] += 1
            if call_counter['count'] >= expected_cycles:
                # Set polling_frequency to 0 to break loop after 2nd cycle
                controller.polling_frequency = 0
            return orig_process_cycle()
        with patch.object(controller, 'process_cycle', side_effect=limited_cycle) as mock_cycle, \
             patch('time.sleep') as mock_sleep:
            controller.run()
            assert mock_cycle.call_count == expected_cycles
            assert mock_sleep.call_count == expected_cycles - 1 # Should sleep between cycles
    finally:
        if os.path.exists(config_path):
            os.remove(config_path)