import pytest
import requests
from unittest.mock import patch, Mock
from targets.endpoint import EndpointTarget

@pytest.fixture
def mock_response():
    """Create a mock response object."""
    mock = Mock(spec=requests.Response)
    mock.status_code = 200
    mock.json.return_value = [{"id": 1, "name": "test"}]
    return mock

@pytest.fixture
def target_config():
    """Create a sample target configuration."""
    return {
        "base_url": "https://api.example.com",
        "endpoint": "/data",
        "headers": {"Authorization": "Bearer token"},
        "auth": ("user", "pass"),
        "verify_ssl": True,
        "timeout": 30,
        "method": "POST",
        "batch_size": 100
    }

class TestEndpointTarget:
    def test_initialization(self, target_config):
        """Test target initialization with valid config."""
        target = EndpointTarget()
        target.initialize(target_config)
        
        assert target.base_url == target_config["base_url"]
        assert target.endpoint == target_config["endpoint"]
        assert target.headers == target_config["headers"]
        assert target.auth == target_config["auth"]
        assert target.verify_ssl == target_config["verify_ssl"]
        assert target.timeout == target_config["timeout"]
        assert target.method == target_config["method"]
        assert target.batch_size == target_config["batch_size"]
        assert target.session is not None
    
    def test_initialization_missing_base_url(self):
        """Test target initialization with missing base_url."""
        target = EndpointTarget()
        with pytest.raises(ValueError, match="base_url is required"):
            target.initialize({})
    
    def test_create_entries(self, target_config, mock_response):
        """Test sending entries to the endpoint."""
        with patch('requests.Session.request', return_value=mock_response):
            target = EndpointTarget()
            target.initialize(target_config)
            
            entries = [{"id": 1, "name": "test"}]
            target.create_entries(entries)
    
    def test_create_entries_not_initialized(self):
        """Test creating entries without initialization."""
        target = EndpointTarget()
        with pytest.raises(RuntimeError, match="not initialized"):
            target.create_entries([{"id": 1}])
    
    def test_create_entries_request_error(self, target_config):
        """Test handling of request errors."""
        with patch('requests.Session.request', side_effect=requests.RequestException("Test error")):
            target = EndpointTarget()
            target.initialize(target_config)
            
            with pytest.raises(requests.RequestException):
                target.create_entries([{"id": 1}])
    
    def test_create_entries_batching(self, target_config, mock_response):
        """Test batch processing of entries."""
        with patch('requests.Session.request', return_value=mock_response) as mock_request:
            target = EndpointTarget()
            target.initialize(target_config)
            
            # Create more entries than the batch size
            entries = [{"id": i} for i in range(250)]
            target.create_entries(entries)
            
            # Should make 3 requests (2 full batches + 1 partial)
            assert mock_request.call_count == 3
    
    def test_close(self, target_config):
        """Test closing the target connection."""
        target = EndpointTarget()
        target.initialize(target_config)
        target.close()
        assert target.session is None 