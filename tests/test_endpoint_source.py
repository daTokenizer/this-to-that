import pytest
import requests
from unittest.mock import patch, Mock
from sources.endpoint import EndpointSource

@pytest.fixture
def mock_response():
    """Create a mock response object."""
    mock = Mock(spec=requests.Response)
    mock.status_code = 200
    mock.json.return_value = [{"id": 1, "name": "test"}]
    return mock

@pytest.fixture
def source_config():
    """Create a sample source configuration."""
    return {
        "base_url": "https://api.example.com",
        "endpoint": "/data",
        "headers": {"Authorization": "Bearer token"},
        "params": {"limit": 10},
        "auth": ("user", "pass"),
        "verify_ssl": True,
        "timeout": 30
    }

class TestEndpointSource:
    def test_initialization(self, source_config):
        """Test source initialization with valid config."""
        source = EndpointSource()
        source.initialize(source_config)
        
        assert source.base_url == source_config["base_url"]
        assert source.endpoint == source_config["endpoint"]
        assert source.headers == source_config["headers"]
        assert source.params == source_config["params"]
        assert source.auth == source_config["auth"]
        assert source.verify_ssl == source_config["verify_ssl"]
        assert source.timeout == source_config["timeout"]
        assert source.session is not None
    
    def test_initialization_missing_base_url(self):
        """Test source initialization with missing base_url."""
        source = EndpointSource()
        with pytest.raises(ValueError, match="base_url is required"):
            source.initialize({})
    
    def test_get_entries(self, source_config, mock_response):
        """Test fetching entries from the endpoint."""
        with patch('requests.Session.get', return_value=mock_response):
            source = EndpointSource()
            source.initialize(source_config)
            
            entries = source.get_entries()
            assert len(entries) == 1
            assert entries[0]["id"] == 1
            assert entries[0]["name"] == "test"
    
    def test_get_entries_not_initialized(self):
        """Test getting entries without initialization."""
        source = EndpointSource()
        with pytest.raises(RuntimeError, match="not initialized"):
            source.get_entries()
    
    def test_get_entries_request_error(self, source_config):
        """Test handling of request errors."""
        with patch('requests.Session.get', side_effect=requests.RequestException("Test error")):
            source = EndpointSource()
            source.initialize(source_config)
            
            with pytest.raises(requests.RequestException):
                source.get_entries()
    
    def test_close(self, source_config):
        """Test closing the source connection."""
        source = EndpointSource()
        source.initialize(source_config)
        source.close()
        assert source.session is None 