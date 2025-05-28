import logging
import requests
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin
import json

logger = logging.getLogger("ETLController")

class EndpointSource:
    """Source for fetching data from HTTP/HTTPS endpoints."""
    
    def __init__(self):
        """Initialize the endpoint source."""
        self.base_url = None
        self.endpoint = None
        self.headers = None
        self.params = None
        self.auth = None
        self.session = None
        self.verify_ssl = True
        self.timeout = 30
    
    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the endpoint source with configuration.
        
        Args:
            config: Dictionary containing:
                - base_url: Base URL for the endpoint
                - endpoint: Path to append to base_url
                - headers: Optional dictionary of HTTP headers
                - params: Optional dictionary of query parameters
                - auth: Optional tuple of (username, password) for basic auth
                - verify_ssl: Optional boolean to verify SSL certificates
                - timeout: Optional timeout in seconds
        """
        self.base_url = config.get('base_url')
        if not self.base_url:
            raise ValueError("base_url is required in endpoint source configuration")
        
        self.endpoint = config.get('endpoint', '')
        self.headers = config.get('headers', {})
        self.params = config.get('params', {})
        self.auth = config.get('auth')
        self.verify_ssl = config.get('verify_ssl', True)
        self.timeout = config.get('timeout', 30)
        
        # Create a session for connection pooling
        self.session = requests.Session()
        if self.auth:
            self.session.auth = self.auth
        
        logger.info(f"Initialized endpoint source with base URL: {self.base_url}")
    
    def get_entries(self) -> List[Dict[str, Any]]:
        """Fetch entries from the endpoint.
        
        Returns:
            List of dictionaries containing the fetched data.
            
        Raises:
            RuntimeError: If the source is not initialized
            requests.RequestException: If the request fails
        """
        if not self.session:
            raise RuntimeError("Endpoint source not initialized")
        
        url = urljoin(self.base_url, self.endpoint)
        
        try:
            response = self.session.get(
                url,
                headers=self.headers,
                params=self.params,
                verify=self.verify_ssl,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Try to parse as JSON, fall back to text if not JSON
            try:
                data = response.json()
            except json.JSONDecodeError:
                data = {"content": response.text}
            
            # If the response is a single object, wrap it in a list
            if isinstance(data, dict):
                data = [data]
            
            logger.info(f"Successfully fetched {len(data)} entries from endpoint")
            return data
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from endpoint: {e}")
            raise
    
    def close(self) -> None:
        """Close the session and clean up resources."""
        if self.session:
            self.session.close()
            self.session = None
            logger.info("Closed endpoint source connection") 