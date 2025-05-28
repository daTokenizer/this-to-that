import logging
import requests
from typing import Dict, Any, Iterable
from urllib.parse import urljoin
import json

logger = logging.getLogger("ETLController")

class EndpointTarget:
    """Target for sending data to HTTP/HTTPS endpoints."""
    
    def __init__(self):
        """Initialize the endpoint target."""
        self.base_url = None
        self.endpoint = None
        self.headers = None
        self.auth = None
        self.session = None
        self.verify_ssl = True
        self.timeout = 30
        self.method = 'POST'
        self.batch_size = 100
    
    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the endpoint target with configuration.
        
        Args:
            config: Dictionary containing:
                - base_url: Base URL for the endpoint
                - endpoint: Path to append to base_url
                - headers: Optional dictionary of HTTP headers
                - auth: Optional tuple of (username, password) for basic auth
                - verify_ssl: Optional boolean to verify SSL certificates
                - timeout: Optional timeout in seconds
                - method: Optional HTTP method (default: POST)
                - batch_size: Optional number of entries to send in each request
        """
        self.base_url = config.get('base_url')
        if not self.base_url:
            raise ValueError("base_url is required in endpoint target configuration")
        
        self.endpoint = config.get('endpoint', '')
        self.headers = config.get('headers', {})
        self.auth = config.get('auth')
        self.verify_ssl = config.get('verify_ssl', True)
        self.timeout = config.get('timeout', 30)
        self.method = config.get('method', 'POST').upper()
        self.batch_size = config.get('batch_size', 100)
        
        # Create a session for connection pooling
        self.session = requests.Session()
        if self.auth:
            self.session.auth = self.auth
        
        logger.info(f"Initialized endpoint target with base URL: {self.base_url}")
    
    def create_entries(self, entries: Iterable[Dict[str, Any]]) -> None:
        """Send entries to the endpoint.
        
        Args:
            entries: Iterable of dictionaries containing the data to send.
            
        Raises:
            RuntimeError: If the target is not initialized
            requests.RequestException: If the request fails
        """
        if not self.session:
            raise RuntimeError("Endpoint target not initialized")
        
        url = urljoin(self.base_url, self.endpoint)
        entries_list = list(entries)
        
        # Process entries in batches
        for i in range(0, len(entries_list), self.batch_size):
            batch = entries_list[i:i + self.batch_size]
            
            try:
                # Prepare the request data
                if self.method in ['POST', 'PUT', 'PATCH']:
                    data = json.dumps(batch if len(batch) > 1 else batch[0])
                    headers = {**self.headers, 'Content-Type': 'application/json'}
                else:
                    data = None
                    headers = self.headers
                
                # Send the request
                response = self.session.request(
                    method=self.method,
                    url=url,
                    headers=headers,
                    data=data,
                    verify=self.verify_ssl,
                    timeout=self.timeout
                )
                response.raise_for_status()
                
                logger.info(f"Successfully sent batch of {len(batch)} entries to endpoint")
                
            except requests.RequestException as e:
                logger.error(f"Failed to send data to endpoint: {e}")
                raise
    
    def close(self) -> None:
        """Close the session and clean up resources."""
        if self.session:
            self.session.close()
            self.session = None
            logger.info("Closed endpoint target connection") 