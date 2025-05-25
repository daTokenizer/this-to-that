from typing import Dict, Any, List
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import logging
from controller import DataSource

logger = logging.getLogger("SQLSource")

class SQLSource(DataSource):
    """Generic SQL database source that can work with different SQL databases."""
    
    def __init__(self):
        self.engine: Engine = None
        self.connection = None
        self.query = None
        self.params = {}
    
    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the SQL source with connection details and query configuration.
        
        Args:
            config: Dictionary containing:
                - connection: Dict with database connection details
                    - url: SQLAlchemy connection URL
                    - username: Database username (optional)
                    - password: Database password (optional)
                    - host: Database host (optional)
                    - port: Database port (optional)
                    - database: Database name (optional)
                - query: SQL query to execute
                - params: Optional parameters for the query
        """
        try:
            # Extract connection details
            conn_config = config.get('connection', {})
            if 'url' in conn_config:
                connection_url = conn_config['url']
                # For SQLite, ensure proper URL format
                if connection_url.startswith('sqlite://'):
                    if ':memory:' in connection_url:
                        connection_url = 'sqlite:///:memory:'
                    elif not connection_url.startswith('sqlite:///'):
                        connection_url = connection_url.replace('sqlite://', 'sqlite:///')
            else:
                # Construct URL from components
                dialect = conn_config.get('dialect', 'postgresql')
                username = conn_config.get('username', '')
                password = conn_config.get('password', '')
                host = conn_config.get('host', 'localhost')
                port = conn_config.get('port', '')
                database = conn_config.get('database', '')
                
                # Special handling for SQLite
                if dialect == 'sqlite':
                    if database == ':memory:':
                        connection_url = 'sqlite:///:memory:'
                    else:
                        connection_url = f'sqlite:///{database}'
                else:
                    # Construct URL for other databases
                    if username and password:
                        connection_url = f"{dialect}://{username}:{password}@{host}"
                    else:
                        connection_url = f"{dialect}://{host}"
                    
                    if port:
                        connection_url += f":{port}"
                    if database:
                        connection_url += f"/{database}"
            
            # Create engine
            self.engine = create_engine(connection_url)
            
            # Store query and parameters
            self.query = config.get('query')
            if not self.query:
                raise ValueError("No query specified in configuration")
            
            self.params = config.get('params', {})
            
            # Log connection info
            if 'url' in conn_config:
                logger.info(f"Initialized SQL source with connection URL: {connection_url}")
            else:
                logger.info(f"Initialized SQL source with connection to {host}")
            
        except Exception as e:
            logger.error(f"Failed to initialize SQL source: {e}")
            raise
    
    def get_entries(self) -> List[Dict[str, Any]]:
        """Execute the configured query and return results as a list of dictionaries.
        
        Returns:
            List of dictionaries containing query results.
        """
        if not self.engine:
            raise RuntimeError("SQL source not initialized")
        
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(self.query), self.params)
                # Use SQLAlchemy 2.x compatible row-to-dict conversion
                entries = list(result.mappings())
                logger.info(f"Retrieved {len(entries)} entries from SQL source")
                return entries
                
        except SQLAlchemyError as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def close(self) -> None:
        """Close the database connection."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("Closed SQL source connection") 