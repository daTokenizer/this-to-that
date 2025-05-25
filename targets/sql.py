from typing import Dict, Any, Iterable
from sqlalchemy import create_engine, text, Table, MetaData, Column, String, Integer
from sqlalchemy.exc import SQLAlchemyError
import logging
from controller import DataTarget

logger = logging.getLogger("SQLTarget")

class SQLTarget(DataTarget):
    """Generic SQL database target that can work with different SQL databases."""
    
    def __init__(self):
        self.engine = None
        self.table = None
        self.metadata = None
        self.batch_size = 1000
    
    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the SQL target with connection details and table configuration.
        
        Args:
            config: Dictionary containing:
                - connection: Dict with database connection details
                    - url: SQLAlchemy connection URL
                    - username: Database username (optional)
                    - password: Database password (optional)
                    - host: Database host (optional)
                    - port: Database port (optional)
                    - database: Database name (optional)
                - table: Table configuration
                    - name: Table name
                    - schema: Schema name (optional)
                    - columns: List of column definitions
                        - name: Column name
                        - type: Column type (string, integer, etc.)
                        - primary_key: Whether column is primary key (optional)
                - batch_size: Number of records to insert in one batch (optional)
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
            
            # Configure table
            table_config = config.get('table', {})
            if not table_config:
                raise ValueError("No table configuration specified")
            
            self.metadata = MetaData()
            
            # Create table definition
            columns = []
            for col_config in table_config.get('columns', []):
                col_type = self._get_column_type(col_config.get('type', 'string'))
                col = Column(
                    col_config['name'],
                    col_type,
                    primary_key=col_config.get('primary_key', False)
                )
                columns.append(col)
            
            # Handle schema for SQLite differently
            schema = table_config.get('schema')
            if schema and connection_url.startswith('sqlite://'):
                # For SQLite, include schema in table name
                table_name = f"{schema}_{table_config['name']}"
                schema = None
            else:
                table_name = table_config['name']
            
            self.table = Table(
                table_name,
                self.metadata,
                *columns,
                schema=schema
            )
            
            # Create table in database
            self.metadata.create_all(self.engine)
            
            # Set batch size
            self.batch_size = config.get('batch_size', 1000)
            
            # Log connection info
            if 'url' in conn_config:
                logger.info(f"Initialized SQL target with connection URL")
            else:
                logger.info(f"Initialized SQL target with connection to {host}")
            
        except Exception as e:
            logger.error(f"Failed to initialize SQL target: {e}")
            raise
    
    def _get_column_type(self, type_name: str):
        """Convert string type name to SQLAlchemy type."""
        type_map = {
            'string': String,
            'integer': Integer,
            # Add more types as needed
        }
        return type_map.get(type_name.lower(), String)
    
    def create_entries(self, entries: Iterable[Dict[str, Any]]) -> None:
        """Insert entries into the target table.
        
        Args:
            entries: Iterable of dictionaries containing data to insert.
        """
        if self.engine is None or self.table is None:
            raise RuntimeError("SQL target not initialized")
        
        try:
            # Convert entries to list for batch processing
            entries_list = list(entries)
            total_entries = len(entries_list)
            
            # Process in batches
            for i in range(0, total_entries, self.batch_size):
                batch = entries_list[i:i + self.batch_size]
                with self.engine.begin() as connection:
                    connection.execute(self.table.insert(), batch)
                
                logger.info(f"Inserted batch of {len(batch)} entries")
            
            logger.info(f"Successfully inserted {total_entries} entries")
            
        except SQLAlchemyError as e:
            logger.error(f"Error inserting entries: {e}")
            raise
    
    def close(self) -> None:
        """Close the database connection."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("Closed SQL target connection") 