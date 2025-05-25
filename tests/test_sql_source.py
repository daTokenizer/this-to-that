import os
import pytest
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, DateTime
from datetime import datetime
from sources.sql import SQLSource

@pytest.fixture
def test_db_path(tmp_path):
    return str(tmp_path / "test_sql_source.db")

@pytest.fixture
def test_engine(test_db_path):
    """Create a file-based SQLite database for testing."""
    engine = create_engine(f'sqlite:///{test_db_path}')
    metadata = MetaData()
    
    # Create test table
    users = Table('users', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('email', String),
        Column('created_at', DateTime)
    )
    
    metadata.create_all(engine)
    
    # Insert test data
    with engine.connect() as conn:
        conn.execute(users.insert(), [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'created_at': datetime(2024, 1, 15)},
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'created_at': datetime(2024, 2, 1)},
            {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'created_at': datetime(2023, 12, 1)}
        ])
        conn.commit()
    
    yield engine
    engine.dispose()
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

@pytest.fixture
def sql_source():
    """Create a SQL source instance."""
    return SQLSource()

def test_sql_source_initialization(sql_source, test_db_path):
    """Test SQL source initialization with connection details."""
    config = {
        'connection': {
            'dialect': 'sqlite',
            'database': test_db_path
        },
        'query': 'SELECT * FROM users',
        'params': {}
    }
    
    sql_source.initialize(config)
    assert sql_source.engine is not None
    assert sql_source.query == 'SELECT * FROM users'
    assert sql_source.params == {}

def test_sql_source_initialization_with_url(sql_source, test_db_path):
    """Test SQL source initialization with connection URL."""
    config = {
        'connection': {
            'url': f'sqlite:///{test_db_path}'
        },
        'query': 'SELECT * FROM users',
        'params': {}
    }
    
    sql_source.initialize(config)
    assert sql_source.engine is not None

def test_sql_source_initialization_missing_query(sql_source, test_db_path):
    """Test SQL source initialization with missing query."""
    config = {
        'connection': {
            'dialect': 'sqlite',
            'database': test_db_path
        },
        'params': {}
    }
    
    with pytest.raises(ValueError, match="No query specified"):
        sql_source.initialize(config)

def test_sql_source_get_entries(sql_source, test_engine, test_db_path):
    """Test retrieving entries from SQL source."""
    config = {
        'connection': {
            'url': f'sqlite:///{test_db_path}'
        },
        'query': 'SELECT * FROM users',
        'params': {}
    }
    
    sql_source.initialize(config)
    entries = sql_source.get_entries()
    
    assert len(entries) == 3
    assert entries[0]['name'] == 'John Doe'
    assert entries[1]['email'] == 'jane@example.com'
    assert entries[2]['id'] == 3

def test_sql_source_get_entries_with_params(sql_source, test_engine, test_db_path):
    """Test retrieving entries with query parameters."""
    config = {
        'connection': {
            'url': f'sqlite:///{test_db_path}'
        },
        'query': 'SELECT * FROM users WHERE created_at > :start_date',
        'params': {
            'start_date': '2024-01-01'
        }
    }
    
    sql_source.initialize(config)
    entries = sql_source.get_entries()
    
    assert len(entries) == 2  # Only entries from 2024
    assert all(datetime.fromisoformat(entry['created_at'].replace('Z', '+00:00')) > datetime(2024, 1, 1)
              for entry in entries)

def test_sql_source_get_entries_not_initialized(sql_source):
    """Test getting entries without initialization."""
    with pytest.raises(RuntimeError, match="SQL source not initialized"):
        sql_source.get_entries()

def test_sql_source_close(sql_source, test_db_path):
    """Test closing SQL source connection."""
    config = {
        'connection': {
            'url': f'sqlite:///{test_db_path}'
        },
        'query': 'SELECT * FROM users',
        'params': {}
    }
    
    sql_source.initialize(config)
    sql_source.close()
    assert sql_source.engine is None 