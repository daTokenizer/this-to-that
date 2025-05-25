import pytest
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, select
from datetime import datetime
from targets.sql_target import SQLTarget

@pytest.fixture
def test_engine():
    """Create an in-memory SQLite database for testing."""
    return create_engine('sqlite:///:memory:')

@pytest.fixture
def sql_target():
    """Create a SQL target instance."""
    return SQLTarget()

def test_sql_target_initialization(sql_target):
    """Test SQL target initialization with connection details."""
    config = {
        'connection': {
            'dialect': 'sqlite',
            'database': ':memory:'
        },
        'table': {
            'name': 'test_table',
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'name', 'type': 'string'},
                {'name': 'email', 'type': 'string'}
            ]
        },
        'batch_size': 100
    }
    
    sql_target.initialize(config)
    assert sql_target.engine is not None
    assert sql_target.table is not None
    assert sql_target.batch_size == 100

def test_sql_target_initialization_with_url(sql_target):
    """Test SQL target initialization with connection URL."""
    config = {
        'connection': {
            'url': 'sqlite:///:memory:'
        },
        'table': {
            'name': 'test_table',
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'name', 'type': 'string'}
            ]
        }
    }
    
    sql_target.initialize(config)
    assert sql_target.engine is not None
    assert sql_target.table is not None

def test_sql_target_initialization_missing_table(sql_target):
    """Test SQL target initialization with missing table configuration."""
    config = {
        'connection': {
            'dialect': 'sqlite',
            'database': ':memory:'
        }
    }
    
    with pytest.raises(ValueError, match="No table configuration specified"):
        sql_target.initialize(config)

def test_sql_target_create_entries(sql_target, test_engine):
    """Test creating entries in the target table."""
    config = {
        'connection': {
            'url': 'sqlite:///:memory:'
        },
        'table': {
            'name': 'users',
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'name', 'type': 'string'},
                {'name': 'email', 'type': 'string'},
                {'name': 'created_at', 'type': 'string'}
            ]
        },
        'batch_size': 2
    }
    
    sql_target.initialize(config)
    
    # Create test data
    entries = [
        {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'created_at': '2024-01-15'},
        {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'created_at': '2024-02-01'},
        {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'created_at': '2024-02-15'}
    ]
    
    sql_target.create_entries(entries)
    
    # Verify data was inserted
    with test_engine.connect() as conn:
        result = conn.execute(select(sql_target.table)).fetchall()
        assert len(result) == 3
        assert result[0].name == 'John Doe'
        assert result[1].email == 'jane@example.com'
        assert result[2].id == 3

def test_sql_target_create_entries_batch_processing(sql_target, test_engine):
    """Test batch processing of entries."""
    config = {
        'connection': {
            'url': 'sqlite:///:memory:'
        },
        'table': {
            'name': 'users',
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'name', 'type': 'string'}
            ]
        },
        'batch_size': 2
    }
    
    sql_target.initialize(config)
    
    # Create test data
    entries = [
        {'id': 1, 'name': 'John Doe'},
        {'id': 2, 'name': 'Jane Smith'},
        {'id': 3, 'name': 'Bob Johnson'},
        {'id': 4, 'name': 'Alice Brown'}
    ]
    
    sql_target.create_entries(entries)
    
    # Verify data was inserted in batches
    with test_engine.connect() as conn:
        result = conn.execute(select(sql_target.table)).fetchall()
        assert len(result) == 4
        assert [r.name for r in result] == ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown']

def test_sql_target_create_entries_not_initialized(sql_target):
    """Test creating entries without initialization."""
    entries = [{'id': 1, 'name': 'John Doe'}]
    with pytest.raises(RuntimeError, match="SQL target not initialized"):
        sql_target.create_entries(entries)

def test_sql_target_close(sql_target):
    """Test closing SQL target connection."""
    config = {
        'connection': {
            'url': 'sqlite:///:memory:'
        },
        'table': {
            'name': 'test_table',
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'name', 'type': 'string'}
            ]
        }
    }
    
    sql_target.initialize(config)
    sql_target.close()
    assert sql_target.engine is None

def test_sql_target_schema_support(sql_target, test_engine):
    """Test SQL target with schema support."""
    config = {
        'connection': {
            'url': 'sqlite:///:memory:'
        },
        'table': {
            'name': 'users',
            'schema': 'etl',
            'columns': [
                {'name': 'id', 'type': 'integer', 'primary_key': True},
                {'name': 'name', 'type': 'string'}
            ]
        }
    }
    
    sql_target.initialize(config)
    
    # Create test data
    entries = [{'id': 1, 'name': 'John Doe'}]
    sql_target.create_entries(entries)
    
    # Verify table was created with schema
    with test_engine.connect() as conn:
        result = conn.execute(select(sql_target.table)).fetchall()
        assert len(result) == 1
        assert result[0].name == 'John Doe' 