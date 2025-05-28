import os
import pytest
from sqlalchemy import create_engine, MetaData, select
from targets.sql import SQLTarget

@pytest.fixture
def test_db_path(tmp_path):
    return str(tmp_path / "test_sql_target.db")

@pytest.fixture
def test_engine(test_db_path):
    """Create a file-based SQLite database for testing."""
    engine = create_engine(f'sqlite:///{test_db_path}')
    yield engine
    engine.dispose()
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

@pytest.fixture
def sql_target():
    """Create a SQL target instance."""
    return SQLTarget()

def test_sql_target_initialization(sql_target, test_db_path):
    """Test SQL target initialization with connection details."""
    config = {
        'connection': {
            'dialect': 'sqlite',
            'database': test_db_path
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
    print(test_db_path)

    sql_target.initialize(config)
    
    # Verify engine was created and connected
    assert sql_target.engine is not None
    assert sql_target.engine.dialect.name == 'sqlite'
    
    # Verify table was created with correct schema
    assert sql_target.table is not None
    assert sql_target.table.name == 'test_table'
    assert len(sql_target.table.columns) == 3
    
    # Verify batch size was set
    assert sql_target.batch_size == 100
    
    # Verify table exists in database
    metadata = MetaData()
    metadata.reflect(bind=sql_target.engine)
    assert 'test_table' in metadata.tables

def test_sql_target_initialization_with_url(sql_target, test_db_path):
    """Test SQL target initialization with connection URL."""
    config = {
        'connection': {
            'url': f'sqlite:///{test_db_path}'
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

def test_sql_target_initialization_missing_table(sql_target, test_db_path):
    """Test SQL target initialization with missing table configuration."""
    config = {
        'connection': {
            'dialect': 'sqlite',
            'database': test_db_path
        }
    }
    
    with pytest.raises(ValueError, match="No table configuration specified"):
        sql_target.initialize(config)

def test_sql_target_create_entries(sql_target, test_engine, test_db_path):
    """Test creating entries in the target table."""
    config = {
        'connection': {
            'url': f'sqlite:///{test_db_path}'
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

def test_sql_target_create_entries_batch_processing(sql_target, test_engine, test_db_path):
    """Test batch processing of entries."""
    config = {
        'connection': {
            'url': f'sqlite:///{test_db_path}'
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

def test_sql_target_close(sql_target, test_db_path):
    """Test closing SQL target connection."""
    config = {
        'connection': {
            'url': f'sqlite:///{test_db_path}'
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

def test_sql_target_schema_support(sql_target, test_engine, test_db_path):
    """Test SQL target with schema support."""
    config = {
        'connection': {
            'url': f'sqlite:///{test_db_path}'
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
    
    # Verify table was created with schema (as prefix in SQLite)
    with test_engine.connect() as conn:
        # Table name will be 'etl_users' for SQLite
        result = conn.execute(select(sql_target.table)).fetchall()
        assert len(result) == 1
        assert result[0].name == 'John Doe' 