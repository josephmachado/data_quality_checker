import shutil
import tempfile
from pathlib import Path

import pytest

from data_quality_checker.connector.output_log import DBConnector


@pytest.fixture
def temp_db_path():
    """Create a temporary directory and database path for testing"""
    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "test_db.sqlite"

    yield db_path

    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def db_connector(temp_db_path):
    """Create a DBConnector instance with a temporary database"""
    connector = DBConnector(temp_db_path)
    yield connector
    # No need to close since connections are per-operation


@pytest.fixture
def mock_db_connector(mocker):
    """Create a mocked DBConnector for testing without actual database operations"""
    mock_connector = mocker.Mock(spec=DBConnector)
    mock_connector.log = mocker.Mock()
    return mock_connector
