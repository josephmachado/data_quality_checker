from collections.abc import Iterator
import shutil
import tempfile
from pathlib import Path
from pytest_mock import MockerFixture


import pytest

from data_quality_checker.connector.output_log import DBConnector


@pytest.fixture
def temp_db_path() -> Iterator[Path]:
    """Create a temporary directory and database path for testing"""
    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "test_db.sqlite"

    yield db_path

    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def db_connector(temp_db_path: Path) -> Iterator[DBConnector]:
    """Create a DBConnector instance with a temporary database"""
    connector = DBConnector(temp_db_path)
    yield connector
    # No need to close since connections are per-operation


@pytest.fixture
def mock_db_connector(mocker: MockerFixture) -> DBConnector:
    """Create a mocked DBConnector for testing without actual database operations"""
    mock_connector = mocker.Mock(spec=DBConnector)
    mock_connector.log = mocker.Mock()
    return mock_connector


@pytest.fixture
def data_dir() -> Path:
    """Return the absolute path to the tests/data directory"""
    return Path(__file__).parent / "data"


@pytest.fixture
def unique_data_path(data_dir: Path) -> str:
    return str(data_dir / "unique_data.csv")


@pytest.fixture
def duplicate_data_path(data_dir: Path) -> str:
    return str(data_dir / "duplicate_data.csv")


@pytest.fixture
def empty_data_path(data_dir: Path) -> str:
    return str(data_dir / "empty_data.csv")


@pytest.fixture
def no_nulls_path(data_dir: Path) -> str:
    return str(data_dir / "no_nulls.csv")


@pytest.fixture
def has_nulls_path(data_dir: Path) -> str:
    return str(data_dir / "has_nulls.csv")


@pytest.fixture
def valid_enum_path(data_dir: Path) -> str:
    return str(data_dir / "valid_enum.csv")


@pytest.fixture
def invalid_enum_path(data_dir: Path) -> str:
    return str(data_dir / "invalid_enum.csv")


@pytest.fixture
def null_enum_path(data_dir: Path) -> str:
    return str(data_dir / "null_enum.csv")


@pytest.fixture
def users_data_path(data_dir: Path) -> str:
    return str(data_dir / "users.csv")


@pytest.fixture
def orders_data_path(data_dir: Path) -> str:
    return str(data_dir / "orders.csv")


@pytest.fixture
def orphaned_orders_path(data_dir: Path) -> str:
    return str(data_dir / "orphaned_orders.csv")


@pytest.fixture
def corrupt_data_path(data_dir: Path) -> str:
    return str(data_dir / "corrupt_data.parquet")
