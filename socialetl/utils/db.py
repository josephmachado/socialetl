import sqlite3
from contextlib import contextmanager
from typing import Iterator


class DatabaseConnection:
    def __init__(
        self, db_type: str = 'sqlite3', db_file: str = 'data/socialetl.db'
    ) -> None:
        """Class to connect to a database.

        Args:
            db_type (str, optional): Database type.
                Defaults to 'sqlite3'.
            db_file (str, optional): Database file.
                Defaults to 'data/socialetl.db'.
        """
        self._db_type = db_type
        self._db_file = db_file

    @contextmanager
    def managed_cursor(self) -> Iterator[sqlite3.Cursor]:
        """Function to create a managed database cursor.

        Yields:
            sqlite3.Cursor: A sqlite3 cursor.
        """
        if self._db_type == 'sqlite3':
            _conn = sqlite3.connect(self._db_file)
            cur = _conn.cursor()
            try:
                yield cur
            finally:
                _conn.commit()
                cur.close()
                _conn.close()

    def __str__(self) -> str:
        return f'{self._db_type}://{self._db_file}'


def db_factory(
    db_type: str = 'sqlite3', db_file: str = 'data/socialetl.db'
) -> DatabaseConnection:
    """Function to create an ETL object.

    Args:
        db_type (str, optional): Database type.
            Defaults to 'sqlite3'.
        db_file (str, optional): Database file.
            Defaults to 'data/socialetl.db'.

    Returns:
        DatabaseConnection: A DatabaseConnection object.
    """
    return DatabaseConnection(db_type=db_type, db_file=db_file)
