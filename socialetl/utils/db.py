import atexit
import logging
import sqlite3
from contextlib import contextmanager
from typing import Any, Dict, Iterator


class SingletonMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    Ref:
    https://refactoring.guru/design-patterns/singleton/python/example#example-0
    """

    _instances: Dict[Any, Any] = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class DatabaseConnection(metaclass=SingletonMeta):
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
        logging.info(f'Opening connection to {str(self)}')
        self._conn = sqlite3.connect(self._db_file)

    @contextmanager
    def managed_cursor(self) -> Iterator[sqlite3.Cursor]:
        """Function to create a managed database cursor.

        Yields:
            sqlite3.Cursor: A sqlite3 cursor.
        """
        if self._db_type == 'sqlite3':
            cur = self._conn.cursor()
            try:
                yield cur
            finally:
                self._conn.commit()
                cur.close()

    def close(self) -> None:
        """Function to close the database connection."""
        self._conn.close()

    def __str__(self) -> str:
        return f'{self._db_type}://{self._db_file}'


@atexit.register
def close() -> None:
    """Function to close the database connection."""
    db = DatabaseConnection()
    logging.info(f'Closing Database connection to {str(db)}')
    db.close()
