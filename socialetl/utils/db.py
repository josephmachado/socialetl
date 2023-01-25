import atexit
import logging
import sqlite3
from contextlib import contextmanager


class SingletonMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    Ref: https://refactoring.guru/design-patterns/singleton/python/example#example-0
    """

    _instances = {}

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
            db_type (str, optional): Database type. Defaults to 'sqlite3'.
            db_file (str, optional): Database file. Defaults to 'data/socialetl.db'.
        """
        self.db_type = db_type
        self.db_file = db_file
        logging.info(f'Opening connection to {self.db_file}')
        self.conn = sqlite3.connect(self.db_file)

    @contextmanager
    def managed_cursor(self) -> sqlite3.Cursor:
        """Function to create a managed database cursor.

        Yields:
            sqlite3.Cursor: A sqlite3 cursor.
        """
        if self.db_type == 'sqlite3':
            cur = self.conn.cursor()
            try:
                yield cur
            finally:
                self.conn.commit()
                cur.close()


@atexit.register
def close() -> None:
    """Function to close the database connection."""
    db = DatabaseConnection()
    logging.info(f'Closing Database connection with file {db.db_file}')
    db.conn.close()
