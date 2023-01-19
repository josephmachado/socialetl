from contextlib import contextmanager
import sqlite3


class DatabaseConnection:
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

    @contextmanager
    def managed_cursor(self) -> sqlite3.Cursor:
        """Function to create a managed database cursor.

        Yields:
            sqlite3.Cursor: A sqlite3 cursor.
        """
        if self.db_type == 'sqlite3':
            conn = sqlite3.connect(self.db_file)
            cur = conn.cursor()
            try:
                yield cur
            finally:
                conn.commit()
                cur.close()
                conn.close()
