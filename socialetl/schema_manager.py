import argparse
import logging

from utils.db import db_factory


def setup_db_schema():
    """Function to setup the database schema."""
    db = db_factory()
    with db.managed_cursor() as cur:
        logging.info('Creating social_posts table.')
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS social_posts (
                id TEXT PRIMARY KEY,
                source TEXT,
                social_data TEXT,
                dt_created datetime default current_timestamp
            )
            """
        )
        logging.info('Creating ETL metadata table.')
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS log_metadata (
                dt_created datetime default current_timestamp,
                function_name TEXT,
                input_params TEXT
            )
            """
        )


def teardown_db_schema():
    """Function to teardown the database schema."""
    db = db_factory()
    with db.managed_cursor() as cur:
        logging.info('Dropping social_posts table.')
        cur.execute('DROP TABLE IF EXISTS social_posts')
        logging.info('Dropping log_metadata table.')
        cur.execute('DROP TABLE IF EXISTS log_metadata')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--reset-db',
        action='store_true',
        help='Reset your database objects',
    )
    args = parser.parse_args()
    logging.basicConfig(level='INFO')
    if args.reset_db:
        teardown_db_schema()
        setup_db_schema()
