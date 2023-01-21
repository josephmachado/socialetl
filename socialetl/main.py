import argparse
import logging

from dotenv import load_dotenv
from utils.db import DatabaseConnection

from socialetl import ETLFactory

load_dotenv()


def setup_db_schema():
    """Function to setup the database schema."""
    with DatabaseConnection().managed_cursor() as cur:
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


def teardown_db_schema():
    """Function to teardown the database schema."""
    logging.info('Dropping social_posts table.')
    with DatabaseConnection().managed_cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS social_posts')


def main(source: str = 'reddit') -> None:
    """Function to call the ETL code

    Args:
        source (str, optional): Defines which ata to pull. Defaults to 'reddit'.
    """
    logging.info(f'Starting {source} ETL')
    logging.info(f'Getting {source} ETL object from factory')
    client, id, num_records, social_etl = ETLFactory().create_etl(source)
    DB_CURSOR = DatabaseConnection().managed_cursor()
    social_etl.run(
        db_cursor_context=DB_CURSOR,
        client=client,
        num_records=num_records,
        id=id,
    )
    logging.info(f'Finished {source} ETL')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--etl',
        choices=['reddit', 'twitter'],
        default='reddit',
        type=str,
        help='Indicates which ETL to run.',
    )
    parser.add_argument(
        '-log',
        '--loglevel',
        default='warning',
        help='Provide logging level. Example --loglevel debug, default=warning',
    )
    parser.add_argument(
        '--reset-db',
        action='store_true',
        help='Reset your database objects',
    )

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel.upper())
    if args.reset_db:
        teardown_db_schema()
        setup_db_schema()
    main(source=args.etl)
