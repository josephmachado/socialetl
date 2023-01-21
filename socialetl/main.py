import argparse
import logging
import os

import praw
import tweepy
from dotenv import load_dotenv
from utils.db import DatabaseConnection

from socialetl import RedditETL, TwitterETL, ETLFactory

load_dotenv()


def setup_db_schema():
    """Function to setup the database schema."""
    with DatabaseConnection().managed_cursor() as cur:
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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reddit_posts (
                title TEXT,
                score INTEGER,
                post_id TEXT PRIMARY KEY,
                url TEXT,
                comms_num INTEGER,
                created TEXT,
                body TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS twitter_posts (
                id INTEGER PRIMARY KEY,
                text TEXT
            )
            """
        )


def teardown_db_schema():
    """Function to teardown the database schema."""
    with DatabaseConnection().managed_cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS social_posts')
        cur.execute('DROP TABLE IF EXISTS reddit_posts')
        cur.execute('DROP TABLE IF EXISTS twitter_posts')


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

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel.upper())
    setup_db_schema()
    main(source=args.etl)
