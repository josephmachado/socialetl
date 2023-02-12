import argparse
import logging

from social_etl import etl_factory  # type: ignore
from transform import transformation_factory
from utils.db import db_factory


def main(source: str, transformation: str) -> None:
    """Function to call the ETL code

    Args:
        source (str, optional): Defines which ata to pull.
        Defaults to 'reddit'.
    """
    logging.info(f'Starting {source} ETL')
    logging.info(f'Getting {source} ETL object from factory')
    client, social_etl = etl_factory(source)
    db = db_factory()
    social_etl.run(
        db_cursor_context=db.managed_cursor(),
        client=client,
        transform_function=transformation_factory(transformation),
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
        '--tx',
        choices=['sd', 'no_tx', 'rand'],
        default='no_tx',
        type=str,
        help='Indicates which transformation algorithm to run.',
    )
    parser.add_argument(
        '-log',
        '--loglevel',
        default='warning',
        help=(
            'Provide logging level. Example --loglevel debug, default=warning'
        ),
    )

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel.upper())
    main(source=args.etl, transformation=args.tx)
