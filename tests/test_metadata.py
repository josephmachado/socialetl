import json
import logging

from metadata import log_metadata
from utils.db import db_factory


class TestMetadata:
    def test_log_metadata(self, mocker):
        logging.info("Testing log_metadata decorator")

        @log_metadata
        def test_function(a, b, c, d=None):
            return a + b + c + d

        test_function(1, 2, 3, d=4)

        # check if test_function is logged in the database
        db = db_factory(db_file="data/test.db")
        with db.managed_cursor() as cur:
            cur.execute(
                "SELECT input_params FROM log_metadata WHERE function_name ="
                " 'test_function'"
            )
            rows = cur.fetchall()
            assert len(rows) == 1

            assert json.loads(rows[0][0].replace("'", '"')) == {
                'a': 1,
                'b': 2,
                'c': 3,
                'd': 4,
            }
