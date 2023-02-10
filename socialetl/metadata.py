import inspect

from utils.db import db_factory


def log_metadata(func):
    def log_wrapper(*args, **kwargs):
        input_params = dict(
            zip(list(locals().keys())[:-1], list(locals().values())[:-1])
        )
        param_names = list(
            inspect.signature(func).parameters.keys()
        )  # order is preserved
        # match with input_params.get('args') and
        # then input_params.get('kwargs')
        input_dict = {}
        for v in input_params.get('args'):
            input_dict[param_names.pop(0)] = v

        db = db_factory()
        with db.managed_cursor() as cur:
            cur.execute(
                (
                    'INSERT INTO log_metadata (function_name, input_params)'
                    ' VALUES (:func_name, :input_params)'
                ),
                {
                    'func_name': func.__name__,
                    'input_params': str(
                        input_dict | input_params.get('kwargs')
                    ),
                },
            )
        return func(*args, **kwargs)

    return log_wrapper
