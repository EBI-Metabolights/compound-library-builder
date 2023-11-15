import logging
from functools import wraps


def file_write_exception_angel(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError as e:
            print(f'Tried and failed to save {str(func)} (likely owing to permission problems): {str(e)}')
        except Exception as e:
            logging.exception(f'Unexpected error when saving {str(func)}: {str(e)}')
    return wrapper
