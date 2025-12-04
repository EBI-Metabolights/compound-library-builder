from functools import wraps


def dict_exception_angel(func):
    """
    Wrapper to swallow and report dictionary / JSON access errors.
    Use sparingly where failure should not halt the script.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)

        except KeyError as e:
            print(f"KeyError accessing JSON: missing key {e} in {func.__name__}")

        except TypeError as e:
            # e.g. data['x']['y'] when 'x' is None or a list
            print(f"TypeError in JSON access during {func.__name__}: {e}")

        except AttributeError as e:
            # e.g. calling .get on something not a dict
            print(f"AttributeError in JSON access during {func.__name__}: {e}")

        except ValueError as e:
            # optional: covers bad type conversions
            print(f"ValueError in JSON parsing during {func.__name__}: {e}")

    return wrapper
