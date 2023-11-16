
import json
import logging
from functools import wraps
from requests.exceptions import SSLError, ConnectionError, HTTPError, Timeout


def http_exception_angel(func):
    """
    A function wrapper to catch and log common http request exceptions. To be used only where it is not crucial for the
    method to fail gracefully - you will get a readout, a stacktrace in the event of connection error, but python will
    not stop. The wrapper is intended to save lines where previously there were identical try except blocks - again, use
    this wisely and sparingly.
    :param func: Function to wrap.
    :return: Results of function, or failing that, nothing, but log out the error.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError as e:
            print(f'KeyError when processing http request : {str(e)}')
        except Timeout as e:
            print(f'Request timed out when processing http request : {str(e)}')
        except HTTPError as e:
            print(f'HTTP Error code received when processing http request : {str(e)}')
            print(f'args: {str(args)}')
        except SSLError as e:
            print(f'Secure Sockets Layer Error when processing http request. Is the endpoint HTTPS enabled?')
        except ConnectionError as e:
            logging.exception(str(e))
            print(f'args: {str(args)}')
            print(f'ConnectionError when processing http request, is server reachable?: {str(e)}')
        except json.decoder.JSONDecodeError as e:
            print(f'JSONDecode error in {str(func)} args:{args} kwargs{kwargs}: {str(e)}')

    return wrapper
