# wrote a decorator to save try/except-ing the same exception in every method
from functools import wraps
import xml.etree.ElementTree as ET


def xml_exception_angel(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ET.ParseError as e:
            print(f'XML parsing error occurred: {str(e)}')
        except AttributeError as e:
            print(f'Attribute error while calling .find on xml document: {str(func)} {str(e)}')

    return wrapper