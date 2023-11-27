import errno
import json
import os
from typing import Any

import yaml


class GeneralFileUtils:
    """
    Collection of general file read write methods.
    """

    @staticmethod
    def save_json_file(filename: str, data: dict) -> None:
        """
        Dump a given dict as a .json file. Check first that the directory we want to save to exists, and if it doesn't,
        create it.
        :param filename: string representation of the full path of the .json file to be.
        :param data: dict to be saved as a .json file
        :return: None
        """
        print(f"Attempting to save {filename}")
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        with open(filename, "w") as fp:
            try:
                json.dump(data, fp)
            except json.decoder.JSONDecodeError as e:
                print("what the hell " + str(e))
        if os.path.exists(filename):
            print(f"Successfully saved {filename}")
        else:
            print(f"Failed to save {filename}")

    @staticmethod
    def open_yaml_file(path_to_yaml: str) -> Any:
        """
        Open a given yaml file.
        :param path_to_yaml: Absolute path to given yaml file.
        :return: Loaded yaml file, likely as a dict.
        """
        with open(path_to_yaml, "r") as f:
            thing = yaml.safe_load(f)
        return thing
