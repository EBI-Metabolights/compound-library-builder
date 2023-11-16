import os
from typing import List


class DirUtils:
    """
    Collection of static directory methods
    """

    @staticmethod
    def get_mtblc_ids_from_directory(directory: str) -> List[str]:
        """
        Get a list of directories from a given directory.
        :param directory: Directory to search through.
        :return: List of directories, as strings.
        """
        list_of_ids = []
        for entry in os.scandir(directory):
            if entry.is_dir():
                list_of_ids.append(entry.name)
        return list_of_ids
