import os

class DirUtils:

    @staticmethod
    def get_mtblc_ids_from_directory(directory: str):
        list_of_ids = []
        for entry in os.scandir(directory):
            if entry.is_dir():
                list_of_ids.append(entry.name)
        return list_of_ids
