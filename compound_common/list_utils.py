from typing import List


class ListUtils:
    """
    Collection of list operation methods
    """

    @staticmethod
    def get_lol(master_list: List[str], count: int) -> List[List[str]]:
        """
        Get list of lists - make chunks out of a given list of strings and put all those chunks in a list.
        :param master_list: A list of MTBLS accessions ie ['MTBLS1','MTBLS2'....]
        :param count: The size of each sublist
        :return: A list of sublists of MTBLS accessions.
        """
        num_sublists = len(master_list) // count

        result = [master_list[i * count : (i + 1) * count] for i in range(num_sublists)]
        if len(master_list) % num_sublists != 0:
            remaining_items = master_list[num_sublists * count :]
            result.append(remaining_items)
        return result

    @staticmethod
    def get_delta(webservice_list: List[str], filesystem_list: List[str]) -> List[str]:
        """
        Return IDs that are found only in the webservice list.
        :param webservice_list: List of MTBLC ids from the webservice.
        :param filesystem_list: List of MTBLC ids from filesystem.
        :return: List of ids found only in webservice
        """
        return list(set(webservice_list) - set(filesystem_list))
