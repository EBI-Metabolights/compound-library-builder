import ast
import json
import sys
from typing import List
import yaml
import requests

from argparse_classes.parsers import ArgParsers
from compound_common.dir_utils import DirUtils
from compound_common.list_utils import ListUtils
from compound_common.transport_clients.redis_client import RedisClient
from config_classes.builder_config_files import MtblsWsUrls
from config_classes.transport.redis_config import (
    RedisConfig,
    CompoundBuilderRedisConfig,
)
from function_wrappers.builder_wrappers.http_exception_angel import http_exception_angel


class CompoundRedisQueueManager:
    """
    Manager built on top of redis client to handle compound queue operations.
    """

    def __init__(
        self,
        compound_builder_redis_config: CompoundBuilderRedisConfig,
        session: requests.Session = None,
        redis_client: RedisClient = None,
        mtbls_ws_config: MtblsWsUrls = MtblsWsUrls(),
    ):
        self.redis_client = redis_client
        self.mtbls_ws_config = mtbls_ws_config
        self.cbrc = compound_builder_redis_config
        self.session = session

    def populate_queue(self):
        """
        Populate the 'compounds' queue with chunks of MTBLC ids. First retrieves the complete list, and then 'chunks' it
         by breaking into a list of smaller sublists (the size of dictated by the CompoundBuilderRedisConfig) and then
        giving those chunks to the _push_compound_ids_to_redis method.
        :return: None
        """
        if self.redis_client.check_queue_exists(self.cbrc.name)["items"] > 0:
            print("Queue populated. Risk of duplication. Evacuating Queue.")
            self.redis_client.empty_queue(self.cbrc.name)
            return
        compounds = self.get_compounds_ids()
        chunked = ListUtils.get_lol(compounds, self.cbrc.chunk_size)
        self.push_compound_ids_to_redis(chunked)

    def push_compound_ids_to_redis(self, chunked_compound_lists: List[List[str]]):
        """
        Take in the 'chunked' lists of MTBLC ids, and push them each in turn to the 'compounds' redis queue.
        :param chunked_compound_list: A list of lists, where each interior list is a sequence of MTBLC123 ids.
        :return: None
        """
        sublist_index, success = 0, 0
        for lis in chunked_compound_lists:
            resp = self.redis_client.push_to_queue(self.cbrc.name, json.dumps(lis))
            if resp is not None:
                success += 1
                print(f"Pushed sublist {sublist_index} to {self.cbrc.name} queue")
            else:
                print(f"Unable to push sublist {sublist_index} to {self.cbrc.name} queue")
            sublist_index += 1

    @http_exception_angel
    def get_compounds_ids(self, mtblc_dir: str = None) -> List[str]:
        """
        Make a GET request to the MetaboLights webservice (v2) to get the full list of compound ids from the database.
        :return: List of compound ids retrieved from the webservice.
        """
        response = self.session.get(self.mtbls_ws_config.metabolights_ws_compounds_list)
        compounds = response.json()["content"]
        if self.cbrc.new_compounds_only:
            compounds = ListUtils.get_delta(
                compounds, DirUtils.get_mtblc_ids_from_directory(mtblc_dir)
            )
        return compounds

    def consume_queue(self) -> List[str]:
        """
        Pop a chunk of compound ids off of the compounds queue. The response comes back stringified, so we use ast to
        evaluate back as a proper List.
        :return: List of compound ids.
        """
        compound_chunk = self.redis_client.consume_queue(self.cbrc.name)
        return ast.literal_eval(compound_chunk)

    def annihilate_queue(self) -> int:
        """
        Delete the compounds queue and everything in it.
        :return: 1 if deleted successfully, 0 otherwise.
        """
        result = self.redis_client.empty_queue(self.cbrc.name)
        return result


if __name__ == "__main__":
    parser = ArgParsers.compound_queue_parser()
    args = parser.parse_args(sys.argv[1:])
    with open(f"{args.redis_config}", "r") as f:
        redis_config_yaml_data = yaml.safe_load(f)
    redis_client_config = RedisConfig(**redis_config_yaml_data)

    with open(f"{args.compound_queue_config}", "r") as qf:
        compound_queue_manager_config_yaml_data = yaml.safe_load(qf)
    compound_queue_manager_config = CompoundBuilderRedisConfig(
        **compound_queue_manager_config_yaml_data
    )

    CompoundRedisQueueManager(
        compound_builder_redis_config=compound_queue_manager_config,
        session=requests.Session(),
        redis_client=RedisClient(config=redis_client_config),
    ).populate_queue()
