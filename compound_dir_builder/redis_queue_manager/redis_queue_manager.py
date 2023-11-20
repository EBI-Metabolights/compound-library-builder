import argparse
import ast
import json
import sys
from typing import List
import yaml
import requests

from compound_common.dir_utils import DirUtils
from compound_common.list_utils import ListUtils
from compound_common.transport_clients.redis_client import RedisClient
from configs.builder_config_files import MtblsWsUrls
from configs.transport.redis_config import RedisConfig, CompoundBuilderRedisConfig
from function_wrappers.builder_wrappers.http_exception_angel import http_exception_angel


class CompoundRedisQueueManager:
    """
    Manager built on top of redis client to handle compound queue operations.
    """

    def __init__(
        self,
        config: RedisConfig = None,
        session: requests.Session = None,
        redis_client: RedisClient = None,
        mtbls_ws_config: MtblsWsUrls = MtblsWsUrls(),
        compound_builder_redis_config: CompoundBuilderRedisConfig = CompoundBuilderRedisConfig(),
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
        if self.redis_client.check_queue_exists("compounds")["items"] > 0:
            print("Queue populated. Risk of duplication. Aborting.")
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
            resp = self.redis_client.push_to_queue("compounds", json.dumps(lis))
            if resp is not None:
                success += 1
                print(f"Pushed sublist {sublist_index} to queue")
            else:
                print(f"Unable to push sublist {sublist_index} to compound queue")
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
        compound_chunk = self.redis_client.consume_queue("compounds")
        return ast.literal_eval(compound_chunk)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        help="Absolute path to redis config.yaml file",
        default="/Users/cmartin/Projects/compound-directory-builder/.secrets/redis.yaml",
    )
    args = parser.parse_args(sys.argv[1:])
    with open(f"{args.config}", "r") as f:
        yaml_data = yaml.safe_load(f)
    config = RedisConfig(**yaml_data)
    CompoundRedisQueueManager(
        config=config,
        session=requests.Session(),
        redis_client=RedisClient(config=config),
    )  # .go()
