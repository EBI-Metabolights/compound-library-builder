import datetime
import sys
import requests
import yaml

import build_compound_dir
from argparse_classes.parsers import ArgParsers
from compound_common.timer import Timer
from compound_common.transport_clients.redis_client import RedisClient
from compound_dir_builder.redis_queue_manager.redis_queue_manager import CompoundRedisQueueManager
from configs.transport.redis_config import RedisConfig
from function_wrappers.builder_wrappers.debug_harness import compound_debug_harness
from mapping_file_builder.managers.mapping_persistence_manager import MappingPersistenceManager

"""
Warning! this script will fail unless it has its requirements.txt requirements installed!
"""

def main(args):
    parser = ArgParsers.compound_builder_parser()
    args = parser.parse_args(args)

    destination_directory = args.destination
    ftp = args.ftp
    new_compounds_only = args.new_compounds_only
    using_queue = args.queue
    overall_process_timer = Timer(datetime.datetime.now(), None)
    with open(f'{args.queue_config}', 'r') as f:
        yaml_data = yaml.safe_load(f)
    redis_config = RedisConfig(**yaml_data)

    mpm = MappingPersistenceManager(root=ftp, timers_enabled=False)
    crqm = CompoundRedisQueueManager(config=redis_config, session=requests.Session(), redis_client=RedisClient(config=redis_config))

    ml_mapping = mpm.msgpack.load('mapping')
    reactome_data = mpm.vanilla.load('reactome')

    # If we are using the redis queue, pop a chunk of compound IDs from the queue, otherwise get the full list
    compound_list = crqm.consume_queue() if using_queue else crqm.get_compounds_ids()
    # TODO: Re implement new compounds only

    for compound in compound_list:
        current_compound_timer = Timer(datetime.datetime.now(), None)
        # build process returns dict, no use for it in prod but handy when debugging
        __ = execute(
            metabolights_id=compound.strip(), ml_mapping=ml_mapping,reactome_data=reactome_data,
            data_directory=destination_directory)
        current_compound_timer.end = datetime.datetime.now()
        print(f'{compound} processing time: {current_compound_timer.delta()}')

    overall_process_timer.end = datetime.datetime.now()
    print(f'Time taken for compound building process: {overall_process_timer.delta()}')


@compound_debug_harness(enabled=True)
def execute(metabolights_id: str, ml_mapping: dict, reactome_data: dict, data_directory: str):
    result = build_compound_dir.build(
        metabolights_id=metabolights_id.strip(), ml_mapping=ml_mapping, reactome_data=reactome_data,
        data_directory=data_directory)
    return result


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))