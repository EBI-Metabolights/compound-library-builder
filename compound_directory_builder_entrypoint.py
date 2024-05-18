"""
Warning! this script will fail unless it has its requirements.txt requirements installed!
"""
import datetime
import sys

import requests

from argparse_classes.parsers import ArgParsers
from compound_common.timer import Timer
from compound_common.transport_clients.redis_client import RedisClient
from compound_dir_builder import build_compound_dir
from compound_dir_builder.redis_queue_manager.redis_queue_manager import (
    CompoundRedisQueueManager,
)
from config_classes.transport.redis_config import RedisConfig
from config_classes.transport.redis_config import CompoundBuilderRedisConfig as CPRG
from function_wrappers.builder_wrappers.debug_harness import compound_debug_harness
from reference_file_builders.mapping_file_builder.managers.mapping_persistence_manager import (
    MappingPersistenceManager,
)
from utils.command_line_utils import CommandLineUtils
from utils.general_file_utils import GeneralFileUtils



def main(args):
    # Extract command line arguments and ready up configs
    parser = ArgParsers.compound_builder_parser()
    args = parser.parse_args(args)
    overall_process_timer = Timer(datetime.datetime.now())

    redis_config = RedisConfig(**GeneralFileUtils.open_yaml_file(args.redis_config))
    compound_queue_manager_config = CPRG(
        **GeneralFileUtils.open_yaml_file(args.compound_queue_config)
    )

    CommandLineUtils.readout(args, redis_config, compound_queue_manager_config)

    # Initialise Managers
    mpm = MappingPersistenceManager(root=args.ref, timers_enabled=False)
    crqm = CompoundRedisQueueManager(
        compound_builder_redis_config=compound_queue_manager_config,
        session=requests.Session(),
        redis_client=RedisClient(config=redis_config),
    )

    # Load reference files
    ml_mapping = mpm.msgpack.load("mapping")
    reactome_data = mpm.vanilla.load("reactome")

    # if there are chunks of MTBLC ids on the queue still, keep popping until there aren't
    if args.queue:
        while crqm.redis_client.check_queue_exists(compound_queue_manager_config.name)['items'] > 0:
            compound_list = crqm.consume_queue()
            if not compound_list:
                break
            print(f"Number of compounds received from list: {len(compound_list)}")
            process_compounds(compound_list, ml_mapping, reactome_data, args.destination)

    # do the whole list of MTBLC IDs in one batch (legacy)
    else:
        compound_list = crqm.get_compounds_ids()
        print(f"Number of compounds received from list: {len(compound_list)}")
        process_compounds(compound_list, ml_mapping, reactome_data, args.destination)

    overall_process_timer.end = datetime.datetime.now()
    print(f"Time taken for compound building process: {overall_process_timer.delta()}")


def process_compounds(compound_list, ml_mapping, reactome_data, data_directory):
    for compound in compound_list:
        current_compound_timer = Timer(datetime.datetime.now())
        __ = execute(
            metabolights_id=compound.strip(),
            ml_mapping=ml_mapping,
            reactome_data=reactome_data,
            data_directory=data_directory,
        )
        current_compound_timer.end = datetime.datetime.now()
        print(f"{compound} processing time: {current_compound_timer.delta()}")

# TODO: make enabled configurable
@compound_debug_harness(enabled=True)
def execute(
    metabolights_id: str, ml_mapping: dict, reactome_data: dict, data_directory: str
):
    result = build_compound_dir.build(
        metabolights_id=metabolights_id.strip(),
        ml_mapping=ml_mapping,
        reactome_data=reactome_data,
        data_directory=data_directory,
    )
    return result


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
