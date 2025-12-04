"""
Warning! this script will fail unless it has its requirements.txt requirements installed!
"""
import datetime
import sys

import requests

from compound_common.argparse_classes.parsers import ArgParsers
from compound_common.config_classes.transport.redis_config import RedisConfig, CompoundBuilderRedisConfig
from compound_common.list_utils import ListUtils
from compound_common.timer import Timer
from compound_common.transport_clients.redis.redis_client import RedisClient
from compound_common.transport_clients.redis.redis_queue_manager import CompoundRedisQueueManager
from compound_library_builder import build_compound_library

from compound_common.function_wrappers.builder_wrappers.debug_harness import compound_debug_harness
from reference_file_builders.mapping_file_builder.managers.mapping_persistence_manager import (
    MappingPersistenceManager,
)
from utils.command_line_utils import CommandLineUtils
from utils.general_file_utils import GeneralFileUtils

DEBUG_ENABLED=False


def main(args):
    # Extract command line arguments and ready up configs
    parser = ArgParsers.compound_builder_parser()
    args = parser.parse_args(args)
    overall_process_timer = Timer(datetime.datetime.now())

    redis_config = RedisConfig(**GeneralFileUtils.open_yaml_file(args.redis_config))
    compound_queue_manager_config = CompoundBuilderRedisConfig(
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
    chebi_bulk_session = requests.Session()

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
            process_compounds(compound_list, ml_mapping, reactome_data, args.destination, chebi_bulk_session, args.database)

    # do the whole list of MTBLC IDs in one batch (legacy)
    else:
        compound_list = crqm.get_compounds_ids()
        print(f"Number of compounds received from list: {len(compound_list)}")
        process_compounds(compound_list, ml_mapping, reactome_data, args.destination, chebi_bulk_session)

    overall_process_timer.end = datetime.datetime.now()
    print(f"Time taken for compound building process: {overall_process_timer.delta()}")


def process_compounds(compound_list, ml_mapping, reactome_data, data_directory,session: requests.Session, save_to_db=False):
    chebi_compound_objects = session.get(f"https://www.ebi.ac.uk/chebi/backend/api/public/compounds/?chebi_ids={ListUtils.mtblc_list_to_encoded_chebi(compound_list)}").json()
    clean = {k.strip(): v for k, v in chebi_compound_objects.items()}
    for compound in compound_list:
        current_compound_timer = Timer(datetime.datetime.now())
        obj_key = f"CHEBI:{compound.replace('MTBLC', '').strip().lstrip()}"
        __ = execute(
            metabolights_id=compound.strip(),
            ml_mapping=ml_mapping,
            reactome_data=reactome_data,
            data_directory=data_directory,
            save_to_db=save_to_db,
            chebi_obj=clean.get(obj_key)
        )
        current_compound_timer.end = datetime.datetime.now()
        print(f"{compound} processing time: {current_compound_timer.delta()}")


# TODO: make enabled configurable
@compound_debug_harness(enabled=DEBUG_ENABLED)
def execute(
    metabolights_id: str, ml_mapping: dict, reactome_data: dict, data_directory: str, save_to_db: bool, chebi_obj: dict
):
    result = build_compound_library.build_compound(
        metabolights_id=metabolights_id.strip(),
        ml_mapping=ml_mapping,
        reactome_data=reactome_data,
        data_directory=data_directory,
        save_to_db=save_to_db,
        chebi_obj=chebi_obj
    )
    return result


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
