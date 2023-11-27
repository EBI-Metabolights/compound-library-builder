import sys

import requests
import yaml

from argparse_classes.parsers import ArgParsers
from compound_common.transport_clients.redis_client import RedisClient
from compound_dir_builder.redis_queue_manager.redis_queue_manager import (
    CompoundRedisQueueManager,
)
from config_classes.transport.redis_config import (
    RedisConfig,
    CompoundBuilderRedisConfig,
)


def main(args):
    parser = ArgParsers.compound_queue_parser()
    args = parser.parse_args(args)

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


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
