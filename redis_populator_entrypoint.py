import argparse
import sys

import requests
import yaml

from compound_common.transport_clients.redis_client import RedisClient
from compound_dir_builder.redis_queue_manager.redis_queue_manager import CompoundRedisQueueManager
from configs.transport.redis_config import RedisConfig

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
    session=requests.Session(),
    redis_client=RedisClient(config=config),
).populate_queue()
