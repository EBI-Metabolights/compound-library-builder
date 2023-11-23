import ast
import sys

import yaml

from argparse_classes.parsers import ArgParsers
from compound_common.transport_clients.redis_client import RedisClient
from configs.transport.redis_config import RedisConfig


def main(args):
    current_queue = "compounds"
    with open(f"{args.redis_config}", "r") as f:
        yaml_data = yaml.safe_load(f)
    rc = RedisClient(config=RedisConfig(**yaml_data))
    while True:
        print(f"Current queue is: {current_queue}")
        print("Available commands: ")
        print("\tpop - pop an item off the queue, and have it printed")
        print("\texit - quit the cli")
        print("\tset - set the current queue. Usage: set {queue_name}")
        print("\tlen - get length of current queue")
        command = input("Enter command: ")
        if command == "exit":
            break
        if command == "pop":
            result = rc.consume_queue(current_queue)
            if current_queue == "compounds":
                result = ast.literal_eval(result)
                print(f"Number of things in queue item: {len(result)}")
            print(str(result))
        if "set" in command:
            current_queue = command.split(" ")[1]
            print(f"queue set to {current_queue}")
        if command == "len":
            print(rc.check_queue_exists(current_queue))


if __name__ == "__main__":
    parser = ArgParsers.redis_config_parser()
    args = parser.parse_args(sys.argv[1:])
    main(args)
