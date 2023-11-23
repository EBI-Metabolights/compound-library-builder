import ast
import json
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
        print('\tpopnlock - pop an item off the queue, and have it printed, and then push it back to the queue')
        print("\tpush - push an item to the current queue")
        print("\texit - quit the cli")
        print("\tset - set the current queue. Usage: set {queue_name}")
        print("\tlen - get length of current queue")
        print("\tevac - delete current queue and its contents")
        command = input("Enter command: ")
        if command == "exit":
            break
        if command == "pop":
            result = rc.consume_queue(current_queue)
            if current_queue == "compounds":
                result = ast.literal_eval(result)
                print(f"Number of things in queue item: {len(result)}")
            print(str(result))
        if "push" in command:
            result = rc.push_to_queue(current_queue, command.split(" ")[1:])
            print(f'Push to {current_queue} result: {result}')
        if command == "popnlock":
            result = rc.consume_queue(current_queue)
            result = ast.literal_eval(result)
            print(f"Number of things in queue item: {len(result)}")
            print(str(result))
            rc.push_to_queue(current_queue, json.dumps(result))
        if "set" in command:
            current_queue = command.split(" ")[1]
            print(f"queue set to {current_queue}")
        if command == "len":
            print(rc.check_queue_exists(current_queue))
        if command == 'evac':
            result = rc.empty_queue(current_queue)
            print(f'Evac result: [{result}].')



if __name__ == "__main__":
    parser = ArgParsers.redis_config_parser()
    args = parser.parse_args(sys.argv[1:])
    main(args)
