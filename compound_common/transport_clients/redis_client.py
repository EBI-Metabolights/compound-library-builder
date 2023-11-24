import json
import logging
from typing import Any, Union

from config_classes.transport.redis_config import RedisConfig
import redis


class RedisClient:
    """
    Basic wrapper around pythons redis library. Takes in a pydantic config file, and currently allows for
    PUSH, POP, LLEN and DEL operations. Needs an instantiated redisConfig object to work. Necessary to have a redis.yaml
    file with the redis credentials (not included in this repository).
    """

    def __init__(self, config: RedisConfig):
        self.config = config

        self.redis = redis.Redis(
            host=config.host,
            port=config.port,
            db=config.db,
            decode_responses=config.decode_responses,
            password=config.password,
        )
        if config.debug:
            print(self.check_queue_exists("compounds"))

    def push_to_queue(self, queue_name, payload: Any) -> Union[Any, None]:
        """
        Push an item to a given queue. Queue will be created if it doesn't already exist.
        :param queue_name: Name of queue to be pushed to.
        :param payload: Item to be pushed to queue.
        :return: Response from redis, often an int (1) to indicate success.
        """
        serialized_message = None
        try:
            serialized_message = json.dumps(payload)
        except Exception as e:
            logging.exception(f"Couldnt serialize payload: {str(e)}")
        response = (
            self.redis.lpush(queue_name, serialized_message)
            if serialized_message
            else None
        )
        return response

    def check_queue_exists(self, queue_name) -> dict:
        """
        Check whether a given queue exists or not.
        :param queue_name: Name of queue to check is extant.
        :return: dict indicating queue's existence, and the number of items in the queue if it does exist.
        """
        exists = self.redis.exists(queue_name)
        length_of_list = self.redis.llen(queue_name) if exists else -1

        return {"exists": exists, "items": length_of_list}

    def empty_queue(self, queue_name) -> int:
        """
        Delete a given queue and all the items held within.
        :param queue_name: The name of the queue to delete.
        :return: int indicating number of keys deleted - 1 if queue deleted, 0 if not.
        """
        resp = self.redis.delete(queue_name)
        return resp

    def consume_queue(self, queue_name: str) -> Any:
        """
        Consume a single item from a given queue.
        :param queue_name: Queue to consume a single item from.
        :return: Item from queue/
        """
        seria = self.redis.lpop(queue_name)
        if seria is None:
            print(f"Nothing on {queue_name} queue")

        payload = json.loads(seria) if seria is not None else seria
        return payload
