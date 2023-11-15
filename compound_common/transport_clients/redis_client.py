import json
import logging
from typing import Any, Union

from configs.transport.redis_config import RedisConfig
import redis


class RedisClient:

    def __init__(self, config: RedisConfig):
        self.config = config

        self.redis = redis.Redis(
            host=config.host, port=config.port, db=config.db, decode_responses=config.decode_responses,
            password=config.password
        )
        print(self.check_queue_exists('compounds'))

    def push_to_queue(self, queue_name, payload: Any) -> Union[Any, None]:
        serialized_message = None
        try:
            serialized_message = json.dumps(payload)
        except Exception as e:
            logging.exception(f'Couldnt serialize payload: {str(e)}')
        response = self.redis.lpush(queue_name, serialized_message) if serialized_message else None
        return response

    def check_queue_exists(self, queue_name) -> dict:
        exists = self.redis.exists(queue_name)
        length_of_list = self.redis.llen(queue_name) if exists else -1

        return {'exists': exists, 'items': length_of_list}

    def empty_queue(self, queue_name) -> Any:
        resp = self.redis.delete(queue_name)
        return None

    def consume_queue(self, queue_name: str) -> Any:
        seria = self.redis.lpop(queue_name)
        if seria is None:
            print(f'Nothing on {queue_name} queue')

        payload = json.loads(seria) if seria is not None else seria
        return payload
