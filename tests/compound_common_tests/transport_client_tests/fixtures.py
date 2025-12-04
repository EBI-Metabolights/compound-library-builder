import pytest

from compound_common.transport_clients.redis.redis_client import RedisClient
from compound_common.config_classes import RedisConfig


@pytest.fixture
def redis_client_fixture():
    redis_config = RedisConfig(db=0, port=123, host="nohost", decode_responses=False)
    rc = RedisClient(redis_config)
    yield rc
    del rc
