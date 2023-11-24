import pytest

from compound_dir_builder.redis_queue_manager.redis_queue_manager import (
    CompoundRedisQueueManager,
)
from config_classes.transport.redis_config import CompoundBuilderRedisConfig


@pytest.fixture
def compound_redis_queue_manager_fixture():
    config = CompoundBuilderRedisConfig()
    crqm = CompoundRedisQueueManager(compound_builder_redis_config=config)
    yield crqm
    del crqm


@pytest.fixture
def compound_list_fixture():
    compounds = [
        "MTBLC123",
        "MTBLC246",
        "MTBLC369",
        "MTBLC492",
        "MTBLC615",
        "MTBLC738",
        "MTBLC861",
        "MTBLC984",
        "MTBLC1107",
        "MTBLC1230",
        "MTBLC1353",
        "MTBLC1476",
        "MTBLC1599",
        "MTBLC1722",
        "MTBLC1845",
        "MTBLC1968",
        "MTBLC2091",
        "MTBLC2214",
        "MTBLC2337",
        "MTBLC2460",
        "MTBLC2583",
        "MTBLC2706",
        "MTBLC2829",
        "MTBLC2952",
        "MTBLC3075",
        "MTBLC3198",
        "MTBLC3321",
        "MTBLC3444",
        "MTBLC3567",
        "MTBLC3690",
        "MTBLC3813",
        "MTBLC3936",
        "MTBLC4059",
        "MTBLC4182",
        "MTBLC4305",
        "MTBLC4428",
        "MTBLC4551",
        "MTBLC4674",
        "MTBLC4797",
        "MTBLC4920",
        "MTBLC5043",
        "MTBLC5166",
        "MTBLC5289",
        "MTBLC5412",
        "MTBLC5535",
        "MTBLC5658",
        "MTBLC5781",
        "MTBLC5904",
        "MTBLC6027",
        "MTBLC6150",
    ]
    yield compounds
    del compounds
