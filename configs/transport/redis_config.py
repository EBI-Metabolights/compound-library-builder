from typing import Optional

from pydantic import BaseModel


class RedisConfig(BaseModel):
    """
    Basic redis config and authentication class. No defaults supplied, it should be populated by a redis.yaml
    """

    host: str
    port: int
    db: int
    decode_responses: bool
    password: Optional[str] = ""
    debug: Optional[bool] = False


class CompoundBuilderRedisConfig(BaseModel):
    """
    Config for Compound Manager that is built on top of the redis client - NOT for any redis config values
    """

    chunk_size: int = 200
    new_compounds_only: bool = False
