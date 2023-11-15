from typing import Optional

from pydantic import BaseModel

class RedisConfig(BaseModel):
    host: str
    port: int
    db: int
    decode_responses: bool
    password: Optional[str] = ''


class CompoundBuilderRedisConfig(BaseModel):
    chunk_size: int = 200
    new_compounds_only: bool = False
