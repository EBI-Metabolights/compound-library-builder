from pydantic import BaseModel


class FTPConfig(BaseModel):
    enabled: bool
    root: str
    study: str
    user: str
    password: str