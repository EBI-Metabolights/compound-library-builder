from pydantic import BaseModel


class ReactomeFileBuilderConfig(BaseModel):
    url: str = "http://www.reactome.org/download/current/ChEBI2Reactome.txt"
    destination: str
    reactome_keys_map: dict = {
        "reactomeId": 1,
        "reactomeUrl": 2,
        "pathway": 3,
        "pathwayId": 4,
        "species": 5,
    }
