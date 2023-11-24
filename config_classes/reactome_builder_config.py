from pydantic import BaseModel

from config_classes.builder_config_files import MiscUrls


class ReactomeFileBuilderConfig(BaseModel):
    url: str = MiscUrls.reactome_url
    destination: str
    reactome_keys_map: dict = {
        "reactomeId": 1,
        "reactomeUrl": 2,
        "pathway": 3,
        "pathwayId": 4,
        "species": 5,
    }
