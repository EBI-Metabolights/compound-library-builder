from compound_common.config_classes.builder_config_files import MtblsWsUrls
from pydantic import BaseModel

from reference_file_builders.mapping_file_builder.mapping_file_builder_enums import (
    PersistenceEnum,
)


class MappingFileBuilderConfig(BaseModel):
    mtbls_ws: MtblsWsUrls = MtblsWsUrls()
    timeout: int = 500
    thread_count: int = 6
    debug: bool = False
    pers: PersistenceEnum = PersistenceEnum.msgpack
    destination: str = ""
