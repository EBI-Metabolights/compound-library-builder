from configs.builder_config_files import MtblsWsUrls
from mapping_file_builder.mapping_file_builder import PersistenceEnum
from pydantic import BaseModel


class MappingFileBuilderConfig(BaseModel):
    mtbls_ws: MtblsWsUrls = MtblsWsUrls()
    timeout: int = 500
    thread_count: int = 6
    debug: bool = False
    pers: PersistenceEnum = PersistenceEnum.msgpack
    destination: str = ""
