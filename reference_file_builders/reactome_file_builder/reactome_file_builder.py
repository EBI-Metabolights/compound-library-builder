import requests

from config_classes.reactome_builder_config import ReactomeFileBuilderConfig
from reference_file_builders.mapping_file_builder.managers.mapping_persistence_manager import (
    MappingPersistenceManager,
)


def build(config: ReactomeFileBuilderConfig):
    """
    Build the Reactome reference file. Hits the reactome API to get the reactome2ChEBI.txt file, then iterates over that
    line by line, building up a dict of MTBLC-reactome mapping objects (also dicts). It is then saved using the mapping
    persistence manager.
    :param config: ReactomeBuilderConfigObject. Initialised by the entrypoint method.
    :return: N/A
    """
    session = requests.Session()
    mpm = MappingPersistenceManager(root=config.destination, timers_enabled=False)
    response = session.get(config.url)
    final_dict = {}

    for line in response.text.split("\n"):
        if line:
            data_array = line.split("\t")
            mtbls_id = f"MTBLC{str(data_array[0])}"
            tmp = {
                key: str(data_array[value])
                for key, value in config.reactome_keys_map.items()
            }
            final_dict.setdefault(mtbls_id, []).append(tmp)

    mpm.vanilla.save(final_dict, "reactome")
