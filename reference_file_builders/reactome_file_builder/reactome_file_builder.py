import requests

from compound_common.config_classes import ReactomeFileBuilderConfig
from compound_common.function_wrappers.builder_wrappers.http_exception_angel import http_exception_angel
from reference_file_builders.mapping_file_builder.managers.mapping_persistence_manager import (
    MappingPersistenceManager,
)


class ReactomeFileBuilder:
    def __init__(self, config: ReactomeFileBuilderConfig):
        self.config = config
        self.mpm = MappingPersistenceManager(
            root=config.destination, timers_enabled=False
        )
        self.session = requests.Session()

    @http_exception_angel
    def build(self):
        """
        Build the Reactome reference file. Hits the reactome API to get the reactome2ChEBI.txt file, then iterates over
        that line by line, building up a dict of MTBLC-reactome mapping objects (also dicts). It is then saved using
        the mapping persistence manager.
        :return: final reactome dict.
        """

        response = self.session.get(self.config.url)
        final_dict = {}
        if response.status_code == 200:
            for line in response.text.split("\n"):
                if line:
                    data_array = line.split("\t")
                    mtbls_id = f"MTBLC{str(data_array[0])}"
                    tmp = {
                        key: str(data_array[value])
                        for key, value in self.config.reactome_keys_map.items()
                    }
                    final_dict.setdefault(mtbls_id, []).append(tmp)
        else:
            print(
                f"Non 200 status code received from reactome API: {response.status_code} / {response.text}"
            )

        self.mpm.vanilla.save(final_dict, "reactome") if len(final_dict) > 0 else None
        return final_dict
