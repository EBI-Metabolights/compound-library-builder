import pytest

from config_classes.reactome_builder_config import ReactomeFileBuilderConfig
from reference_file_builders.reactome_file_builder.reactome_file_builder import (
    ReactomeFileBuilder,
)


@pytest.fixture
def reactome_response_fixture():
    data = (
        "10033\tR-BTA-6806664\thttps://reactome.org/PathwayBrowser/#/R-BTA-6806664\tMetabolism of vitamin K\tIEA\tBos taurus\n"
        "10033\tR-CFA-6806664\thttps://reactome.org/PathwayBrowser/#/R-CFA-6806664\tMetabolism of vitamin K\tIEA\tCanis familiaris\n"
        "10033\tR-DME-6806664\thttps://reactome.org/PathwayBrowser/#/R-DME-6806664\tMetabolism of vitamin K\tIEA\tDrosophila melanogaster\n"
        "10033\tR-DRE-6806664\thttps://reactome.org/PathwayBrowser/#/R-DRE-6806664\tMetabolism of vitamin K\tIEA\tDanio rerio\n"
        "10033\tR-HSA-6806664\thttps://reactome.org/PathwayBrowser/#/R-HSA-6806664\tMetabolism of vitamin K\tTAS\tHomo sapiens\n"
        "10033\tR-MMU-6806664\thttps://reactome.org/PathwayBrowser/#/R-MMU-6806664\tMetabolism of vitamin K\tIEA\tMus musculus"
    )
    yield data
    del data


@pytest.fixture
def reactome_builder_fixture():
    config = ReactomeFileBuilderConfig(**{"destination": "yo", "url": "test.me"})
    rfb = ReactomeFileBuilder(config=config)
    yield rfb
    del rfb
