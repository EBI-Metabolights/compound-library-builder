from unittest.mock import MagicMock
import maf_chebi_cross_checker.checker as chck

import pandas
import pytest


@pytest.fixture
def checker_fixture():
    checker = chck.Checker(session=MagicMock(), handler=MagicMock(), token="blerg")
    yield checker
    del checker


@pytest.fixture
def good_dataframe():
    sample_data = {"database_identifier": ["CHEBI:12345"]}
    return pandas.DataFrame(sample_data)


@pytest.fixture
def chebi_complete_entity():
    response = MagicMock()
    response.text = (
        "CHEBI:16449alanineAn alpha-amino acid that consists of propionic acid bearing an amino "
        "substituent at position 2."
    )
    yield response


@pytest.fixture
def overview():
    overview = chck.OverviewMetrics(
        total_studies=50, studies_processed=48, total_mafs=80, mafs_processed=78
    )
    yield overview


@pytest.fixture
def registry():
    registry = chck.IDRegistry(
        total=99,
        primary={num for num in range(10000, 10033)},
        secondary={num for num in range(10034, 10067)},
        incorrect={num for num in range(10068, 10101)},
    )
    yield registry


@pytest.fixture
def compound_ids():
    ids = {"MTBLC1", "MTBLC2", "MTBLC3", "MTBLC4", "MTBLC5", "MTBLC6"}
    compound_list = [
        "MTBLC3",
        "MTBLC4",
        "MTBLC5",
        "MTBLC6",
        "MTBLC7",
        "MTBLC8",
        "MTBLC9",
    ]
    return ids, compound_list


@pytest.fixture
def study_file_endpoint_fixture():
    study_file_dict = {
        "latest": [],
        "obfuscationCode": "blerg",
        "private": [],
        "study": [
            {
                "createdAt": "November 02 2020 20:11:03",
                "directory": False,
                "file": "m_mtbls1_metabolite_profiling_NMR_spectroscopy_v2_maf.tsv",
                "status": "active",
                "timestamp": "20201102201103",
                "type": "metadata_maf",
            },
            {
                "createdAt": "October 10 2017 11:19:23",
                "directory": False,
                "file": "metexplore_mapping.json",
                "status": "active",
                "timestamp": "20171010111923",
                "type": "internal_mapping",
            },
            {
                "createdAt": "March 10 2017 17:53:55",
                "directory": False,
                "file": "m_s_aureus_adaptation_to_different_human_cell_lines_metabolite_profiling_NMR_spectroscopy_v2_"
                        "maf.tsv",
                "status": "active",
                "timestamp": "20170310175355",
                "type": "metadata_maf",
            },
        ],
    }
    yield study_file_dict
