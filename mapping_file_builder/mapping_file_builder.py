import argparse
import concurrent.futures
import datetime
import sys
from dataclasses import dataclass, asdict
from typing import List
from enum import Enum, auto

import requests
import yaml

from pydantic import BaseModel

from compound_common.list_utils import ListUtils
from compound_common.timer import Timer
from configs.builder_config_files import MtblsWsUrls
from function_wrappers.builder_wrappers.http_exception_angel import http_exception_angel
from mapping_file_builder.managers.mapping_persistence_manager import MappingPersistenceManager


@dataclass
class RefMapping:
    study_mapping: dict
    compound_mapping: dict
    species_list: List[str]


class PersistenceEnum(Enum):
    pickle = auto()
    msgpack = auto()
    vanilla = auto()


class MappingFileBuilderConfig(BaseModel):
    mtbls_ws: MtblsWsUrls = MtblsWsUrls()
    timeout: int = 500
    thread_count: int = 6
    debug: bool = False
    pers: PersistenceEnum = PersistenceEnum.msgpack
    destination: str = ''


def build(config: MappingFileBuilderConfig):
    """
    Build the mapping reference file.
    :return: N/A, mapping file saved.
    """
    config = config
    session = requests.Session()
    master_mapping = RefMapping({}, {}, [])
    overall_process_timer = Timer(datetime.datetime.now(), None)
    mpm = MappingPersistenceManager(root=config.destination, timers_enabled=True)

    studies_list = session.get(config.mtbls_ws.metabolights_ws_studies_list).json()[
        "content"
    ]
    list_of_lists = ListUtils.get_lol(studies_list, config.thread_count)
    list_of_lists = list_of_lists[:120] if config.debug is True else list_of_lists

    for lis in list_of_lists:
        ephemera = ataronchronon(accessions=lis, session=session, config=config)
        for ephemeron in ephemera:
            master_mapping = RefMapOperationsHandler.merge_refmaps(
                master_mapping, ephemeron
            )

    master_mapping.species_list = list(set(master_mapping.species_list))
    benchmark_persistence_clients(
        master_mapping=master_mapping, mpm=mpm, list_of_lists=list_of_lists
    ) if config.debug else None

    print(f"Saving mapping file using {config.pers.name} as persistence medium.")
    mpm.__getattribute__(config.pers.name).save(asdict(master_mapping), "mapping")
    overall_process_timer.end = datetime.datetime.now()
    print(
        f"Overall, the reference file building process took {str(overall_process_timer.delta())}"
    )


def benchmark_persistence_clients(
    master_mapping: RefMapping,
    mpm: MappingPersistenceManager,
    list_of_lists: List[List[str]],
):
    """
    Record the read op performance of each persistence client.
    :param master_mapping: The Refmapping object to be saved.
    :param mpm: MappingPersistenceManager object to interface with  perisstence clients.
    :param list_of_lists: The list of sublists that was processed.
    :return:
    """
    tp = mpm.pickle.save(asdict(master_mapping), "mapping")
    tmp = mpm.msgpack.save(asdict(master_mapping), "mapping")
    tvj = mpm.vanilla.save(asdict(master_mapping), "mapping")

    print(f"Pickle: Saved {len(list_of_lists) * 6} in {str(tp.delta())}")
    print(f"MsgPack: Saved {len(list_of_lists) * 6} in {str(tmp.delta())}")
    print(f"VanillaJSON: Saved {len(list_of_lists) * 6} in {str(tvj.delta())}")


def ataronchronon(
    accessions: List[str], session: requests.Session, config: MappingFileBuilderConfig
) -> List[RefMapping]:
    """
    Process a sub-list of MTBLS Accessions. Each accession is given to a thread in a ThreadPool, the task for each
    thread is submitted, and we await the results of each thread before returning the results as a list of RefMapping
    objects.
    :param accessions: A List of MTBLS accessions as string IE ['MTBLS1','MTBLS2'...]
    :param session: A requests.Session object.
    :param config: A MappingFileBuilderConfig object, to pass to the threads.
    :return: A List of RefMapping objects, where each one is the output from a single thread having processed an
        accession.
    """
    input_list = [(acc, RefMapping({}, {}, []), session, config) for acc in accessions]
    method_list = [process_accession_wrapper for acc in accessions]
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ur_executor:
        the_futures = [
            ur_executor.submit(method, args)
            for method, args in zip(method_list, input_list)
        ]
        the_results = [
            future.result()
            for future in concurrent.futures.as_completed(the_futures, config.timeout)
        ]
        return the_results


def process_accession_wrapper(input_tuple) -> RefMapping:
    """
    Wrapper method that unpacks the input tuple in the process accession method call
    :param input_tuple: Tuple of inputs required by the process_accession method - see that methods docstring
    :return: RefMapping object
    """
    return process_accession(*input_tuple)


def process_accession(
    accession: str,
    mapping: RefMapping,
    session: requests.Session,
    config: MappingFileBuilderConfig,
) -> RefMapping:
    """
    Populate a RefMapping object for the given study. This RefMapping object is later merged with the 'master'
    RefMapping object in the `build` method. It first makes a GET request to the study detail API, and then assuming no
    errors with the assay sheet, makes a GET request to the MAF API, and processes each MAF sheet line by line, updating
    the compound_mapping and study_mapping dicts within mapping. It also updates the species_list within mapping with
    any previously unfound species.
    :param accession: IE MTBLS1
    :param mapping: RefMapping object, empty but intialised.
    :param session: requests.Session object.
    :param config: MappingFileBuilderConfig object.
    :return: Populated RefMapping object. May be next to blank if there is a problem with getting assay sheets.
    """
    print(f"Processing {accession}")
    print(f"Getting study details for {accession}")

    study_details = get_study_details(
        session, f"{config.mtbls_ws.metabolights_ws_study_url}/{accession}"
    )
    organism_data = study_details["organism"]
    has_multiple_organisms = len(organism_data) > 1 if organism_data else False

    if study_details["assays"] is None:
        print(f"{accession} has no assay information")
        return mapping

    assay_index = 1

    for assay in study_details["assays"]:
        maf_lines = session.get(
            f"{config.mtbls_ws.metabolights_ws_study_url}/{accession}/assay/{assay_index}/maf"
        ).json()["content"]
        if maf_lines is None:
            return mapping
        for line in maf_lines["data"]["rows"]:
            db_id = str(line["database_identifier"])
            part = ""
            if db_id != "":
                species = str(line["species"]) if "species" in line else ""
                if not has_multiple_organisms:
                    species = organism_data[0]["organismName"]
                    part = organism_data[0]["organismPart"]
                mapping.species_list.append(
                    species
                ) if species not in mapping.species_list and species != "" else None

                mapping.compound_mapping[db_id] = (
                    []
                    if db_id not in mapping.compound_mapping.keys()
                    else mapping.compound_mapping[db_id]
                )
                mapping.compound_mapping[db_id].append(
                    {
                        "study": accession,
                        "assay": assay_index,
                        "species": species,
                        "part": part,
                        "taxid": line["taxid"] if "taxid" in line else "",
                        "mafEntry": line,
                    }
                )

                mapping.study_mapping[accession] = (
                    []
                    if accession not in mapping.study_mapping.keys()
                    else mapping.study_mapping[accession]
                )
                mapping.study_mapping[accession].append(
                    {
                        "compound": db_id,
                        "assay": assay_index,
                        "species": species,
                        "part": part,
                    }
                )
        assay_index += 1

    return mapping


@http_exception_angel
def get_study_details(session: requests.Session, url: str) -> dict:
    """
    Make a GET request to the given url, and return the responses content field as a dict.
    :param session: Request.session object.
    :param url: Request path.
    :return: Response's content field as a dict.
    """
    response = session.get(url).json()["content"]
    return response


class RefMapOperationsHandler:
    @staticmethod
    def merge_refmaps(master: RefMapping, absorb: RefMapping) -> RefMapping:
        """
        Merge two RefMapping objects together. Species lists are just lists of strings and so can just be added
        together. Compound mapping and Study mapping are both dicts, and each might have the same key but different
        values, and we don't want to lose any of those values.
        :param master: The 'master' RefMapping.
        :param absorb: The Refmapping to be absorbed.
        :return: Merged RefMapping object.
        """
        master.species_list += absorb.species_list
        new_master = RefMapping({}, {}, master.species_list + absorb.species_list)

        new_master = RefMapOperationsHandler.dict_merger(
            new_master, master, absorb, "compound_mapping"
        )
        new_master = RefMapOperationsHandler.dict_merger(
            new_master, master, absorb, "study_mapping"
        )
        return new_master

    @staticmethod
    def dict_merger(
        new_master: RefMapping, old_master: RefMapping, absorb: RefMapping, which: str
    ) -> RefMapping:
        """
        Merge two dicts from different RefMapping objects together while preserving all values.
        :param new_master: A new RefMapping object to store the results of the merge
        :param old_master: The old 'master' RefMapping
        :param absorb: The new RefMapping object to be 'absorbed'
        :param which: Which dicts from the two RefMapping objects to merge.
        :return: New, merged RefMapping object.
        """
        for key, value in absorb.__getattribute__(which).items():
            if key in old_master.__getattribute__(which):
                if isinstance(value, list) and isinstance(
                    old_master.__getattribute__(which)[key], list
                ):
                    new_master.__getattribute__(which)[key] = (
                        value + old_master.__getattribute__(which)[key]
                    )
            else:
                new_master.__getattribute__(which)[key] = value

        for key, value in old_master.__getattribute__(which).items():
            if key not in new_master.__getattribute__(which).keys():
                new_master.__getattribute__(which)[key] = value
        return new_master


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        help="Absolute path to the mapping_file_builder.yaml file",
        default="/Users/cmartin/Projects/compound-directory-builder/.secrets/mapping_file_builder.yaml",
    )
    args = parser.parse_args(sys.argv[1:])
    with open(f"{args.config}", "r") as f:
        yaml_data = yaml.safe_load(f)
    config = MappingFileBuilderConfig(**yaml_data)
    build(config=config)
