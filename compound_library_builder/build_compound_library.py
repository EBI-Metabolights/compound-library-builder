import concurrent.futures
import json
from typing import Dict

from requests import Session

from compound_common.config_classes.builder_config_files import CompoundBuilderConfig, RuntimeFlags

from compound_library_builder.chebi.populator import get_chebi_data
from compound_library_builder.threaded_api_caller.caller import ThreadedAPICaller
from compound_library_builder.threaded_api_caller.sorter import ExternalAPIResultSorter
from persistence.db.mongo.mongo_client import MongoWrapper
from utils.command_line_utils import CommandLineUtils
from utils.general_file_utils import GeneralFileUtils
from pymongo.errors import DuplicateKeyError, WriteError, PyMongoError

from utils.mongo_utils import MongoUtils


def build_compound(metabolights_id, ml_mapping, reactome_data, data_directory, save_to_db, chebi_obj):
    """
    Entrypoint method for the script, to build an MTBLC compound directory.

    :param metabolights_id: the MTBLC12345 ID retrieved from the web service.
    :param ml_mapping: The mapping file, which associates studies to compound ids referenced in that study.
    :param reactome_data: Reactome data json file.
    :param data_directory: Directory to save the built compound subdirectory to.
    :param save_to_db: Whether to save to db instead of fs
    :param chebi_obj: JSON representation of chebi compound.

    :return: N/A but saves the directory to the data directory.
    """
    config = CompoundBuilderConfig()
    session = Session()
    chebi_id = metabolights_id.replace("MTBLC", "").strip()
    mongo_client = None
    if save_to_db:
        mongo_client = MongoWrapper()

    # call our java webservice
    mtblcs = None
    try:
        mtblcs = session.get(
            f"{config.urls.mtbls.metabolights_ws_compounds_url}{metabolights_id}"
        ).json()["content"]
    except json.JSONDecodeError as e:
        print(
            f"Error getting info from MTBLS webservice for compound {chebi_id}: {str(e)}"
        )
    if mtblcs is None:
        print(f"Exiting compound building process for compound {chebi_id}")
        return {}

    # init our compound dict, build the chebi dict
    """"This compound dict is the master copy, and is the one that gets saved at the end, and is passed to various 
    classes and methods, and is modified in place."""
    compound_dict = _InternalUtils.initialize_compound_dict()

    chebi_dict = get_chebi_data(chebi_id, ml_mapping, config, chebi_obj)
    if not chebi_dict:
        return compound_dict
    chebi_dict["id"] = chebi_id
    # do some updating
    compound_dict["id"] = metabolights_id

    # essentially just copies values from the chebi compound dict to the mtbl dict, referring to
    # `ml_compound_chebi_compound_map` so it knows which key on the chebi dict matches which key on the mtbl dict.
    # Also, if a value is not present, and the type of that value is not a string, it refers to the
    # `ml_compound_absent_type_value` map, which specifies what kind of empty value to concatenate
    compound_dict.update(
        {
            key: chebi_dict.get(
                value,
                "NA"
                if key not in config.objs.ml_compound_absent_value_type_map
                else config.objs.ml_compound_absent_value_type_map[key],
            )
            if chebi_dict.get(value) is not None
            else (
                print(f"{value} not assigned"),
                "NA"
                if key not in config.objs.ml_compound_absent_value_type_map
                else config.objs.ml_compound_absent_value_type_map[key],
            )[1]
            for key, value in config.objs.ml_compound_chebi_compound_map.items()
        }
    )

    # initialise the pathways dicts and the spectra lists
    compound_dict.update(
        {
            "pathways": {
                "WikiPathways": {},
                "KEGGPathways": {},
                "ReactomePathways": {},
            },
            "spectra": {"NMR": [], "MS": []},
        }
    )

    mementos = configure_thread_pool_and_execute_tasks(
        chebi_compound_dict=chebi_dict,
        config=config,
        session=session,
        mtbls_id=metabolights_id,
        data_directory=data_directory,
    )

    sorter = ExternalAPIResultSorter(mementos)
    compound_dict = sorter.sort(compound_dict)

    # last bits of data that couldn't be integrated anywhere else added here
    compound_dict["pathways"]["ReactomePathways"] = get_reactome_data(
        metabolights_id, reactome_data
    )
    compound_dict["spectra"]["NMR"] = (
        get_nmr(mtblcs["mc"]["metSpectras"] if "mc" in mtblcs.keys() else [])
        if mtblcs
        else []
    )

    # update NMR, species, pathways flags.
    if (
        len(compound_dict["pathways"]["ReactomePathways"])
        + len(compound_dict["pathways"]["KEGGPathways"])
        + len(compound_dict["pathways"]["WikiPathways"])
    ) > 0:
        compound_dict["flags"]["hasPathways"] = "true"
    if len(compound_dict["spectra"]["NMR"]) > 0:
        compound_dict["flags"]["hasNMR"] = "true"
    if compound_dict["species"]:
        compound_dict["flags"]["hasSpecies"] = "true"

    if config.rt_flags.verbose_logging:
        mementos_readout(mementos, metabolights_id)

    if save_to_db:
        save_compound_to_db(mongo_client, compound_dict)
    else:
        GeneralFileUtils.save_json_file(
            f"{data_directory}/{metabolights_id}/{metabolights_id}_data.json", compound_dict
        )
    return compound_dict


def configure_thread_pool_and_execute_tasks(
    chebi_compound_dict: dict,
    config: CompoundBuilderConfig,
    session: Session,
    mtbls_id: str,
    data_directory: str,
):
    """
    Configure a ThreadPoolExecutor, and instruct each thread to execute a different external API related task.
    The threads for each task will only start if the corresponding flag is enabled. The RuntimeFlags object starts
    with all flags enabled by default.
    :param chebi_compound_dict: compound dict built from results of chebi API response earlier in compound building.
    :param config: CompoundBuilderConfig object.
    :param session: Session object, shared among threads, to make http calls.
    :param mtbls_id: the MTBLC12335 ID of the current compound.
    :param data_directory: Where the compound directory and spectra files will be saved.
    :return: list of result dicts.
    """
    # collect the inputs for each method wrapper wrapper into a tuple. We do this as threadpool executors can only take
    # one argument.
    citation_input = (chebi_compound_dict["Citations"], config, session)
    cactus_input = (
        config.urls.misc_urls.cactus_api,
        chebi_compound_dict["inchiKey"],
        session,
    )
    reactions_input = (
        chebi_compound_dict,
        config.urls.misc_urls.rhea_api,
        config.objs,
        session,
    )
    ms_from_mona_input = (
        mtbls_id,
        data_directory,
        chebi_compound_dict["inchiKey"],
        config,
        session,
    )
    wiki_pathways_input = (chebi_compound_dict["inchiKey"], mtbls_id, config, session)
    kegg_pathways_input = (chebi_compound_dict, config, session)

    input_list = [
        citation_input,
        cactus_input,
        reactions_input,
        ms_from_mona_input,
        wiki_pathways_input,
        kegg_pathways_input,
    ]
    method_list = [
        ThreadedAPICaller.citation_wrapper,
        ThreadedAPICaller.cactus_wrapper,
        ThreadedAPICaller.reactions_wrapper,
        ThreadedAPICaller.ms_from_mona_wrapper,
        ThreadedAPICaller.wikipathways_wrapper,
        ThreadedAPICaller.kegg_wrapper,
    ]

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ur_executor:
        # create a list of empty result dicts to make processing easier later
        the_duds = [
            {
                "name": config.rt_flags.mapping[
                    _InternalUtils.extract_name_from_function(method)
                ],
                "results": None,
            }
            for method in method_list
            if _InternalUtils.flag_is_enabled(config.rt_flags, method) is False
        ]

        # create a list of futures object, where each future is a thread corresponding to an enabled runtime flag.
        the_futures = [
            ur_executor.submit(method, args)
            for method, args in zip(method_list, input_list)
            if _InternalUtils.flag_is_enabled(config.rt_flags, method)
        ]

        # wait for and collect the results of each individual thread
        the_results = [
            future.result()
            for future in concurrent.futures.as_completed(
                the_futures, config.rt_flags.timeout
            )
        ]

        # add the duds to the results
        the_results.extend(the_duds)
        return the_results


def get_reactome_data(mtblc_compound_id: str, reactome_data: dict) -> dict:
    """
    Go over the pathways in the reactome file that correspond to this particular compound. Return the results
    as a dict.
    :param mtblc_compound_id: Compound ID to slice the reactome data with.
    :param reactome_data: Reactome data as a dict.
    :return: Reactome pathways for this compound, as a dict.
    """
    temp_reactome_pathways = (
        reactome_data[mtblc_compound_id] if mtblc_compound_id in reactome_data else []
    )
    reactome_pathways = {}
    try:
        for pathway in temp_reactome_pathways:
            temp_pathway = {
                "name": pathway["pathway"],
                "pathwayId": pathway["pathwayId"],
                "url": pathway["reactomeUrl"],
                "reactomeId": pathway["reactomeId"],
            }
            if pathway["species"] not in reactome_pathways:
                reactome_pathways[pathway["species"]] = [temp_pathway]
            else:
                reactome_pathways[pathway["species"]].append(temp_pathway)
    except KeyError as e:
        print(f"Error populating dict for {mtblc_compound_id}: {str(e)}")
    finally:
        return reactome_pathways


def get_nmr(spectra) -> list:
    """
    Iterate over the java webservice result for this compound, and populate a dict for each spectra in the results.
    Any KeyErrors are logged and the offending spectra skipped.
    :param spectra: Spectra info from java webservice.
    :return: List of NMR spectra dicts.
    """
    nmr = []
    for spec in spectra:
        try:
            if spec["spectraType"] == "NMR":
                temp_spec = {
                    "name": spec["name"],
                    "id": str(spec["id"]),
                    "url": f"http://www.ebi.ac.uk/metabolights/webservice/compounds/spectra/{str(spec['id'])}/json",
                    "path": spec["pathToJsonSpectra"],
                    "type": spec["spectraType"],
                    "attributes": [
                        {
                            "attributeName": attr["attributeDefinition"]["name"],
                            "attributeDescription": attr["attributeDefinition"]["name"],
                            "attributeValue": attr["value"],
                        }
                        for attr in spec["attributes"]
                    ],
                }
                nmr.append(temp_spec)
        except KeyError as e:
            print(f'Error populating dict for {spec["id"]}: {str(e)}')
            continue
    return nmr

def mementos_readout(mementos, metabolights_id):
    print(
        f"___________________________multithreaded api results for {metabolights_id}_____________"
    )
    for d in mementos:
        print(d.values())

def save_compound_to_db(mongo_client: MongoWrapper, compound_dict: Dict):
    """
    Save a single compound to mongodb using our wrapper. Handle any resultant errors.
    :param mongo_client: custom mongo wrapper instance
    :param compound_dict: Compound object finished with enrichment pipeline.
    :return: Result of db up/insert operations
    """
    db_normalised_compound = MongoUtils.normalize_compound_for_mongo(compound_dict)
    try:
        result = mongo_client.upsert("compounds", {"id": compound_dict["id"]}, db_normalised_compound)
    except DuplicateKeyError as e:
        print(f"Duplicate key for id={compound_dict.get('id')}: {e}")
        return None

    except WriteError as e:
        print(f"Write error for id={compound_dict.get('id')}: {e}")
        return None
    except PyMongoError as e:
        print(f"[MongoDB ERROR] Failed upserting compound {compound_dict.get('id')}: {e}")
        return None
    return result


class _InternalUtils:
    """
    Internal utils class, private to this script, sticking all static methods that don't belong anywhere else here.
    """

    @staticmethod
    def initialize_compound_dict() -> dict:
        """
        Initialize the compound dict that will ultimately be saved. Pulled this almost wholesale from the old script,
        and chucked it into this method.

        :return: Initialized compound dict.
        """
        metabolights_compound = {}
        metabolights_compound["flags"] = {}
        metabolights_compound["flags"]["hasLiterature"] = "false"
        metabolights_compound["flags"]["hasReactions"] = "false"
        metabolights_compound["flags"]["hasSpecies"] = "false"
        metabolights_compound["flags"]["hasPathways"] = "false"
        metabolights_compound["flags"]["hasNMR"] = "false"
        metabolights_compound["flags"]["hasMS"] = "false"
        return metabolights_compound

    @staticmethod
    def preliminary_log_lines(metabolights_id) -> None:
        """
        Spit out the initial log lines.

        :param metabolights_id: The current accession ID.
        :return: None
        """
        CommandLineUtils.print_line_of_token()
        print("Compound ID: " + metabolights_id)
        CommandLineUtils.print_line_of_token()
        print("Process started: " + metabolights_id)
        print("Requesting compound chemical information from ChEBI:")

    @staticmethod
    def pascal_case(string: str) -> str:
        """
        Return a string with the first character lower case-d.

        :param string: String to pascal case-ify.
        :return: Pascal case-ified string.
        """
        return string[0].lower() + string[1:]

    @staticmethod
    def flag_is_enabled(rt_config: RuntimeFlags, method) -> bool:
        """
        Check whether a given config flag is enabled.
        :param rt_config: RuntimeFlags config object.
        :param method: Function wrapper that we extract the runtime flag from.
        :return: boolean indicating whether flag is enabled or not.
        """
        key = _InternalUtils.extract_name_from_function(method)
        string = rt_config.mapping[key]
        boolean = rt_config.__getattribute__(string)
        return boolean

    @staticmethod
    def extract_name_from_function(method) -> str:
        """
        Extract the exact function name from the function wrapper object.
        :param method: Function wrapper object.
        :return: Function name.
        """
        return method.__name__.split(".")[-1]
