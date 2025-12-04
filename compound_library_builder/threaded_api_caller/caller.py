import json

import requests as requests

from compound_common.config_classes.builder_config_files import CompoundBuilderConfig, CompoundBuilderObjs
from compound_common.function_wrappers.builder_wrappers.http_exception_angel import http_exception_angel
from compound_library_builder.ancillary_classes.spectra_file_handler import SpectraFileHandler


class ThreadedAPICaller:
    """
    The methods in this class come in twos:
    - method_wrapper(tuple_of_inputs)
    - actual_method(and, its, inputs)

    The method wrapper is what gets passed to the ThreadPoolExecutor, and methods that get passed to the TPE can only
    take one argument, so we package all argument for a given method into a tuple, and the unpack it using the `*`
    operator when we call the method proper.
    """

    @staticmethod
    def citation_wrapper(citation_tuple) -> dict:
        return {
            "results": ThreadedAPICaller.get_citations(*citation_tuple),
            "name": "citations",
        }

    @staticmethod
    @http_exception_angel
    def get_citations(
        citations, config: CompoundBuilderConfig, session: requests.Session
    ) -> list:
        """
        For each citation for a given compound, hit the europePMC API, get json format of the result, and update the
        existing
        :param citations: citations that we have from the chebi response from earlier in the compound building process.
        :param config: CompoundBuilderConfig object, used to get dictionary keys and api endpoints.
        :param session: Session object to make http calls.
        :return: list of updated citations
        """
        print("Attempting to get data from europePMC API.")
        epmc_list = []
        for citation in citations:
            val = f'{config.urls.misc_urls.epmc_api}{str(citation["value"])}'
            print(
                f"attempting to hit {val}&format=json&resultType=core&cursorMark=*&pageSize=25"
            )
            try:
                citation_epmc_data = session.get(
                    f'{config.urls.misc_urls.epmc_api}{str(citation["value"])}&format=json&resultType=core'
                ).json()["resultList"]["result"][0]
            except json.decoder.JSONDecodeError as e:
                print(
                    f'No response for individual citation {str(citation["value"])}:{str(e)}'
                )
                continue
            except IndexError as e:
                print(
                    f'No response for individual citation {str(citation["value"])}:{str(e)}'
                )
                continue
            citation.update(
                {
                    key: citation_epmc_data[value]
                    if value in citation_epmc_data
                    else "NA"
                    for key, value in config.objs.epmc_citation_keys_map.items()
                }
            )
            epmc_list.append(citation)
        return epmc_list

    @staticmethod
    def cactus_wrapper(cactus_tuple) -> dict:
        return {
            "results": ThreadedAPICaller.get_cactus_structure(*cactus_tuple),
            "name": "cactus",
        }

    @staticmethod
    @http_exception_angel
    def get_cactus_structure(cactus_api: str, inchi_key: str, session: requests.Session) -> str:
        """
        Hit the cactus API, and return the text of the result.
        :param cactus_api: cactus api endpoint to hit.
        :param inchi_key: inchi_key associated with the current compound.
        :param session: Sesion object used to make http call.
        :return: string of cactus API response.
        """
        print("Attempting to get data from cactus API.")
        return session.get(f"{cactus_api}{inchi_key}/sdf").text

    @staticmethod
    def reactions_wrapper(reactions_tuple) -> dict:
        return {
            "results": ThreadedAPICaller.get_reactions(*reactions_tuple),
            "name": "reactions",
        }

    @staticmethod
    @http_exception_angel
    def get_reactions(
        chebi_compound_dict, rhea_api, conf_objs: CompoundBuilderObjs, session: requests.Session
    ) -> list:
        """
        Ping the RHEA API with the chebi ID for the current compound as a query parameter. Then, for each result, parse
        that result into a dict, and include that dict in the list of reactions. Note that the dict comprehension that
        builds a single dict, and the list comprehension which builds the list of reaction dicts take place at the same
        time.
        In config.reaction_keys.items(), there are two keys, biopax2 and cmlreact that ultimately go unused. This is
        because version 1 of the script hit a long outdated version of the rhea api, that would also given chemical
        markup language (cml) output, and biopax2 information in it's response. The current rhea api does not offer this
        information, but I have left the keys in for posterity, in case we discover another way to get the missing
        information.
        :param chebi_compound_dict: compound dict built from results of chebi API response earlier in compound building.
        :param rhea_api: Endpoint for the swiss bioinformatics institute's rhea api
        :param conf_objs: CompoundBuilderObjs object used to get reaction dict keys.
        :param session: Session object to make http call.
        :return: list of reaction dicts.
        """
        print("Attempting to get data from rhea API.")
        query = "?query="
        columns = "&columns=rhea-id,equation,chebi-id"
        format = "&format=json"
        limit = "&limit=10"
        rhea_data = session.get(
            f'{rhea_api}{query}{chebi_compound_dict["id"]}{columns}{format}{limit}'
        ).json()
        print(
            f'rhea data for chebi id {chebi_compound_dict["id"]} : {rhea_data["results"]}'
        )
        reactions = [
            {
                key: result[value] if value in result else ""
                for key, value in conf_objs.reactions_keys.items()
            }
            for result in rhea_data["results"]
        ]
        return reactions

    @staticmethod
    def ms_from_mona_wrapper(spectra_tuple) -> dict:
        return {
            "results": ThreadedAPICaller.get_ms_from_mona(*spectra_tuple),
            "name": "spectra",
        }

    @staticmethod
    @http_exception_angel
    def get_ms_from_mona(
        mtbls_id: str,
        dest: str,
        inchi_key: str,
        config: CompoundBuilderConfig,
        session: requests.Session,
    ) -> list:
        """
        Ping the MoNA API with the inchikey for a given compound as a query parameter. Then, for each result, parse that
        result into a spectra dict, and pass that dict to `_FileHandler.save_spectra` to further process the spectral
        data and save it as a .json file.
        :param mtbls_id: The MTBLC accession number associated with the MTBLC compound we are building.
        :param dest: The parent compound reference directory.
        :param inchi_key: inchi_key associated with a given compound.
        :param config: CompoundBuilderConfig object, mona_api endpoint extracted from within.
        :param session: Session object to make http call.
        :return: list of spectra objects representing a spectrum.
        """
        print(
            f"Attempting to get spectral data from MoNa at {f'{config.urls.misc_urls.new_mona_api.format(inchi_key)}'}"
        )
        ml_spectrum = []
        response = session.get(
            f"{config.urls.misc_urls.new_mona_api.format(inchi_key)}"
        )
        if response.status_code not in [200, 201, 202, 203]:
            return ml_spectrum
        result = response.json()
        for spectra in result:
            ml_spectra = {
                "splash": spectra["splash"],
                "type": "MS",
                "name": str(spectra["id"]),
                "url": f'/metabolights/webservice/beta/spectra/{mtbls_id}/{str(spectra["id"])}',
            }
            temp_submitter = spectra["submitter"]
            ml_spectra["submitter"] = (
                f"{str(temp_submitter['firstName'])}  {str(temp_submitter['lastName'])} ; "
                f"{str(temp_submitter['emailAddress'])} ; {str(temp_submitter['institution'])}"
            )
            ml_spectra["attributes"] = []
            for metadata in spectra["metaData"]:
                if not metadata["computed"]:
                    temp_attribute = {
                        "attributeName": metadata["name"],
                        "attributeValue": metadata["value"],
                        "attributeDescription": "",
                    }
                    ml_spectra["attributes"].append(temp_attribute)
            ml_spectrum.append(ml_spectra)
            SpectraFileHandler.save_spectra(
                str(spectra["id"]), spectra["spectrum"], mtbls_id, dest
            )
        return ml_spectrum

    @staticmethod
    def wikipathways_wrapper(pathway_tuple) -> dict:
        return {
            "results": ThreadedAPICaller.get_wikipathways(*pathway_tuple),
            "name": "wikipathways",
        }

    @staticmethod
    @http_exception_angel
    def get_wikipathways(
        inchi_key: str, chebi_id: str, config: CompoundBuilderConfig, session: requests.Session
    ) -> dict:
        """
        Hit the wikipathways API, and for each result / pathway, parse it into a new dict and append that pathway to a
        parent `final_pathways` object. The final object is the representation of all pathways for a given compound,
        sorted by species.
        :param inchi_key: inchi_key of a particular compound to use as a query param.
        :param config: CompoundBuilderConfig object to pull out the wikipathways url.
        :param session: Session object to make http call.
        :return: dict object representing pathways for particular compound.
        """
        chebi_code = "Ce"
        inchi_code = "Ik"
        format_params = f"&codes={chebi_code}&format=json"
        val = f"{config.urls.misc_urls.wikipathways_api}{chebi_id.strip(('MTBLC'))}{format_params}"
        print(f"Attempting to retrieve wikipathways data from {val}")
        final_pathways = {}
        wikipathways_response = session.get(
            val
        )
        wikipathways = wikipathways_response.json()['result']

        for pathway in wikipathways:
            if pathway["species"] not in final_pathways:
                final_pathways[pathway["species"]] = []
            pathway_dict = {
                "id": pathway["id"],
                "url": pathway["url"],
                "name": pathway["name"],
            }
            if pathway_dict not in final_pathways[pathway["species"]]:
                final_pathways[pathway["species"]].append(pathway_dict)

        return final_pathways

    @staticmethod
    def kegg_wrapper(kegg_tuple) -> dict:
        return {
            "results": ThreadedAPICaller.get_kegg_pathways(*kegg_tuple),
            "name": "kegg_pathways",
        }

    @staticmethod
    @http_exception_angel
    def get_kegg_pathways(
        chebi_compound_dict: dict, config: CompoundBuilderConfig, session: requests.Session
    ) -> list:
        """
        Hit the kegg API using the compounds chebi ID as a query parameter. Then, hit kegg's pathway list API using the
        kegg id that we got from the previous request. Then, for each line in the pathways list response, hit kegg's
        individual pathway API for that line, and then parse the response into a pathway dict.
        :param chebi_compound_dict: compound dict built from results of chebi API response earlier in compound building.
        :param config: CompoundBuilderConfig object, used to pull out kegg api endpoints.
        :param session: Session object to make http calls.
        :return: list of kegg pathway objects.
        """
        print(
            f'Attempting to retrieve KEGG data from {config.urls.kegg.kegg_api}{chebi_compound_dict["id"].lower()}'
        )
        final_kegg_pathways = []
        kegg_id_q_r = session.get(
            f'{config.urls.kegg.kegg_api}{chebi_compound_dict["id"].lower()}'
        )
        kegg_id = None
        try:
            kegg_id = kegg_id_q_r.text.split("\t")[1].strip()
        except IndexError as e:
            print(
                f'Unable to get a corresponding CPD number for chebi compound {chebi_compound_dict["id"]}: {str(e)}'
            )
        if kegg_id is None:
            return final_kegg_pathways

        print(
            f"Attempting step 2 of getting kegg data with url {config.urls.kegg.kegg_pathways_list_api}{kegg_id}"
        )
        pathways_data = session.get(
            f"{config.urls.kegg.kegg_pathways_list_api}{kegg_id}"
        ).text
        for line in pathways_data.strip().split("\n"):
            if line == "":
                continue
            try:
                pathway_id = line.split("\t")[1].strip()
            except IndexError as e:
                print(
                    f"Couldnt get pathway id due to index error when parsing pathways response: {str(e)}"
                )
                continue
            pathway_data = session.get(
                f"{config.urls.kegg.kegg_pathway_api}{pathway_id}"
            ).text
            pathway_dict = {"id": pathway_id}
            for pline in pathway_data.strip().split("\n"):
                if "NAME" in pline:
                    pathway_dict["name"] = pline.replace("NAME", "").strip()
                elif "KO_PATHWAY" in pline:
                    pathway_dict["KO_PATHWAYS"] = pline.replace(
                        "KO_PATHWAYS", ""
                    ).strip()
                elif "DESCRIPTION" in pline:
                    pathway_dict["description"] = pline.replace(
                        "DESCRIPTION", ""
                    ).strip()
            final_kegg_pathways.append(pathway_dict)
        return final_kegg_pathways

