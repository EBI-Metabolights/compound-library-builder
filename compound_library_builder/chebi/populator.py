from typing import Optional

import requests as requests

from compound_common.config_classes.builder_config_files import CompoundBuilderConfig
from compound_common.function_wrappers.builder_wrappers.dict_exception_angel import dict_exception_angel
from utils.command_line_utils import CommandLineUtils


def get_chebi_data(id, ml_mapping, config, chebi_obj) -> dict:
    """
    Hit the ChEBI API and parse the *JSON* response. It then populates a 'basic'
    dict from the initial response (basic as all it does is make a single access
    per key, and the keys can be found in `CompoundBuilderObjs.chebi_basic_keys`).
    A ChebiPopulator instance is then used to build an 'advanced' chebi dict
    (advanced as we need bespoke logic to extract the necessary information for
    each key, and the keys can be found in `CompoundBuilderObjs.chebi_adv_keys_map`).
    The two dicts are combined in the return statement.
    """

    data = chebi_obj.get("data", {}) or {}

    # Log out the ID so we can discern if it's valid / what we got back
    CommandLineUtils.print_line_of_token()
    primary_id = chebi_obj.get("primary_chebi_id") or chebi_obj.get("chebi_accession")
    print(primary_id or f"CHEBI:{id}")

    # ----- BASIC DICT -----
    # Map existing chebi_basic_keys onto the new JSON structure.
    # We avoid touching _InternalUtils.get_val here to keep this logic local.
    chemical_data = data.get("chemical_data", {}) or {}
    structure = data.get("default_structure", {}) or {}

    chebi_basic_dict = {
        "definition": data.get("definition"),
        "smiles": structure.get("smiles"),
        "inchi": structure.get("standard_inchi"),
        "inchiKey": structure.get("standard_inchi_key"),
        "charge": chemical_data.get("charge"),
        "mass": chemical_data.get("mass"),
        "monoisotopicMass": chemical_data.get("monoisotopic_mass"),
        "chebiAsciiName": data.get("ascii_name") or data.get("name"),
    }
    chebi_basic_dict["id"] = id

    # ----- ADVANCED DICT -----
    chebi_advanced_populator = ChebiPopulator(data, config)
    # fmt: off
    chebi_advanced_populator \
        .get_synonyms() \
        .get_iupac_names() \
        .get_formulae() \
        .get_citations() \
        .get_database_links() \
        .get_species_via_compound_origins() \
        .get_species_via_compound_mapping(
            ml_mapping, chebi_basic_dict["id"]
        )
    # fmt: on

    chebi_advanced_dict = {
        key: getattr(chebi_advanced_populator, value)
        for key, value in config.objs.chebi_adv_keys_map.items()
    }

    # combine the two dicts – same as before
    return {**chebi_basic_dict, **chebi_advanced_dict}


class ChebiPopulator:
    def __init__(self, data: dict, config: CompoundBuilderConfig):
        """
        data: the `payload["data"]` dict from the new ChEBI JSON API.
        """
        self.data = data or {}
        self.config = config

        self.synonyms: list[str] = []
        self.iupac_names: list[str] = []
        self.formulae: Optional[str] = None
        self.citations: list[dict] = []
        self.database_links: list[dict] = []
        self.compound_origins: list[dict] = []
        self.species: dict[str, list[dict]] = {}

    @dict_exception_angel
    def get_synonyms(self):
        """
        Populate `self.synonyms` using the new JSON:
        data["names"]["SYNONYM"] is a list of dicts with at least
        'name' / 'ascii_name'.
        """
        names = self.data.get("names", {}) or {}
        for syn in names.get("SYNONYM", []):
            # Prefer ascii_name if present; fallback to name.
            name = syn.get("ascii_name") or syn.get("name")
            if name:
                self.synonyms.append(name)
        return self

    @dict_exception_angel
    def get_iupac_names(self):
        """
        Populate `self.iupac_names` using data["names"]["IUPAC NAME"].
        """
        names = self.data.get("names", {}) or {}
        for iupac in names.get("IUPAC NAME", []):
            name = iupac.get("ascii_name") or iupac.get("name")
            if name:
                self.iupac_names.append(name)
        return self

    @dict_exception_angel
    def get_formulae(self):
        """
        Populate `self.formulae` from data["chemical_data"]["formula"].
        """
        chemical_data = self.data.get("chemical_data", {}) or {}
        formula = chemical_data.get("formula")
        if formula:
            self.formulae = formula
        return self

    @dict_exception_angel
    def get_citations(self):
        """
        Populate `self.citations` from data["database_accessions"]["CITATION"].

        We preserve the existing contract:
          - list of dicts
          - keys determined by `chebi_citation_keys_map`:
            'source' -> whatever we treat as source_name
            'type'   -> 'CITATION' or the provided type
            'data'   -> accession number / url
        """
        db_accs = self.data.get("database_accessions", {}) or {}
        citations = db_accs.get("CITATION", []) or []

        for acc in citations:
            # Map into the old shape
            source_val = acc.get("source_name") or acc.get("prefix")
            type_val = acc.get("type") or "CITATION"
            value_val = acc.get("accession_number") or acc.get("url")

            citation_dict = {
                "source": source_val or "N/A",
                "type": type_val or "N/A",
                "value": value_val or "N/A",
            }
            self.citations.append(citation_dict)

        return self

    @dict_exception_angel
    def get_database_links(self):
        """
        Populate `self.database_links` from non-CITATION entries in
        data["database_accessions"].

        We keep the idea of returning dicts with 'source' and 'value'
        (and optionally 'type') so downstream code still sees a similar
        structure to the XML-based version.
        """
        db_accs = self.data.get("database_accessions", {}) or {}

        for acc_type, entries in db_accs.items():
            if acc_type == "CITATION":
                continue

            for acc in entries or []:
                source_val = acc.get("source_name") or acc.get("prefix") or acc_type
                value_val = acc.get("accession_number") or acc.get("url")
                db_link = {
                    "source": source_val or "N/A",
                    "type": acc_type,
                    "value": value_val or "N/A",
                }
                self.database_links.append(db_link)

        return self

    @dict_exception_angel
    def get_species_via_compound_origins(self):
        """
        Populate self.species based on data["compound_origins"].

        The new JSON example has `compound_origins: []`, but real responses
        may contain species-related information. We try a conservative mapping:

          - species text from 'species_text' / 'speciesText' / 'species'
          - SpeciesAccession / SourceType / SourceAccession fields inferred
            from similarly named keys if present; otherwise 'N/A'.

        The resulting dicts are grouped under self.species[species_name].
        """
        origins = self.data.get("compound_origins", []) or []
        if not origins:
            return self

        for origin in origins:
            # Try to discover species text under a few plausible keys
            raw_species = (
                origin.get("species_text")
                or origin.get("speciesText")
                or origin.get("species")
            )
            if not raw_species:
                continue

            chebi_species = str(raw_species).lower()
            if chebi_species not in self.species:
                self.species[chebi_species] = []

            # Build the origin dict according to chebi_species_keys
            origin_dict = {}
            for key in self.config.objs.chebi_species_keys:
                if key == "SpeciesAccession":
                    val = (
                        origin.get("species_accession")
                        or origin.get("SpeciesAccession")
                        or origin.get("speciesAccession")
                    )
                elif key == "SourceType":
                    val = origin.get("SourceType") or origin.get("source_type")
                elif key == "SourceAccession":
                    val = (
                        origin.get("SourceAccession")
                        or origin.get("source_accession")
                    )
                else:
                    val = None

                origin_dict[key] = val if val is not None else "N/A"

            self.species[chebi_species].append(origin_dict)

        return self

    @dict_exception_angel
    def get_species_via_compound_mapping(self, mapping: dict, id: str):
        """
        Uses the big study-compound-species mapping file to add species entries.
        This logic is JSON-agnostic and stays essentially the same.
        """
        compound_key = f"CHEBI:{id}"
        if compound_key in mapping.get("compound_mapping", {}):
            study_species_list = mapping["compound_mapping"][compound_key]
            for study_s in study_species_list:
                temp_study_species = str(study_s["species"]).lower()
                if temp_study_species not in self.species:
                    self.species[temp_study_species] = []

                origin_dict = {
                    key: (
                        study_s[
                            self.config.objs.chebi_species_via_mapping_file_map[key]
                        ]
                        if key != "Species"
                        else temp_study_species
                    )
                    for key in self.config.objs.chebi_species_via_mapping_file_map.keys()
                }
                self.species[temp_study_species].append(origin_dict)

        return self
