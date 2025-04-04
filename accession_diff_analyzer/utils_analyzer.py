import argparse
import datetime
import json
import logging
import pathlib
import pickle
import re
from dataclasses import dataclass, fields, field
from io import StringIO
from typing import Optional, List, Set

import requests

from accession_diff_analyzer.analyzer_dataclasses import IDRegistrySet, IDRegistry, \
    DiffAnalyzerOverviewMetrics
from compound_common.collectors.local_folder_metadata_collector import LocalFolderMetadataCollector
from compound_common.doc_clients.jinja_wrapper import JinjaWrapper
from metabolights_utils.models.metabolights.model import (
    MetabolightsStudyModel,
)
from metabolights_utils.isatab import Reader

from metabolights_utils.provider.study_provider import (
    MetabolightsStudyProvider,
)
from metabolights_utils.isatab.reader import IsaTableFileReader, IsaTableFileReaderResult

from compound_common.doc_clients.xml_utils import XmlResponseUtils


@dataclass
class Compound:
    database_identifier: Optional[str] = '' # to account for entries from other databases like HMDB
    chemical_formula: Optional[str] = None
    smiles: Optional[str] = None
    inchi: Optional[str] = None
    metabolite_identification: Optional[str] = None
    mass_to_charge: Optional[str] = None
    fragmentation: Optional[str] = None
    charge: Optional[str] = None
    retention_time: Optional[str] = None
    database: Optional[str] = None

    def isnumber(self):
        if self.database_identifier is None:
            return False
        if self.database_identifier.isdigit():
            return True
        try:
            float(self.database_identifier)
            return True
        except ValueError:
            return False

@dataclass
class StudyBreakdown:
    has_chebi: set[str] = field(default_factory=set)
    has_alternate_identifier: set[str] = field(default_factory=set)
    has_no_id: set[str] = field(default_factory=set)
    has_smiles: set[str] = field(default_factory=set)
    has_inchi: set[str] = field(default_factory=set)
    has_chemical_formulae: set[str] = field(default_factory=set)

@dataclass
class MAFBreakdown:
    study_id: str
    chebi: List[Compound] = field(default_factory=list)
    alternate: List[Compound] = field(default_factory=list)
    no_id: List[Compound] = field(default_factory=list)

@dataclass
class ReportedCompoundsStats:
    study: StudyBreakdown = field(default_factory=StudyBreakdown)
    maf: List[MAFBreakdown] = field(default_factory=list)


class UtilsAnalyzer:

    def __init__(
            self,
            session: requests.Session = requests.Session(),
            token: str = None,
            jinja_wrapper: JinjaWrapper = JinjaWrapper(),
            output_location: str = "./ephemeral/",
            study_root_path: str = None
                 ):
        self.session = session
        self.token = token
        self.jinja_wrapper = jinja_wrapper
        self.output_location = output_location
        self.study_root_path = study_root_path

        self.duds = ["|", "unknown", "Unknown", "-", " "]
        self.chebi_complete_entity_url = (
            "http://www.ebi.ac.uk/webservices/chebi/2.0/test/getCompleteEntity?chebiId="
        )

    def go(self):
        """Process a study -> process each MAF in a study -> process each page in a MAF -> process each row on page"""

        self.jinja_wrapper.load_template("cross-checker-report.j2")
        valid_fields = {f.name for f in fields(Compound)}
        response = self.session.get("https://www.ebi.ac.uk:443/metabolights/ws/studies")
        studies = json.loads(response.text)["content"]

        rcs = ReportedCompoundsStats()
        overview = DiffAnalyzerOverviewMetrics(len(studies), 0, 0, 0)
        for study in studies:
            print("____________________________________________________________________________")
            print(f"Processing {study}")
            mtbls_folder = f"{self.study_root_path}/{study}"
            print(f'attempting to load {mtbls_folder}')
            model: MetabolightsStudyModel = self.load_study(mtbls_folder)
            mb = MAFBreakdown(study)
            for maf in model.referenced_assignment_files:
                try:
                    mb = self.process_maf(
                        study, maf, valid_fields, mb
                    )
                    overview.mafs_processed += 1
                except Exception as e:
                    overview.bad_mafs.append(maf)
                    logging.exception(f"Unable to load {maf}: {str(e)}")


            # assess this maf breakdown for the study breakdown
            rcs.study.has_chebi.add(study) if len(mb.chebi) > 0 else None
            rcs.study.has_alternate_identifier.add(study) if len(mb.alternate) > 0 else None
            rcs.study.has_no_id.add(study) if len(mb.no_id) > 0 else None
            # TODO: check somehow that smiles, inchi or chemical formulae are in this particular MB
            rcs.maf.append(mb)

        compound_list = self.session.get(
            "https://www.ebi.ac.uk/metabolights/ws/compounds/list"
        ).json()["content"]
        registry_set = self.assemble_registries(compound_list, [
            compound.database_identifier for compound in self.deduplicate_many(
                [mb.chebi for mb in rcs.maf]
                )
            ]
        )
        # do some saving or plotting from here
        with open("ephemeral/compound_statistics.pkl", "wb") as f:
            pickle.dump(rcs, f)
        with open("ephemeral/id_registry_set.pkl", "wb") as f:
            pickle.dump(registry_set, f)
        print(overview)

    def process_maf(self, study: str, maf: str, valid_fields: Set[str], mb: MAFBreakdown) -> MAFBreakdown:
        file_path = pathlib.Path(f'{self.study_root_path}/{study}/{maf}')
        reader: IsaTableFileReader = Reader.get_assignment_file_reader()
        with open(file_path, "r") as f:
            file_contents = f.read()

        def make_buffer() -> StringIO:
            buf = StringIO(file_contents)
            buf.name = str(file_path)
            return buf

        page_count = reader.get_total_pages(file_buffer_or_path=make_buffer())
        for page in range(1, page_count + 1):
            result: IsaTableFileReaderResult = reader.get_page(
                file_buffer_or_path=make_buffer(),
                page=page,
            )

            num_rows = len(next(iter(result.isa_table_file.table.data)))
            num_rows = result.isa_table_file.table.row_count
            for i in range(num_rows):
                row_values = {
                    key: col[i]
                    for key, col in result.isa_table_file.table.data.items()
                    if key in valid_fields and col[i] not in ("", None)
                }
                compound_row = Compound(**row_values)
                mb = self.process_row(compound_row, mb, self.duds)
        return mb

    @staticmethod
    def process_row(compound_row: Compound, maf_breakdown: MAFBreakdown, duds: List[str]) -> MAFBreakdown:
        pattern = r'^[a-zA-Z]{1,10}:?[0-9]{1,10}$'
        if compound_row.isnumber():
            maf_breakdown.alternate.append(compound_row)
            return maf_breakdown
        if 'CHEBI' in compound_row.database_identifier:
            maf_breakdown.chebi.append(compound_row)
            return maf_breakdown
        if compound_row.database_identifier is None:
            maf_breakdown.no_id.append(compound_row)
            return maf_breakdown
        if compound_row.database_identifier is '':
            maf_breakdown.no_id.append(compound_row)
            return maf_breakdown
        if any(dud in compound_row.database_identifier for dud in duds):
            maf_breakdown.no_id.append(compound_row)
            return maf_breakdown
        if re.search(pattern, compound_row.database_identifier):
            # assume this is an alternate identifier
            maf_breakdown.alternate.append(compound_row)
            return maf_breakdown
        print(f"Unexpected entry in database_identifier column: {compound_row.database_identifier}")
        raise ValueError


    def load_study(self, study_path: str) -> MetabolightsStudyModel:
        number = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
        temp_study_id = f"REQ{number}"
        provider = MetabolightsStudyProvider(
            db_metadata_collector=None,
            folder_metadata_collector=LocalFolderMetadataCollector(),
        )
        model: MetabolightsStudyModel = provider.load_study(
            temp_study_id,
            study_path,
            load_assay_files=False,  # TODO: Enable this if it is needed
            load_sample_file=False,  # TODO: Enable this if it is needed
            load_maf_files=True,  # TODO: Enable this if it is needed
            load_folder_metadata=False,  # TODO: Disable this if it is needed
        )
        return model

    def assemble_registries(self, compound_list, maf_ids) -> IDRegistrySet:
        """

        :param compound_list: List from the webservice, reflecting contents of db
        :param maf_ids: List of chebi IDs scraped from maf sheets of all public studies.
        :return:
        """
        compound_list_numeric = {
            re.sub(r"\D", "", compound) for compound in compound_list
        }
        maf_list_numeric = {re.sub(r"\D", "", compound) for compound in maf_ids}

        ids_unique_to_mafs = self.get_delta(maf_list_numeric, compound_list_numeric)
        ids_unique_to_db = self.get_delta(compound_list_numeric, maf_list_numeric)

        maf_registry = IDRegistry(total=len(ids_unique_to_mafs))
        db_registry = IDRegistry(total=len(ids_unique_to_db))

        for identifier in ids_unique_to_mafs:
            maf_registry.primary.add(identifier) if self.is_primary(
                identifier
            ) else maf_registry.secondary.add(identifier) if self.is_primary(
                identifier
            ) is not None else maf_registry.incorrect.add(
                identifier
            )

        for identifier in ids_unique_to_db:
            db_registry.primary.add(identifier) if self.is_primary(
                identifier
            ) else db_registry.secondary.add(identifier) if self.is_primary(
                identifier
            ) is not None else db_registry.incorrect.add(
                identifier
            )

        return IDRegistrySet(maf=maf_registry, db=db_registry)

    def is_primary(self, identifier: str, enabled = False) -> bool:
        """
        Check whether a given id is a primary id in ChEBI. Ping the ChEBI completeEntity endpoint, and if the ID in the
        response is the same as the one you queried, then the ID is primary (if the ID is secondary, the primary
        compound and its associated ID and other information is returned)
        :param identifier: string representation of ChEBI ID.
        :return: bool indicating whether the present ID is primary or not.
        """
        """"
        Two potential ways to check:
        - Ping the chebi completeEntity endpoint with the ID, and if you get the same ID back, you know it was a primary
        - Consult the list of files within the local chebi_index directory (if and when it exists)
        """
        # there are low double digit instances of secondary ids in MAFS, and we will end up making over 27000 network
        # requests if the flag is enabled, so it is disabled by default.
        if not enabled:
            return True
        entity_response = self.session.get(
            f"{self.chebi_complete_entity_url}{identifier}"
        )
        chebi_webservice_id = XmlResponseUtils.get_chebi_id(entity_response.text)

        return (
            identifier in chebi_webservice_id
            if chebi_webservice_id is not None
            else None
        )

    @staticmethod
    def get_delta(subject: set, comparator: set) -> list:
        """
        Returns items in a subject set unique to the subject relative to a comparator
        :param subject: set of ids
        :param comparator: set of ids
        :return: list of unique ids in subject
        """
        return list(subject - comparator)

    @staticmethod
    def deduplicate_by_database_identifier(compounds: List[Compound]) -> List[Compound]:
        seen = set()
        unique = []
        for compound in compounds:
            db_id = compound.database_identifier
            if db_id not in seen:
                seen.add(db_id)
                unique.append(compound)
        return unique

    @staticmethod
    def deduplicate_many(compound_lists: List[List[Compound]]) -> List[Compound]:
        seen: Set[str] = set()
        result: List[Compound] = []

        for compound_list in compound_lists:
            for compound in compound_list:
                db_id = compound.database_identifier
                if db_id and db_id not in seen:
                    seen.add(db_id)
                    result.append(compound)

        return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--token", help="MetaboLights API Token")
    parser.add_argument("-s", "--study-root", help="Absolute path to study root")
    args = parser.parse_args()
    token = args.token
    study_root = args.study_root
    UtilsAnalyzer(
        session=requests.Session(),
        token=token,
        study_root_path=study_root
    ).go()