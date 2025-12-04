import argparse
import json
import logging
import math
import re
from abc import abstractmethod, ABC
from typing import List, Any

import requests
import pandas as pd

from accession_diff_analyzer.analyzer_dataclasses import OverviewMetrics, IDWatchdog, IDRegistry
from compound_common.transport_clients.ebi_ftp_handler import EBIFTPHandler
from compound_common.doc_clients.xml_utils import XmlResponseUtils
from compound_common.doc_clients.jinja_wrapper import JinjaWrapper
from compound_common.config_classes import FTPConfig


from compound_common.function_wrappers.checker_wrappers.file_write_exception_angel import (
    file_rw_exception_angel,
)



class MAFProcessorBase(ABC):

    @abstractmethod
    def get_maf(self, maf: str, study: str) -> Any:
        pass

    @abstractmethod
    def process_maf(self, maf_object: Any) -> None:
        pass




class DataFrameMAFProcessor(MAFProcessorBase):
    def __init__(self, handler, ids: set, is_dud_fn, process_identifier_fn):
        self.handler = handler
        self._ids = set()
        self.is_dud = is_dud_fn
        self.process_identifier = process_identifier_fn

    def get_maf(self, maf: str, study: str) -> pd.DataFrame:
        return self.handler.load_isa_file(isatab_file=maf, study=study)

    def process_maf(self, maf_dataframe: pd.DataFrame) -> None:
        if maf_dataframe is None:
            return

        for _, row in maf_dataframe.iterrows():
            identifier = row.get("database_identifier")
            if not self.is_dud(identifier):
                self.process_identifier(identifier)

class MtblsUtilsMAFProcessor(MAFProcessorBase):
    def __init__(self):
        pass

class Analyzer:
    def __init__(
        self,
        session: requests.Session,
        handler: EBIFTPHandler,
        token: str,
        jinja_wrapper: JinjaWrapper = JinjaWrapper(),
        output_location: str = "./ephemeral/",
        maf_processor: MAFProcessorBase = None,
    ):
        self.handler = handler
        self.session = session
        self.token = token
        self.j = jinja_wrapper

        self.ids = set()
        self.bad_mafs = []

        self.duds = ["|", "unknown", "Unknown", "-", " "]
        self.debug = True
        self.limit = 10
        self.output_location = output_location

        self.chebi_complete_entity_url = (
            "http://www.ebi.ac.uk/webservices/chebi/2.0/test/getCompleteEntity?chebiId="
        )

        self.maf_processor = maf_processor or DataFrameMAFProcessor(
            handler=self.handler,
            ids=self.ids,
            is_dud_fn=self.is_dud,
            process_identifier_fn=self.process_identifier,
        )

    def go(self):
        self.j.load_template("cross-checker-report.j2")

        response = self.session.get("https://www.ebi.ac.uk:443/metabolights/ws/studies")
        studies = json.loads(response.text)["content"]

        overview = OverviewMetrics(len(studies), 0, 0, 0, 0)

        for study in studies:
            if overview.studies_processed > self.limit and self.debug:
                break

            print("____________________________________________________________________________")
            print(f"Processing {study}")

            maf_files = self.get_list_of_maf_files_in_study(study, overview)
            for maf in maf_files:
                try:
                    maf_data = self.maf_processor.get_maf(maf["file"], study)
                except Exception as e:
                    self.bad_mafs.append(maf)
                    logging.exception(f"couldnt load {maf}")
                    continue
                self.maf_processor.process_maf(maf_data)
                overview.mafs_processed += 1

        compound_list = self.session.get(
            "https://www.ebi.ac.uk/metabolights/ws/compounds/list"
        ).json()["content"]
        watchdog = self.assemble_registries(compound_list)

        self.save_report(
            maf_registry=watchdog.maf, db_registry=watchdog.db, overview=overview
        )
        self.save_primary_maf_ids(watchdog.maf, "maf")
        self.save_primary_maf_ids(watchdog.db, "db")

    def get_list_of_maf_files_in_study(
        self, study, overview: OverviewMetrics
    ) -> List[dict]:
        """

        :param study:
        :param overview:
        :return:
        """
        url = f"https://www.ebi.ac.uk:443/metabolights/ws/studies/{study}/files?include_raw_data=false"
        headers = {"user_token": self.token}
        self.session.headers = headers

        try:
            response = self.session.get(url)
        except ConnectionError as e:
            print(f"Could not get contents of study {study}: {str(e)}")
            return []
        overview.studies_processed += 1

        maf_files = (
            [
                file
                for file in json.loads(response.text)["study"]
                if file["file"].startswith("m_") and file["file"].endswith(".tsv")
            ]
            if response is not None
            else None
        )
        overview.total_mafs += len(maf_files)
        return maf_files

    def assemble_registries(self, compound_list) -> IDWatchdog:
        """

        :param compound_list:
        :return:
        """
        compound_list_numeric = {
            re.sub(r"\D", "", compound) for compound in compound_list
        }
        maf_list_numeric = {re.sub(r"\D", "", compound) for compound in self.ids}

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

        return IDWatchdog(maf=maf_registry, db=db_registry)

    def is_primary(self, identifier: str) -> bool:
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

        entity_response = self.session.get(
            f"{self.chebi_complete_entity_url}{identifier}"
        )
        chebi_webservice_id = XmlResponseUtils.get_chebi_id(entity_response.text)

        return (
            identifier in chebi_webservice_id
            if chebi_webservice_id is not None
            else None
        )

    def get_delta(self, subject: set, comparator: set) -> list:
        """
        Returns items in a subject set unique to the subject relative to a comparator
        :param subject: set of ids
        :param comparator: set of ids
        :return: list of unique ids in subject
        """
        return list(subject - comparator)

    def get_maf(self, maf: str, study: str) -> pd.DataFrame:
        """
        Retrieve a specific maf sheet from a study via the FTP handler.
        :param maf: Filename of the given maf.
        :param study: Study accession number.
        :return: MAF sheet as a pandas dataframe
        """
        maf_dataframe = self.handler.load_isa_file(isatab_file=maf, study=study)
        return maf_dataframe

    def process_maf(self, maf_dataframe: pd.DataFrame) -> None:
        """
        Process a maf in the form of a dataframe by going over each row, pulling out the database_identifier column
        entry and throwing that entry into a processing method (which also has a dud checking method in the same
        ternary statement that it lives in)
        :param maf_dataframe: A single MAF sheets as a pandas dataframe
        :return: N/A
        """
        if maf_dataframe is None:
            return
        for index, row in maf_dataframe.iterrows():
            database_identifier = (
                row["database_identifier"]
                if "database_identifier" in row.keys()
                else None
            )
            self.process_identifier(database_identifier) if not self.is_dud(
                database_identifier
            ) else None

    def process_identifier(self, identifier) -> None:
        """
        Assess a single cell from the database_identifier column in a MAF sheet
        Currently we are only interested in CHEBI identifiers, we just log everything else, but we may want  to do
        something with the other identifiers / structural information that we sometimes find.
        :param identifier: entry from the database identifier column, could be one of several types.
        :return: N/A but adds id to objects.ids list if it is a CHEBI id
        """
        if isinstance(identifier, float) or isinstance(identifier, int):
            print(
                f"Found non zero numeric identifier or structure descriptor {identifier}"
            )
            
        elif identifier.startswith("CHEBI") or identifier.count("CHEBI") > 0:
            if identifier.count("CHEBI") > 1:
                identifiers = {
                    ident
                    for ident in identifier.split("|")
                    if ident.startswith("CHEBI")
                }
                self.ids.update(identifiers)
            else:
                if any(dud in identifier for dud in self.duds):
                    for dud in self.duds:
                        identifier = identifier.replace(dud, "")
                if len(identifier) > 12:
                    print(identifier)
                self.ids.add(identifier)
        else:
            print(identifier)

    def is_dud(self, identifier) -> bool:
        """
        Check if a given identifier is a 'dud', that is to say it is one of:
        - The dud list in the Checker class
        - Numeric value equivalent to zero
        :param identifier: Identifier to discern whether is dud
        :return: bool value indicating dud-ness
        """
        if identifier is None:
            return True
        if identifier in self.duds:
            return True
        if isinstance(identifier, float):
            if identifier == 0:
                return True
            return math.isnan(identifier)
        if isinstance(identifier, int):
            if identifier == 0:
                return True
            return math.isnan(identifier)
        for dud in self.duds:
            # this might seem strange, but catches cases like 'unknownId'
            if dud in identifier:
                return True

    @file_rw_exception_angel
    def save_report(
        self,
        maf_registry: IDRegistry,
        db_registry: IDRegistry,
        overview: OverviewMetrics,
    ) -> None:
        """
        Save a report with metrics from the run of Checker.go
        :param maf_registry: IDRegistry object containing information about the number of unique primary/secondary IDs
            within MAF sheets.
        :param db_registry: IDRegistry object containing information about the number of unique primary/secondary IDs
            within the postgres table.
        :param overview: OverviewMetrics object containing information about the studies and MAF sheets processed.
        :return: None
        """
        jinja_vars = {
            "studies_processed": overview.studies_processed,
            "total_studies": overview.total_studies,
            "mafs_processed": overview.mafs_processed,
            "total_mafs": overview.total_mafs,
            "total_unique_to_mafs": maf_registry.total,
            "total_unique_maf_primary_ids": len(maf_registry.primary),
            "total_unique_maf_secondary_ids": len(maf_registry.secondary),
            "total_unique_maf_incorrect": len(maf_registry.incorrect),
            "total_unique_to_db": db_registry.total,
            "total_unique_db_primary_ids": len(db_registry.primary),
            "total_unique_db_secondary_ids": len(db_registry.secondary),
            "total_unique_db_incorrect": len(db_registry.incorrect),
        }
        rendered_report = self.j.template.render(jinja_vars)

        with open(f"{self.output_location}report.txt", "w") as report_file:
            report_file.write(rendered_report)

    @file_rw_exception_angel
    def save_primary_maf_ids(self, registry: IDRegistry, name: str) -> None:
        """
        Write a bunch of primary IDs to text file for later use.
        :param registry: The ID registry for which we will preserve primary IDs in disk.
        :param name: Name of the registry
        :return: None
        """
        with open(f"{self.output_location}{str(name)}_primaries.txt", "w") as id_file:
            id_file.write(str(registry.primary))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--token", help="MetaboLights API Token")
    args = parser.parse_args()
    token = args.token

    Analyzer(
        session=requests.Session(),
        handler=EBIFTPHandler(
            config=FTPConfig(
                enabled=True,
                root="ftp.ebi.ac.uk",
                study="/pub/databases/metabolights/studies/public/",
                user="anonymous",
                password="",
            )
        ),
        token=token,
    ).go()
