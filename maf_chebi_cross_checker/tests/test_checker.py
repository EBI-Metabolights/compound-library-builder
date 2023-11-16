import json
from unittest.mock import patch, mock_open, MagicMock

import pytest

from compound_common.doc_clients.xml_utils import XmlResponseUtils
from maf_chebi_cross_checker.checker_dataclasses import OverviewMetrics


class TestChecker:
    """
    All whatever_fixture seen in test function arguments can be found in maf_chebi_cross_checker/tests/fixtures.py
    """

    def test_go_bad_maf(self, checker_fixture, study_file_endpoint_fixture, good_dataframe):
        """Test the 'go' method by checking that it correctly catches a bad maf file - this required, evidently, a whole
         lot of mocking was required, as this method mostly just calls other class methods """
        checker_fixture.j.load_template = MagicMock()
        checker_fixture.j.load_template.return_value = MagicMock()
        checker_fixture.session.get = MagicMock()
        json.loads = MagicMock()
        json.loads.return_value = {'content': ['MTBLS1']}
        checker_fixture.get_list_of_maf_files_in_study = MagicMock()
        checker_fixture.get_list_of_maf_files_in_study.return_value = [study_file_endpoint_fixture['study'][0]]
        checker_fixture.get_maf = MagicMock()
        checker_fixture.get_maf.side_effect = pytest.raises(Exception)
        checker_fixture.process_maf = MagicMock()
        checker_fixture.assemble_registries = MagicMock()
        checker_fixture.save_report = MagicMock()
        checker_fixture.save_primary_maf_ids = MagicMock()

        checker_fixture.go()

        assert checker_fixture.session.get.call_count is 2
        assert checker_fixture.get_list_of_maf_files_in_study.call_count is 1
        assert checker_fixture.get_maf.call_count is 1
        assert checker_fixture.process_maf.call_count is 0
        assert checker_fixture.bad_mafs.__len__() is 1

    def test_get_list_of_maf_files_in_studies_good_resp(self, checker_fixture, study_file_endpoint_fixture):
        """Test get_list_of_maf_files by providing it with a list of imitation file dicts, and expecting it to return
        only the ones that are MAF sheets."""
        checker_fixture.session.get = MagicMock()
        checker_fixture.session.get.return_value = MagicMock()
        json.loads = MagicMock()
        json.loads.return_value = study_file_endpoint_fixture
        overview = OverviewMetrics(0,0,0,0)

        results = checker_fixture.get_list_of_maf_files_in_study('MTBLS999999', overview)
        filenames = [result['file'] for result in results]
        assert overview.total_mafs == 2
        assert overview.studies_processed == 1
        assert 'm_mtbls1_metabolite_profiling_NMR_spectroscopy_v2_maf.tsv' in filenames
        assert 'm_s_aureus_adaptation_to_different_human_cell_lines_metabolite_profiling_NMR_spectroscopy_v2_maf.tsv'\
               in filenames

    def test_get_list_of_maf_files_no_resp(self, checker_fixture):
        """Test get_list_of_maf_files by throwing an exception and expecting it to fail gracefully by returning an empty
         list (since a study cannot exist without a MAF sheet, an empty list can only occur in an error scenario)."""
        checker_fixture.session.get = MagicMock()
        checker_fixture.session.get.side_effect = ConnectionError('Oh no!')
        overview = OverviewMetrics(0, 0, 0, 0)

        results = checker_fixture.get_list_of_maf_files_in_study('MTBLS999999', overview)
        assert results == []

    def test_assemble_registries_all_primaries(self, checker_fixture, compound_ids):
        """Test the assemble_registries method by giving it two iterable of ids, instructing the mocked assessing
        function to deem all unique ID's as primary, and expecting the unique IDs from each iterable to be sorted into
        the primary set in the respective IDRegistry object"""
        checker_fixture.ids = compound_ids[0]
        compound_list = compound_ids[1]

        checker_fixture.is_primary = MagicMock()
        checker_fixture.is_primary.return_value = True
        result = checker_fixture.assemble_registries(compound_list)

        assert len(result.maf.primary) + len(result.maf.secondary) + len(result.maf.incorrect) is 2
        assert result.maf.primary == {'1', '2'}

        assert len(result.db.primary) + len(result.db.secondary) + len(result.db.incorrect) is 3
        assert result.db.primary == {'7', '8', '9'}

    def test_assemble_registries_all_secondaries(self, checker_fixture, compound_ids):
        """Test the assemble_registries method by giving it two iterable of ids, instructing the mocked assessing
        function to deem all unique ID's as secondary, and expecting the unique IDs from each iterable to be sorted into
        the secondary set in the respective IDRegistry object"""
        checker_fixture.ids = compound_ids[0]
        compound_list = compound_ids[1]
        checker_fixture.is_primary = MagicMock()
        checker_fixture.is_primary.return_value = False
        result = checker_fixture.assemble_registries(compound_list)

        assert len(result.maf.primary) + len(result.maf.secondary) + len(result.maf.incorrect) is 2
        assert result.maf.secondary == {'1', '2'}

        assert len(result.db.primary) + len(result.db.secondary) + len(result.db.incorrect) is 3
        assert result.db.secondary == {'7', '8', '9'}

    def test_assemble_registries_all_incorrect(self, checker_fixture, compound_ids):
        """Test the assemble_registries method by giving it two iterable of ids, instructing the mocked assessing
        function to deem all unique ID's as incorrect, and expecting the unique IDs from each iterable to be sorted into
        the incorrect set in the respective IDRegistry object"""
        checker_fixture.ids = compound_ids[0]
        compound_list = compound_ids[1]
        checker_fixture.is_primary = MagicMock()
        checker_fixture.is_primary.return_value = None
        result = checker_fixture.assemble_registries(compound_list)

        assert len(result.maf.primary) + len(result.maf.secondary) + len(result.maf.incorrect) is 2
        assert result.maf.incorrect == {'1', '2'}

        assert len(result.db.primary) + len(result.db.secondary) + len(result.db.incorrect) is 3
        assert result.db.incorrect == {'7', '8', '9'}

    def test_is_primary_id(self, checker_fixture, chebi_complete_entity):
        """Test the is_primary method against a primary ID, a secondary ID and a dud ID"""
        id = 'CHEBI:16449'
        checker_fixture.session.get.return_value = chebi_complete_entity

        fake_chebi_get = MagicMock()
        fake_chebi_get.return_value = 'CHEBI:16449'
        XmlResponseUtils.get_chebi_id = fake_chebi_get

        result = checker_fixture.is_primary(id)
        assert fake_chebi_get.call_count is 1
        assert result is True

        fake_chebi_get.return_value = 'CHEBI:16450'
        result = checker_fixture.is_primary(id)

        assert fake_chebi_get.call_count is 2
        assert result is False

        fake_chebi_get.return_value = None
        result = checker_fixture.is_primary(id)

        assert fake_chebi_get.call_count is 3
        assert result is None

    def test_get_delta(self, checker_fixture):
        """Test the get_delta method by making sure the exact list we expect is returned in each scenario"""
        subject = {'12345', '67890', '81818', '20010'}
        comparator = {'12345', '67890', '81818', '00000', '11111'}
        result = checker_fixture.get_delta(subject, comparator)
        assert result == ['20010']

        subject_two = {'12345', '67890'}
        comparator_two = {'12345', '67890'}
        result = checker_fixture.get_delta(subject_two, comparator_two)
        assert result == []

    def test_process_maf_none(self, checker_fixture):
        """Test the process_maf method by giving it a None, which should cause it to halt immediately and therefore make
         no further function calls"""
        checker_fixture.process_identifier = MagicMock()
        checker_fixture.process_maf(None)
        assert checker_fixture.process_identifier.call_count == 0

    def test_process_maf_legitimate(self, checker_fixture, good_dataframe):
        """Test the process_maf method by giving it the good_dataframe fixture, therefore meaning the dataframe is
        iterated over, and its database_identifer entry used as an argument in process_identifier"""
        checker_fixture.process_identifier = MagicMock()
        checker_fixture.process_maf(good_dataframe)
        assert checker_fixture.process_identifier.call_count == 1

    def test_process_identifier_float_and_int(self, checker_fixture):
        """Test the process_identifier method by giving it a float and an int, and expecting neither to be added to the
        Checkers ids set."""
        identifier = 0.0
        checker_fixture.process_identifier(identifier)
        assert len(checker_fixture.ids) is 0

        identifier = 1
        checker_fixture.process_identifier(identifier)
        assert len(checker_fixture.ids) is 0

    def test_process_identifier_multiple_chebi_ids(self, checker_fixture):
        """Test the process_identifier method by giving it an identifier with multiple chebi IDs, and expecting each ID
        to be added to the Checkers ids set."""
        identifier = 'CHEBI:123|CHEBI:456|CHEBI:789'
        checker_fixture.process_identifier(identifier)
        assert len(checker_fixture.ids) is 3

    def test_process_identifier_multiple_chebi_ids_with_duds(self, checker_fixture):
        """Test the process_identifier method by giving it an identifier with multiple chebi IDs and expecting each
        legitimate ID to be added to the Checkers ids set, with the duds cast aside."""
        identifier = 'unknown|CHEBI:123|unknown'
        checker_fixture.process_identifier(identifier)
        assert len(checker_fixture.ids) is 1

    def test_process_identifier_unexpected(self, checker_fixture):
        """Test the process_identifier method by giving it an unexpected string that should not be added to the Checkers
        ids set."""
        identifier = 'chemistry'
        checker_fixture.process_identifier(identifier)
        assert len(checker_fixture.ids) is 0

    def test_is_dud(self, checker_fixture):
        """Test the is_dud method by giving it a bunch of duds and expecting it to say they are all duds."""
        dud_ids = ['|', 'unknownGarbage', '-', ' ']
        for dud in dud_ids:
            assert checker_fixture.is_dud(dud) is True

    @patch("builtins.open", new_callable=mock_open)
    def test_save_report(self, mock_file, checker_fixture, registry, overview):
        """Test the save_report method by checking that when it is given OverMetrics and IDRegistry objects, the values
        from those objects are deposited into the template, and the rendered template is saved at the location specified
        By Checkers output_path (all actual file I/O mocked)"""

        checker_fixture.j = MagicMock()
        checker_fixture.j.template = MagicMock()
        checker_fixture.j.template.render = MagicMock()
        checker_fixture.j.template.render.return_value = 'cut_the_jinja'
        checker_fixture.output_location = 'a/path/to/nowhere/'
        checker_fixture.save_report(maf_registry=registry, db_registry=registry, overview=overview)

        checker_fixture.j.template.render.assert_called_with(
            {
                'studies_processed': 48,
                'total_studies': 50,
                'mafs_processed': 78,
                'total_mafs': 80,
                'total_unique_to_mafs': 99,
                'total_unique_maf_primary_ids': 33,
                'total_unique_maf_secondary_ids': 33,
                'total_unique_maf_incorrect': 33,
                'total_unique_to_db': 99,
                'total_unique_db_primary_ids': 33,
                'total_unique_db_secondary_ids': 33,
                'total_unique_db_incorrect': 33
            }
        )
        mock_file.assert_called_once_with('a/path/to/nowhere/report.txt', 'w')
        mock_file().write.assert_called_once_with('cut_the_jinja')
