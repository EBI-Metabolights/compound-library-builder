import json
import unittest
from typing import List

import pytest
import redis
from unittest.mock import MagicMock, patch

from compound_common.dir_utils import DirUtils
from compound_common.list_utils import ListUtils
from tests.compound_builder_tests.redis_queue_manager_tests.fixtures import compound_redis_queue_manager_fixture, compound_list_fixture


class TestCompoundRedisQueueManager:

    def test_populate_queue_happy(self, compound_redis_queue_manager_fixture):
        crqm_fixt = compound_redis_queue_manager_fixture
        crqm_fixt.redis_client = MagicMock()
        redis.Redis = MagicMock()
        crqm_fixt.redis_client.check_queue_exists = MagicMock(return_value={'items': 0})

        crqm_fixt.get_compounds_ids = MagicMock(return_value=[])
        ListUtils.get_lol = MagicMock(return_value=[])
        crqm_fixt.populate_queue()

        assert crqm_fixt.get_compounds_ids.call_count == 1
        assert ListUtils.get_lol.call_count == 1

    @patch('builtins.print')
    def test_populate_queue_sad(self, mock_print, compound_redis_queue_manager_fixture):
        crqm_fixt = compound_redis_queue_manager_fixture
        crqm_fixt.redis_client = MagicMock()
        crqm_fixt.redis_client.check_queue_exists = MagicMock(return_value={'items': 1})

        crqm_fixt.get_compounds_ids = MagicMock()
        ListUtils.get_lol = MagicMock()
        crqm_fixt.populate_queue()

        assert crqm_fixt.get_compounds_ids.call_count == 0
        assert ListUtils.get_lol.call_count == 0
        mock_print.assert_called_once_with('Queue populated. Risk of duplication. Aborting.')

    def test_get_compound_ids(self, compound_list_fixture, compound_redis_queue_manager_fixture):
        """
        It should make a GET request to the MetaboLights webservice to get the list of compounds
        AND it should return only new compounds if the config option is set as so
        """
        crqm_fixt = compound_redis_queue_manager_fixture
        crqm_fixt.session = MagicMock()
        crqm_fixt.cbrc.new_compounds_only = True
        response = MagicMock()
        response.json = MagicMock(return_value={'content': compound_list_fixture})
        crqm_fixt.session.get = MagicMock(return_value=response)
        ListUtils.get_delta = MagicMock()
        DirUtils.get_mtblc_ids_from_directory = MagicMock()

        crqm_fixt.get_compounds_ids(mtblc_dir='none')

        crqm_fixt.session.get.assert_called_once_with("http://www.ebi.ac.uk/metabolights/ws/compounds/list")
        ListUtils.get_delta.assert_called_once_with(compound_list_fixture, DirUtils.get_mtblc_ids_from_directory('none'))

    @patch('builtins.print')
    def test_push_compound_ids_to_redis_happy(self, mock_print, compound_redis_queue_manager_fixture, compound_list_fixture):
        """
        It should push a stringified version of a sublist to the compounds queue
        AND it should indicate success if a response is given
        """
        crqm_fixt = compound_redis_queue_manager_fixture
        crqm_fixt.redis_client = MagicMock()
        crqm_fixt.redis_client.push_to_queue = MagicMock(return_value=1)
        dumped = json.dumps(compound_list_fixture)

        crqm_fixt.push_compound_ids_to_redis([compound_list_fixture])

        mock_print.assert_called_once_with('Pushed sublist 0 to queue')
        crqm_fixt.redis_client.push_to_queue.assert_called_once_with('compounds', dumped)

    @patch('builtins.print')
    def test_push_compound_ids_to_redis_sad(self, mock_print, compound_redis_queue_manager_fixture, compound_list_fixture):
        """
        It should push a stringified version of a sublist to the compounds queue
        AND it should indicate failure if no response is given
        BUT not if there are no sublists
        """
        crqm_fixt = compound_redis_queue_manager_fixture
        crqm_fixt.redis_client = MagicMock()
        crqm_fixt.redis_client.push_to_queue = MagicMock(return_value=None)
        dumped = json.dumps(compound_list_fixture)

        crqm_fixt.push_compound_ids_to_redis([])

        mock_print.assert_not_called()
        crqm_fixt.redis_client.push_to_queue.assert_not_called()

        crqm_fixt.push_compound_ids_to_redis([compound_list_fixture])

        mock_print.assert_called_once_with('Unable to push sublist 0 to compound queue')
        crqm_fixt.redis_client.push_to_queue.assert_called_once_with('compounds', dumped)

    def test_consume_queue(self, compound_redis_queue_manager_fixture, compound_list_fixture):
        """
        I should get a list of MTBLC ids back
        WHEN an item is popped from the compounds queue
        """
        crqm_fixt = compound_redis_queue_manager_fixture
        crqm_fixt.redis_client = MagicMock()
        crqm_fixt.redis_client.consume_queue = MagicMock(return_value=str(compound_list_fixture))
        result = crqm_fixt.consume_queue()

        assert isinstance(result, list)
        crqm_fixt.redis_client.consume_queue.assert_called_once_with('compounds')

    def test_consume_queue_error(self, compound_redis_queue_manager_fixture):
        """
        We should get a ValueError
        IF there is nothing on the queue

        """
        crqm_fixt = compound_redis_queue_manager_fixture
        crqm_fixt.redis_client = MagicMock()
        crqm_fixt.redis_client.consume_queue = MagicMock(return_value=None)
        with pytest.raises(ValueError) as excinfo:
            result = crqm_fixt.consume_queue()
            
        assert 'ValueError' in str(excinfo)
        crqm_fixt.redis_client.consume_queue.assert_called_once_with('compounds')




