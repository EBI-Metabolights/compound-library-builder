import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from tests.compound_common_tests.transport_client_tests.fixtures import redis_client_fixture


class TestRedisClient:

    def test_push_to_queue_happy(self, redis_client_fixture):
        """
        It should push a block MTBLC IDs to the queue in string form
        IF they were correctly serialized
        :param redis_client_fixture:
        :return:
        """
        rc = redis_client_fixture
        rc.redis.lpush = MagicMock(return_value=1)
        payload = ["MTBLC1", "MTBLC2", "MTBLC3"]
        correct_payload = json.dumps(payload)

        result = rc.push_to_queue('compounds', payload)

        rc.redis.lpush.assert_called_once_with('compounds', correct_payload)
        assert result is not None

    def test_push_to_queue(self, redis_client_fixture):
        rc = redis_client_fixture
        rc.redis.lpush = MagicMock()
        with patch('json.dumps', side_effect=ValueError("an error")), \
                patch('logging.exception') as mock_logging:
            result = rc.push_to_queue('compounds', ['nonsense'])
            assert result is None
            mock_logging.assert_called_once_with('Couldnt serialize payload: an error')