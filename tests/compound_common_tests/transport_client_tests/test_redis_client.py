import json
from unittest.mock import MagicMock, patch

from tests.compound_common_tests.transport_client_tests.fixtures import (
    redis_client_fixture,
)


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

        result = rc.push_to_queue("compounds", payload)

        rc.redis.lpush.assert_called_once_with("compounds", correct_payload)
        assert result is not None

    def test_push_to_queue_bad_payload(self, redis_client_fixture):
        rc = redis_client_fixture
        rc.redis.lpush = MagicMock()
        with patch("json.dumps", side_effect=ValueError("an error")), patch(
            "logging.exception"
        ) as mock_logging:
            result = rc.push_to_queue("compounds", ["nonsense"])
            assert result is None
            mock_logging.assert_called_once_with("Couldnt serialize payload: an error")
            assert rc.redis.lpush.call_count == 0

    def test_check_queue_exists(self, redis_client_fixture):
        """
        The client should tell us a queue exists by using pythons redis interface and returning the info in a dict
        """
        rc = redis_client_fixture
        rc.redis.exists = MagicMock(return_value=True)
        rc.redis.llen = MagicMock(return_value=10)
        result = rc.check_queue_exists("test")
        assert result == {"exists": True, "items": 10}

        rc.redis.exists.return_value = False
        result = rc.redis.exists

    def test_check_queue_doesnt_exist(self, redis_client_fixture):
        """
        The client should tell us that a queue does not exist, and if it doesnt it shouldnt make LLEN call as there
        is no need.
        """
        rc = redis_client_fixture
        rc.redis.exists = MagicMock(return_value=False)
        rc.redis.llen = MagicMock()
        result = rc.check_queue_exists("test")
        assert result == {"exists": False, "items": -1}
        assert rc.redis.llen.call_count == 0

    def test_empty_queue(self, redis_client_fixture):
        rc = redis_client_fixture
        rc.redis.delete = MagicMock(return_value=1)

        result = rc.empty_queue('compounds')
        assert result == 1
        rc.redis.delete.assert_called_once_with('compounds')

    def test_consume_queue_happy(self, redis_client_fixture):
        """
        It should consume an item from the queue and that item should be returned in deserialised form.
        """
        rc = redis_client_fixture
        rc.redis.lpop = MagicMock(return_value=json.dumps(["MTBLC12345"]))

        result = rc.consume_queue("compounds")
        assert result == ["MTBLC12345"]

    @patch("builtins.print")
    def test_consume_queue(self, mock_print, redis_client_fixture):
        """
        If there is nothing on the queue, the client should return None
        AND tell us that there is nothing on the specified queue in the log output.
        """
        rc = redis_client_fixture
        json.loads = MagicMock()
        rc.redis.lpop = MagicMock(return_value=None)

        result = rc.consume_queue("compounds")

        assert json.loads.call_count == 0
        assert result is None
        mock_print.assert_called_once_with("Nothing on compounds queue")
