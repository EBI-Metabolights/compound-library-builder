from unittest.mock import MagicMock, patch

from tests.reference_file_builders_tests.reactome_builder_tests.fixtures import (
    reactome_response_fixture,
    reactome_builder_fixture,
)


class TestReactomeFileBuilder:
    def test_reactome_builder_happy(
        self, reactome_builder_fixture, reactome_response_fixture
    ):
        rb = reactome_builder_fixture
        rb.mpm = MagicMock()
        rb.mpm.vanilla = MagicMock()
        rb.mpm.vanilla.save = MagicMock()

        mock_response = MagicMock()
        mock_response.text = reactome_response_fixture
        mock_response.status_code = 200
        rb.session = MagicMock()
        rb.session.get = MagicMock(return_value=mock_response)

        result = rb.build()

        rb.session.get.assert_called_once_with("test.me")
        assert len(result["MTBLC10033"]) == 6
        for dic in result["MTBLC10033"]:
            for key, value in dic.items():
                assert len(value) > 0

    @patch("builtins.print")
    def test_reactome_builder_sad(self, mock_print, reactome_builder_fixture):
        rb = reactome_builder_fixture
        rb.mpm = MagicMock()
        rb.mpm.vanilla = MagicMock()
        rb.mpm.vanilla.save = MagicMock()

        mock_response = MagicMock()
        mock_response.status_code = 999
        mock_response.text = "Oh no!"
        rb.session.get = MagicMock(return_value=mock_response)

        result = rb.build()

        mock_print.assert_called_once_with(
            "Non 200 status code received from reactome API: 999 / Oh no!"
        )
        assert rb.mpm.vanilla.save.call_count == 0
        assert result == {}
