from unittest import TestCase
from unittest.mock import patch
from http import HTTPStatus

from collection.zip_to_tract_lookup import zip_to_tract_lookup


def prefix(name):
    return f'collection.zip_to_tract_lookup.{name}'


class ZipTractLookupTests(TestCase):
    @patch(prefix('requests'), autospec=True)
    def test_zip_to_tract_failure(self, mock_requests):
        mock_requests.get.return_value.status_code = HTTPStatus.BAD_REQUEST
        self.assertIsNone(zip_to_tract_lookup('123', 2021))

    @patch(prefix('requests'), autospec=True)
    def test_zip_to_tract_success(self, mock_requests):
        success_record = {'data': {'results': [{'geoid': 123, 'tot_ratio': 1}]}}
        mock_requests.get.return_value.status_code = HTTPStatus.OK
        mock_requests.get.return_value.json.return_value = success_record
        self.assertEqual(zip_to_tract_lookup('123', 2021), 123)
