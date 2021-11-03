from unittest import TestCase

from collection.load_data import remove_special_chars


class LoadDataTests(TestCase):
    def test_remove_special_chars(self):
        self.assertEqual(remove_special_chars(1234), '')
        self.assertEqual(remove_special_chars('Test!'), 'Test')
        self.assertEqual(remove_special_chars('{!}'), '')
