from unittest import TestCase

from pkg_resources import resource_filename

from load_data import main


class LoadDataTestCase(TestCase):
    def test_main(self):
        test_path = resource_filename('collection.tests', 'resources/')
        main(test_path)
