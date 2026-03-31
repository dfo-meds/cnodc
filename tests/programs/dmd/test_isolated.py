import netCDF4

from cnodc.programs.dmd.metadata import Thesaurus, Keyword, QuickWebPage, ResourcePurpose, Encoding, Variable, \
    TemporalResolution, DatasetMetadata
from helpers.base_test_case import BaseTestCase

class TestNothing(BaseTestCase):


    def test_bad_org_id_vocab(self):
        md = DatasetMetadata()
        with self.assertLogs('cnodc.dmd.metadata', 'WARNING'):
            md._add_netcdf_contact('hello', 'hello@hello', '12345', None, None, 'editor', 'institution', 'DOI')
            self.assertEqual(len(md.responsibles), 1)
            self.assertIsNone(md.responsibles[0]._data['contact'].ror)
