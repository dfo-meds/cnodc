import unittest as ut
import datetime

from cnodc.storage import StorageController
from cnodc.storage.base import StorageTier
from cnodc.storage.core import SecurityLevel, AccessLevel

class TestStorageController(ut.TestCase):

    def test_default_metadata(self):
        sc = StorageController()
        md = sc.build_metadata(
            'pname',
            'dname',
            'cunit',
            True,
            AccessLevel.Controlled,
            SecurityLevel.ProtectedA,
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            automate_release=True,
            storage_tier=StorageTier.ARCHIVAL
        )
        self.assertEqual({
            'Program': 'pname',
            'Dataset': 'dname',
            'CostUnit': 'cunit',
            'Gzip': 'YES',
            'AccessLevel': AccessLevel.Controlled.value,
            'SecurityLabel': SecurityLevel.ProtectedA.value,
            'AutomatedRelease': 'YES',
            'ReleaseDate': '2015-01-02T03:04:05',
            'StorageTier': StorageTier.ARCHIVAL.value
        }, md)


    def test_applying_metadata_on_metadata(self):
        sc = StorageController()
        md = {
            'Program': 'pname',
            'Dataset': 'dname',
            'CostUnit': 'cunit',
            'Gzip': 'YES',
            'AccessLevel': AccessLevel.Controlled.value,
            'SecurityLabel': SecurityLevel.ProtectedA.value,
            'AutomatedRelease': 'YES',
            'ReleaseDate': '2015-01-02T03:04:05',
            'StorageTier': ''
        }
        sc.apply_default_metadata(md, storage_tier=StorageTier.ARCHIVAL)
        self.assertEqual(md['Program'], 'pname')
        self.assertEqual(md['Dataset'], 'dname')
        self.assertEqual(md['CostUnit'], 'cunit')
        self.assertEqual(md['Gzip'], 'YES')
        self.assertEqual(md['AccessLevel'], AccessLevel.Controlled.value)
        self.assertEqual(md['SecurityLabel'], SecurityLevel.ProtectedA.value)
        self.assertEqual(md['AutomatedRelease'], 'YES')
        self.assertEqual(md['ReleaseDate'], '2015-01-02T03:04:05+00:00')
        self.assertEqual(md['StorageTier'], StorageTier.ARCHIVAL.value)

    def test_applying_metadata_on_metadata_with_tier(self):
        sc = StorageController()
        md = {
            'StorageTier': 'frequent'
        }
        sc.apply_default_metadata(md, storage_tier=StorageTier.ARCHIVAL)
        self.assertEqual(md['StorageTier'], 'frequent')

    def test_default_access_level_embargoed(self):
        sc = StorageController()
        md = sc.build_metadata(release_date=datetime.datetime(2015, 1, 2, 3, 4, 5))
        self.assertEqual(md['AccessLevel'], AccessLevel.Embargoed.value)
        self.assertEqual(md['SecurityLabel'], SecurityLevel.Unclassified.value)
        self.assertEqual(md['ReleaseDate'], '2015-01-02T03:04:05')


    def test_default_access_level_controlled(self):
        sc = StorageController()
        md = sc.build_metadata(security_label=SecurityLevel.ProtectedA)
        self.assertEqual(md['AccessLevel'], AccessLevel.Controlled.value)
        self.assertEqual(md['SecurityLabel'], SecurityLevel.ProtectedA.value)
        self.assertEqual(md['ReleaseDate'], '')


    def test_no_handle(self):
        sc = StorageController()
        self.assertIsNone(sc.get_handle('protocol://helloworld.txt'))