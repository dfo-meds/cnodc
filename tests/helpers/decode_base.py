import medsutil.ocproc2 as ocproc2
import datetime

from tests.helpers.base_test_case import BaseTestCase


class CodecTestCase(BaseTestCase):

    def _build_standard_records(self):
        record = ocproc2.ParentRecord()
        record.metadata['TInt'] = 1
        record.metadata['TFloat'] = 1.1
        record.metadata['TString'] = 'hello'
        record.metadata['TList'] = [1, 2]
        record.metadata['TDict'] = {'a': 1, 'b': 2}
        record.metadata['TBoolean'] = False
        record.metadata['TNone'] = None
        record.metadata['TDateTime'] = datetime.datetime(2023, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
        yield record
        record2 = ocproc2.ParentRecord()
        child = ocproc2.ChildRecord()
        child.metadata['Test'] = 'Foo'
        child.metadata['Test'].metadata['Units'] = 'm'
        record2.subrecords.append_to_record_set('PROFILE', 0, child)
        yield record2

    def _verify_standard_records(self, records):
        self.assertEqual(2, len(records))
        self.assertEqual(1, records[0].metadata['TInt'].value)
        self.assertEqual(1.1, records[0].metadata['TFloat'].value)
        self.assertEqual('hello', records[0].metadata['TString'].value)
        self.assertEqual([1, 2], records[0].metadata['TList'].value)
        self.assertEqual({'a': 1, 'b': 2}, records[0].metadata['TDict'].value)
        self.assertFalse(records[0].metadata['TBoolean'].value)
        self.assertIsNone(records[0].metadata['TNone'].value)
        self.assertEqual("2023-01-02T03:04:05+00:00", records[0].metadata['TDateTime'].value)
        self.assertIn('PROFILE', records[1].subrecords)
        self.assertIn(0, records[1].subrecords['PROFILE'])
        self.assertEqual(1, len(records[1].subrecords['PROFILE'][0].records))
        self.assertIn('Test', records[1].subrecords['PROFILE'][0].records[0].metadata)
        self.assertEqual("Foo", records[1].subrecords["PROFILE"][0].records[0].metadata['Test'].value)
        self.assertEqual("m", records[1].subrecords["PROFILE"][0].records[0].metadata['Test'].metadata['Units'].value)
