import json
import unittest as ut
import pathlib
import cnodc.ocproc2.structures as ocproc2
from cnodc.codecs.ocproc2bin import OCProc2BinCodec
import datetime

class TestOCProc2BinaryFormat(ut.TestCase):

    def test_basic_encode_decode(self):
        record = ocproc2.DataRecord()
        record.metadata['TInt'] = 1
        record.metadata['TFloat'] = 1.1
        record.metadata['TString'] = 'hello'
        record.metadata['TList'] = [1, 2]
        record.metadata['TDict'] = {'a': 1, 'b': 2}
        record.metadata['TBoolean'] = False
        record.metadata['TNone'] = None
        record.metadata['TDateTime'] = datetime.datetime.now(datetime.timezone.utc)
        codec = OCProc2BinCodec()
        byte_iterable = codec.encode_records([record])
        new_records = [x for x in codec.decode_messages(byte_iterable)]
        self.assertEqual(1, len(new_records))
        nr = new_records[0]
        for x in ('TInt', 'TFloat', 'TString', 'TList', 'TDict', 'TBoolean', 'TNone', 'TDateTime'):
            with self.subTest(x=x):
                self.assertIn(x, nr.metadata)
                self.assertEqual(record.metadata[x], nr.metadata[x])
