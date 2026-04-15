import datetime
import hashlib
import unittest as ut

from medsutil.ocproc2 import HistoryEntry, MessageType, normalize_ocproc_path, QCMessage, QCTestRunInfo, QCResult


class TestHistory(ut.TestCase):

    def test_mapping(self):
        h = HistoryEntry(
            'hello world',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test',
            '1.0',
            '12345',
            MessageType.NOTE
        )
        self.assertEqual(h.message, 'hello world')
        self.assertEqual(h.timestamp, '2015-01-02T03:04:05')
        self.assertEqual(h.source_name, 'test')
        self.assertEqual(h.source_version, '1.0')
        self.assertEqual(h.source_instance, '12345')
        self.assertIs(h.message_type, MessageType.NOTE)
        map_ = h.to_mapping()
        self.assertEqual(map_, {
            '_message': 'hello world',
            '_timestamp': '2015-01-02T03:04:05',
            '_source': ('test', '1.0', '12345'),
            '_message_type': MessageType.NOTE.value
        })
        h2 = HistoryEntry.from_mapping(map_)
        self.assertEqual(h2.message, 'hello world')
        self.assertEqual(h2.timestamp, '2015-01-02T03:04:05')
        self.assertEqual(h2.source_name, 'test')
        self.assertEqual(h2.source_version, '1.0')
        self.assertEqual(h2.source_instance, '12345')
        self.assertIs(h2.message_type, MessageType.NOTE)

    def test_hash_history_same(self):
        h = HistoryEntry(
            'hello world',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test',
            '1.0',
            '12345',
            MessageType.NOTE
        )
        hs1 = hashlib.new('sha256')
        h.update_hash(hs1)
        h2 = HistoryEntry(
            'hello world',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test',
            '1.0',
            '12345',
            MessageType.NOTE
        )
        hs2 = hashlib.new('sha256')
        h2.update_hash(hs2)
        self.assertEqual(hs1.digest(), hs2.digest())

    def test_hash_history_diff_message(self):
        h = HistoryEntry(
            'hello world2',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test',
            '1.0',
            '12345',
            MessageType.NOTE
        )
        hs1 = hashlib.new('sha256')
        h.update_hash(hs1)
        h2 = HistoryEntry(
            'hello world',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test',
            '1.0',
            '12345',
            MessageType.NOTE
        )
        hs2 = hashlib.new('sha256')
        h2.update_hash(hs2)
        self.assertNotEqual(hs1.digest(), hs2.digest())

    def test_hash_history_diff_timestamp(self):
        h = HistoryEntry(
            'hello world',
            datetime.datetime(2016, 1, 2, 3, 4, 5),
            'test',
            '1.0',
            '12345',
            MessageType.NOTE
        )
        hs1 = hashlib.new('sha256')
        h.update_hash(hs1)
        h2 = HistoryEntry(
            'hello world',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test',
            '1.0',
            '12345',
            MessageType.NOTE
        )
        hs2 = hashlib.new('sha256')
        h2.update_hash(hs2)
        self.assertNotEqual(hs1.digest(), hs2.digest())

    def test_hash_history_diff_source(self):
        h = HistoryEntry(
            'hello world',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test2',
            '1.0',
            '12345',
            MessageType.NOTE
        )
        hs1 = hashlib.new('sha256')
        h.update_hash(hs1)
        h2 = HistoryEntry(
            'hello world',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test',
            '1.0',
            '12345',
            MessageType.NOTE
        )
        hs2 = hashlib.new('sha256')
        h2.update_hash(hs2)
        self.assertNotEqual(hs1.digest(), hs2.digest())

    def test_hash_history_diff_version(self):
        h = HistoryEntry(
            'hello world',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test',
            '1.1',
            '12345',
            MessageType.NOTE
        )
        hs1 = hashlib.new('sha256')
        h.update_hash(hs1)
        h2 = HistoryEntry(
            'hello world',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test',
            '1.0',
            '12345',
            MessageType.NOTE
        )
        hs2 = hashlib.new('sha256')
        h2.update_hash(hs2)
        self.assertNotEqual(hs1.digest(), hs2.digest())

    def test_hash_history_diff_instance(self):
        h = HistoryEntry(
            'hello world',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test',
            '1.0',
            '123456',
            MessageType.NOTE
        )
        hs1 = hashlib.new('sha256')
        h.update_hash(hs1)
        h2 = HistoryEntry(
            'hello world',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test',
            '1.0',
            '12345',
            MessageType.NOTE
        )
        hs2 = hashlib.new('sha256')
        h2.update_hash(hs2)
        self.assertNotEqual(hs1.digest(), hs2.digest())

    def test_hash_history_diff_type(self):
        h = HistoryEntry(
            'hello world',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test',
            '1.0',
            '12345',
            MessageType.ERROR
        )
        hs1 = hashlib.new('sha256')
        h.update_hash(hs1)
        h2 = HistoryEntry(
            'hello world',
            datetime.datetime(2015, 1, 2, 3, 4, 5),
            'test',
            '1.0',
            '12345',
            MessageType.NOTE
        )
        hs2 = hashlib.new('sha256')
        h2.update_hash(hs2)
        self.assertNotEqual(hs1.digest(), hs2.digest())

class TestQCMessage(ut.TestCase):

    def test_normalize_blank_qc_path(self):
        self.assertEqual('', normalize_ocproc_path(None))

    def test_normalize_qc_path(self):
        self.assertEqual('metadata/Property/metadata/Quality', normalize_ocproc_path('/metadata/Property//metadata/Quality/'))

    def test_mapping(self):
        qcm = QCMessage('code', 'path//test', 'ref')
        self.assertEqual(qcm.code, 'code')
        self.assertEqual(qcm.record_path, 'path/test')
        self.assertEqual(qcm.ref_value, 'ref')
        map_ = qcm.to_mapping()
        self.assertEqual(map_, {
            '_code': 'code',
            '_path': 'path/test',
            '_ref': 'ref'
        })
        qcm2 = QCMessage.from_mapping(map_)
        self.assertEqual(qcm2.code, 'code')
        self.assertEqual(qcm2.record_path, 'path/test')
        self.assertEqual(qcm2.ref_value, 'ref')

    def test_hash_qc_message_code(self):
        qcm1 = QCMessage('hello', '', None)
        h1 = hashlib.new('sha256')
        qcm1.update_hash(h1)
        qcm2 = QCMessage('hello', '', None)
        h2 = hashlib.new('sha256')
        qcm2.update_hash(h2)
        self.assertEqual(h1.digest(), h2.digest())

    def test_hash_qc_message_full(self):
        qcm1 = QCMessage('hello', 'metadata/Property', '5')
        h1 = hashlib.new('sha256')
        qcm1.update_hash(h1)
        qcm2 = QCMessage('hello', 'metadata/Property', '5')
        h2 = hashlib.new('sha256')
        qcm2.update_hash(h2)
        self.assertEqual(h1.digest(), h2.digest())

    def test_hash_qc_message_diff_code(self):
        qcm1 = QCMessage('hello', 'metadata/Property', '5')
        h1 = hashlib.new('sha256')
        qcm1.update_hash(h1)
        qcm2 = QCMessage('hello2', 'metadata/Property', '5')
        h2 = hashlib.new('sha256')
        qcm2.update_hash(h2)
        self.assertNotEqual(h1.digest(), h2.digest())

    def test_hash_qc_message_diff_path(self):
        qcm1 = QCMessage('hello', 'metadata/Property', '5')
        h1 = hashlib.new('sha256')
        qcm1.update_hash(h1)
        qcm2 = QCMessage('hello', 'metadata/Property2', '5')
        h2 = hashlib.new('sha256')
        qcm2.update_hash(h2)
        self.assertNotEqual(h1.digest(), h2.digest())

    def test_hash_qc_message_diff_ref(self):
        qcm1 = QCMessage('hello', 'metadata/Property', '5')
        h1 = hashlib.new('sha256')
        qcm1.update_hash(h1)
        qcm2 = QCMessage('hello', 'metadata/Property', '6')
        h2 = hashlib.new('sha256')
        qcm2.update_hash(h2)
        self.assertNotEqual(h1.digest(), h2.digest())

    def test_hash_qc_message_diff_ref_null(self):
        qcm1 = QCMessage('hello', 'metadata/Property', '5')
        h1 = hashlib.new('sha256')
        qcm1.update_hash(h1)
        qcm2 = QCMessage('hello', 'metadata/Property', None)
        h2 = hashlib.new('sha256')
        qcm2.update_hash(h2)
        self.assertNotEqual(h1.digest(), h2.digest())


class TestTestRunInfo(ut.TestCase):

    def test_mapping(self):
        info = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS, [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        self.assertEqual(info.test_name, 'test')
        self.assertEqual(info.test_version, '1.0')
        self.assertEqual(info.test_date, '2015-01-02T03:04:05')
        self.assertIs(info.result, QCResult.PASS)
        self.assertEqual(1, len(info.messages))
        self.assertEqual('code', info.messages[0].code)
        self.assertEqual('notes', info.notes)
        self.assertFalse(info.is_stale)
        self.assertIn('gtspp1', info.test_tags)
        map_ = info.to_mapping()
        self.assertEqual(map_, {
            '_name': 'test',
            '_version': '1.0',
            '_date': '2015-01-02T03:04:05',
            '_messages': [
                {
                    '_code': 'code',
                    '_path': 'path',
                    '_ref': 'ref'
                }
            ],
            '_result': QCResult.PASS.value,
            '_notes': 'notes',
            '_stale': False,
            '_tags': ['gtspp1']
        })
        info2 = QCTestRunInfo.from_mapping(map_)
        self.assertEqual(info2.test_name, 'test')
        self.assertEqual(info2.test_version, '1.0')
        self.assertEqual(info2.test_date, '2015-01-02T03:04:05')
        self.assertIs(info2.result, QCResult.PASS)
        self.assertEqual(1, len(info2.messages))
        self.assertEqual('code', info2.messages[0].code)
        self.assertEqual('notes', info2.notes)
        self.assertFalse(info2.is_stale)
        self.assertIn('gtspp1', info2.test_tags)

    def test_hash(self):
        info = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS, [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h = hashlib.new('sha256')
        info.update_hash(h)
        info2 = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS, [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h2 = hashlib.new('sha256')
        info2.update_hash(h2)
        self.assertEqual(h.digest(), h2.digest())

    def test_hash_different_name(self):
        info = QCTestRunInfo('test2', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                             [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h = hashlib.new('sha256')
        info.update_hash(h)
        info2 = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                              [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h2 = hashlib.new('sha256')
        info2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_hash_different_version(self):
        info = QCTestRunInfo('test', '1.1', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                             [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h = hashlib.new('sha256')
        info.update_hash(h)
        info2 = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                              [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h2 = hashlib.new('sha256')
        info2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_hash_different_date(self):
        info = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 6), QCResult.PASS,
                             [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h = hashlib.new('sha256')
        info.update_hash(h)
        info2 = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                              [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h2 = hashlib.new('sha256')
        info2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_hash_different_result(self):
        info = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.FAIL,
                             [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h = hashlib.new('sha256')
        info.update_hash(h)
        info2 = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                              [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h2 = hashlib.new('sha256')
        info2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_hash_different_message(self):
        info = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                             [QCMessage('code2', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h = hashlib.new('sha256')
        info.update_hash(h)
        info2 = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                              [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h2 = hashlib.new('sha256')
        info2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_hash_more_messages(self):
        info = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                             [QCMessage('code2', 'path2', 'ref2'), QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h = hashlib.new('sha256')
        info.update_hash(h)
        info2 = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                              [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h2 = hashlib.new('sha256')
        info2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_hash_different_notes(self):
        info = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                             [QCMessage('code', 'path', 'ref')], 'notes2', False, ['gtspp1'])
        h = hashlib.new('sha256')
        info.update_hash(h)
        info2 = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                              [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h2 = hashlib.new('sha256')
        info2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_hash_different_stale(self):
        info = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                             [QCMessage('code', 'path', 'ref')], 'notes', True, ['gtspp1'])
        h = hashlib.new('sha256')
        info.update_hash(h)
        info2 = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                              [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h2 = hashlib.new('sha256')
        info2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_hash_different_tags(self):
        info = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                             [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp2'])
        h = hashlib.new('sha256')
        info.update_hash(h)
        info2 = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                              [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h2 = hashlib.new('sha256')
        info2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_hash_more_tags(self):
        info = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                             [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1', 'gtspp1.1'])
        h = hashlib.new('sha256')
        info.update_hash(h)
        info2 = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                              [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h2 = hashlib.new('sha256')
        info2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_hash_different_no_tags(self):
        info = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                             [QCMessage('code', 'path', 'ref')], 'notes', False)
        h = hashlib.new('sha256')
        info.update_hash(h)
        info2 = QCTestRunInfo('test', '1.0', datetime.datetime(2015, 1, 2, 3, 4, 5), QCResult.PASS,
                              [QCMessage('code', 'path', 'ref')], 'notes', False, ['gtspp1'])
        h2 = hashlib.new('sha256')
        info2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

