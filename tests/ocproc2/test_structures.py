import datetime
import hashlib
import unittest as ut

from cnodc.ocproc2 import BaseRecord, MultiElement, ParentRecord, QCTestRunInfo, QCResult, QCMessage, HistoryEntry, \
    MessageType, SingleElement, RecordSet, ChildRecord, RecordMap


class TestBaseRecord(ut.TestCase):

    def test_set_element_metadata(self):
        r = BaseRecord()
        r.set_element('metadata/Foo', 'Bar')
        self.assertEqual(r.metadata.best('Foo'), 'Bar')

    def test_set_element_coordinates(self):
        r = BaseRecord()
        r.set_element('coordinates/Foo', 'Bar')
        self.assertEqual(r.coordinates.best('Foo'), 'Bar')

    def test_set_element_parameters(self):
        r = BaseRecord()
        r.set_element('parameters/Foo', 'Bar')
        self.assertEqual(r.parameters.best('Foo'), 'Bar')

    def test_set_element_bad_group(self):
        r = BaseRecord()
        with self.assertRaises(ValueError):
            r.set_element('parameter/Foo', 'Bar')

    def test_add_element(self):
        r = BaseRecord()
        r.add_element('parameters/Foo', 'Bar')
        r.add_element('parameters/Foo', 'Bar2')
        self.assertIsInstance(r.parameters['Foo'], MultiElement)
        self.assertEqual(r.parameters['Foo'].value[0].value, 'Bar')
        self.assertEqual(r.parameters['Foo'].value[1].value, 'Bar2')

    def test_set_multiple(self):
        r = BaseRecord()
        r.set_multiple('parameters/Foo', ['Bar', 'Bar2'])
        self.assertIsInstance(r.parameters['Foo'], MultiElement)
        self.assertEqual(r.parameters['Foo'].value[0].value, 'Bar')
        self.assertEqual(r.parameters['Foo'].value[1].value, 'Bar2')

    def test_find_child(self):
        r = BaseRecord()
        r.metadata['Foo'] = 'Bar'
        sr = BaseRecord()
        r.subrecords.append_to_record_set('PROFILE', 0, sr)
        self.assertIs(r.find_child(''), r)
        self.assertIs(r.find_child('metadata'), r.metadata)
        self.assertIs(r.find_child('metadata/Foo'), r.metadata['Foo'])
        self.assertIs(r.find_child('subrecords/PROFILE/0/0'), sr)

    def test_subrecords(self):
        r = BaseRecord()
        sr1 = BaseRecord()
        sr1.coordinates['Record'] = 1
        r.subrecords.append_to_record_set('PROFILE', 0, sr1)
        sr2 = BaseRecord()
        sr2.coordinates['Record'] = 2
        r.subrecords.append_to_record_set('PROFILE', 0, sr2)
        sr2 = BaseRecord()
        sr2.coordinates['Record'] = 3
        r.subrecords.append_to_record_set('WAVE_SENSOR', 0, sr2)
        x = [sr.coordinates['Record'].value for sr in r.iter_subrecords()]
        self.assertEqual(3, len(x))
        x = [sr.coordinates['Record'].value for sr in r.iter_subrecords('PROFILE')]
        self.assertEqual(2, len(x))
        self.assertNotIn(3, x)
        x = [sr.coordinates['Record'].value for sr in r.iter_subrecords('WAVE_SENSOR')]
        self.assertEqual(1, len(x))
        self.assertNotIn(2, x)
        self.assertNotIn(1, x)

    def test_coordinate_hash(self):
        r = BaseRecord()
        r.coordinates['Record'] = 1
        h = hashlib.new('sha256')
        r.update_hash(h)
        r2 = BaseRecord()
        r2.coordinates['Record'] = 1
        h2 = hashlib.new('sha256')
        r2.update_hash(h2)
        self.assertEqual(h.digest(), h2.digest())

    def test_coordinate_hash_different(self):
        r = BaseRecord()
        r.coordinates['Record'] = 1
        h = hashlib.new('sha256')
        r.update_hash(h)
        r2 = BaseRecord()
        r2.coordinates['Record'] = 2
        h2 = hashlib.new('sha256')
        r2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_parameter_hash(self):
        r = BaseRecord()
        r.parameters['Record'] = 1
        h = hashlib.new('sha256')
        r.update_hash(h)
        r2 = BaseRecord()
        r2.parameters['Record'] = 1
        h2 = hashlib.new('sha256')
        r2.update_hash(h2)
        self.assertEqual(h.digest(), h2.digest())

    def test_parameters_hash_different(self):
        r = BaseRecord()
        r.parameters['Record'] = 1
        h = hashlib.new('sha256')
        r.update_hash(h)
        r2 = BaseRecord()
        r2.parameters['Record'] = 2
        h2 = hashlib.new('sha256')
        r2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_metadata_hash(self):
        r = BaseRecord()
        r.metadata['Record'] = 1
        h = hashlib.new('sha256')
        r.update_hash(h)
        r2 = BaseRecord()
        r2.metadata['Record'] = 1
        h2 = hashlib.new('sha256')
        r2.update_hash(h2)
        self.assertEqual(h.digest(), h2.digest())

    def test_metadata_hash_different(self):
        r = BaseRecord()
        r.metadata['Record'] = 1
        h = hashlib.new('sha256')
        r.update_hash(h)
        r2 = BaseRecord()
        r2.metadata['Record'] = 2
        h2 = hashlib.new('sha256')
        r2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_subrecord_hash(self):
        r = BaseRecord()
        sr = BaseRecord()
        sr.metadata['Foo'] = 'Bar'
        r.subrecords.append_to_record_set('PROFILE', 0, sr)
        h = hashlib.new('sha256')
        r.update_hash(h)
        r2 = BaseRecord()
        sr = BaseRecord()
        sr.metadata['Foo'] = 'Bar'
        r2.subrecords.append_to_record_set('PROFILE', 0, sr)
        h2 = hashlib.new('sha256')
        r2.update_hash(h2)
        self.assertEqual(h.digest(), h2.digest())

    def test_subrecord_different_hash(self):
        r = BaseRecord()
        sr = BaseRecord()
        sr.metadata['Foo'] = 'Bar'
        r.subrecords.append_to_record_set('PROFILE', 0, sr)
        h = hashlib.new('sha256')
        r.update_hash(h)
        r2 = BaseRecord()
        sr = BaseRecord()
        sr.metadata['Foo'] = 'Bar2'
        r.subrecords.append_to_record_set('PROFILE', 0, sr)
        h2 = hashlib.new('sha256')
        r2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())


class TestParentRecord(ut.TestCase):

    def test_record_qc_result(self):
        pr = ParentRecord()
        pr.record_qc_test_result(
            'test1',
            '1_0',
            QCResult.FAIL,
            [QCMessage('hello', 'metadata/Stuff')],
            'hello world',
            ['gtspp1']
        )
        self.assertEqual(1, len(pr.qc_tests))
        tr = pr.qc_tests[0]
        self.assertIsInstance(tr, QCTestRunInfo)
        self.assertEqual(tr.test_name, 'test1')
        self.assertEqual(tr.test_version, '1_0')
        self.assertEqual(tr.result, QCResult.FAIL)
        self.assertFalse(tr.is_stale)
        self.assertEqual(1, len(tr.messages))
        self.assertEqual(tr.messages[0].code, 'hello')
        self.assertEqual(tr.messages[0].record_path, 'metadata/Stuff')
        self.assertEqual(tr.notes, 'hello world')
        self.assertIn('gtspp1', tr.test_tags)

    def test_flag_result_stale(self):
        pr = ParentRecord()
        pr.record_qc_test_result(
            'test1',
            '1_0',
            QCResult.FAIL,
            [QCMessage('hello', 'metadata/Stuff')],
            'hello world',
            ['gtspp1']
        )
        self.assertEqual(1, len(pr.qc_tests))
        tr = pr.qc_tests[0]
        self.assertIsInstance(tr, QCTestRunInfo)
        self.assertFalse(tr.is_stale)
        pr.mark_test_results_stale('test2')
        self.assertFalse(tr.is_stale)
        pr.mark_test_results_stale('test1')
        self.assertTrue(tr.is_stale)

    def test_did_test_run(self):
        pr = ParentRecord()
        self.assertFalse(pr.test_already_run('test1'))
        pr.record_qc_test_result(
            'test1',
            '1_0',
            QCResult.FAIL,
            [QCMessage('hello', 'metadata/Stuff')],
            ''
        )
        self.assertTrue(pr.test_already_run('test1'))

    def test_latest_run(self):
        pr = ParentRecord()
        pr.record_qc_test_result('test1', '1_0', QCResult.FAIL, [], notes='3')
        pr.record_qc_test_result('test1', '1_0', QCResult.FAIL, [], notes='2')
        self.assertEqual(pr.latest_test_result('test1', False).notes, '2')
        pr.mark_test_results_stale('test1')
        self.assertEqual(pr.latest_test_result('test1', True).notes, '2')
        self.assertIsNone(pr.latest_test_result('test1', False))
        pr.record_qc_test_result('test1', '1_0', QCResult.PASS, [], notes='1')
        pr.record_qc_test_result('test2', '1_0', QCResult.PASS, [], notes='4')
        self.assertEqual(pr.latest_test_result('test1', True).notes, '1')
        self.assertEqual(pr.latest_test_result('test1', False).notes, '1')

    def test_did_stale_test_run(self):
        pr = ParentRecord()
        pr.record_qc_test_result(
            'test1',
            '1_0',
            QCResult.FAIL,
            [QCMessage('hello', 'metadata/Stuff')],
            ''
        )
        pr.mark_test_results_stale('test1')
        self.assertTrue(pr.test_already_run('test1', True))
        self.assertFalse(pr.test_already_run('test1', False))

    def test_hash_qc(self):
        pr = ParentRecord()
        pr.record_qc_test_result(
            'test1',
            '1_0',
            QCResult.FAIL,
            [QCMessage('hello', 'metadata/Stuff')],
            ''
        )
        pr2 = ParentRecord()
        pr2.record_qc_test_result(
            'test1',
            '1_0',
            QCResult.FAIL,
            [QCMessage('hello', 'metadata/Stuff')],
            ''
        )
        self.assertEqual(pr2.generate_hash(), pr.generate_hash())

    def test_hash_no_qc(self):
        pr = ParentRecord()
        pr.record_qc_test_result(
            'test1',
            '1_0',
            QCResult.PASS,
            [QCMessage('hello', 'metadata/Stuff')],
            ''
        )
        pr2 = ParentRecord()
        pr2.record_qc_test_result(
            'test1',
            '1_0',
            QCResult.FAIL,
            [QCMessage('hello', 'metadata/Stuff')],
            ''
        )
        self.assertNotEqual(pr2.generate_hash(), pr.generate_hash())

    def test_hash_history(self):
        pr = ParentRecord()
        pr.add_history_entry('Hello', 'foo', '1_0', 'foobar', change_time=datetime.datetime(2015, 1, 2, 3, 4, 5))
        pr2 = ParentRecord()
        pr2.add_history_entry('Hello', 'foo', '1_0', 'foobar', change_time=datetime.datetime(2015, 1, 2, 3, 4, 5))
        self.assertEqual(pr2.generate_hash(), pr.generate_hash())

    def test_hash_no_history(self):
        pr = ParentRecord()
        pr.add_history_entry('Hello', 'foo', '1_0', 'foobar', change_time=datetime.datetime(2015, 1, 2, 3, 4, 5))
        pr2 = ParentRecord()
        pr2.add_history_entry('Hello', 'foo2', '1_0', 'foobar', change_time=datetime.datetime(2015, 1, 2, 3, 4, 5))
        self.assertNotEqual(pr2.generate_hash(), pr.generate_hash())

    def test_record_note(self):
        pr = ParentRecord()
        pr.record_note('Hello', 'foo', '1', 'bar')
        self.assertEqual(1, len(pr.history))
        self.assertIsInstance(pr.history[0], HistoryEntry)
        self.assertEqual(pr.history[0].message, 'Hello')
        self.assertEqual(pr.history[0].source_name, 'foo')
        self.assertEqual(pr.history[0].source_version, '1')
        self.assertEqual(pr.history[0].source_instance, 'bar')
        self.assertIsNotNone(pr.history[0].timestamp)
        self.assertEqual(pr.history[0].message_type, MessageType.NOTE)

    def test_report_error(self):
        pr = ParentRecord()
        pr.report_error('Hello', 'foo', '1', 'bar')
        self.assertEqual(1, len(pr.history))
        self.assertIsInstance(pr.history[0], HistoryEntry)
        self.assertEqual(pr.history[0].message, 'Hello')
        self.assertEqual(pr.history[0].source_name, 'foo')
        self.assertEqual(pr.history[0].source_version, '1')
        self.assertEqual(pr.history[0].source_instance, 'bar')
        self.assertIsNotNone(pr.history[0].timestamp)
        self.assertEqual(pr.history[0].message_type, MessageType.ERROR)

    def test_report_warning(self):
        pr = ParentRecord()
        pr.report_warning('Hello', 'foo', '1', 'bar')
        self.assertEqual(1, len(pr.history))
        self.assertIsInstance(pr.history[0], HistoryEntry)
        self.assertEqual(pr.history[0].message, 'Hello')
        self.assertEqual(pr.history[0].source_name, 'foo')
        self.assertEqual(pr.history[0].source_version, '1')
        self.assertEqual(pr.history[0].source_instance, 'bar')
        self.assertIsNotNone(pr.history[0].timestamp)
        self.assertEqual(pr.history[0].message_type, MessageType.WARNING)

    def test_internal_note(self):
        pr = ParentRecord()
        pr.internal_note('Hello', 'foo', '1', 'bar')
        self.assertEqual(1, len(pr.history))
        self.assertIsInstance(pr.history[0], HistoryEntry)
        self.assertEqual(pr.history[0].message, 'Hello')
        self.assertEqual(pr.history[0].source_name, 'foo')
        self.assertEqual(pr.history[0].source_version, '1')
        self.assertEqual(pr.history[0].source_instance, 'bar')
        self.assertIsNotNone(pr.history[0].timestamp)
        self.assertEqual(pr.history[0].message_type, MessageType.INFO)

class TestRecordSet(ut.TestCase):

    def test_metadata(self):
        rs = RecordSet()
        rs.metadata['Foo'] = 'Bar'
        self.assertIsInstance(rs.metadata['Foo'], SingleElement)
        self.assertEqual(rs.metadata['Foo'].value, 'Bar')

    def test_metadata_hash(self):
        rs = RecordSet()
        rs.metadata['Foo'] = 'Bar'
        h = hashlib.new('sha256')
        rs.update_hash(h)
        rs2 = RecordSet()
        rs2.metadata['Foo'] = 'Bar'
        h2 = hashlib.new('sha256')
        rs2.update_hash(h2)
        self.assertEqual(h.digest(), h2.digest())

    def test_diff_metadata_hash(self):
        rs = RecordSet()
        rs.metadata['Foo'] = 'Bar'
        h = hashlib.new('sha256')
        rs.update_hash(h)
        rs2 = RecordSet()
        rs2.metadata['Foo'] = 'Bar2'
        h2 = hashlib.new('sha256')
        rs2.update_hash(h2)
        self.assertNotEqual(h.digest(), h2.digest())

    def test_to_mapping(self):
        rs = RecordSet()
        rs.metadata['Foo'] = 'Bar'
        self.assertEqual(rs.to_mapping(), {
            '_records': [],
            '_metadata': {
                'Foo': 'Bar'
            }
        })

    def test_from_mapping(self):
        rs = RecordSet()
        rs.from_mapping({
            '_records': [],
            '_metadata': {
                'Foo': 'Bar'
            }
        })
        self.assertIsInstance(rs.metadata['Foo'], SingleElement)
        self.assertEqual(rs.metadata['Foo'].value, 'Bar')

    def test_find_child(self):
        rs = RecordSet()
        rs.metadata['Foo'] = 'Bar'
        r = ChildRecord()
        rs.records.append(r)
        self.assertIs(rs.find_child([]), rs)
        self.assertIs(rs.find_child(['metadata']), rs.metadata)
        self.assertIs(rs.find_child(['metadata', 'Foo']), rs.metadata['Foo'])
        self.assertIsNone(rs.find_child(['metadata', 'Bar']))
        self.assertIsNone(rs.find_child(['hello']))
        self.assertIs(rs.find_child(['0']), r)
        self.assertIsNone(rs.find_child(['1']))
        self.assertIsNone(rs.find_child(['-1']))


class TestRecordMap(ut.TestCase):

    def test_iter_subrecords_bad(self):
        rm = RecordMap()
        x = [y for y in rm.iter_subrecords('foobar')]
        self.assertEqual(len(x), 0)

    def test_find_child(self):
        rm = RecordMap()
        cr = ChildRecord()
        rm.append_to_record_set('FOO', 0, cr)
        self.assertIs(rm.find_child([]), rm)
        self.assertIsNone(rm.find_child(['BAR']))
        self.assertIs(rm.find_child(['FOO']), rm.record_sets['FOO'])
        self.assertIs(rm.find_child(['FOO', '0']), rm.record_sets['FOO'][0])
        self.assertIs(rm.find_child(['FOO', '0', '0']), rm.record_sets['FOO'][0].records[0])
        self.assertIsNone(rm.find_child(['FOO', '1']))
        self.assertIsNone(rm.find_child(['FOO', '-1']))
        self.assertIsNone(rm.find_child(['FOO', 'BAR']))

    def test_build_submaps(self):
        rm = RecordMap()
        prof1 = rm.new_recordset('PROFILE')
        prof2 = rm.new_recordset('PROFILE')
        self.assertIsNot(prof1, prof2)
        self.assertIn(1, rm.record_sets['PROFILE'])

    def test_get(self):
        rm = RecordMap()
        prof1 = rm.new_recordset('PROFILE')
        self.assertIsInstance(rm.get('PROFILE', 0), RecordSet)
        self.assertIsNone(rm.get('PROFILE', 1))
        self.assertIsNone(rm.get('FOOBAR', 0))

    def test_set(self):
        rm = RecordMap()
        rs = RecordSet()
        rm.set('PROFILE', 0, rs)
        self.assertIn('PROFILE', rm.record_sets)
        self.assertIn(0, rm.record_sets['PROFILE'])
        self.assertIs(rs, rm.record_sets['PROFILE'][0])

    def test_set_many(self):
        rm = RecordMap()
        rs = RecordSet()
        rs2 = RecordSet()
        rm.set('PROFILE', 0, rs)
        rm.set('PROFILE', 1, rs2)
        self.assertIn('PROFILE', rm.record_sets)
        self.assertIn(0, rm.record_sets['PROFILE'])
        self.assertIs(rs, rm.record_sets['PROFILE'][0])
        self.assertIn(1, rm.record_sets['PROFILE'])
        self.assertIs(rs2, rm.record_sets['PROFILE'][1])
