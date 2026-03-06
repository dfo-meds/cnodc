from cnodc.nodb import NODBUploadWorkflow
from cnodc.nodb.base import NODBValidationError
from core import BaseTestCase


class TestWorkflow(BaseTestCase):

    def test_configuration(self):
        wf = NODBUploadWorkflow()
        with wf._readonly_access():
            wf.configuration = {
                'hello': 'bar'
            }
        self.assertEqual(wf.get_config('hello'), 'bar')
        self.assertIsNone(wf.get_config('foo'))

    def test_find_by_name(self):
        wf = NODBUploadWorkflow(workflow_name='foobar')
        self.db.insert_object(wf)
        wf2 = NODBUploadWorkflow.find_by_name(self.db, 'foobar')
        self.assertIs(wf, wf2)

    def test_default_no_access(self):
        wf = NODBUploadWorkflow()
        with wf._readonly_access():
            wf.configuration = {}
        self.assertFalse(wf.check_access([]))

    def test_admin_override(self):
        wf = NODBUploadWorkflow()
        with wf._readonly_access():
            wf.configuration = {}
        self.assertTrue(wf.check_access(['__admin__']))

    def test_open_override(self):
        wf = NODBUploadWorkflow()
        with wf._readonly_access():
            wf.configuration = {
                'permissions': ['__any__']
            }
        self.assertTrue(wf.check_access([]))

    def test_has_perm(self):
        wf = NODBUploadWorkflow()
        with wf._readonly_access():
            wf.configuration = {
                'permissions': ['foobar', 'monkey']
            }
        self.assertTrue(wf.check_access(['foobar', 'ape']))

    def test_no_has_perm(self):
        wf = NODBUploadWorkflow()
        with wf._readonly_access():
            wf.configuration = {
                'permissions': ['foobar', 'monkey']
            }
        self.assertFalse(wf.check_access(['barfoo', 'ape']))

    def test_build_ordered_processing_steps(self):
        tests = [
            ({}, []),
            ({'1': {'order': 1, 'name': '1'}}, ['1']),
            ({'1': {'order': 1, 'name': '1'}, '2': {'order': 2, 'name': '2'}}, ['1', '2']),
            ({'1': {'order': 2, 'name': '1'}, '2': {'order': 1, 'name': '2'}}, ['2', '1']),
        ]
        for data, result in tests:
            with self.subTest():
                wf = NODBUploadWorkflow()
                with wf._readonly_access():
                    wf.configuration = {'processing_steps': data}
                self.assertEqual(result, wf.ordered_processing_steps())
                self.assertEqual(result, wf.build_ordered_processing_steps(data))

    def test_blank_steps(self):
        wf = NODBUploadWorkflow()
        with wf._readonly_access():
            self.assertEqual(0, len(wf.ordered_processing_steps()))
            wf.configuration = {}
            self.assertEqual(0, len(wf.ordered_processing_steps()))
            wf.configuration['processing_steps'] = {}
            self.assertEqual(0, len(wf.ordered_processing_steps()))

    def test_change_step_order(self):
        base_config = {
            'label': {'und': 'test'},
            'working_target': {'directory': str(self.temp_dir)}
        }
        tests = [
            ({}, {'1': {'order': 1, 'name': '1'}}, None, "add new step"),
            ({'1': {'order': 1, 'name': '1'}}, {'1': {'order': 1, 'name': '1'}}, None, "no changes, one step"),
            ({'1': {'order': 1, 'name': '1'}}, {'1': {'order': 1, 'name': '1'}, '2': {'order': 2, 'name': '2'}}, None, "add step to existing"),
            ({'1': {'order': 1, 'name': '1'}, '2': {'order': 2, 'name': '2'}}, {'2': {'order': 2, 'name': '2'}, '1': {'order': 1, 'name': '1'}}, None, "add step to existing, change order" ),
            ({'1': {'order': 1, 'name': '1'}, '2': {'order': 2, 'name': '2'}}, {'2': {'order': 2, 'name': '2'}, '1': {'order': 1, 'name': '1'}}, None, "add step to existing, change order" ),
            ({'1': {'order': 1, 'name': '1'}, '2': {'order': 2, 'name': '2'}}, {'2': {'order': 2, 'name': '2'}}, 'NODB-VALIDATION-2020', "cannot remove step"),
            ({'1': {'order': 1, 'name': '1'}, '2': {'order': 2, 'name': '2'}, '3': {'order': 3, 'name': '3'}}, {'1': {'order': 1, 'name': '1'}, '2': {'order': 2, 'name': '2'}, '3': {'order': 0, 'name': '3'}}, 'NODB-VALIDATION-2016', "cannot move step before"),
            ({'1': {'order': 1, 'name': '1'}, '2': {'order': 2, 'name': '2'}, '3': {'order': 3, 'name': '3'}}, {'1': {'order': 1, 'name': '1'}, '2': {'order': 4, 'name': '2'}, '3': {'order': 3, 'name': '3'}}, 'NODB-VALIDATION-2016', "cannot move step after"),
        ]
        for old_steps, new_steps, exc, msg in tests:
            old_config = base_config.copy()
            old_config['processing_steps'] = old_steps
            new_config = base_config.copy()
            new_config['processing_steps'] = new_steps
            with self.subTest(msg=msg):
                wf = NODBUploadWorkflow()
                with wf._readonly_access():
                    wf.configuration = old_config
                if exc is not None:
                    with self.assertRaises(NODBValidationError) as h:
                        wf.set_config(new_config)
                    self.assertEqual(h.exception.internal_code, exc)
                else:
                    wf.set_config(new_config)

    def test_config_problems(self):
        tests = [
            ({}, 'NODB-VALIDATION-2020', 'missing label'),
            ({'label': 'foobar'}, 'NODB-VALIDATION-2021', 'label as str'),
            ({'label': {'en': 'foo'}}, 'NODB-VALIDATION-2023', 'only english label'),
            ({'label': {'fr': 'foo'}}, 'NODB-VALIDATION-2022', 'only french label'),
            ({'label': {'und': 'foobar'}}, 'NODB-VALIDATION-2002', 'good label (und)'),
            ({'label': {'en': 'foo', 'fr': 'bar'}}, 'NODB-VALIDATION-2002', 'good label (both)'),
            ({'label': {'und': '2'}, 'validation': 'autoinject.injector'}, 'NODB-VALIDATION-2000', 'require callable validation'),
            ({'label': {'und': '2'}, 'validation': 'tests.core.DatabaseMockFunny'}, 'NODB-VALIDATION-2001', 'require validation callable to exist'),
            ({'label': {'und': '2'}, 'working_target': {}, 'additional_targets': []}, 'NODB-VALIDATION-2002', 'no targets specified'),
            ({'label': {'und': '2'}, 'working_target': {}, 'additional_targets': {}}, 'NODB-VALIDATION-2002', 'no targets specified but dict'),
            ({'label': {'und': '2'}, 'working_target': 5}, 'NODB-VALIDATION-2026', 'bad working_target'),
            ({'label': {'und': '2'}, 'working_target': {'tier': 'foo'}}, 'NODB-VALIDATION-2007', 'missing directory for working_target'),
            ({'label': {'und': '2'}, 'working_target': {'directory': 'http://foo/bar.html'}}, 'NODB-VALIDATION-2008', 'invalid file handle path'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir), 'allow_overwrite': 'foobar'}}, 'NODB-VALIDATION-2009', 'invalid allow_overwrite setting'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir), 'allow_overwrite': 'user'}}, None, 'good allow overwrite [user]'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir), 'allow_overwrite': 'always'}}, None, 'good allow overwrite [always]'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir), 'allow_overwrite': 'never'}}, None, 'good allow overwrite [never]'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir), 'tier': 'bad'}}, 'NODB-VALIDATION-2006', 'bad tier'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir), 'tier': 'frequent'}}, None, 'good tier [frequent]'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir), 'tier': 'infrequent'}}, None, 'good tier [infrequent]'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir), 'tier': 'archival'}}, None, 'good tier [archival]'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir), 'metadata': 'foobar'}}, 'NODB-VALIDATION-2005', 'bad metadata for working'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir), 'metadata': {5: 'foobar'}}}, 'NODB-VALIDATION-2004', 'bad metadata key [must be str, is int]'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir), 'metadata': {'foobar': 5}}}, 'NODB-VALIDATION-2003', 'bad metadata value [must be str, is int]'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir), 'metadata': {'foobar': 'foobar'}}}, None, 'good metadata for working'),
            ({'label': {'und': '2'}, 'additional_targets': 5}, 'NODB-VALIDATION-2027', 'bad additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [5]}, 'NODB-VALIDATION-2026', 'bad additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [{'tier': 'frequent'}]}, 'NODB-VALIDATION-2007', 'missing directory for additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [{'directory': 'http://foo/bar.html'}]}, 'NODB-VALIDATION-2008', 'invalid file handle path for additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [{'directory': str(self.temp_dir), 'allow_overwrite': 'foobar'}]}, 'NODB-VALIDATION-2009', 'invalid allow_overwrite setting for additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [{'directory': str(self.temp_dir), 'allow_overwrite': 'user'}]}, None, 'good allow overwrite [user] for additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [{'directory': str(self.temp_dir), 'allow_overwrite': 'always'}]}, None, 'good allow overwrite [always] for additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [{'directory': str(self.temp_dir), 'allow_overwrite': 'never'}]}, None, 'good allow overwrite [never] for additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [{'directory': str(self.temp_dir), 'tier': '12345'}]}, 'NODB-VALIDATION-2006', 'bad for additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [{'directory': str(self.temp_dir), 'tier': 'frequent'}]}, None, 'good tier [frequent] for additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [{'directory': str(self.temp_dir), 'tier': 'infrequent'}]}, None, 'good tier [infrequent] for additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [{'directory': str(self.temp_dir), 'tier': 'archival'}]}, None, 'good tier [archival] for additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [{'directory': str(self.temp_dir), 'metadata': 'foobar'}]}, 'NODB-VALIDATION-2005', 'bad metadata for additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [{'directory': str(self.temp_dir), 'metadata': {5: 'foobar'}}]}, 'NODB-VALIDATION-2004', 'bad metadata key [must be str, is int] for additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': [{'directory': str(self.temp_dir), 'metadata': {'foobar': 5}}]}, 'NODB-VALIDATION-2003', 'bad metadata value [must be str, is int] for additional_target[0]'),
            ({'label': {'und': '2'}, 'additional_targets': {"one": {'directory': str(self.temp_dir), 'metadata': {'foobar': 'foobar'}}}}, None, 'good metadata for additional_target[0]'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'processing_steps': 'foobar'}, 'NODB-VALIDATION-2010', 'bad processing steps'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'processing_steps': {'key': 'bar'}}, 'NODB-VALIDATION-2011', 'step must be a dict'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'processing_steps': {'key': {'foo': 'bar'}}}, 'NODB-VALIDATION-2012', 'step must have an order'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'processing_steps': {'key': {'order': 'bar'}}}, 'NODB-VALIDATION-2013', 'order must be an int'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'processing_steps': {'key': {'order': 1, 'name': '1'}, 'key2': {'order': 1, 'name': '2'}}}, 'NODB-VALIDATION-2017', 'duplicate orders'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'processing_steps': {'key': {'order': 2}}}, 'NODB-VALIDATION-2014', 'step must have a name'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'processing_steps': {'key': {'order': 2, 'name': ''}}}, 'NODB-VALIDATION-2014', 'name cannot be blank'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'processing_steps': {'key': {'order': 2, 'name': 'foobar', 'priority': '1e13qwewsd'}}}, 'NODB-VALIDATION-2015', 'priority must be a number'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'processing_steps': {'key': {'order': 2, 'name': 'foobar', 'priority': '5'}}}, None, 'good step definition'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'filename_pattern': 4}, 'NODB-VALIDATION-2018', 'bad filename pattern'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'filename_pattern': 'test.txt'}, None, 'good filename pattern, bad metadata'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'filename_pattern': 'test.txt', 'default_metadata': 'foobar'}, 'NODB-VALIDATION-2019', 'bad metadata'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'filename_pattern': 'test.txt', 'default_metadata': {'six': 'seven'}}, None, 'good metadata'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'filename_pattern': 'test.txt', 'permissions': 'foobar'}, 'NODB-VALIDATION-2024', 'permissions must be list'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'filename_pattern': 'test.txt', 'permissions': [1, 2]}, 'NODB-VALIDATION-2025', 'permissions must be str'),
            ({'label': {'und': '2'}, 'working_target': {'directory': str(self.temp_dir)}, 'filename_pattern': 'test.txt', 'permissions': ["foobar"]}, None, "good permissions"),
        ]
        wf = NODBUploadWorkflow()
        for config, error_key, msg in tests:
            with self.subTest(msg=msg):
                if error_key is not None:
                    with self.assertRaises(NODBValidationError) as h:
                        wf.set_config(config)
                    self.assertEqual(h.exception.internal_code, error_key)
                else:
                    wf.set_config(config)
                    wf.check_config()
