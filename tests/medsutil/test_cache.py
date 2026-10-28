from medsutil.awaretime import AwareDateTime
from medsutil.cached import LeastRecentCache, CachedObjectMixin
from tests.helpers.base_test_case import BaseTestCase


class TestCacheMixin(BaseTestCase):

    def test_from_cache_only(self):
        c = CachedObjectMixin()
        self.assertIsNone(c._from_cache_only('five'))
        self.assertIsNone(c._from_cache_only('five', (5, 1)))
        self.assertIsNone(c._from_cache_only('five', [5, 1]))

    def test_set_with_parameters(self):
        c = CachedObjectMixin()
        self.assertIsNone(c._from_cache_only('five', (1,)))
        c._set_cache('five', 5, (2,))
        self.assertIsNone(c._from_cache_only('five', (1,)))
        c._set_cache('five', 5, (1,))
        self.assertEqual(c._from_cache_only('five', (1,)), 5)


class TestLRUCache(BaseTestCase):

    def test_set_get_contains(self):
        x = LeastRecentCache(5)
        self.assertFalse('5' in x)
        x['5'] = 5
        self.assertTrue('5' in x)
        self.assertEqual(x['5'], 5)
        self.assertEqual(x.get('5'), 5)

    def test_set_clear(self):
        x = LeastRecentCache(5)
        self.assertFalse('5' in x)
        x.set('5', 5)
        self.assertTrue('5' in x)
        x.clear()
        self.assertFalse('5' in x)

    def test_get_default(self):
        x = LeastRecentCache(5)
        self.assertEqual(x.get('5', 'five'), 'five')
        x['5'] = 5
        self.assertEqual(x.get('5', 'five'), 5)

    def test_with_cache(self):
        def _make_five():
            return 5
        def _make_six():
            return 6
        x = LeastRecentCache(5)
        self.assertEqual(x.with_cache('five', _make_five), 5)
        self.assertIn('five', x._values)
        self.assertEqual(x.with_cache('five', _make_six), 5)

    def test_prune_cache(self):
        x = LeastRecentCache(2)
        x._values['five'] = 5
        x._value_used_times['five'] = AwareDateTime(2015, 1, 2, 3, 4, 7)
        x._values['six'] = 6
        x._value_used_times['six'] = AwareDateTime(2015, 1, 2, 3, 4, 5)
        x._values['seven'] = 7
        x._value_used_times['seven'] = AwareDateTime(2015, 1, 2, 3, 4, 6)
        x.prune_cache()
        self.assertIn('five', x._values)
        self.assertIn('seven', x._values)
        self.assertNotIn('six', x._values)