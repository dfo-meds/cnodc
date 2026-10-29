import unittest as ut

from medsutil.lazy_load import LazyLoadDict, LazyLoadList


class IntegerDictionary(LazyLoadDict[str, str, int]):

    def _load_item(self, item: str) -> int:
        return int(item)

    def _unload_item(self, item: int) -> str:
        return str(item)


class TestLazyLoadDict(ut.TestCase):

    def test_basics(self):
        lld = IntegerDictionary()
        lld.from_mapping({
            'one': '1',
            'two': '2',
            'three': '3',
            'four': '4',
            'five': '5'
        })
        self.assertEqual(5, len(lld))
        self.assertTrue(all(not lld._loaded[x] for x in ('one', 'two', 'three', 'four', 'five')))
        self.assertIn('two', lld)
        two = lld['two']
        self.assertIsInstance(two, int)
        self.assertTrue(lld._loaded['two'])
        lld.clear()
        self.assertEqual(0, len(lld))
        self.assertFalse(lld._dict)
        self.assertFalse(lld._loaded)

    def test_del(self):
        lld = IntegerDictionary()
        lld['1'] = 1
        self.assertIn('1', lld)
        del lld['1']
        self.assertNotIn('1', lld)
        self.assertNotIn('1', lld._loaded)
        self.assertNotIn('1', lld._dict)
        with self.assertRaises(KeyError):
            del lld['1']

    def test_not_equal_other_obj(self):
        lld = IntegerDictionary()
        self.assertNotEqual(lld, self)

    def test_bool(self):
        lld = IntegerDictionary()
        self.assertFalse(lld)
        lld['X'] = 1
        self.assertTrue(lld)

    def test_set(self):
        lld = IntegerDictionary()
        lld.set('1', 1)
        self.assertIsInstance(lld['1'], int)

    def test_get(self):
        lld = IntegerDictionary()
        bi = 1
        lld.set('1', bi)
        self.assertIs(lld.get('1'), bi)
        self.assertIsNone(lld.get('2'))

    def test_iter(self):
        lld = IntegerDictionary()
        lld['1'] = 1
        lld['2'] = 2
        lld['3'] = 3
        keys = [x for x in lld]
        self.assertEqual(keys, ['1', '2', '3'])


class IntegerList(LazyLoadList[str, int]):

    def _load_item(self, item: str) -> int:
        return int(item)

    def _unload_item(self, item: int) -> str:
        return str(item)


class TestLazyLoadList(ut.TestCase):

    def test_set_del_item(self):
        lll = IntegerList()
        lll.append(1)
        lll.append(3)
        lll.append(4)
        lll[0] = 2
        self.assertEqual(lll[0], 2)
        self.assertEqual(len(lll), 3)
        del lll[0]
        self.assertEqual(lll[0], 3)
        self.assertEqual(len(lll), 2)

    def test_clear(self):
        lll = IntegerList()
        lll.append(1)
        lll.append(3)
        lll.append(4)
        self.assertEqual(len(lll), 3)
        lll.clear()
        self.assertEqual(len(lll), 0)
        self.assertFalse(lll._loaded)
        self.assertFalse(lll._list)

    def test_insert(self):
        lll = IntegerList()
        lll.append(1)
        lll.append(3)
        lll.insert(1, 2)
        self.assertEqual(len(lll), 3)
        self.assertEqual(lll[0], 1)
        self.assertEqual(lll[1], 2)
        self.assertEqual(lll[2], 3)

    def test_extend(self):
        lll = IntegerList()
        lll.extend([
            1,2,3
        ])
        self.assertEqual(len(lll), 3)
        self.assertEqual(lll[0], 1)
        self.assertEqual(lll[1], 2)
        self.assertEqual(lll[2], 3)

    def test_to_mapping(self):
        lll = IntegerList()
        lll._loaded = [True, False, True]
        lll._list = [1, "2", 3]
        self.assertEqual(lll.to_mapping(), ["1", "2", "3"])
