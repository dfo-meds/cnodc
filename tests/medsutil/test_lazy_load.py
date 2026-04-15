import unittest as ut

from medsutil.lazy_load import LazyLoadDict, LazyLoadList


class BasicInt:

    def __init__(self, i):
        self._int = i

    def __int__(self):
        return int(self._int)

    def to_int(self):
        return int(self._int)

    def to_mapping(self):
        return str(self._int)

class TestLazyLoadDict(ut.TestCase):

    def test_basics(self):
        lld = LazyLoadDict[BasicInt](BasicInt)
        lld.from_mapping({
            'one': '1',
            'two': '2',
            'three': '3',
            'four': 4,
            'five': 5
        })
        self.assertEqual(5, len(lld))
        self.assertTrue(all(not lld._loaded[x] for x in ('one', 'two', 'three', 'four', 'five')))
        self.assertIn('two', lld)
        two = lld['two']
        self.assertIsInstance(two, BasicInt)
        self.assertEqual(two.to_int(), 2)
        self.assertTrue(lld._loaded['two'])
        lld.clear()
        self.assertEqual(0, len(lld))
        self.assertFalse(lld._dict)
        self.assertFalse(lld._loaded)

    def test_del(self):
        lld = LazyLoadDict[BasicInt](BasicInt)
        lld['1'] = BasicInt('1')
        self.assertIn('1', lld)
        del lld['1']
        self.assertNotIn('1', lld)
        self.assertNotIn('1', lld._loaded)
        self.assertNotIn('1', lld._dict)
        with self.assertRaises(KeyError):
            del lld['1']

    def test_not_equal_other_obj(self):
        lld = LazyLoadDict[BasicInt](BasicInt)
        self.assertNotEqual(lld, self)

    def test_bool(self):
        lld = LazyLoadDict[BasicInt](BasicInt)
        self.assertFalse(lld)
        lld['X'] = BasicInt('1')
        self.assertTrue(lld)

    def test_set(self):
        lld = LazyLoadDict[BasicInt](BasicInt)
        lld.set('1', BasicInt('1'))
        self.assertIsInstance(lld['1'], BasicInt)

    def test_get(self):
        lld = LazyLoadDict[BasicInt](BasicInt)
        bi = BasicInt('1')
        lld.set('1', bi)
        self.assertIs(lld.get('1'), bi)
        self.assertIsNone(lld.get('2'))

    def test_iter(self):
        lld = LazyLoadDict[BasicInt](BasicInt)
        lld['1'] = BasicInt('1')
        lld['2'] = BasicInt('2')
        lld['3'] = BasicInt('3')
        keys = [x for x in lld]
        self.assertEqual(keys, ['1', '2', '3'])


class TestLazyLoadList(ut.TestCase):

    def test_set_del_item(self):
        lll = LazyLoadList[BasicInt](BasicInt)
        lll.append(BasicInt("1"))
        lll.append(BasicInt("3"))
        lll.append(BasicInt("4"))
        lll[0] = BasicInt("2")
        self.assertEqual(lll[0].to_int(), 2)
        self.assertEqual(len(lll), 3)
        del lll[0]
        self.assertEqual(lll[0].to_int(), 3)
        self.assertEqual(len(lll), 2)

    def test_clear(self):
        lll = LazyLoadList[BasicInt](BasicInt)
        lll.append(BasicInt("1"))
        lll.append(BasicInt("3"))
        lll.append(BasicInt("4"))
        self.assertEqual(len(lll), 3)
        lll.clear()
        self.assertEqual(len(lll), 0)
        self.assertFalse(lll._loaded)
        self.assertFalse(lll._list)

    def test_insert(self):
        lll = LazyLoadList[BasicInt](BasicInt)
        lll.append(BasicInt("1"))
        lll.append(BasicInt("3"))
        lll.insert(1, BasicInt("2"))
        self.assertEqual(len(lll), 3)
        self.assertEqual(lll[0].to_int(), 1)
        self.assertEqual(lll[1].to_int(), 2)
        self.assertEqual(lll[2].to_int(), 3)

    def test_extend(self):
        lll = LazyLoadList[BasicInt](BasicInt)
        lll.extend([
            BasicInt("1"), BasicInt("2"), BasicInt("3")
        ])
        self.assertEqual(len(lll), 3)
        self.assertEqual(lll[0].to_int(), 1)
        self.assertEqual(lll[1].to_int(), 2)
        self.assertEqual(lll[2].to_int(), 3)

    def test_to_mapping(self):
        lll = LazyLoadList[BasicInt](BasicInt)
        lll._loaded = [True, False, True]
        lll._list = [BasicInt("1"), "2", BasicInt("3")]
        self.assertEqual(lll.to_mapping(), ["1", "2", "3"])

