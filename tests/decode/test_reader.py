import datetime
import unittest as ut
from cnodc.codecs.base import ByteSequenceReader
import zirconium as zr
from autoinject import injector

from cnodc.util import CNODCError


class ByteSequenceTests(ut.TestCase):

    def test_consume_all(self):
        stream = ByteSequenceReader([b'abcdef', b'ghijkl', b'mnopqrs', b'tuvwxyz'])
        output_ = stream.consume_all()
        self.assertEqual(output_, b'abcdefghijklmnopqrstuvwxyz')
        self.assertTrue(stream.at_eof())
        self.assertEqual(stream.offset(), 26)

    def test_consume_until(self):
        stream = ByteSequenceReader([b'abcde', b'fghij', b'klmno', b'pqrst', b'uvwxy', b'z'])
        data1 = stream.consume_until(b'a')
        self.assertEqual(data1, b'')
        self.assertEqual(stream.offset(), 0)
        self.assertEqual(stream[0], b'a')
        self.assertFalse(stream.at_eof())
        data2 = stream.consume_until(b'g')
        self.assertEqual(data2, b'abcdef')
        self.assertEqual(stream.offset(), 6)
        self.assertEqual(stream[0], b'g')
        self.assertFalse(stream.at_eof())
        data3 = stream.consume_until(b'jkl')
        self.assertEqual(data3, b'ghi')
        self.assertEqual(stream.offset(), 9)
        self.assertFalse(stream.at_eof())
        data4 = stream.consume_until(b'op', True)
        self.assertEqual(data4, b'jklmnop')
        self.assertEqual(stream.offset(), 16)
        self.assertFalse(stream.at_eof())
        data5 = stream.consume_until(b'a')
        self.assertEqual(data5, b'qrstuvwxyz')
        self.assertEqual(stream.offset(), 26)
        self.assertTrue(stream.at_eof())

    def test_consume_until_options(self):
        stream = ByteSequenceReader([b'abcde', b'fghij', b'klmno', b'pqrst', b'uvwxy', b'z'])
        data1 = stream.consume_until([b'de', b'df', b'ef', b'cd'])
        self.assertEqual(data1, b'ab')
        self.assertEqual(stream.offset(), 2)
        self.assertEqual(stream[0], b'c')
        self.assertFalse(stream.at_eof())

    def test_consume_linux_lines(self):
        stream = ByteSequenceReader([b"line1\n", b"line2\n", b"line3\nline4", b"\n\nline5"])
        lines = [x for x in stream.consume_lines()]
        self.assertEqual(len(lines), 6)
        self.assertEqual(lines[0], b"line1")
        self.assertEqual(lines[1], b"line2")
        self.assertEqual(lines[2], b"line3")
        self.assertEqual(lines[3], b"line4")
        self.assertEqual(lines[4], b"")
        self.assertEqual(lines[5], b"line5")
        self.assertTrue(stream.at_eof())

    def test_consume_windows_lines(self):
        stream = ByteSequenceReader([b"line1\r\n", b"line2\r\n", b"line3\r\nline4", b"\r\n\r\nline5"])
        lines = [x for x in stream.consume_lines()]
        self.assertEqual(len(lines), 6)
        self.assertEqual(lines[0], b"line1")
        self.assertEqual(lines[1], b"line2")
        self.assertEqual(lines[2], b"line3")
        self.assertEqual(lines[3], b"line4")
        self.assertEqual(lines[4], b"")
        self.assertEqual(lines[5], b"line5")
        self.assertTrue(stream.at_eof())

    def test_consume_mac_lines(self):
        stream = ByteSequenceReader([b"line1\r", b"line2\r", b"line3\rline4", b"\r\rline5"])
        lines = [x for x in stream.consume_lines()]
        self.assertEqual(len(lines), 6)
        self.assertEqual(lines[0], b"line1")
        self.assertEqual(lines[1], b"line2")
        self.assertEqual(lines[2], b"line3")
        self.assertEqual(lines[3], b"line4")
        self.assertEqual(lines[4], b"")
        self.assertEqual(lines[5], b"line5")
        self.assertTrue(stream.at_eof())


