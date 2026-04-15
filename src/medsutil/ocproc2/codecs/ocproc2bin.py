import typing as t
import medsutil.ocproc2 as ocproc2
from medsutil.ocproc2 import ParentRecord
from medsutil.ocproc2.codecs.base import BaseCodec
from medsutil.byteseq import ByteSequenceReader
from medsutil import types as ct
from medsutil.types import ByteStrings
from pipeman.exceptions import CNODCError
from medsutil.vlq import vlq_encode
import lzma
import zlib
import bz2

class StreamWrapper:

    def wrap_stream(self, stream: ByteStrings) -> ByteStrings:
        yield from stream  # pragma: no coverage

    def unwrap_stream(self, stream: ByteStrings) -> ByteStrings:
        yield from stream  # pragma: no coverage


class OCProc2BinCodec(BaseCodec):

    FILE_EXTENSION = '.ocp2'

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            log_name="cnodc.codecs.bin",
            is_encoder=True,
            is_decoder=True,
            default_options={
                'codec': 'PICKLE',
                'compression': None,
                'correction': None,
            },
            **kwargs
        )

    def _encode_records(self, data: t.Iterable[ocproc2.ParentRecord], options: dict) -> ByteStrings:
        options['_codec'] = self._get_codec(options['codec'])
        wrappers = self._get_wrappers(options['compression'], options['correction'])
        yield from self._make_header(options)
        out_stream = self._all_bytes(data, options)
        for wrapper in wrappers:
            out_stream = wrapper.wrap_stream(out_stream)
        yield from out_stream

    def _make_header(self, options: dict) -> ByteStrings:
        s = f"{options['codec']},{options['compression'] or ''},{options['correction'] or ''}"
        if len(s) > 65000:
            raise CNODCError(f'Header string is too long', 'OCPROC2BIN', 1006)
        yield len(s).to_bytes(2, 'little', signed=False)
        yield s.encode('ascii')

    def _all_bytes(self, data: t.Iterable[ocproc2.ParentRecord], options: dict) -> ByteStrings:
        for x in self._encode_record_wrapper(data, options):
            yield from self._encode_record_data_for_file(x, options)

    def _encode_single_record(self, record: ParentRecord, options: dict) -> ct.ByteStrings:
        yield from options['_codec']._encode_single_record(record, options)

    def _encode_record_data_for_file(self, record_data: ByteStrings, options: dict):
        ba = bytearray()
        for bytes_ in record_data:
            ba.extend(bytes_)
        yield vlq_encode(len(ba))
        yield ba

    def _parse_into_messages(self,
                            data: ByteStrings,
                            options: dict) -> ByteStrings:
        stream = ByteSequenceReader(data)
        header = self._parse_header(stream)
        options['_codec'] = self._get_codec(header[0])
        data = stream.iterate_rest()
        for wrapper in reversed(self._get_wrappers(*header[1:])):
            data = wrapper.unwrap_stream(data)
        stream = self._as_byte_sequence(data)
        while not stream.at_eof():
            yield stream.consume(stream.consume_vlq_int())

    def _parse_header(self, stream: ByteSequenceReader):
        leading_bytes = stream.consume(2)
        if not len(leading_bytes) == 2:
            raise CNODCError(f"Invalid format for OCPROC2BIN [min length<2]", "OCPROC2BIN", 1007)
        header_length = int.from_bytes(leading_bytes, 'little', signed=False)
        header_bytes = stream.consume(header_length)
        if len(header_bytes) < header_length:
            raise CNODCError(f"Invalid format for OCPROC2BIN [header wrong length]", "OCPROC2BIN", 1008)
        try:
            header_str = header_bytes.decode('ascii')
        except UnicodeDecodeError as ex:
            raise CNODCError(f"Invalid header format for OCPROC2BIN [header not ASCII]", "OCPROC2BIN", 1009) from ex
        comma_count = header_str.count(',')
        if comma_count != 2:
            raise CNODCError(f"Invalid format for OCPROC2BIN [header wrong number of elements]", "OCPROC2BIN", 1010)
        return header_str.split(',', maxsplit=2)

    def _decode_single_message(self, data: t.ByteString, options: dict) -> t.Iterable[ParentRecord]:
        yield from options['_codec']._decode_single_message(data, options)

    def _get_codec(self, codec: str) -> BaseCodec:
        if codec == 'JSON':
            from medsutil.ocproc2.codecs.ocproc2json import OCProc2JsonCodec
            return OCProc2JsonCodec(halt_flag=self._halt_flag)
        elif codec == 'YAML':
            from medsutil.ocproc2.codecs.ocproc2yaml import OCProc2YamlCodec
            return OCProc2YamlCodec(halt_flag=self._halt_flag)
        elif codec == 'PICKLE':
            from medsutil.ocproc2.codecs.ocproc2pickle import OCProc2PickleCodec
            return OCProc2PickleCodec(halt_flag=self._halt_flag)
        raise CNODCError(f"Invalid codec: {codec}", 'OCPROC2BIN', 1000)

    def _get_wrappers(self, compression: t.Optional[str], correction: t.Optional[str]) -> list[StreamWrapper]:
        stream_wrappers = []
        og_compression = compression
        og_correction = correction
        if compression == '' or compression is None:
            pass
        elif compression.startswith('LZMA'):
            import lzma
            level = None
            check = lzma.CHECK_NONE
            if compression.endswith('CRC4'):
                check = lzma.CHECK_CRC32
                compression = compression[:-4]
            elif compression.endswith('CRC8'):
                check = lzma.CHECK_CRC64
                compression = compression[:-4]
            if compression is not None and len(compression) == 5 and compression[4].isdigit():
                level = int(compression[4])
                compression = compression[:-1]
            if compression != 'LZMA':
                raise CNODCError(f'Invalid LZMA compression {og_compression}', 'OCPROC2BIN', 1001)
            stream_wrappers.append(_LZMACompression(check, level))
        elif compression.startswith('ZLIB'):
            preset = None
            if len(compression) == 5 and compression[4].isdigit():
                preset = int(compression[4])
            elif compression != 'ZLIB':
                raise CNODCError(f'Invalid ZLIB compression {og_compression}', 'OCPROC2BIN', 1002)
            stream_wrappers.append(_ZlibCompression(preset))
        elif compression.startswith('BZ2'):
            preset = None
            if len(compression) == 4 and compression[3].isdigit():
                preset = int(compression[3])
            elif compression != 'BZ2':
                raise CNODCError(f'Invalid BZ2 compression {og_compression}', 'OCPROC2BIN', 1003)
            stream_wrappers.append(_Bz2Compression(preset))
        else:
            raise CNODCError(f'Invalid compression format {og_compression}', 'OCPROC2BIN', 1004)
        if correction == "" or correction is None:
            pass
        elif correction == "RS32":
            stream_wrappers.append(_ReedSoloCorrection(32, 255))
        else:
            raise CNODCError(f'Invalid correction format {og_correction}', 'OCPROC2BIN', 1005)
        return stream_wrappers


class _Bz2Compression(StreamWrapper):

    def __init__(self, preset=None):
        self._preset = preset or 6

    def wrap_stream(self, stream: ByteStrings) -> ByteStrings:
        compressor = bz2.BZ2Compressor(self._preset)
        for bytes_ in stream:
            yield compressor.compress(bytes_)
        yield compressor.flush()

    def unwrap_stream(self, stream: ByteStrings) -> ByteStrings:
        decompressor = bz2.BZ2Decompressor()
        for bytes_ in stream:
            yield decompressor.decompress(bytes_)
        while not decompressor.eof:
            yield decompressor.decompress(b'')  # pragma: no coverage (doesn't happen on shorter calls)



class _ZlibCompression(StreamWrapper):

    def __init__(self, preset=None):
        self._preset = preset or 6

    def wrap_stream(self, stream: ByteStrings) -> ByteStrings:
        compressor = zlib.compressobj(self._preset)
        for bytes_ in stream:
            yield compressor.compress(bytes_)
        yield compressor.flush()

    def unwrap_stream(self, stream: ByteStrings) -> ByteStrings:
        decompressor = zlib.decompressobj()
        for bytes_ in stream:
            yield decompressor.decompress(bytes_)
        yield decompressor.flush()


class _LZMACompression(StreamWrapper):

    def __init__(self, crc_check=None, preset=None):
        self._crc_check = crc_check
        self._preset = preset or 6

    def wrap_stream(self, stream: ByteStrings) -> ByteStrings:
        compressor = lzma.LZMACompressor(check=self._crc_check, preset=self._preset)
        for bytes_ in stream:
            yield compressor.compress(bytes_)
        yield compressor.flush()

    def unwrap_stream(self, stream: ByteStrings) -> ByteStrings:
        decompressor = lzma.LZMADecompressor()
        for bytes_ in stream:
            yield decompressor.decompress(bytes_)


class _ReedSoloCorrection(StreamWrapper):
    """Not working well at the moment!"""

    def __init__(self, nsym=10, nsize=255, batch_size=5):
        import reedsolo
        self.rsc = reedsolo.RSCodec(nsym=nsym, nsize=nsize)
        self._out_chunk_size = batch_size * (nsize - nsym)
        self._in_chunk_size = batch_size * nsize

    def wrap_stream(self, stream: ByteStrings) -> ByteStrings:
        buffer = bytearray()
        for input_ in stream:
            buffer.extend(input_)
            while len(buffer) >= self._out_chunk_size:  # pragma: no coverage (doesn't happen on shorter calls)
                yield self.rsc.encode(buffer[0:self._out_chunk_size])
                buffer = buffer[self._out_chunk_size:]
        if buffer:
            yield self.rsc.encode(buffer)

    def unwrap_stream(self, stream: ByteStrings) -> ByteStrings:
        buffer = bytearray()
        for input_ in stream:
            buffer.extend(input_)
            while len(buffer) >= self._in_chunk_size:    # pragma: no coverage (doesn't happen on shorter calls)
                results = self.rsc.decode(buffer[0:self._in_chunk_size])
                yield results[0]
        if buffer:
            yield self.rsc.decode(buffer)[0]
