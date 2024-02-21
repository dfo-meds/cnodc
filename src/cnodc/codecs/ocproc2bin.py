import typing as t
import cnodc.ocproc2 as ocproc2
from .base import BaseCodec, ByteIterable, ByteSequenceReader
from ..util import CNODCError, vlq_encode, vlq_decode


class StreamWrapper:

    def wrap_stream(self, stream: ByteIterable) -> ByteIterable:
        yield from stream

    def unwrap_stream(self, stream: ByteIterable) -> ByteIterable:
        yield from stream


class OCProc2BinCodec(BaseCodec):

    FILE_EXTENSION = ('.ocp2',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.codecs.bin", is_encoder=True, is_decoder=True, **kwargs)

    def encode_records(self, data: t.Iterable[ocproc2.ParentRecord], **kwargs) -> ByteIterable:
        bin_args, ds_args = self._separate_kwargs(kwargs)
        codec = self._get_codec(bin_args['codec'])
        wrappers = self._get_wrappers(bin_args['compression'], bin_args['correction'])
        yield from self._make_header(**bin_args)
        if bin_args['stream']:
            for record in data:
                out_stream = codec.encode_records([record], **ds_args)
                for wrapper in wrappers:
                    out_stream = wrapper.wrap_stream(out_stream)
                full_out = b''.join(out_stream)
                yield vlq_encode(len(full_out))
                yield full_out
        else:
            out_stream = codec.encode_records(data, **ds_args)
            for wrapper in wrappers:
                out_stream = wrapper.wrap_stream(out_stream)
            yield from out_stream

    def decode_messages(self,
                        data: ByteIterable,
                        **kwargs) -> t.Iterable[ocproc2.ParentRecord]:
        stream = ByteSequenceReader(data)
        leading_bytes = stream.consume(2)
        if not len(leading_bytes) == 2:
            raise CNODCError(f"Invalid format for OCPROC2BIN [min length<2]", "OCPROC2BIN", 1007)
        header_length = int.from_bytes(leading_bytes, 'little', signed=False)
        header_bytes = stream.consume(header_length)
        if len(header_bytes) < header_length:
            raise CNODCError(f"Invalid format for OCPROC2BIN [header wrong length]", "OCPROC2BIN", 1008)
        try:
            header_str = header_bytes.decode('ascii')
            comma_count = header_str.count(',')
            if comma_count < 2 or comma_count > 3:
                raise CNODCError(f"Invalid format for OCPROC2BIN [header wrong number of elements]", "OCPROC2BIN", 1010)
            header = header_str.split(',', maxsplit=3)
            codec = self._get_codec(header[0])
            if len(header) == 2 or header[2] == 'F':
                in_stream = stream.iterate_rest()
                for wrapper in reversed(self._get_wrappers(*header[1:-1])):
                    in_stream = wrapper.unwrap_stream(in_stream)
                yield from codec.decode_messages(in_stream, **kwargs)
            else:
                wrappers = self._get_wrappers(*header[1:-1])
                wrappers.reverse()
                while not stream.at_eof():
                    idx = 0
                    while stream.peek(0)[0] > 127:
                        idx += 1
                    record_length, byte_count = vlq_decode(stream.consume(idx + 1))
                    content = [stream.consume(record_length)]
                    for wrapper in wrappers:
                        content = wrapper.unwrap_stream(content)
                    yield from codec.decode_messages(content, **kwargs)
        except UnicodeDecodeError as ex:
            raise CNODCError(f"Invalid header format for OCPROC2BIN [header not ASCII]", "OCPROC2BIN", 1009) from ex

    def _separate_kwargs(self, kwargs) -> tuple[dict, dict]:
        bin_kwargs = {
            'codec': kwargs.pop('codec') if 'codec' in kwargs else 'JSON',
            'compression': kwargs.pop('compression') if 'compression' in kwargs else None,
            'correction': kwargs.pop('correction') if 'correction' in kwargs else None,
            'stream': bool(kwargs.pop('stream')) if 'stream' in kwargs else False
        }
        return bin_kwargs, kwargs

    def _make_header(self, codec: str, compression: t.Optional[str], correction: t.Optional[str], stream: bool) -> t.Iterable[bytes]:
        s = f"{codec},{compression or ''},{correction or ''},{'T' if stream else 'F'}"
        if len(s) > 65000:
            raise CNODCError(f'Header string is too long', 'OCPROC2BIN', 1006)
        yield len(s).to_bytes(2, 'little', signed=False)
        yield s.encode('ascii')

    def _get_codec(self, codec: str) -> BaseCodec:
        if codec == 'JSON':
            from cnodc.codecs.ocproc2json import OCProc2JsonCodec
            return OCProc2JsonCodec(halt_flag=self._halt_flag)
        elif codec == 'YAML':
            from cnodc.codecs.ocproc2yaml import OCProc2YamlCodec
            return OCProc2YamlCodec(halt_flag=self._halt_flag)
        elif codec == 'PICKLE':
            from cnodc.codecs.ocproc2pickle import OCProc2PickleCodec
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
            if len(compression) == 5 and compression[4].isdigit():
                level = int(compression[4])
                compression = compression[:-1]
            if compression != 'LZMA':
                raise CNODCError(f'Invalid LZMA compression {og_compression}', 'OCPROC2BIN', 1001)
            stream_wrappers.append(_LZMACompression(check, level))
        elif compression.startswith('ZLIB'):
            preset = -1
            if len(compression) == 5 and compression[4].isdigit():
                preset = int(compression[4])
            elif compression != 'ZLIB':
                raise CNODCError(f'Invalid ZLIB compression {og_compression}', 'OCPROC2BIN', 1002)
            stream_wrappers.append(_ZlibCompression(preset))
        elif compression.startswith('BZ2'):
            preset = -1
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
        self._preset = preset

    def wrap_stream(self, stream: ByteIterable) -> ByteIterable:
        import bz2
        compressor = bz2.BZ2Compressor(self._preset)
        for bytes_ in stream:
            yield compressor.compress(bytes_)
        yield compressor.flush()

    def unwrap_stream(self, stream: ByteIterable) -> ByteIterable:
        import bz2
        decompressor = bz2.BZ2Decompressor()
        for bytes_ in stream:
            yield decompressor.decompress(bytes_)
        while not decompressor.eof:
            yield decompressor.decompress(b'')


class _ZlibCompression(StreamWrapper):

    def __init__(self, preset=None):
        self._preset = preset

    def wrap_stream(self, stream: ByteIterable) -> ByteIterable:
        import zlib
        compressor = zlib.compressobj(self._preset)
        for bytes_ in stream:
            yield compressor.compress(bytes_)
        yield compressor.flush()

    def unwrap_stream(self, stream: ByteIterable) -> ByteIterable:
        import zlib
        decompressor = zlib.decompressobj()
        for bytes_ in stream:
            yield decompressor.decompress(bytes_)
        yield decompressor.flush()


class _LZMACompression(StreamWrapper):

    def __init__(self, crc_check=None, preset=None):
        self._crc_check = crc_check
        self._preset = preset

    def wrap_stream(self, stream: ByteIterable) -> ByteIterable:
        import lzma
        compressor = lzma.LZMACompressor(check=self._crc_check, preset=self._preset)
        for bytes_ in stream:
            yield compressor.compress(bytes_)
        yield compressor.flush()

    def unwrap_stream(self, stream: ByteIterable) -> ByteIterable:
        import lzma
        decompressor = lzma.LZMADecompressor()
        for bytes_ in stream:
            yield decompressor.decompress(bytes_)


class _ReedSoloCorrection(StreamWrapper):

    def __init__(self, nsym=10, nsize=255, batch_size=5):
        import reedsolo
        self.rsc = reedsolo.RSCodec(nsym=nsym, nsize=nsize)
        self._out_chunk_size = batch_size * (nsize - nsym)
        self._in_chunk_size = batch_size * nsize

    def wrap_stream(self, stream: ByteIterable) -> ByteIterable:
        buffer = bytearray()
        for input_ in stream:
            buffer.extend(input_)
            while len(buffer) >= self._out_chunk_size:
                yield self.rsc.encode(buffer[0:self._out_chunk_size])
                buffer = buffer[self._out_chunk_size:]
        if buffer:
            yield self.rsc.encode(buffer)

    def unwrap_stream(self, stream: ByteIterable) -> ByteIterable:
        buffer = bytearray()
        for input_ in stream:
            buffer.extend(input_)
            while len(buffer) >= self._in_chunk_size:
                results = self.rsc.decode(buffer[0:self._in_chunk_size])
                yield results[0]
