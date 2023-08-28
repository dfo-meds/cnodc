import sys

from cnodc.decode.common import BufferedBinaryReader, BaseCodec, TranscodingResult
from cnodc.ocproc2 import DataRecord
import typing as t
from cnodc.decode.ocproc2_json import OCProc2JsonCodec
from cnodc.decode.ocproc2_yaml import OCProc2YamlCodec
import lzma
import reedsolo


class OCProc2BinaryCodec(BaseCodec):

    def __init__(self):
        super().__init__("Compressible binary format for OCPROC2", ".op2b")

    def encode(self,
               records: TranscodingResult,
               text_format: str = 'YAML',
               compression: t.Optional[str] = 'LZMACRC4',
               correction: t.Optional[str] = 'RS32',
               **kwargs) -> t.Iterable[bytes]:
        if len(text_format) > 256:
            raise ValueError(f"Text format {text_format} is too long")
        if compression is not None and len(compression) > 256:
            raise ValueError(f"Compression format {compression} is too long")
        if correction is not None and len(correction) > 256:
            raise ValueError(f"Correction format {correction} is too long")
        formatter = self.get_formatter(text_format)
        compressor = self.get_compressor(compression)
        corrector = self.get_error_correction(correction)
        output = bytearray([1, len(text_format)])
        output.extend(text_format.encode('ascii'))
        if compression:
            output.append(len(compression))
            output.extend(compression.encode('ascii'))
        else:
            output.append(0)
        if correction:
            output.append(len(correction))
            output.extend(correction.encode('ascii'))
        else:
            output.append(0)
        yield output
        yield from corrector.handle_outgoing(compressor.compress_stream(formatter.encode(records)))

    def decode_messages(self, data: t.Iterable[bytes], **kwargs) -> t.Iterable[DataRecord]:
        reader = BufferedBinaryReader(data)
        format_version = int(reader.consume(1)[0])
        if format_version == 1:
            text_format_length = int(reader.consume(1)[0])
            text_format = reader.consume(text_format_length).decode('ascii')
            compression_length = int(reader.consume(1)[0])
            compression = reader.consume(compression_length).decode('ascii') if compression_length > 0 else None
            correction_length = int(reader.consume(1)[0])
            correction = reader.consume(correction_length).decode('ascii') if correction_length > 0 else None
            formatter = self.get_formatter(text_format)
            compressor = self.get_compressor(compression)
            corrector = self.get_error_correction(correction)
            yield from formatter.decode_messages(
                compressor.uncompress_stream(corrector.handle_incoming(reader.read_all_in_chunks())),
                **kwargs
            )
        else:
            raise ValueError(f"Unrecognized OCPROC2 file version")

    def get_formatter(self, format_code: str):
        if format_code == "JSON":
            return OCProc2JsonCodec()
        if format_code == "YAML":
            return OCProc2YamlCodec()
        raise ValueError(f"unknown text format {format_code}")

    def get_compressor(self, format_code: t.Optional[str]):
        if format_code== "LZMACRC4":
            return _LZMACompressor(lzma.CHECK_CRC32)
        if format_code == "LZMACRC8":
            return _LZMACompressor(lzma.CHECK_CRC64)
        if format_code == "LZMA":
            return _LZMACompressor()
        if format_code == "" or format_code is None:
            return _NullCompressor()
        raise ValueError(f"unknown compressor {format_code}")

    def get_error_correction(self, format_code: t.Optional[str]):
        if format_code == "" or format_code is None:
            return _NullCorrector()
        if format_code == "RS32":
            return _ReedSoloCorrection(32, 255)
        raise ValueError(f"unknown corrector {format_code}")


class _NullCorrector:

    def __init__(self):
        pass

    def handle_outgoing(self, stream: t.Iterable[bytes]) -> t.Iterable[bytes]:
        yield from stream

    def handle_incoming(self, stream: t.Iterable[bytes]) -> t.Iterable[bytes]:
        yield from stream


class _NullCompressor:

    def __init__(self):
        pass

    def compress_stream(self, stream: t.Iterable[bytes]) -> t.Iterable[bytes]:
        yield from stream

    def uncompress_stream(self, stream: t.Iterable[bytes]) -> t.Iterable[bytes]:
        yield from stream


class _LZMACompressor:

    def __init__(self, crc_check=None):
        self._crc_check = crc_check

    def compress_stream(self, stream: t.Iterable[bytes]) -> t.Iterable[bytes]:
        compressor = lzma.LZMACompressor(check=self._crc_check)
        for bytes_ in stream:
            yield compressor.compress(bytes_)
        yield compressor.flush()

    def uncompress_stream(self, stream: t.Iterable[bytes]) -> t.Iterable[bytes]:
        iterator = iter(stream)
        decompressor = lzma.LZMADecompressor()
        while not decompressor.eof:
            if decompressor.needs_input:
                yield decompressor.decompress(iterator.__next__(), 1024 * 1024 * 8)
            else:
                yield decompressor.decompress(b'')


class _ReedSoloCorrection:

    def __init__(self, nsym=10, nsize=255, batch_size=5):
        self.rsc = reedsolo.RSCodec(nsym=nsym, nsize=nsize)
        self._out_chunk_size = batch_size * (nsize - nsym)
        self._in_chunk_size = batch_size * nsize

    def handle_outgoing(self, stream: t.Iterable[bytes]) -> t.Iterable[bytes]:
        buffer = bytearray()
        for input_ in stream:
            buffer.extend(input_)
            while len(buffer) >= self._out_chunk_size:
                yield self.rsc.encode(buffer[0:self._out_chunk_size])
                buffer = buffer[self._out_chunk_size:]
        if buffer:
            yield self.rsc.encode(buffer)

    def handle_incoming(self, stream: t.Iterable[bytes]) -> t.Iterable[bytes]:
        buffer = bytearray()
        for input_ in stream:
            buffer.extend(input_)
            while len(buffer) >= self._in_chunk_size:
                results = self.rsc.decode(buffer[0:self._in_chunk_size])
                yield results[0]
