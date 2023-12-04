import typing as t
from cnodc.ocproc2 import DataRecord
from .base import BaseCodec, ByteIterable, ByteSequenceReader
from .ocproc2json import OCProc2JsonCodec


class StreamWrapper:

    def wrap_stream(self, stream: ByteIterable) -> ByteIterable:
        yield from stream

    def unwrap_stream(self, stream: ByteIterable) -> ByteIterable:
        yield from stream


class OCProc2BinCodec(BaseCodec):

    def encode_messages(self, data: t.Iterable[DataRecord], **kwargs) -> ByteIterable:
        bin_args, ds_args = self._separate_kwargs(kwargs)
        out_stream = self._get_codec(bin_args['codec']).encode_messages(data, **ds_args)
        for wrapper in self._get_wrappers(bin_args['compression'], bin_args['correction']):
            out_stream = wrapper.wrap_stream(out_stream)
        yield from self._make_header(**bin_args)
        yield from out_stream

    def decode_messages(self,
                        data: ByteIterable,
                        **kwargs) -> t.Iterable[DataRecord]:
        stream = ByteSequenceReader(data)
        leading_bytes = stream.consume(2)
        if not len(leading_bytes) == 2:
            raise ValueError(f"need at least two bytes")
        header = stream.consume(int.from_bytes(leading_bytes, 'little', signed=False)).decode('ascii').split(',', maxsplit=2)
        codec = self._get_codec(header[0])
        in_stream = stream.iterate_rest()
        for wrapper in reversed(self._get_wrappers(*header)):
            in_stream = wrapper.wrap_stream(in_stream)
        yield from codec.decode_messages(in_stream, **kwargs)

    def _separate_kwargs(self, kwargs) -> tuple[dict, dict]:
        bin_kwargs = {
            'codec': kwargs.pop('codec') if 'codec' in kwargs else 'JSON',
            'compression': kwargs.pop('compression') if 'compression' in kwargs else None,
            'correction': kwargs.pop('correction') if 'correction' in kwargs else None
        }
        return bin_kwargs, kwargs

    def _make_header(self, codec: str, compression: t.Optional[str], correction: t.Optional[str]) -> t.Iterable[bytes]:
        s = f"{codec},{compression or ''},{correction or ''}"
        yield len(s).to_bytes(2, 'little', signed=False)
        yield s.encode('ascii')

    def _get_codec(self, codec: str) -> BaseCodec:
        if codec == 'JSON':
            return OCProc2JsonCodec(halt_flag=self._halt_flag)
        raise ValueError(f"Invalid codec: {codec}")

    def _get_wrappers(self, compression: t.Optional[str], correction: t.Optional[str]) -> list[StreamWrapper]:
        return []




