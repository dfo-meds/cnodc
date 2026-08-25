import typing as t

from medsutil import types as ct
from medsutil.ocproc2 import ParentRecord
from medsutil.ocproc2.codecs.base import BaseCodec, DecodeResult
from medsutil.ocproc2.codecs.meds.convert import ocproc2_to_station, station_to_ocproc2
from medsutil.ocproc2.codecs.meds.structs import MedsEncoding, unpack, StationRecord


class _MedsCodec(BaseCodec):

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            is_encoder=True,
            is_decoder=True,
            force_single_mode=True,
            **kwargs
        )

    def encoding(self) -> MedsEncoding:
        raise NotImplementedError

    def _decode_single_message(self, data: t.ByteString, options: dict) -> t.Iterable[ParentRecord]:
        for station in unpack(self._as_byte_sequence([data]), self.encoding()):
            yield station_to_ocproc2(station)

    def _encode_single_record(self, record: ParentRecord, options: dict) -> ct.ByteStrings:
        if "__index" not in options:
            options["__index"] = 1
        else:
            options["__index"] += 1
        yield from ocproc2_to_station(record).encode(self.encoding(), options["__index"])


class OCPROCCodec(_MedsCodec):

    def __init__(self, *args, **kwargs):
        super().__init__(log_name="cnodc.codecs.ocproc1", *args, **kwargs)

    def encoding(self) -> MedsEncoding:
        return MedsEncoding.OCPROC


class MEDSASCIICodec(_MedsCodec):

    def __init__(self, *args, **kwargs):
        super().__init__(log_name="cnodc.codecs.medsascii", *args, **kwargs)

    def encoding(self) -> MedsEncoding:
        return MedsEncoding.MEDS_ASCII
