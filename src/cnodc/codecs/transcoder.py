import pathlib
import typing as t

from cnodc.codecs.base import BaseCodec


def transcode(source_file: str, destination_file: str, source_encoding: t.Optional[str] = None, destination_encoding: t.Optional[str] = None):
    source_file = pathlib.Path(source_file) if not isinstance(source_file, pathlib.Path) else source_file
    destination_file = pathlib.Path(destination_file) if not isinstance(destination_file, pathlib.Path) else destination_file
    source_codec = find_codec(source_file.name, source_encoding)
    destination_codec = find_codec(destination_file.name, destination_encoding)
    destination_codec.dump(destination_file, source_codec.load_all(source_file, fail_on_error=True))


def find_codec(file_name: str, encoding: t.Optional[str] = None) -> BaseCodec:
    from cnodc.codecs import CODECS
    if encoding is not None:
        if encoding in CODECS:
            return CODECS[encoding]()
        raise ValueError(f'Codec [{encoding}] not found')
    for x in CODECS:
        if CODECS[x].check_file_type(file_name):
            return CODECS[x]()
    raise ValueError(f'No suitable codec found for [{file_name}]')
