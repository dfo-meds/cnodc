from autoinject import injector
from .common import CodecProtocol
import pathlib
import typing as t
import importlib
import pkgutil


class DecoderNotFound(Exception):
    pass


@injector.injectable_global
class DecoderRegistry:

    def __init__(self):
        self._codecs: dict[str, CodecProtocol] = {}
        import cnodc.decode as base_codecs
        import cnodc.plugins as plugin_codecs
        for _, name, _ in pkgutil.iter_modules(base_codecs.__path__, "cnodc.decode."):
            self._discover_codecs(name)
        for _, name, _ in pkgutil.iter_modules(plugin_codecs.__path__, "cnodc.plugins."):
            self._discover_codecs(name)

    def list_codecs(self):
        for k in self._codecs:
            yield k, self._codecs[k]

    def _discover_codecs(self, module_name: str):
        try:
            module = importlib.import_module(module_name)
            if hasattr(module, "decoders"):
                mod_codecs = getattr(module, "decoders")
                for codec_name in mod_codecs:
                    self.register_codec(codec_name, mod_codecs[codec_name])
        except ModuleNotFoundError:
            pass

    def register_codec(self, codec_name: str, codec: CodecProtocol):
        self._codecs[codec_name] = codec

    def load_codec(self, file_path: t.Union[str, pathlib.Path, None] = None, codec_name: t.Optional[str] = None):
        if codec_name is not None:
            if codec_name in self._codecs:
                return self._codecs[codec_name]
        if file_path is not None:
            if isinstance(file_path, str):
                file_path = pathlib.Path(file_path)
            for cn in self._codecs:
                if self._codecs[cn].check_compatibility(file_path):
                    return self._codecs[cn]
        raise DecoderNotFound(file_path, codec_name)
