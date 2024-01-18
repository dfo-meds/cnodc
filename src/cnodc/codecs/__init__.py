from .ocproc2bin import OCProc2BinCodec
from .ocproc2json import OCProc2JsonCodec
from .ocproc2yaml import OCProc2YamlCodec
from .ocproc2debug import OCProc2DebugCodec
from .gts import GtsCodec

CODECS = {
    'debug': OCProc2DebugCodec,
    'ocproc2yaml': OCProc2YamlCodec,
    'ocproc2json': OCProc2JsonCodec,
    'ocproc2bin': OCProc2BinCodec,
    'gts': GtsCodec
}
