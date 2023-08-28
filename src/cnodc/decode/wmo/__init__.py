from .bufr import GTSBufrStreamCodec

decoders = {
    "wmobufr": GTSBufrStreamCodec(),
}
