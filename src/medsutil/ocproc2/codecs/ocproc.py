from medsutil.ocproc2.codecs.base import BaseCodec


class OCPROCCodec(BaseCodec):

    def __init__(self, *args, **kwargs):
        super().__init__(log_name="cnodc.codecs.ocproc1", *args, **kwargs)


class MEDSASCIICodec(BaseCodec):

    def __init__(self, *args, **kwargs):
        super().__init__(log_name="cnodc.codecs.medsascii", *args, **kwargs)

