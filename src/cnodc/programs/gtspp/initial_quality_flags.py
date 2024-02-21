from cnodc.qc.base import BaseTestSuite, TestContext, RecordTest
import cnodc.ocproc2 as ocproc2


class GTSPPInitialQualityFlagsCheck(BaseTestSuite):

    def __init__(self, **kwargs):
        super().__init__('gtspp_pre_test', '1_0', **kwargs)

    @RecordTest(top_only=True)
    def _set_quality_flags(self, record, context: TestContext):
        station = self.load_station(context)
        use_qc = station.get_metadata('keep_external_qc', False) if station is not None else False
        self.set_all_quality_flags(record, context, use_qc=use_qc)

    def set_all_quality_flags(self, record: ocproc2.BaseRecord, context: TestContext, **kwargs):
        for key in record.parameters:
            with context.parameter_context(key) as ctx2:
                self.test_all_subvalues(ctx2, self._set_flags_on_element, **kwargs)
        for key in record.metadata:
            with context.metadata_context(key) as ctx2:
                self.test_all_subvalues(ctx2, self._set_flags_on_element, **kwargs)
        for key in record.coordinates:
            with context.coordinate_context(key) as ctx2:
                self.test_all_subvalues(ctx2, self._set_flags_on_element, **kwargs)
        self.test_all_subrecords(context, self.set_all_quality_flags, **kwargs)

    def _set_flags_on_element(self, v: ocproc2.AbstractElement, context: TestContext, use_qc: bool):
        if use_qc:
            qual = v.metadata.best_value('Quality', None)
            if qual is not None and int(qual) > 0:
                v.metadata['WorkingQuality'] = int(qual)
            elif 'WorkingQuality' in v.metadata:
                del v.metadata['WorkingQuality']
        elif 'WorkingQuality' in v.metadata:
            del v.metadata['WorkingQuality']
        for key in v.metadata:
            if key in ('WorkingQuality', 'Quality'):
                continue
            with context.element_metadata_context(key) as ctx3:
                self.test_all_subvalues(ctx3, self._set_flags_on_element, use_qc=use_qc)
