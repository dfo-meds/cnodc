from pipeman.programs.qc.base import DeepDiveChecker
from medsutil.ocproc2.refs import ElementRef, RecordSetRef, RecordRef


class NODBPreFlight(DeepDiveChecker):

    def __init__(self, **kwargs):
        super().__init__('nodb_pretest', '1.0', **kwargs)

    def record_check(self, ref: RecordRef):
        if 'WorkingQuality' in ref.record.metadata:
            del ref.record.metadata['WorkingQuality']

    def recordset_check(self, ref: RecordSetRef):
        if 'WorkingQuality' in ref.recordset.metadata:
            del ref.recordset.metadata['WorkingQuality']

    def element_check(self, ref: ElementRef):
        if 'WorkingQuality' in ref.element.metadata:
            del ref.element.metadata['WorkingQuality']