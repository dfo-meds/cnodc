from pipeman.programs.qc.qc import DeepDiveChecker, ElementRef, RecordRef, RecordSetRef


class NODBPreTest(DeepDiveChecker):

    def __init__(self, **kwargs):
        super().__init__('nodb_pretest', '1_0', **kwargs)

    def record_check(self, ref: RecordRef):
        if 'WorkingQuality' in ref.record.metadata:
            del ref.record.metadata['WorkingQuality']
        ref.record.qc_info.clear()

    def recordset_check(self, ref: RecordSetRef):
        if 'WorkingQuality' in ref.recordset.metadata:
            del ref.recordset.metadata['WorkingQuality']
        ref.recordset.qc_info.clear()

    def element_check(self, ref: ElementRef):
        if 'WorkingQuality' in ref.element.metadata:
            del ref.element.metadata['WorkingQuality']
        ref.element.qc_info.clear()
