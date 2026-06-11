from pipeman.programs.qc.base import DeepDiveChecker, ElementRef, RecordRef, RecordSetRef


class NODBPreTest(DeepDiveChecker):

    def __init__(self, **kwargs):
        super().__init__('nodb_pretest', '1_0', **kwargs)

    def record_check(self, ref: RecordRef):
        if 'WorkingQuality' in ref.record.metadata:
            del ref.record.metadata['WorkingQuality']

    def recordset_check(self, ref: RecordSetRef):
        if 'WorkingQuality' in ref.recordset.metadata:
            del ref.recordset.metadata['WorkingQuality']

    def element_check(self, ref: ElementRef):
        if 'WorkingQuality' in ref.element.metadata:
            del ref.element.metadata['WorkingQuality']
