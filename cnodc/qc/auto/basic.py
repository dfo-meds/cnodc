from cnodc.nodb import NODBWorkingObservation
from cnodc.qc.common import qc_test


@qc_test("BQC", "Basic NODB QC")
def basic_qc(obs: NODBWorkingObservation):
    pass
