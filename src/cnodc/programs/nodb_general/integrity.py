from cnodc.ocproc2.structures import AbstractValue, MultiValue
from cnodc.ocproc2.validation import OCProc2Ontology
from cnodc.qc import CNODCBaseQCTestSuite, TestContext, VerificationTestResult
from cnodc.units import UnitConverter
from cnodc.process.queue_worker import QueueWorker
import cnodc.nodb.structures as structures
import typing as t

from cnodc.nodb import NODBController, LockType
from autoinject import injector

from cnodc.workflow.processor import PayloadProcessor
from cnodc.workflow.workflow import BatchPayload


class NODBIntegrityWorker(QueueWorker):

    def __init__(self, **kwargs):
        super().__init__(log_name="cnodc.nodb_aic", **kwargs)
        self._verifier: t.Optional[NODBIntegrityChecker] = None

    def on_start(self):
        self._verifier = NODBIntegrityChecker(
            processor_uuid=self.process_uuid
        )

    def process_queue_item(self, item: structures.NODBQueueItem):
        self._verifier.process_queue_item(item)


class NODBIntegrityChecker(PayloadProcessor):

    nodb: NODBController = None

    NAME = 'nodb_aic'
    VERSION = '1.0'

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            processor_version=NODBIntegrityChecker.NAME,
            processor_name=NODBIntegrityChecker.VERSION,
            require_type=BatchPayload,
            **kwargs
        )
        self._test_suite = NODBIntegrityCheckTestSuite()
        self._pass_queue = 'nodb_processing'
        self._fail_queue = 'nodb_integrity_review'

    def _process(self):
        batch = self.load_batch_from_payload()
        integrity_check_result = True
        for working_record in batch.stream_working_records(self._db, lock_type=LockType.FOR_NO_KEY_UPDATE):
            record = working_record.record
            result = self._test_suite.verify_record(record)
            if result in (VerificationTestResult.PASS, VerificationTestResult.FAIL):
                working_record.record = record
                self._db.update_object(working_record)
                self._db.commit()
            if result in (VerificationTestResult.STALE_FAIL, VerificationTestResult.FAIL):
                integrity_check_result = False
        payload = self.create_batch_payload(batch_uuid=batch.batch_uuid)
        if integrity_check_result:
            self._db.create_queue_item(
                queue_name=self._pass_queue,
                data=payload.to_map()
            )
        else:
            self._db.create_queue_item(
                queue_name=self._fail_queue,
                subqueue_name=payload.headers['manual-subqueue'] if 'manual-subqueue' in payload.headers else None,
                data=payload.to_map()
            )


class NODBIntegrityCheckTestSuite(CNODCBaseQCTestSuite):

    converter: UnitConverter = None
    ontology: OCProc2Ontology = None

    @injector.construct
    def __init__(self):
        super().__init__('nodb_integrity', '1.0')

    def _verify_record(self, context: TestContext):
        scope = "record" if context.is_top_level() else "subrecord"
        original_path = context.current_path
        for key in context.current_record.metadata:
            context.current_path = [*original_path, f'metadata#{key}']
            self._verify_integrity(context, "metadata", scope, key, context.current_record.metadata[key])
        for key in context.current_record.coordinates:
            context.current_path = [*original_path, f'coordinates#{key}']
            self._verify_integrity(context, "coordinates", scope, key, context.current_record.coordinates[key])
        for key in context.current_record.parameters:
            context.current_path = [*original_path, f'parameters#{key}']
            self._verify_integrity(context, "parameters", scope, key, context.current_record.parameters[key])

    def _verify_integrity(self, context: TestContext, domain: str, scope: str, parameter_name: str, parameter_value: AbstractValue):
        if self.ontology.is_defined_parameter(parameter_name):
            target_domain = self.ontology.parameter_domain(parameter_name)
            if target_domain is not None and target_domain != domain:
                context.report_qc_failure('ontology_invalid_domain')
                parameter_value.metadata['WorkingQuality'] = 20
            target_scopes = self.ontology.parameter_scopes(parameter_name)
            if target_scopes is not None and scope not in target_scopes:
                context.report_qc_failure('ontology_invalid_scope')
                parameter_value.metadata['WorkingQuality'] = 20
            if isinstance(parameter_value, MultiValue) and not self.ontology.allow_multiple_values(parameter_name):
                context.report_qc_failure('ontology_multi_not_allowed')
                parameter_value.metadata['WorkingQuality'] = 20
            preferred_unit = self.ontology.preferred_unit(parameter_name)
            data_type = self.ontology.data_type(parameter_name)
            for value in parameter_value.all_values():
                if value.is_empty():
                    continue
                if preferred_unit is not None and value.metadata.has_value('Units'):
                    if not self.converter.compatible(preferred_unit, value.metadata.best_value('Units')):
                        context.report_qc_failure('ontology_incompatible_units')
                        value.metadata['WorkingQuality'] = 20
                if data_type is None:
                    pass
                elif data_type in ('dateTimeStamp', 'date') and not value.is_iso_datetime():
                    context.report_qc_failure('ontology_invalid_datetime')
                    value.metadata['WorkingQuality'] = 20
                elif data_type == 'integer' and not value.is_integer():
                    context.report_qc_failure('ontology_invalid_integer')
                    value.metadata['WorkingQuality'] = 20
                elif data_type == 'decimal' and not value.is_numeric():
                    context.report_qc_failure('ontology_invalid_decimal')
                    value.metadata['WorkingQuality'] = 20
                elif data_type == 'string' and isinstance(value.value, (dict, list, tuple, set)):
                    context.report_qc_failure('ontology_invalid_string')
                    value.metadata['WorkingQuality'] = 20
                elif data_type == 'List' and not isinstance(value.value, (list, tuple, set)):
                    context.report_qc_failure('ontology_invalid_list')
                    value.metadata['WorkingQuality'] = 20
        original_path = context.current_path
        for key in parameter_value.metadata:
            # Never check these just so we avoid an infinite loop of errors
            if key == 'WorkingQuality':
                continue
            context.current_path = [*original_path, f'metadata#{key}']
            self._verify_integrity(context, "metadata", "parameter", key, parameter_value.metadata[key])
