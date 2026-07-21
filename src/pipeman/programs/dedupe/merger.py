import math
import typing as t
import uuid

from autoinject import injector

from medsutil.awaretime import AwareDateTime
from medsutil.exceptions import CodedError
from medsutil.math import ScienceNumber
from medsutil.ocproc2 import ParentRecord, ElementMap, AbstractElement, ChildRecord, SingleElement, MultiElement
from medsutil.ocproc2.codecs import OCProc2JsonCodec
from medsutil.ocproc2.history import ActionType
from medsutil.ocproc2.refs import ParentRecordRef, RecordRef, ElementRef, RecordSetRef, ChildRecordRef
from medsutil.ocproc2.util import pair_up_records, pair_up_recordsets, pair_up_single_elements
from medsutil.storage import StorageController, FilePath
from nodb.queue import NODBQueueItem
from pipeman.processing.payloads import FilePayload
from pipeman.processing.queue_worker import QueueItemResult, QueueWorker

from nodb.observations import NODBObservation, NODBObservationData


class MergeError(CodedError): CODE_SPACE = "NODB-MERGE"


class NODBDuplicateMergeWorker(QueueWorker):

    storage: StorageController = None

    @injector.construct
    def __init__(self, **kwargs):
        super().__init__(
            process_name="merger",
            process_version="1.0",
            **kwargs
        )
        self.set_defaults({
            'queue_name': 'nodb_merge',
            'review_queue': 'nodb_merge_review',
            'finish_queue': 'workflow_continue',
            'merge_directory': None,
            'review_all': False,
        })

    def merge_directory(self) -> FilePath:
        return t.cast(FilePath, self.storage.get_filepath(self.get_config('merge_directory'), self._halt_flag, True))

    def process_queue_item(self, item: NODBQueueItem) -> t.Optional[QueueItemResult]:
        if "current_uuid" not in item.data:
            raise MergeError("Missing current_uuid", 1000)
        if "current_date" not in item.data:
            raise MergeError("Missing current_date", 1001)
        if "workflow_name" not in item.data:
            raise MergeError("Missing workflow_name", 1002)
        if "others" not in item.data:
            raise MergeError("Missing others", 1003)
        self.merge_items(
            str(item.data["current_uuid"]),
            str(item.data["current_date"]),
            str(item.data["workflow_name"]),
            list(item.data["others"]),
            item
        )


    def merge_items(self,
                    current_uuid: str,
                    current_date: str,
                    workflow_name: str,
                    others: list[str],
                    item: NODBQueueItem):
        with self.merge_directory() as merge_dir:
            merge_dir.mkdir(0o664, parents=True)

            obs_datas_to_merge = [x for x in self.load_observation_data((current_uuid, current_date), *others)]
            merger = ObservationDataMerger(obs_datas_to_merge, bool(self.get_config('review_all', False)))
            new_record, should_review = merger.merge()

            new_record.add_history_action(
                f"Created by merge processor",
                self.process_name,
                self.process_version,
                self.process_uuid,
                ActionType.CREATED_BY_DEDUPE
            )

            json_codec = OCProc2JsonCodec()
            now_ = AwareDateTime.now()
            file_handle = merge_dir.child(f"{now_.year}{now_.month}{now_.day}_{uuid.uuid4()}")
            k = 0
            while file_handle.exists():
                k += 1
                if k > 5:
                    raise MergeError(f"File {file_handle} already exists", 2001)
                file_handle = merge_dir.child(f"{now_.year}{now_.month}{now_.day}_{uuid.uuid4()}")

            file_handle.upload(json_codec.encode_records([new_record]))

            if should_review:
                self.db.create_queue_item(
                    self.get_config("review_queue"),
                    data={
                        'merged_file': str(file_handle.path()),
                        'workflow_name': workflow_name,
                        'finish_queue': self.get_config('finish_queue'),
                        'data_mode': obs_datas_to_merge[0].data_mode,
                        'quality_checks': obs_datas_to_merge[0].quality_checks,
                    },
                    correlation_id=item.correlation_id,
                    tag=item.tag
                )
            else:
                sf = obs_datas_to_merge[0].find_source_file(self.db)
                payload = FilePayload(
                    file_path=str(file_handle.path()),
                    filename=sf.file_name if sf is not None else 'merged.json',
                    is_gzipped=False,
                    last_modified_date=AwareDateTime.now()
                )
                payload.metadata.update({
                    'source-name': sf.source_name if sf is not None else 'merge',
                    'program-name': sf.program_name if sf is not None else 'merge',
                    'data-mode': obs_datas_to_merge[0].data_mode,
                    'quality-checks': obs_datas_to_merge[0].quality_checks,
                })
                payload.workflow_name = workflow_name
                payload.enqueue(self.db, self.get_config('finish_queue'))

    def load_observation_data(self, items: tuple[str, str] | list[str]) -> t.Iterable[NODBObservationData]:
        for obs_uuid, obs_date in items:
            obs = NODBObservation.find_by_uuid(self.db, obs_uuid, obs_date, key_only=True)
            if obs is None:
                raise MergeError("Missing observation", 1200, is_transient=True)
            obs_data = obs.find_observation_data(self.db)
            if obs_data is None:
                raise MergeError("Missing observation", 1201, is_transient=True)
            yield obs_data


class ObservationDataMerger:

    def __init__(self, items: list[NODBObservationData], review_all: bool = False):
        self._is_reviewable: bool = review_all
        self._items: list[NODBObservationData] = items

    def merge(self) -> tuple[ParentRecord, bool]:
        record = self._attempt_automerge()
        return record, self._is_reviewable

    def _attempt_automerge(self) -> ParentRecord:
        new_record = ParentRecord()
        new_record_ref = ParentRecordRef(new_record)
        refs = [
            ParentRecordRef(t.cast(ParentRecord, x.record))
            for x in self._items
        ]
        self._merge_parent_records(new_record_ref, *refs)
        return new_record

    def _merge_parent_records(self, new_ref, *refs: ParentRecordRef):
        self._merge_records(new_ref, *refs)

    def _merge_records(self, new_ref: RecordRef, *refs: RecordRef):
        p_keys = set()
        m_keys = set()
        c_keys = set()
        s_keys = set()
        for ref in refs:
            p_keys.update(ref.record.parameters.keys())
            m_keys.update(ref.record.metadata.keys())
            c_keys.update(ref.record.coordinates.keys())
            s_keys.update(ref.record.subrecords.keys())
        for p_key in p_keys:
            self._merge_element(new_ref.record.parameters, p_key, *(
                ref.parameter_ref(p_key)
                for ref in refs
            ))
        for m_key in m_keys:
            self._merge_element(new_ref.record.metadata, m_key, *(
                ref.metadata_ref(m_key)
                for ref in refs
            ))
        for c_key in c_keys:
            self._merge_element(new_ref.record.coordinates, c_key, *(
                ref.parameter_ref(c_key)
                for ref in refs
            ))
        for s_key in s_keys:
            self._merge_recordsets(new_ref, *refs, recordset_type=s_key)

    def _merge_recordsets(self, new_ref: RecordRef, *refs: RecordRef, recordset_type: str):
        recordsets_by_ref: list[list[RecordSetRef]] = [
            [x for x in ref.recordset_refs([recordset_type])]
            for ref in refs
        ]
        # if we have more than one recordset of a given type
        # we will review the merging of them one way or another
        for paired_refs in self.pair_up_recordsets(*recordsets_by_ref):
            # this indicates we relied on a comparison that was less than certain
            if any(not(x is None or math.isclose(x, 1)) for _, x in paired_refs):
                self._is_reviewable = True
            rs = new_ref.record.subrecords.new_recordset(recordset_type)
            rs_ref = RecordSetRef(
                f"{new_ref.path.rstrip('/')}/{recordset_type}/{len(new_ref.record.subrecords[recordset_type]) - 1}",
                rs,
                recordset_type,
                new_ref
            )
            self._merge_recordset(rs_ref, *(x[0] for x in paired_refs))

    def _merge_recordset(self, new_ref: RecordSetRef, *refs: RecordSetRef):
        m_keys = set()
        records_by_ref: list[list[RecordRef]] = []
        max_len_records = 0
        for ref in refs:
            m_keys.update(ref.recordset.metadata.keys())
            record_refs = [x for x in ref.record_refs()]
            records_by_ref.append(record_refs)
            if len(record_refs) > max_len_records:
                max_len_records = len(record_refs)
        for m_key in m_keys:
            self._merge_element(new_ref.recordset.metadata, m_key, *(ref.metadata_ref(m_key) for ref in refs))
        for idx, record_refs in enumerate(self.pair_up_records(*records_by_ref)):
            # this indicates we relied on a comparison that was less than certain
            if any(not(x is None or math.isclose(x, 1)) for _, x in record_refs):
                self._is_reviewable = True
            new_record = ChildRecord()
            child_ref = ChildRecordRef(f"{new_ref.path.rstrip("/")}/{idx}", new_record, new_ref.recordset_type, new_ref)
            self._merge_records(child_ref, *(x[0] for x in record_refs))
            new_ref.recordset.records.append(new_record)
        # if we found more records than the largest recordset, this is somewhat suspicious
        if len(new_ref.recordset.records) > max_len_records:
            self._is_reviewable = True

    def _merge_element(self, em: ElementMap, key: str, *refs: ElementRef | None):
        non_none = [x for x in refs if x is not None]
        if len(non_none) == 0:
            ...
        elif len(non_none) == 1:
            em[key] = non_none[0].element
        else:
            em[key] = self._assess_better(*(x.element for x in non_none))

    def pair_up_records(self, *record_refs: list[RecordRef]) -> t.Iterable[tuple[tuple[RecordRef | None, float | None], ...]]:
        yield from pair_up_records(*record_refs)

    def pair_up_recordsets(self, *recordset_refs: list[RecordSetRef]) -> t.Iterable[tuple[tuple[RecordSetRef | None, float | None], ...]]:
        yield from pair_up_recordsets(*recordset_refs)

    def pair_up_elements(self, *elements: list[SingleElement]) -> t.Iterable[tuple[tuple[SingleElement | None, float | None], ...]]:
        yield from pair_up_single_elements(*elements)

    def _assess_better(self, *elements: AbstractElement) -> AbstractElement:
        new_elements = []
        max_len = max([len([y for y in x.all_values() if not y.is_empty()]) for x in elements])
        for element_refs in self.pair_up_elements(*[
            [x for x in e.all_values()]
            for e in elements
        ]):
            # this indicates we relied on a comparison that was less than certain
            if any(not(x is None or math.isclose(x, 1)) for _, x in element_refs):
                self._is_reviewable = True
            new_elements.extend(self._merge_single_elements(*(x for x, _ in element_refs)))
        if len(new_elements) == 0:
            if max_len != 0:
                self._is_reviewable = True
            return SingleElement(None, Quality=9)
        elif len(new_elements) == 1:
            if max_len != 1:
                self._is_reviewable = True
            return new_elements[0]
        else:
            if max_len != len(new_elements):
                self._is_reviewable = True
            return MultiElement(new_elements)

    def _merge_single_elements(self, *elements: SingleElement | None) -> t.Iterable[SingleElement]:
        non_none: list[SingleElement] = [t.cast(SingleElement, x) for x in elements if not (x is None or x.is_empty())]
        if len(non_none) == 0:
            ...
        elif len(non_none) == 1:
            yield non_none[0]
        else:
            if any(x.is_science_number() for x in non_none):
                yield self._merge_science_numbers(*non_none)
            elif any(x.is_iso_datetime() for x in non_none):
                yield self._merge_datetime_numbers(*non_none)
            else:
                yield non_none[0]

    def _merge_datetime_numbers(self, *elements: SingleElement) -> SingleElement:
        best_time: float | None = None
        best_idx: int | None = None
        for idx, element in enumerate(elements):
            dt = element.to_scidate()
            if dt.uncertainty is not None:
                diff = dt.uncertainty.total_seconds()
            else:
                self._is_reviewable = True
                diff = 3600 * 24 # assume day is accurate if we didn't specify otherwise, but we really should!
            if best_time is None or best_time > diff:
                best_time = diff
                best_idx = idx
        return elements[t.cast(int, best_idx)]

    def _merge_science_numbers(self, *elements: SingleElement) -> SingleElement:
        units = None
        best_idx: int | None = None
        best_num: ScienceNumber | None = None
        for idx, element in enumerate(elements):
            sn = element.to_scinum()
            if units is None:
                units = sn.units
            elif sn.units != units:
                sn = sn.convert(units)
            if best_num is None or best_num.std_dev > sn.std_dev:
                best_idx = idx
                best_num = sn
        return elements[t.cast(int, best_idx)]









