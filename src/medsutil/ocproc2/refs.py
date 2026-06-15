import abc
import dataclasses
import enum
import typing as t

if t.TYPE_CHECKING:
    from medsutil import ocproc2 as ocproc2
    from medsutil.ocproc2.util import ObjectWithMetadata

class ElementType(enum.IntFlag):
    COORDINATES = enum.auto()
    PARAMETERS = enum.auto()
    PARENT_METADATA = enum.auto()
    CHILD_METADATA = enum.auto()
    ELEMENT_METADATA = enum.auto()
    RECORDSET_METADATA = enum.auto()

    RECORD_METADATA = PARENT_METADATA | CHILD_METADATA
    METADATA = PARENT_METADATA | CHILD_METADATA | ELEMENT_METADATA | RECORDSET_METADATA


@dataclasses.dataclass
class AnyRef:
    path: str
    parent: AnyRef | None

    def __str__(self):
        return self.path

    def __repr__(self):
        return f"<{self.__class__.__name__}:{self.path}>"

    @property
    def ref_object(self) -> ObjectWithMetadata:
        raise NotImplementedError

    def _child_element_refs(self, element_map: ocproc2.ElementMap, element_type: ElementType, base_path: str) -> t.Iterable[SingleElementRef | MultiElementRef]:
        for element_name, element in element_map.items():
            yield ElementRef.build(
                element=element,
                element_name=element_name,
                element_type=element_type,
                path=base_path + f"/{element_name}",
                parent=self
            )

@dataclasses.dataclass
class ElementRef(AnyRef):
    element: ocproc2.AbstractElement
    element_name: str
    element_type: ElementType

    @property
    def ref_object(self) -> ocproc2.AbstractElement:
        return self.element

    def keyed_sensor_rank_refs(self) -> dict[int, SingleElementRef]:
        nxt = 0
        ranked = {}
        for sref in self.single_element_refs():
            key = sref.element.metadata.best("SensorRank", coerce=int, default=nxt)
            if key >= nxt:
                nxt = key + 1
            ranked[key] = sref
        return ranked

    def single_element_refs(self) -> t.Iterable[SingleElementRef]:
        raise NotImplementedError

    def subelement_refs(self) -> t.Iterable[SingleElementRef | MultiElementRef]:
        raise NotImplementedError

    def metadata_refs(self) -> t.Iterable[SingleElementRef | MultiElementRef]:
        yield from self._child_element_refs(self.element.metadata, ElementType.ELEMENT_METADATA, f"{self.path.rstrip("/")}/metadata")

    def setdefault_metadata_ref(self, name: str) -> SingleElementRef | MultiElementRef:
        return t.cast(SingleElementRef | MultiElementRef, self.metadata_ref(name, True))

    def metadata_ref(self, name: str, create_when_missing: bool = False) -> SingleElementRef | MultiElementRef | None:
        return ElementRef.build_ref_from_element_map(self.element.metadata, name, ElementType.ELEMENT_METADATA, f"{self.path.rstrip("/")}/metadata", create_when_missing, self)

    @staticmethod
    def build(element: ocproc2.AbstractElement, **kwargs) -> SingleElementRef | MultiElementRef:
        if not element.is_multivalue():
            return SingleElementRef(element=t.cast(ocproc2.SingleElement, element), **kwargs)
        else:
            return MultiElementRef(element=t.cast(ocproc2.MultiElement, element), **kwargs)

    @staticmethod
    def build_ref_from_element_map(element_map: ocproc2.ElementMap,
                                   name: str,
                                   element_type: ElementType,
                                   parent_path: str,
                                   create_when_missing: bool,
                                   parent: AnyRef) -> t.Optional[SingleElementRef | MultiElementRef]:
        if name not in element_map:
            if create_when_missing:
                element_map[name] = None
            else:
                return None
        return ElementRef.build(
            element_map[name],
            element_name=name,
            element_type=element_type,
            path=parent_path.rstrip("/") + f"/{name}",
            parent=parent
        )


@dataclasses.dataclass
class SingleElementRef(ElementRef):
    element: ocproc2.SingleElement

    def single_element_refs(self) -> t.Iterable[SingleElementRef]:
        yield self

    def subelement_refs(self) -> t.Iterable[SingleElementRef | MultiElementRef]:
        yield self


@dataclasses.dataclass
class MultiElementRef(ElementRef):
    element: ocproc2.MultiElement

    def single_element_refs(self) -> t.Iterable[SingleElementRef]:
        for sub_element in self.subelement_refs():
            if isinstance(sub_element, MultiElementRef):
                yield from sub_element.single_element_refs()
            else:
                yield sub_element

    def subelement_refs(self) -> t.Iterable[SingleElementRef | MultiElementRef]:
        for idx, sub_element in enumerate(self.element.values()):
            yield ElementRef.build(
                element=sub_element,
                element_type=self.element_type,
                element_name=self.element_name,
                path=self.path.rstrip("/") + f"/{idx}",
                parent=self
            )


@dataclasses.dataclass
class RecordSetRef(AnyRef):
    recordset: ocproc2.RecordSet
    recordset_type: str

    def metadata_refs(self) -> t.Iterable[SingleElementRef | MultiElementRef]:
        yield from self._child_element_refs(self.recordset.metadata, ElementType.RECORDSET_METADATA, f"{self.path.rstrip("/")}/metadata")

    def metadata_ref(self, name: str, create_when_missing: bool = False) -> SingleElementRef | MultiElementRef | None:
        return ElementRef.build_ref_from_element_map(self.recordset.metadata, name, ElementType.RECORDSET_METADATA, f"{self.path.rstrip("/")}/metadata", create_when_missing, self)

    def setdefault_metadata_ref(self, name: str) -> SingleElementRef | MultiElementRef:
        return t.cast(SingleElementRef | MultiElementRef, self.metadata_ref(name, True))

    def record_refs(self) -> t.Iterable[ChildRecordRef]:
        for idx, record in self.recordset.records.iterate_with_load():
            yield ChildRecordRef(
                record=record,
                recordset_type=self.recordset_type,
                path=(self.path.rstrip("/") + f"/{idx}"),
                parent=self
            )

    @property
    def ref_object(self):
        return self.recordset


@dataclasses.dataclass
class RecordRef(AnyRef):
    record: ocproc2.BaseRecord
    RECORD_METADATA_TYPE = ElementType.CHILD_METADATA

    def profiles(self) -> t.Iterable[RecordSetRef]:
        yield from self.recordset_refs("PROFILE")

    def recordset_refs(self, limit_types: t.Container[str] | None = None) -> t.Iterable[RecordSetRef]:
        for recordset_type, recordset_dict in self.record.subrecords.record_sets:
            if limit_types is None or recordset_type in limit_types:
                base_path = f"{self.path.rstrip("/")}/{recordset_type}"
                for idx, recordset in recordset_dict:
                    yield RecordSetRef(
                        recordset=recordset,
                        recordset_type=recordset_type,
                        path=f"{base_path}/{idx}",
                        parent=self
                    )

    def record_refs(self, limit_types: t.Container[str] | None = None) -> t.Iterable[ChildRecordRef]:
        for rs_ref in self.recordset_refs(limit_types):
            yield from rs_ref.record_refs()

    def single_element_refs(self, limit_types: ElementType | None = None) -> t.Iterable[SingleElementRef]:
        for element in self.element_refs(limit_types):
            yield from element.single_element_refs()

    def element_refs(self, limit_types: ElementType = None) -> t.Iterable[SingleElementRef | MultiElementRef]:
        if limit_types is None or ElementType.PARAMETERS in limit_types:
            yield from self.parameter_refs()
        if limit_types is None or ElementType.COORDINATES in limit_types:
            yield from self.coordinate_refs()
        if limit_types is None or self.RECORD_METADATA_TYPE in limit_types:
            yield from self.metadata_refs()

    def metadata_refs(self) -> t.Iterable[SingleElementRef | MultiElementRef]:
        yield from self._child_element_refs(self.record.metadata, self.RECORD_METADATA_TYPE, f"{self.path.rstrip("/")}/metadata")

    def parameter_refs(self) -> t.Iterable[SingleElementRef | MultiElementRef]:
        yield from self._child_element_refs(self.record.parameters, ElementType.PARAMETERS, f"{self.path.rstrip("/")}/parameters")

    def coordinate_refs(self) -> t.Iterable[SingleElementRef | MultiElementRef]:
        yield from self._child_element_refs(self.record.coordinates, ElementType.COORDINATES, f"{self.path.rstrip("/")}/coordinates")

    def metadata_ref(self, name: str, create_when_missing: bool = False) -> SingleElementRef | MultiElementRef | None:
        return ElementRef.build_ref_from_element_map(self.record.metadata, name, self.RECORD_METADATA_TYPE, f"{self.path.rstrip("/")}/metadata", create_when_missing, self)

    def setdefault_metadata_ref(self, name: str) -> SingleElementRef | MultiElementRef:
        return t.cast(SingleElementRef | MultiElementRef, self.metadata_ref(name, True))

    def coordinate_ref(self, name: str, create_when_missing: bool = False) -> SingleElementRef | MultiElementRef | None:
        return ElementRef.build_ref_from_element_map(self.record.coordinates, name, ElementType.COORDINATES, f"{self.path.rstrip("/")}/coordinates", create_when_missing, self)

    def setdefault_coordinate_ref(self, name: str) -> SingleElementRef | MultiElementRef:
        return t.cast(SingleElementRef | MultiElementRef, self.coordinate_ref(name, True))

    def parameter_ref(self, name: str, create_when_missing: bool = False) -> SingleElementRef | MultiElementRef | None:
        return ElementRef.build_ref_from_element_map(self.record.parameters, name, ElementType.PARAMETERS, f"{self.path.rstrip("/")}/parameters", create_when_missing, self)

    def setdefault_parameter_ref(self, name: str) -> SingleElementRef | MultiElementRef:
        return t.cast(SingleElementRef | MultiElementRef, self.parameter_ref(name, True))

    @property
    def ref_object(self) -> ocproc2.BaseRecord:
        return self.record


@dataclasses.dataclass
class ParentRecordRef(RecordRef):
    record: ocproc2.ParentRecord
    RECORD_METADATA_TYPE = ElementType.PARENT_METADATA


@dataclasses.dataclass
class ChildRecordRef(RecordRef):
    record: ocproc2.ChildRecord
    recordset_type: str


class RecordCrawler:

    def __init__(self,
                 record_cb: t.Callable[[RecordRef], t.Any] | None = None,
                 parent_record_cb: t.Callable[[ParentRecordRef], t.Any] | None = None,
                 child_record_cb: t.Callable[[ChildRecordRef], t.Any] | None = None,
                 element_cb: t.Callable[[ElementRef], t.Any] | None = None,
                 multi_element_cb: t.Callable[[MultiElementRef], t.Any] | None = None,
                 single_element_cb: t.Callable[[SingleElementRef], t.Any] | None = None,
                 recordset_cb: t.Callable[[RecordSetRef], t.Any] | None = None,
                 limit_element_types: ElementType | None = None,
                 limit_subrecord_types: t.Container[str] | None = None):
        self._record_cb = record_cb
        self._parent_record_cb = parent_record_cb
        self._child_record_cb = child_record_cb
        self._element_cb = element_cb
        self._multi_element_cb = multi_element_cb
        self._single_element_cb = single_element_cb
        self._recordset_cb = recordset_cb
        self._limit_element_types = limit_element_types
        self._limit_subrecord_types = limit_subrecord_types
        self._crawl_elements = any(x is not None for x in (element_cb, multi_element_cb, single_element_cb))
        self._crawl_children = self._crawl_elements or any(x is not None for x in (recordset_cb, record_cb, child_record_cb))
        self._crawl_record_elements = self._crawl_elements and (
            self._limit_element_types is not ElementType.RECORDSET_METADATA
        )
        self._crawl_recordset_metadata = self._crawl_elements and (
            self._limit_element_types is None
            or (ElementType.RECORDSET_METADATA in self._limit_element_types)
            or (ElementType.ELEMENT_METADATA in self._limit_element_types)
        )
        self._crawl_element_metadata = self._crawl_elements and (
            self._limit_element_types is None
            or (ElementType.ELEMENT_METADATA in self._limit_element_types)
        )

    def crawl_record(self, ref: ParentRecordRef | ChildRecordRef):
        if self._record_cb is not None:
            self._record_cb(ref)
        if self._parent_record_cb is not None or self._child_record_cb is not None:
            if ref.RECORD_METADATA_TYPE is ElementType.PARENT_METADATA:
                if self._parent_record_cb is not None:
                    self._parent_record_cb(t.cast(ParentRecordRef, ref))
            elif self._child_record_cb is not None:
                self._child_record_cb(t.cast(ChildRecordRef, ref))
        if self._crawl_elements:
            for element in ref.element_refs(self._limit_element_types):
                self.crawl_element(element)
        if self._crawl_children:
            for recordset in ref.recordset_refs(self._limit_subrecord_types):
                self.crawl_recordset(recordset)

    def crawl_element(self, ref: SingleElementRef | MultiElementRef):
        if self._element_cb is not None:
            self._element_cb(ref)
        if isinstance(ref, MultiElementRef):
            if self._multi_element_cb is not None:
                self._multi_element_cb(ref)
            for sub_element in ref.subelement_refs():
                self.crawl_element(sub_element)
        elif self._single_element_cb is not None:
            self._single_element_cb(ref)
        if self._crawl_element_metadata:
            for element_md in ref.metadata_refs():
                self.crawl_element(element_md)

    def crawl_recordset(self, ref: RecordSetRef):
        if self._recordset_cb is not None:
            self._recordset_cb(ref)
        if self._crawl_recordset_metadata:
            for metadata_ref in ref.metadata_refs():
                self.crawl_element(metadata_ref)
        for child_record in ref.record_refs():
            self.crawl_record(child_record)