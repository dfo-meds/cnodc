import datetime
import typing as t

import nodb.interface as interface
from nodb.interface import NODBObjectType
from medsutil.cached import CachedObjectMixin
import medsutil.datadict as ddo
import medsutil.types as ct
from medsutil.datadict import DataDictModifiedTracker
from nodb.interface import NODBValidationError
from pipeman.exceptions import CNODCError


def parse_received_date(rdate: t.Union[str, datetime.date]) -> datetime.date:
    """Convert a date string or date object into a date object."""
    if isinstance(rdate, str):
        try:
            return datetime.date.fromisoformat(rdate)
        except (TypeError, ValueError) as ex:
            raise CNODCError(f"Invalid received date [{rdate}]", "NODB", 1000) from ex
    else:
        return rdate


IntColumn = ddo.p_int
BooleanColumn = ddo.p_bool
StringColumn = ddo.p_str
FloatColumn = ddo.p_float
UUIDColumn = ddo.p_str
ByteColumn = ddo.p_bytes
ByteArrayColumn = ddo.p_bytearray
DateTimeColumn = ddo.p_awaretime
DateColumn = ddo.p_date
EnumColumn = ddo.p_enum
JsonDictColumn = ddo.p_json_dict
JsonListColumn = ddo.p_json_list
JsonSetColumn = ddo.p_json_set
WKTColumn = ddo.p_str


class NODBBaseObject(DataDictModifiedTracker, CachedObjectMixin):
    """Base class for all NODB objects.

        This provides a lot of tools for building database classes.
    """

    @classmethod
    def get_table_name(cls) -> interface.DatabaseIdentifier:
        """Get the name of the table."""
        if hasattr(cls, 'TABLE_NAME'):
            return cls.TABLE_NAME
        return cls.__name__

    @classmethod
    def get_mock_index_keys(cls) -> list[list[str]]:
        keys = []
        pks = list(x for x in cls.get_primary_keys())
        pks.sort()
        keys.append(pks)
        if hasattr(cls, 'MOCK_INDEX_KEYS'):
            keys.extend(sorted(list(x)) for x in cls.MOCK_INDEX_KEYS)
        return keys

    @classmethod
    def get_primary_keys(cls) -> t.Sequence[str]:
        """Get the list of primary keys."""
        if hasattr(cls, 'PRIMARY_KEYS'):
            return cls.PRIMARY_KEYS
        return []

    @classmethod
    def find_all(cls, db: interface.NODBInstance, **kwargs) -> t.Iterable[t.Self]:
        """Find all workflows."""
        yield from db.stream_objects(cls, **kwargs)

    def get_for_db(self, item: str) -> t.Any:
        """Get an item from the data dictionary for insertion into the database."""
        sd = self.get_sanitized_data(item)
        if not (sd is None or isinstance(sd, (int, str, bool, float, bytes, bytearray))):
            raise NODBValidationError(f"Invalid export value [{sd.__class__.__name__}] for value [{self.__class__.__name__}:{item}]", 9000)
        return sd

    def set_from_db(self, item: str, value: t.Any):
        try:
            self.set_from_managed_name(value, item)
        except TypeError:
            pass


class MetadataMixin(NODBBaseObject):

    metadata: dict[str, ct.SupportsExtendedJson] = JsonDictColumn()

    def set_metadata(self, key: str, value: ct.SupportsExtendedJson):
        """Set a metadata property."""
        if self.metadata is None:
            self.metadata = {key: value}
            self.modified_values.add("metadata")
        else:
            self.metadata[key] = value
            self.modified_values.add("metadata")

    def delete_metadata(self, key: str):
        """Clear a metadata property."""
        if key not in self.metadata:
            return
        del self.metadata[key]
        self.modified_values.add("metadata")

    def get_metadata(self, key: str, default: t.Any = None) -> t.Any:
        """Get a metadata property."""
        if key not in self.metadata:
            return default
        return self.metadata[key]


class NODBRelationship:

    def __init__(self, obj_cls_a: interface.NODBObjectType, obj_cls_b: interface.NODBObjectType, relation_table: str):
        self.obj_cls_a = obj_cls_a
        self.obj_cls_b = obj_cls_b
        self.relation_table = relation_table

    def load_for_parent(self, db: interface.NODBInstance, parent_object: interface.NODBObject, **kwargs) -> t.Iterable[
        interface.NODBObject]:
        if "filters" not in kwargs:
            kwargs["filters"] = {}
        for pk in parent_object.get_primary_keys():
            kwargs["filters"][f"{parent_object.get_table_name()}.{pk}"] = parent_object.get_for_db(pk)
        if isinstance(parent_object, self.obj_cls_a):
            other_type = self.obj_cls_b
        elif isinstance(parent_object, self.obj_cls_b):
            other_type = self.obj_cls_a
        else:
            raise TypeError(
                f"Invalid parent object type, found [{parent_object.__class__.__name__}] expecting [{self.obj_cls_b.__name__}] or [{self.obj_cls_a.__name__}")
        yield from db.stream_relation_objects(
            obj_cls=other_type,
            relation_table=self.relation_table,
            relation_keys={
                f"{other_type.get_table_name()}.{pk}": f"{self.relation_table}.{pk}"
                for pk in other_type.get_primary_keys()
            }
        )
