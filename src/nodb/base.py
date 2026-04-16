import datetime
import typing as t

import nodb.interface as interface
from medsutil.cached import CachedObjectMixin
import medsutil.datadict as ddo
import medsutil.types as ct
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


class NODBBaseObject(ddo.DataDictObject, CachedObjectMixin, interface.NODBObject):
    """Base class for all NODB objects.

        This provides a lot of tools for building database classes.
    """

    def __init__(self, *, is_new: bool = True, **kwargs):
        self._modified_values: set[str] = set()
        self.is_new = is_new
        super().__init__(**kwargs)
        if not is_new:
            # Reset modified values if we loaded an original object
            # so we don't update all the values all the time.
            self._modified_values.clear()

    def after_set(self, managed_name: str, value: t.Any, original: t.Any = None):
        super().after_set(managed_name, value, original)
        if original != value:
            self.mark_modified(managed_name)

    @classmethod
    def get_table_name(cls):
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

    @property
    def modified_values(self) -> set[str]:
        return self._modified_values

    def mark_modified(self, item: str):
        """Mark an item as modified"""
        self._modified_values.add(item)

    def clear_modified(self):
        """Clear the set of modified values."""
        self._modified_values.clear()

    def get_for_db(self, item: str) -> interface.SupportsPostgres:
        """Get an item from the data dictionary for insertion into the database."""
        sd = self.get_sanitized_data(item)
        if not (sd is None or isinstance(sd, (int, str, bool, float))):
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
