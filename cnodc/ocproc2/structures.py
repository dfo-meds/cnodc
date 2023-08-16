import itertools
import typing as t
import enum
import datetime


def normalize_data_value(dv: t.Any):
    if isinstance(dv, datetime.date):
        return dv.isoformat()
    elif isinstance(dv, (set, list, tuple)):
        return [normalize_data_value(x) for x in dv]
    elif isinstance(dv, dict):
        return {
            str(x): normalize_data_value(dv[x])
            for x in dv
        }
    return dv


class NODBQCFlag(enum.Enum):

    NOT_DONE = 0
    GOOD = 1
    PROBABLY_GOOD = 2
    DOUBTFUL = 3
    BAD = 4
    CHANGED = 5
    MISSING = 9
    FOR_REVIEW = 'R'


class DataValue:

    def __init__(self,
                 reported_value=None,
                 corrected_value=None,
                 metadata: dict = None):
        self.reported_value = normalize_data_value(reported_value)
        self.corrected_value = normalize_data_value(corrected_value)
        self.metadata = DataValueMap(metadata, ref_table="metadata")

    def __str__(self):
        return str(self.value())

    def __eq__(self, other):
        return (self.reported_value == other.reported_value and
                self.corrected_value == other.corrected_value and
                all(x in other.metadata for x in self.metadata) and
                all(x in self.metadata for x in other.metadata) and
                all(self.metadata[x] == other.metadata[x] for x in self.metadata))

    def nodb_flag(self):
        if 'NODB_QC' in self.metadata:
            return NODBQCFlag(self.metadata['NODB_QC'].value())
        else:
            return None

    def set_nodb_flag(self, nodb_flag: NODBQCFlag):
        self.metadata['NODB_QC'] = nodb_flag.value()

    def qc_done(self):
        return 'NODB_QC' in self.metadata and self.metadata['NODB_QC'].value() not in (0, 'R')

    def set(self, key, value):
        self.metadata[key] = value

    def empty(self):
        return self.reported_value is None and self.corrected_value is None

    def to_mapping(self):
        if self.metadata.empty():
            if self.corrected_value is None and self.reported_value is None:
                return None
            elif self.corrected_value is None:
                if not isinstance(self.corrected_value, (list, dict)):
                    return self.reported_value
                else:
                    return [self.reported_value]
            else:
                return [self.reported_value, self.corrected_value]
        else:
            map_ = {
                'M': self.metadata.to_mapping()
            }
            if self.reported_value is not None:
                map_['R'] = self.reported_value
            if self.corrected_value is not None:
                map_['C'] = self.corrected_value
            return map_

    def from_mapping(self, map_: t.Union[dict, list, None]):
        if isinstance(map_, dict):
            if 'R' in map_:
                self.reported_value = map_['R']
            if 'C' in map_:
                self.corrected_value = map_['C']
            if 'M' in map_:
                self.metadata.from_mapping(map_['M'])
        elif isinstance(map_, list):
            self.reported_value = map_[0]
            if len(map_) > 1:
                self.corrected_value = map_[1]
        else:
            self.reported_value = map_
        return self

    def value(self):
        if self.corrected_value is not None:
            return self.corrected_value
        return self.reported_value

    def pretty(self, prefix="", nested="  "):
        extras = []
        if self.corrected_value:
            extras.append(f"corrected={self.corrected_value}")
        if self.metadata:
            extras.extend(f"{k}={self.metadata[k].pretty('', '')}" for k in self.metadata)
        if extras:
            return f"{self.reported_value} [{';'.join(extras)}]"
        else:
            return f"{self.reported_value}"

    @staticmethod
    def wrap(val):
        if isinstance(val, DataValue):
            return val
        else:
            return DataValue(val)


class DataValueMap:

    def __init__(self, map_: dict = None, ref_table: str = None):
        self._ref_table = ref_table
        self._map: dict[str, DataValue] = {}
        if map_:
            self.update(map_)

    def __str__(self):
        return '{' + ",".join(f"{k}: {str(self._map[k])}" for k in self._map) + '}'

    def update(self, existing):
        if isinstance(existing, DataValueMap):
            for k in existing._map:
                self[k] = existing._map[k]
        else:
            for k in existing:
                self[k] = existing[k]

    def to_mapping(self):
        return {
            k: self._map[k].to_mapping()
            for k in self._map
        }

    def from_mapping(self, map_: dict):
        for k in map_ or {}:
            self._map[k] = DataValue()
            self._map[k].from_mapping(map_[k])
        return self

    def pretty(self, prefix='', nested='  '):
        return "\n".join(f"{prefix}{k}: {self._map[k].pretty(prefix, nested)}" for k in self._map)

    def __contains__(self, item: str):
        return item in self._map

    def __setitem__(self, key: str, value):
        self._map[key] = DataValue.wrap(value)

    def __getitem__(self, key: t.Union[str, enum.Enum]) -> DataValue:
        return self._map[key]

    def __delitem__(self, key):
        del self._map[key]

    def __iter__(self) -> t.Iterable[str]:
        yield from sorted(self._map.keys())

    def __bool__(self):
        return bool(self._map)

    def as_map(self):
        return self._map

    def empty(self):
        return not bool(self._map)

    def get(self, item, default=None):
        if item in self._map:
            return self._map[item]
        return default

    @staticmethod
    def compress_maps(dvm_list: list):
        values = None
        for dvm in dvm_list:
            if values is None:
                values = {}
                values.update(dvm.as_map())
            else:
                found = set()
                for item in dvm:
                    if item not in values:
                        continue
                    elif dvm[item] != values[item]:
                        del values[item]
                    else:
                        found.add(item)
                for x in values:
                    if x not in found:
                        del values[x]
        for dvm in dvm_list:
            for item in values:
                del dvm[item]
        if values:
            return {x: values[x].to_mapping() for x in values}
        else:
            return {}


class DataRecord:

    def __init__(self):
        self.coordinates = DataValueMap(ref_table='coordinates')
        self.variables = DataValueMap(ref_table='parameters')
        self.metadata = DataValueMap(ref_table='metadata')
        self.subrecords = DataRecordMap()

    def __str__(self):
        return str({
            'c': str(self.coordinates),
            'v': str(self.variables),
            'm': str(self.metadata),
            's': str(self.subrecords)
        })

    def to_mapping(self, compact: bool = False) -> dict:
        if compact:
            keys = [k for k in self.subrecords]
            keys.reverse()
            for key in keys:
                if key in self.subrecords:
                    self.subrecords.merge_check(key)
        map_ = {
            'C': self.coordinates.to_mapping()
        }
        if not self.variables.empty():
            map_['P'] = self.variables.to_mapping()
        if not self.metadata.empty():
            map_['M'] = self.metadata.to_mapping()
        if not self.subrecords.empty():
            map_['S'] = self.subrecords.to_mapping(compact=compact)
        return map_

    def from_mapping(self, map_: dict):
        if 'C' in map_:
            self.coordinates.from_mapping(map_['C'])
        if 'P' in map_:
            self.variables.from_mapping(map_['P'])
        if 'M' in map_:
            self.metadata.from_mapping(map_['M'])
        if 'S' in map_:
            self.subrecords.from_mapping(map_['S'])
        return self

    def pretty(self, prefix='', nested='  '):
        s = []
        if not self.coordinates.empty():
            s.append(f"{prefix}coordinates:")
            s.append(self.coordinates.pretty(prefix + '  ', nested))
        if not self.variables.empty():
            s.append(f"{prefix}variables:")
            s.append(self.variables.pretty(prefix + '  ', nested))
        if not self.metadata.empty():
            s.append(f"{prefix}metadata:")
            s.append(self.metadata.pretty(prefix + '  ', nested))
        if not self.subrecords.empty():
            s.append(f"{prefix}subrecords:")
            s.append(self.subrecords.pretty(prefix + '  ', nested))
        return "\n".join(s)

    def find_child_record(self, child_key: str, properties: dict):
        if child_key in self.subrecords:
            return self.subrecords[child_key].find_record(properties)
        else:
            return None

    def new_subrecord_set(self, child_prefix: str):
        return self.subrecords.next_key_name(child_prefix)

    def merge_subrecord(self, child_key: str, record):
        existing = self.find_child_record(child_key, {
            x: record.coordinates[x].value()
            for x in record.coordinates
        })
        if existing is not None:
            existing.metadata.update(record.metadata)
            existing.variables.update(record.variables)
            if record.subrecords:
                print("no!")
        else:
            self.subrecords.append(child_key, record)


class RecordSet:

    def __init__(self):
        self.records: list[DataRecord] = []
        self.metadata = DataValueMap()

    def __str__(self):
        return '[' + ','.join(str(r) for r in self.records) + ']'

    def __iter__(self):
        return iter(self.records)

    def __len__(self):
        return len(self.records)

    def _compact(self):
        if len(self.records) < 2:
            return {}
        map_ = {}
        metadata_compacted = DataValueMap.compress_maps([r.metadata for r in self.records])
        if metadata_compacted:
            map_['N'] = metadata_compacted
        coord_names = set()
        var_names = set()
        for r in self.records:
            coord_names.update(x for x in r.coordinates)
            var_names.update(x for x in r.variables)
        c_map = {}
        for cn in coord_names:
            compacted = DataValueMap.compress_maps([r.coordinates[cn].metadata for r in self.records if cn in r.coordinates])
            if compacted:
                c_map[cn] = compacted
        if c_map:
            map_['D'] = c_map
        v_map = {}
        for vn in var_names:
            compacted = DataValueMap.compress_maps([r.variables[vn].metadata for r in self.records if vn in r.variables])
            if compacted:
                v_map[vn] = compacted
        if v_map:
            map_['W'] = v_map
        return map_

    def _uncompact(self, compacted_metadata: dict = None, coord_metadata: dict = None, var_metadata: dict = None):
        for r in self.records:
            for k in compacted_metadata or {}:
                dv = DataValue()
                dv.from_mapping(compacted_metadata[k])
                r.metadata[k] = dv
            for cn in coord_metadata or {}:
                if cn in r.coordinates:
                    for k in coord_metadata[cn]:
                        dv = DataValue()
                        dv.from_mapping(coord_metadata[cn][k])
                        r.coordinates[cn].metadata[k] = dv
            for vn in var_metadata or {}:
                if vn in r.variables:
                    for k in var_metadata[vn]:
                        dv = DataValue()
                        dv.from_mapping(var_metadata[vn][k])
                        r.variables[vn].metadata[k] = dv

    def to_mapping(self, compact: bool = True) -> t.Union[list, dict[str, t.Union[list, dict]]]:
        map_: dict[str, t.Union[list, dict]] = self._compact() if compact else {}
        if not self.metadata.empty():
            map_['E'] = self.metadata.to_mapping()
        if not map_:
            return [x.to_mapping() for x in self.records]
        else:
            map_['R'] = [x.to_mapping() for x in self.records]
            return map_

    def from_mapping(self, map_: t.Union[list, dict]):
        if isinstance(map_, dict):
            if 'R' in map_:
                self.records = []
                for r in map_['R']:
                    dr = DataRecord()
                    dr.from_mapping(r)
                    self.records.append(dr)
            if 'E' in map_:
                self.metadata.from_mapping(map_['E'])
            self._uncompact(
                compacted_metadata=map_['N'] if 'N' in map_ else {}
            )
        else:
            self.records = []
            for r in map_:
                dr = DataRecord()
                dr.from_mapping(r)
                self.records.append(dr)
        return self

    def find_record(self, c: dict):
        for rec in self.records:
            if all(x in rec.coordinates and c[x] == rec.coordinates[x].value() for x in c):
                return rec
        return None

    def _normalize_record_storage(self):
        if not isinstance(self.records, list):
            self.records = [x for x in self.records]

    def pretty(self, prefix='', nested='  '):
        self._normalize_record_storage()
        s = []
        for idx, r in enumerate(self.records):
            s.append(f"{prefix}{idx}:")
            s.append(r.pretty(prefix + nested, nested))
        return "\n".join(s)

    def append(self, r: DataRecord):
        self._normalize_record_storage()
        self.records.append(r)


class DataRecordMap:

    def __init__(self):
        self._map: dict[str, RecordSet] = {}

    def __str__(self):
        return '{' + ','.join(f"{k}: {str(self._map[k])}" for k in self._map) + "}"

    def merge_check(self, key: str, is_prefixed: bool = True):
        prefix = key[:key.rfind('_')] if is_prefixed else key
        delete = False
        for k in self._map:
            if k == key:
                continue
            if not k.startswith(prefix):
                continue
            if self._attempt_merge(k, key):
                break

    def _attempt_merge(self, key_old: str, key_new: str) -> bool:
        # Ensure the metadata is the same so we aren't accidently applying different metadata
        rs_old = self._map[key_old]
        rs_new = self._map[key_new]
        # Check that the len matches
        if len(rs_old) != len(rs_new):
            return False
        # Check metadata matches between the two
        for mn in rs_old.metadata:
            if not (mn in rs_new.metadata and rs_new.metadata[mn] == rs_old.metadata[mn]):
                return False
        for mn in rs_new.metadata:
            if not (mn in rs_old.metadata and rs_new.metadata[mn] == rs_old.metadata[mn]):
                return False
        # check we have the same coordinates
        for x in range(0, len(rs_new)):
            record_old = self._map[key_old].records[x]
            record_new = self._map[key_new].records[x]
            coords_old = [cn for cn in record_old.coordinates]
            coords_new = [cn for cn in record_new.coordinates]
            # Need to have the same number coordinates
            if len(coords_old) != len(coords_new):
                return False
            # Need all the coordinates in new to exist and be the same in old
            if not all(x in coords_new and record_old.coordinates[x] == record_new.coordinates[x] for x in coords_old):
                return False
            # Need none of the variables to already exist in old (so we don't overwrite)
            if any(x in record_old.variables and not record_old.variables[x] == record_new.variables[x] for x in record_new.variables):
                return False
            # Need none of the metadata to already exist in hold (so we don't overwrite)
            if any(x in record_old.metadata and not record_old.metadata[x] == record_new.metadata[x] for x in record_new.metadata):
                return False
        # Alright, we have the same coordinates (or a truncated subset) and different variables / metadata,
        # we can probably merge them
        return self._do_merge(key_old, key_new)

    def _do_merge(self, key_old: str, key_new: str) -> bool:
        for x in range(0, len(self._map[key_new])):
            record_old = self._map[key_old].records[x]
            record_new = self._map[key_new].records[x]
            record_old.variables.update(record_new.variables)
            record_old.metadata.update(record_new.metadata)
            record_old.subrecords.update(record_new.subrecords)
        del self._map[key_new]
        return True

    def update(self, rs):
        keys = [x for x in rs]
        keys.sort()
        for key in rs:
            if key in self._map:
                key = self.next_key_name(key[:key.rfind('_')])
            self._map[key] = rs

    def next_key_name(self, prefix):
        i = 0
        key = f"{prefix}_{i}"
        while key in self._map:
            i += 1
            key = f"{prefix}_{i}"
        return key

    def pretty(self, prefix='', nested='  '):
        s = []
        for k in self._map:
            s.append(f"{prefix}{k}:")
            s.append(self._map[k].pretty(prefix + nested, nested))
        return "\n".join(s)

    def to_mapping(self, compact: bool = False):
        return {
            k: self._map[k].to_mapping(compact=compact)
            for k in self._map
        }

    def from_mapping(self, map_: dict):
        for k in map_:
            self._map[k] = RecordSet()
            self._map[k].from_mapping(map_[k])

    def __contains__(self, item: str):
        return item in self._map

    def __getitem__(self, key: str) -> RecordSet:
        return self._map[key]

    def __setitem__(self, key: str, item: RecordSet):
        self._map[key] = item

    def __iter__(self) -> t.Iterable[str]:
        yield from sorted(self._map.keys())

    def append(self, key: str, record: DataRecord):
        if key not in self._map:
            self._map[key] = RecordSet()
        self._map[key].append(record)

    def empty(self):
        return not bool(self._map)

    def __bool__(self):
        return bool(self._map)
