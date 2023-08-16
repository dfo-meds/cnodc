from autoinject import injector
import pathlib
import csv
import typing as t


CONVERSION_FACTOR = tuple[t.Optional[float], t.Optional[float]]


@injector.injectable_global
class ReferenceTables:

    def __init__(self):
        self._tables = {}
        self._fast_lookup_by_lname = {}
        self._conversions = {}
        self._unit_dims = {}
        root = pathlib.Path(__file__).absolute().parent
        with open(root / "tables.csv", "r", encoding="utf-8-sig") as h:
            reader = csv.reader(h)
            for row in reader:
                if len(row) < 4:
                    continue
                if row[0] == 'Table' and row[3] == 'Description':
                    continue
                row[0] = row[0].lower()
                row[1] = row[1].upper()
                if row[0] not in self._tables:
                    self._tables[row[0]] = {}
                self._tables[row[0]][str(row[1])] = row[2]

        with open(root / "units.csv", "r", encoding="utf-8-sig") as h:
            reader = csv.reader(h)
            for row in reader:
                if len(row) < 5:
                    continue
                if row[0] == 'Quantity' and row[-1] == 'Offset':
                    continue
                self._unit_dims[row[1]] = row[0]
                self._unit_dims[row[2]] = row[0]
                if row[1] not in self._conversions:
                    self._conversions[row[1]] = {}
                self._conversions[row[1]][row[2]] = (float(row[3]), float(row[4]))

    def convert(self, val, unit_a, unit_b):
        m, b = self._lookup_conversion(unit_a, unit_b)
        if m is None:
            raise ValueError(f"Cannot convert {unit_a} to {unit_b}")
        return (val * m) + b

    def can_convert(self, unit_a, unit_b):
        m, b = self._lookup_conversion(unit_a, unit_b)
        return m is not None

    def _lookup_conversion(self, unit_a, unit_b) -> CONVERSION_FACTOR:
        # Same units
        if unit_a == unit_b:
            return 1, 0
        # One unit is unrecognized
        if unit_a not in self._unit_dims or unit_b not in self._unit_dims:
            return None, None
        # The two units do not refer to the same physical quantity
        if self._unit_dims[unit_a] != self._unit_dims[unit_b]:
            return None, None
        m, b = self._direct_conversion(unit_a, unit_b)
        if m is not None:
            return m, b
        # Indirect Path
        return self._indirect_conversion(unit_a, unit_b)

    def _direct_conversion(self, unit_a, unit_b) -> CONVERSION_FACTOR:
        # Direct Formula
        if unit_a in self._conversions and unit_b in self._conversions[unit_a]:
            return self._conversions[unit_a][unit_b]
        # Inverse Formula
        if unit_b in self._conversions and unit_a in self._conversions[unit_b]:
            m, b = self._conversions[unit_b][unit_a]
            if unit_a not in self._conversions:
                self._conversions[unit_a] = {}
            self._conversions[unit_a][unit_b] = (1 / m, -b / m)
            return self._conversions[unit_a][unit_b]
        return None, None

    def _indirect_conversion(self, unit_a, unit_b) -> CONVERSION_FACTOR:
        # A => x1 => x2 => ... xy => xz => B
        candidates = [(unit_a,)]
        checked = {unit_a}
        check_again = True
        while check_again:
            new_c = []
            check_again = False
            for candidate in candidates:
                options = set()
                if candidate[-1] in self._conversions:
                    options.update(list(self._conversions[candidate[-1]].keys()))
                options.update(x for x in self._conversions if candidate[-1] in self._conversions[x])
                for opt in options:
                    if opt in checked:
                        continue
                    elif opt == unit_b:
                        return self._set_indirect_conversion(*candidate, opt)
                    else:
                        new_c.append((*candidate, opt))
                        checked.add(opt)
                        check_again = True
        return None, None

    def _set_indirect_conversion(self, *chain) -> CONVERSION_FACTOR:
        m = 1
        b = 0
        first_unit = chain[0]
        last_unit = chain[0]
        for unit_n in chain[1:]:
            m2, b2 = self._direct_conversion(last_unit, unit_n)
            m = m * m2
            b = (m * b) + b2
            last_unit = unit_n
        if first_unit not in self._conversions:
            self._conversions[first_unit] = {}
        self._conversions[first_unit][last_unit] = (m, b)
        return m, b

    def exists(self, table_name, short_name):
        return table_name in self._tables and short_name in self._tables[table_name]
