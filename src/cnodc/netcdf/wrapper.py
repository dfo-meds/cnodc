from __future__ import annotations
import typing as t
from os import PathLike
import enum

import netCDF4 as nc
import numpy as np


class DataType(enum.Enum):

    Float = "f4"
    Double = "f8"
    Long = "i8"
    Integer = "i4"
    Short = "i2"
    Byte = "i1"
    UnsignedLong = "u8"
    UnsignedInteger = "u4"
    UnsignedShort = "u2"
    UnsignedByte = "u1"
    Character = "S1"


class _Variable:

    def __init__(self, var: nc.Variable):
        self._var = var

    @property
    def dimensions(self):
        return self._var.dimensions

    @property
    def name(self):
        return self._var.name

    @property
    def data_type(self):
        return self._var.datatype

    def attribute(self, attr_name):
        return getattr(self._var, attr_name)

    def range(self):
        data = self._var[:]
        return np.min(data), np.max(data)

    def has_attribute(self, attr_name: str):
        return attr_name in self._var.ncattrs()

    def attributes(self) -> t.Iterable[tuple[str, t.Any]]:
        for n in self._var.ncattrs():
            yield n, getattr(self._var, n)

    def set_attribute(self, attr_name, attr_value):
        setattr(self._var, attr_name, attr_value)

    def data(self):
        return self._var[:]

    def set_data(self, data):
        self._var[:] = data

    def set_data_from_string(self, data: str):
        self._var[:] = np.array([data], dtype=object)

    def as_string(self):
        return b''.join(bytes(x) for x in self._var[:]).replace(b'\x00', b'').decode('utf-8')

    def all_as_strings(self):
        return [b''.join(bytes(y) for y in x).strip(b'\x00').decode('utf-8') for x in self.data()]


class Dataset:
    """ A slightly nicer wrapper around the NetCDF file. """

    def __init__(self, path: PathLike, mode: str, **kwargs):
        self._handle: t.Optional[nc.Dataset] = None
        self._file_path = path
        self._args = {
            "mode": mode
        }
        self._args.update(kwargs)

    def has_attribute(self, attr_name):
        return attr_name in self._handle.ncattrs()

    def has_variable(self, var_name):
        return var_name in self._handle.variables

    def data(self, var_name):
        return self._handle.variables[var_name][:]

    def decode_string_variable(self, var_name, encoding='utf-8'):
        if hasattr(self._handle.variables[var_name], '_Encoding'):
            encoding = self._handle.variables[var_name]['_Encoding']
        return ''.join(bytes(x).decode(encoding) for x in self._handle.variables[var_name][:] if bytes(x) > b'0').strip("\0 \r\n\t")

    def variable(self, var_name) -> _Variable:
        return _Variable(self._handle.variables[var_name])

    def copy_data(self, dataset, var_name: str, new_name: t.Optional[str] = None):
        self._handle.variables[new_name or var_name][:] = dataset._handle.variables[var_name][:]

    def copy_attributes(self, ds: Dataset, exclude_attrs: t.Optional[list[str]] = None):
        for attr_name, attr_value in ds.attributes():
            if attr_name not in exclude_attrs:
                self.set_attribute(attr_name, attr_value)

    def attributes(self) -> t.Iterable[tuple[str, t.Any]]:
        for attr_name in self._handle.ncattrs():
            yield attr_name, getattr(self._handle, attr_name)

    def attribute(self, attr_name: str):
        return getattr(self._handle, attr_name)

    def variables(self) -> t.Iterable[_Variable]:
        for vname in self._handle.variables:
            yield _Variable(self._handle.variables[vname])

    def set_attributes(self, attrs: dict[str, t.Any]):
        for key in attrs:
            self.set_attribute(key, attrs[key])

    def set_attribute(self, attr_name, attr_value):
        setattr(self._handle, attr_name, attr_value)

    def create_from_dict(self, netcdf_structure: dict):
        if 'dimensions' in netcdf_structure and netcdf_structure['dimensions']:
            for dim in netcdf_structure['dimensions']:
                self.create_dimension(dim, netcdf_structure['dimensions'][dim])
        if 'variables' in netcdf_structure and netcdf_structure['variables']:
            for var_name in netcdf_structure['variables']:
                if isinstance(netcdf_structure['variables'][var_name], dict):
                    self.create_variable(var_name, **netcdf_structure['variables'][var_name])
                else:
                    self.create_variable(var_name, *netcdf_structure['variables'][var_name])
        if 'attributes' in netcdf_structure and netcdf_structure['attributes']:
            self.set_attributes(netcdf_structure['attributes'])

    def create_dimension(self, dim_name: str, size: t.Optional[int] = None):
        self._handle.createDimension(dim_name, size)

    def copy_variable(self, var: _Variable, new_name: t.Optional[str] = None):
        self.create_variable(
            new_name or var.name,
            var.data_type,
            var.dimensions,
            {n: v for n, v in var.attributes()}
        )

    def create_variable(self,
                        var_name: str,
                        data_type,
                        dimensions: t.Optional[tuple],
                        attributes: dict = None,
                        *args,
                        **kwargs):
        attributes = attributes or {}
        dtype = data_type.value if isinstance(data_type, DataType) else data_type
        if '_FillValue' in attributes:
            attributes['missing_value'] = attributes['_FillValue']
            var = self._handle.createVariable(var_name, dtype, dimensions or [], *args, fill_value=attributes.pop('_FillValue'), **kwargs)
        else:
            if 'missing_value' not in attributes and var_name in nc.default_fillvals:
                attributes['missing_value'] = nc.default_fillvals[dtype]
            var = self._handle.createVariable(var_name, dtype, dimensions or [], *args, **kwargs)
        for attr_name in attributes:
            var.setncattr(attr_name, attributes[attr_name])
        return _Variable(var)

    def open(self) -> Dataset:
        self._handle = nc.Dataset(self._file_path, **self._args)

    def close(self):
        self._handle.close()

    def __enter__(self) -> Dataset:
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()





