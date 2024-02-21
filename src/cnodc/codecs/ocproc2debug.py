import json
import math
from datetime import datetime

import yaml

from .base import BaseCodec, ByteIterable, DecodeResult, ByteSequenceReader, EncodeResult
import typing as t

from ..util import HaltInterrupt, CNODCError
import cnodc.ocproc2 as ocproc2


class OCProc2DebugCodec(BaseCodec):

    FILE_EXTENSION = ('.txt',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, log_name="cnodc.codecs.debug", is_encoder=True, is_decoder=False, **kwargs)

    def _encode(self,
                record: ocproc2.ParentRecord,
                **kwargs) -> t.Iterable[bytes]:
        yield self._record_to_text(record).encode('utf-8')

    def _record_to_text(self, record: ocproc2.ParentRecord):
        s = '--START RECORD--\n'
        if record.metadata:
            s += '\n  METADATA\n'
            for x in sorted(record.metadata.keys()):
                s += self._encode_element(x, record.metadata[x])
                s += "\n"
        if record.coordinates:
            s += '\n  COORDINATES\n'
            for x in sorted(record.coordinates.keys()):
                s += self._encode_element(x, record.coordinates[x])
                s += "\n"
        if record.parameters:
            s += '\n  SURFACE PARAMETERS\n'
            for x in sorted(record.parameters.keys()):
                s += self._encode_element(x, record.parameters[x])
                s += "\n"
        if record.subrecords:
            for srt in sorted(record.subrecords.record_sets.keys()):
                for rs_idx in record.subrecords.record_sets[srt]:
                    s += self._encode_profile(srt, rs_idx, record.subrecords[srt][rs_idx])

        if record.history:
            s += '\n  HISTORY\n'
            for h in record.history:
                s += '    '
                s += datetime.fromisoformat(h.timestamp).strftime('%Y-%m-%d %H:%M')
                s += ' '
                s += h.message
                s += f' [{h.message_type.value} from {h.source_name}:{h.source_version}:{h.source_instance}]'
                s += '\n'
            s += '\n'
        s += '--END RECORD--\n'
        return s

    def _encode_profile(self, record_type, record_idx, rs: ocproc2.RecordSet):
        s = ''
        if rs.records or rs.metadata:
            s += f'\n  {record_type} {record_idx}\n'
        if rs.metadata:
            s += '    METADATA\n'
            for x in rs.metadata:
                s += self._encode_element(x, rs.metadata[x], prefix='      ')
                s += "\n"
        if rs.records:
            rs_count = len(rs.records)
            s += f'    TABULAR DATA n={rs_count}\n'
            m_common = {x: rs.records[0].metadata[x] for x in rs.records[0].metadata.keys()}
            for sr in rs.records:
                remove_keys = []
                for mk in m_common:
                    if mk not in sr.metadata:
                        remove_keys.append(mk)
                    elif sr.metadata[mk] != m_common[mk]:
                        remove_keys.append(mk)
                for mk in remove_keys:
                    del m_common[mk]
                if not m_common:
                    break
            m_headers = set()
            for sr in rs.records:
                m_headers.update(mk for mk in sr.metadata.keys() if mk not in m_common)
            m_headers = [x for x in m_headers]
            m_values = {mk: [] for mk in m_headers}
            for sr in rs.records:
                for mk in m_headers:
                    m_values[mk].append(self._encode_element(mk, sr.metadata[mk], prefix=''))
            m_sizes = {}
            for mk in m_headers:
                m_sizes[mk] = max(len(x) for x in m_values[mk])
                if len(mk) > m_sizes[mk]:
                    m_sizes[mk] = len(mk)

            c_headers, c_display, c_sizes, c_common = self._build_table(rs, 'coordinates')
            p_headers, p_display, p_sizes, p_common = self._build_table(rs, 'parameters')
            c_keys = [x for x in c_display.keys()]
            c_keys.sort()
            p_keys = [x for x in p_display.keys()]
            p_keys.sort()
            m_keys = [x for x in m_headers]
            m_keys.sort()
            s += '      '
            no_col_len = len(str(rs_count))
            s += '#'.rjust(no_col_len, ' ')
            s += ' ' + ' '.join(c_headers[x].rjust(c_sizes[x], ' ') for x in c_keys)
            s += ' ' + ' '.join(p_headers[x].rjust(p_sizes[x], ' ') for x in p_keys)
            s += ' ' + ' '.join(x.rjust(m_sizes[x], ' ') for x in m_keys)
            add_note = False
            for i in range(0, rs_count):
                s += '\n      '
                s += str(i+1).rjust(no_col_len, ' ')
                s += ' ' + ' '.join(c_display[x][i].rjust(c_sizes[x], ' ') for x in c_keys)
                s += ' ' + ' '.join(p_display[x][i].rjust(p_sizes[x], ' ') for x in p_keys)
                s += ' ' + ' '.join(m_values[x][i].rjust(m_sizes[x], ' ') for x in m_keys)
                if rs.records[i].subrecords.record_sets:
                    s += ' *****'
                    add_note = True
            s += "\n\n"
            if c_common or p_common or m_common:
                s += '    COMMON METADATA FOR PRECEDING TABLE\n'
                for mn in sorted(m_common):
                    s += f'      {mn} = {self._encode_element(mn, m_common[mn], "", skip_name=True)}\n'
                for cn in c_keys:
                    if cn not in c_common:
                        continue
                    for mn in sorted(c_common[cn]):
                        s += f'      {cn}[{mn}] = {self._encode_element(mn, c_common[cn][mn], "", skip_name=True)}\n'
                for pn in p_keys:
                    if pn not in p_common:
                        continue
                    for mn in sorted(p_common[pn]):
                        s += f'      {pn}[{mn}] = {self._encode_element(mn, p_common[pn][mn], "", skip_name=True)}\n'
                if add_note:
                    s += f'      ***** indicates sub-records not shown on this output'
        s += '\n'
        return s

    def _build_table(self, rs, group: str):
        c_keys = set()
        for sr in rs.records:
            c_keys.update(getattr(sr, group).keys())
        c_values = {x: [] for x in c_keys}
        for sr in rs.records:
            vmap = getattr(sr, group)
            for x in c_keys:
                c_values[x].append(vmap[x] if x in vmap else None)
        c_common = {}
        for x in c_values:
            c_common[x] = {}
            sub_bvs = [y for y in c_values[x] if y is not None]
            if not sub_bvs:
                continue
            for m in sub_bvs[0].metadata:
                if all(sub_bvs[k].metadata[m] == sub_bvs[0].metadata[m] for k in range(1, len(sub_bvs))):
                    c_common[x][m] = sub_bvs[0].metadata[m]
        c_display = {}
        c_sizes = {}
        for x in c_values:
            c_ignore = [y for y in c_common[x].keys()]
            c_display[x] = [self._encode_element(x, y, '', c_ignore, True) if y is not None else '' for y in
                            c_values[x]]
            c_sizes[x] = max(len(x) for x in c_display[x])

        c_headers = {}
        for x in c_values:
            h = x
            if 'Units' in c_common[x]:
                h += ' [' + c_common[x]['Units'].to_string() + ']'
                del c_common[x]['Units']
                if not c_common[x]:
                    del c_common[x]
            c_headers[x] = h
            if len(h) > c_sizes[x]:
                c_sizes[x] = len(h)
        return c_headers, c_display, c_sizes, c_common

    def _encode_element(self, name: str, v: ocproc2.AbstractElement, prefix='    ', ignore_parameters: t.Optional[list] = None, skip_name: bool = False):
        if ignore_parameters is None:
            ignore_parameters = []
        s = f'{prefix}'
        if not skip_name:
            s += f'{name} = '
        if isinstance(v, ocproc2.MultiElement):
            s += '['
            s += '; '.join(self._encode_element(name, v2, prefix='', ignore_parameters=ignore_parameters, skip_name=True) for v2 in v.all_values())
            s += ']'
        else:
            int_flag = False
            if v.value is None:
                s += '(null)'
            elif 'Uncertainty' in v.metadata and v.is_numeric() and v.metadata['Uncertainty'].is_numeric():
                unc = v.metadata['Uncertainty'].to_float()
                places = int(math.floor(-1 * math.log10(unc)))
                if places >= 0:
                    format_str = "{:." + str(places) + "f}"
                    s += format_str.format(v.to_float())
                else:
                    s += str(v.to_int())
                    int_flag = True
            else:
                s += v.to_string()
            if 'Uncertainty' in v.metadata and 'Uncertainty' not in ignore_parameters:
                if not int_flag:
                    s += f' ± {v.metadata["Uncertainty"].value}'
                else:
                    s += f' ± {v.metadata["Uncertainty"].to_int()}'
            if 'Units' in v.metadata and 'Units' not in ignore_parameters:
                s += f' {v.metadata["Units"].value}'
            if 'Quality' in v.metadata and 'Quality' not in ignore_parameters:
                s += f" [Q={v.metadata['Quality'].value}]"
            other = [x for x in v.metadata if x not in ('Uncertainty', 'Units', 'Quality') and x not in ignore_parameters]
            if other:
                s += ' {'
                s += ';'.join(self._encode_element(x, v.metadata[x], '') for x in other)
                s += '}'
        return s


