import functools
import typing as t

from medsutil import types as ct
from medsutil.halts import HaltFlag
from medsutil.vlq import vlq_decode


class ByteSequenceReader:

    def __init__(self, raw_bytes: ct.ByteStrings, halt_flag: HaltFlag = None):
        self._halt_flag = halt_flag
        self._raw_data = raw_bytes
        self._iterator = None
        self._buffer = bytearray()
        self._buffer_length = 0
        self._last_check = 0
        self._offset = 0
        self._complete = False

    def iterate_rest(self) -> ct.ByteStrings:
        if self._buffer:
            yield self._buffer
            self._offset += self._buffer_length
            self._buffer = bytearray()
            self._buffer_length = 0
        if not self._complete:
            if self._iterator is None:
                self._iterator = iter(self._raw_data)
            next_item = next(self._iterator, None)
            while next_item is not None:
                self._offset += len(next_item)
                yield next_item
                next_item = next(self._iterator, None)
            self._complete = True

    def check_continue(self):
        if self._halt_flag is not None:
            self._last_check += 1
            if self._last_check >= 50:
                self._last_check = 0
                self._halt_flag.check_continue(True)

    def offset(self) -> int:
        return self._offset

    def _read_next(self) -> bool:
        if self._complete:
            return False
        if self._iterator is None:
            self._iterator = iter(self._raw_data)
        next_item = next(self._iterator, None)
        if next_item is None:
            self._complete = True
            return False
        self._buffer.extend(next_item)
        self._buffer_length = len(self._buffer)
        return True

    def _read_rest(self):
        if self._complete:
            return
        if self._iterator is None:
            self._iterator = iter(self._raw_data)
        try:
            next_item = next(self._iterator, None)
            while next_item is not None:
                self._buffer.extend(next_item)
                self.check_continue()
                next_item = next(self._iterator, None)
            self._complete = True
        finally:
            self._buffer_length = len(self._buffer)

    def at_eof(self) -> bool:
        if self._buffer_length > 0:
            return False
        return not self._read_next()

    def _read_up_to(self, actual_index: int) -> bool:
        while actual_index >= self._buffer_length:
            self.check_continue()
            if not self._read_next():
                break
        return actual_index < self._buffer_length

    def _discard_leading(self, length: int):
        if length >= self._buffer_length:
            self._offset += self._buffer_length
            self._buffer_length = 0
            self._buffer = bytearray()
        else:
            self._offset += length
            self._buffer = self._buffer[length:]
            self._buffer_length -= length

    def consume_vlq_int(self) -> int:
        idx = 0
        while True:
            if not self._read_up_to(idx):
                break
            if self._buffer[idx] < 128:
                break
            idx += 1
        return vlq_decode(self.consume(idx + 1))[0]

    def peek(self, length: int) -> t.ByteString:
        self._read_up_to(length + 1)
        return self._buffer[0:length+1]

    def peek_line(self, exclude_line_endings: bool = True) -> t.ByteString:
        n_or_r, index = self._find_first_match_fast({10, 13})
        if index is None:
            return self._buffer
        if not exclude_line_endings:
            index += 1
        return self._buffer[:index]

    def consume_line(self, exclude_line_endings: bool = True) -> t.ByteString:
        res = self.consume_until([b"\n", b"\r"], include_target=True)
        if res[-1] == 13 and self[0] == b"\n":
            self._discard_leading(1)
            if not exclude_line_endings:
                res += b"\n"
        return res.rstrip(b"\r\n") if exclude_line_endings else res

    def consume_lines(self, exclude_line_endings: bool = True) -> ct.ByteStrings:
        while not self.at_eof():
            self.check_continue()
            yield self.consume_line(exclude_line_endings)

    def consume_all(self) -> bytearray:
        self._read_rest()
        res = self._buffer
        self._buffer = bytearray()
        self._offset += self._buffer_length
        self._buffer_length = 0
        return res

    def consume_until(self, matches: t.Union[list, bytes], include_target: bool = False) -> bytearray:
        matching, actual_offset = self.find_first_match([matches] if isinstance(matches, bytes) else matches)
        if actual_offset is None or matching is None:
            return self.consume(self._buffer_length)
        else:
            return self.consume(actual_offset + (len(matching) if include_target else 0))

    def consume(self, length: int) -> bytearray:
        self._read_up_to(length)
        res = self._buffer[0:length]
        self._discard_leading(length)
        return res

    def lstrip(self, bytes_: bytes, max_strip: int = None):
        idx = 0
        while max_strip is None or max_strip > 0:
            if not self._read_up_to(idx):
                break
            if self._buffer[idx] not in bytes_:
                break
            idx += 1
            if max_strip is not None:
                max_strip -= 1
        if idx > 0:
            self._discard_leading(idx)

    def find_first_match(self, matches: t.ByteString | t.Sequence[t.ByteString]) -> tuple[bytes | None, int | None]:
        return self._get_matcher(matches)()

    def split_and_iterate(self, matches: t.ByteString | t.Sequence[t.ByteString], include_target: bool = False):
        matcher = self._get_matcher(matches)
        return self._split_and_iterate(matcher, include_target)

    def _get_matcher(self,
                     matches: t.ByteString | t.Sequence[t.ByteString],
                     ) -> t.Callable[[], tuple[bytes | None, int | None]]:
        m: list[t.ByteString] = [matches] if isinstance(matches, (bytes, bytearray, memoryview)) else list(matches)
        options = set()
        min_length = 0
        max_length = 0
        for o in m:
            o: bytes = bytes(o)
            o_len = len(o)
            if o_len > 0:
                if min_length is None or o_len < min_length:
                    min_length = o_len
                if max_length is None or o_len > max_length:
                    max_length = o_len
                options.add((o, o_len))
        if not options:
            raise ValueError('No matches provided')
        if max_length == 1:
            return functools.partial(self._find_first_match_fast, options=set(x[0] for x, _ in options))
        return functools.partial(self._find_first_match, options=options, min_length=min_length, max_length=max_length)

    def _find_first_match(self,
                          options: set[tuple[bytes | int, int]],
                          min_length: int,
                          max_length: int) -> tuple[bytes | None, int | None]:
        curr_idx = 0
        while True:
            if not self._read_up_to(curr_idx + min_length):
                return None, None
            opt = self._check_any_at(options, curr_idx, max_length)
            if opt is not None:
                return opt, curr_idx
            curr_idx += 1

    def _check_any_at(self, options: set[tuple[bytes | int, int]], start_idx: int, max_length: int) -> t.Optional[bytes]:
        self._read_up_to(start_idx + max_length)
        for opt, opt_len in options:
            if start_idx + opt_len >= self._buffer_length:
                continue
            if all(opt[i] == self._buffer[start_idx + i] for i in range(0, opt_len)):
                return opt if isinstance(opt, bytes) else opt.to_bytes(1)
        return None

    def _find_first_match_fast(self, options: set[bytes | int]):
        """ Find the first match when all the options are single characters.
            This method relies on the built-in index() method which is much faster."""
        curr_idx = 0
        while True:
            best_opt, best_pos = None, None
            for opt in options:
                try:
                    pos = self._buffer.index(opt, curr_idx)
                    if best_pos is None or pos < best_pos:
                        best_opt = opt
                        best_pos = pos
                except ValueError:
                    continue
            if best_pos is not None:
                return best_opt.to_bytes(1, 'little'), best_pos
            curr_idx = self._buffer_length
            if not self._read_next():
                return None, None

    def _split_and_iterate(self, matcher, include_target):
        while not self.at_eof():
            matching, actual_offset = matcher()
            if actual_offset is None:
                yield self.consume_all()
                break
            else:
                if include_target:
                    yield self.consume(actual_offset + len(matching))
                else:
                    yield self.consume(actual_offset)
                    self._discard_leading(len(matching))

    def __getitem__(self, user_index: slice | int) -> t.Iterator[t.ByteString] | t.ByteString:
        if isinstance(user_index, slice):
            self._read_up_to(user_index.stop)
            return self._buffer[user_index]
        else:
            if not self._read_up_to(user_index):
                raise KeyError(user_index)
            return self._buffer[user_index].to_bytes(1, 'little')
