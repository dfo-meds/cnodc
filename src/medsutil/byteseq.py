"""
    Provides a buffered reader class for lazy loading from an iterable stream of bytes.
"""
import functools
import typing as t

from medsutil import types as ct
from medsutil.halts import HaltFlag
from medsutil.vlq import vlq_decode


class ByteSequenceReader:
    """ A binary content reader that works on a stream of bytes, lazily loading them as needed.

        The reader works off any iterator that provides bytes
        or bytearrays. The operations called only load the next element
        when one is necessary. Once data is consumed from the reader,
        it is discarded from the buffer. This keeps the amount of data
        actually in memory to a minimum.

        Example usage:

        with open("very_big_file.dat", "rb") as h:

            # This streams bytes in 1 MiB chunks through the
            # reader
            def read_content():
                while content := h.read(1024 * 1024):
                    yield content

            # The reader only pulls the chunks as needed
            reader = ByteSequenceReader(read_content())

            while line := reader.consume_line():
                ... # do something with line
                # note that the next chunk is only pulled when
                # we don't have a line ending present in the
                # current buffer

    """

    def __init__(self,
                 raw_bytes: ct.ByteStrings,
                 halt_flag: HaltFlag = None,
                 check_frequency: int = 50):
        """
        :param raw_bytes: An iterable of bytes or bytearrays to lazily load from.
        :param halt_flag: A halt flag, which can be used to abort the operation at any time.
        :param check_frequency: The frequency to check the halt flag (every N calls to check_continue).
        """
        self._halt_flag = halt_flag
        self._raw_data = raw_bytes
        self._check_frequency = check_frequency if check_frequency > 0 else 1
        self._iterator = None
        self._buffer = bytearray()
        self._buffer_length = 0
        self._last_check = 0
        self._offset = 0
        self._complete = False

    def iterate_rest(self) -> t.Generator[bytes | bytearray, None, None]:
        """ Iterates through the remaining bytes.
            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        if self._buffer:
            yield self._buffer
            self._offset += self._buffer_length
            self._buffer = bytearray()
            self._buffer_length = 0
            self.check_continue()
        if not self._complete:
            if self._iterator is None:
                self._iterator = iter(self._raw_data)
            next_item = next(self._iterator, None)
            while next_item is not None:
                self._offset += len(next_item)
                yield next_item
                self.check_continue()
                next_item = next(self._iterator, None)
            self._complete = True

    def check_continue(self):
        """ If a halt flag was provided, checks for system shutdown.
            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        if self._halt_flag is not None:
            self._last_check += 1
            if self._last_check >= self._check_frequency:
                self._last_check = 0
                self._halt_flag.check_continue(True)

    def offset(self) -> int:
        """ Retrieves the current offset from the start of the content. """
        return self._offset

    def _read_next(self) -> bool:
        """ Reads the next byte or bytearray from the iterator, returning True if one was added. """
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
        """ Reads all the remaining items into the buffer.
            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
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

    def _read_up_to(self, actual_index: int) -> bool:
        """ Ensures the given index (from the start of the file) has been read into the buffer, returning True if it was read.
            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        while actual_index >= self._buffer_length:
            self.check_continue()
            if not self._read_next():
                break
        return actual_index < self._buffer_length

    def _discard_leading(self, length: int):
        """ Discards leading bytes from the buffer. """
        if length >= self._buffer_length:
            self._offset += self._buffer_length
            self._buffer_length = 0
            self._buffer = bytearray()
        else:
            self._offset += length
            self._buffer = self._buffer[length:]
            self._buffer_length -= length

    def at_eof(self) -> bool:
        """ Checks if we're at the end of the stream. """
        if self._buffer_length > 0:
            return False
        return not self._read_next()

    def consume_vlq_int(self) -> int:
        """ Consumes an integer using the VLQ format.

            See :py:func:`medsutil.vlq.vlq_decode` for documentation.

            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        idx = 0
        while True:
            if not self._read_up_to(idx):
                break
            if self._buffer[idx] < 128:
                break
            idx += 1
        return vlq_decode(self.consume(idx + 1))[0]

    def peek(self, length: int) -> bytearray | bytes:
        """ Loads the next N bytes without consuming them and returns them.
            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        self._read_up_to(length + 1)
        return self._buffer[0:length+1]

    def peek_line(self, exclude_line_endings: bool = True) -> bytearray | bytes:
        """ Finds the next line ending and returns the line without consuming it.
            :param exclude_line_endings: If false, the line ending will be included in the returned bytes.

            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        n_or_r, index = self._find_first_match_fast({10, 13})
        if index is None:
            return self._buffer
        if not exclude_line_endings:
            index += 1
        return self._buffer[:index]

    def consume_line(self, exclude_line_endings: bool = True) -> bytearray | bytes:
        """ Finds the next line ending and returns the line while consuming it from the buffer.
        :param exclude_line_endings: If false, the line ending will be included in the returned bytes.
        :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        res = self.consume_until([b"\n", b"\r"], include_target=True)
        if res and res[-1] == 13 and (not self.at_eof()) and self[0] == b"\n":
            self._discard_leading(1)
            if not exclude_line_endings:
                res += b"\n"
        return res.rstrip(b"\r\n") if exclude_line_endings else res

    def consume_lines(self, exclude_line_endings: bool = True) -> ct.ByteStrings:
        """ Iteratively consumes lines until it breaks or the end of the stream is reached.
            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        while not self.at_eof():
            self.check_continue()
            yield self.consume_line(exclude_line_endings)

    def consume_all(self) -> bytearray:
        """ Consumes the entire remaining stream and returns it as a bytearray.
            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        self._read_rest()
        res = self._buffer
        self._buffer = bytearray()
        self._offset += self._buffer_length
        self._buffer_length = 0
        return res

    def consume_until(self, matches: t.Union[t.Sequence[bytes], bytes], include_target: bool = False) -> bytearray:
        """ Consumes bytes until one of the byte sequences is found.

            :param matches: Either a byte sequence or a sequence of them (if a sequence, any of them might match).
            :param include_target: If true, includes the target byte sequence that was found.
            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        matching, actual_offset = self.find_first_match([matches] if isinstance(matches, bytes) else matches)
        if actual_offset is None or matching is None:
            return self.consume(self._buffer_length)
        else:
            return self.consume(actual_offset + (len(matching) if include_target else 0))

    def consume(self, length: int) -> bytearray:
        """ Consumes the given number of bytes.

            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        self._read_up_to(length)
        res = self._buffer[0:length]
        self._discard_leading(length)
        return res

    def lstrip(self, bytes_: bytes, max_strip: int = None):
        """ Consumes any leading bytes that are found in the first argument.

            :param bytes_: The byte characters to strip (e.g. b"\r\n\t " strips any of these four characters).
            :param max_strip: The maximum number of characters to remove.

            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
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
        """ Finds the first match from the given match options.

            :param matches: The binary string to look for or a sequence of them.

            :returns: The binary string that was found and its position (or None, None if no match was found).
            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        return self._get_matcher(matches)()

    def split_and_iterate(self, matches: t.ByteString | t.Sequence[t.ByteString], include_target: bool = False):
        """ Splits the binary content based on the given match options and returns them.

            Comparable to calling `consume_until()` iteratively until there are no more matches,
            except this is slightly faster.

            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        matcher = self._get_matcher(matches)
        return self._split_and_iterate(matcher, include_target)

    def _get_matcher(self,
                     matches: t.ByteString | t.Sequence[t.ByteString],
                     ) -> t.Callable[[], tuple[bytes | None, int | None]]:
        """ Builds the correct matching function for the given set of matches.

            If all options in matches are of length 1, then this uses
            `_find_first_match_fast`, otherwise it uses the slower
            `_find_first_match`.
        """
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
        """ Slower matching function that works on variable length options.
            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        curr_idx = 0
        while True:
            if not self._read_up_to(curr_idx + min_length):
                return None, None
            opt = self._check_any_at(options, curr_idx, max_length)
            if opt is not None:
                return opt, curr_idx
            curr_idx += 1

    def _check_any_at(self, options: set[tuple[bytes | int, int]], start_idx: int, max_length: int) -> t.Optional[bytes]:
        """ Checks if any of the given options are at the given start index.
            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down.
        """
        self._read_up_to(start_idx + max_length)
        for opt, opt_len in options:
            if start_idx + opt_len >= self._buffer_length:
                continue
            if all(opt[i] == self._buffer[start_idx + i] for i in range(0, opt_len)):
                return opt if isinstance(opt, bytes) else opt.to_bytes(1)
        return None

    def _find_first_match_fast(self, options: set[bytes | int]) -> tuple[bytes | None, int | None]:
        """ Finds the first match when all the options are single characters.
            This method relies on the built-in index() method which is much faster.

            :raises medsutil.exceptions.HaltInterrupt: If the system is shutting down. """
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
            self.check_continue()

    def _split_and_iterate(self,
                           matcher: t.Callable[[], tuple[bytes | None, int | None]],
                           include_target: bool) -> t.Generator[bytes | bytearray]:
        """ Splits the binary content based on a matcher and iterates over the values.

            Note that the splitting is done on-demand, so if you break on the generator,
            there may be content left in the stream.
        """
        while not self.at_eof():
            matching, actual_offset = matcher()
            if actual_offset is None or matching is None:
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
