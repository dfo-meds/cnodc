"""
Support for variable-length quantity (VLQ) binary encoding of integers.

VLQ integers are composed of bytes. In each, the most-significant bit
is treated as a flag to indicate if the next byte is part of the same
number. The other seven bits are a binary encoding of the number.

So, for example, a VLQ integer might be:

1 0110011   1 1001101   0 1010010

The leading 1s indicate the next byte is included, the last one has a leading 0.

The actual number is b011001110011011010010 (the concatenation of the other seven bits

"""


def vlq_encode(number: int) -> bytearray:
    """Encodes an integer as a byte array."""
    result = bytearray()
    while number >= 0b10000000:
        bits = number & 0b01111111
        number >>= 7
        result.append(bits | 0b10000000)
    result.append(number)
    return result


def vlq_decode(bytes_: bytes) -> tuple[int, int]:
    """Given a stream of bytes where the first byte is a VLQ integer, decodes the integer and returns it and the offset."""
    total = 0
    shift = 0
    pos = 0
    while pos < len(bytes_):
        total += (bytes_[pos] & 0b01111111) << shift
        shift += 7
        if not bytes_[pos] & 0b10000000:
            break
        pos += 1
    return total, pos + 1
