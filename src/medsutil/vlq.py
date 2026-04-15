def vlq_encode(number: int) -> bytearray:
    result = bytearray()
    while number >= 0b10000000:
        bits = number & 0b01111111
        number >>= 7
        result.append(bits | 0b10000000)
    result.append(number)
    return result


def vlq_decode(bytes_: bytes) -> tuple[int, int]:
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
