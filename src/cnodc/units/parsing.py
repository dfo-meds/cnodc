
from cnodc.units.structures import Log, Offset, Quotient, Product, Exponent, Real, Integer, Literal, SimpleUnit, UnitExpression
from cnodc.util.exceptions import CNODCError

UNICODE_EXPONENTS = {
    '⁰': '0',
    '¹': '1',
    '²': '2',
    '³': '3',
    '⁴': '4',
    '⁵': '5',
    '⁶': '6',
    '⁷': '7',
    '⁸': '8',
    '⁹': '9',
    '⁺': '+',
    '⁻': '-'
}

UDUNITS_MULTIPLICATION = ('*', '.', '·')
UDUNITS_DIVISION = ('PER', 'per', '/')
UDUNITS_BOTH_OPS = (*UDUNITS_DIVISION, *UDUNITS_MULTIPLICATION)
UDUNITS_EXPONENTIATION = ('^', '**')

UDUNITS_LOG = {
    'log': Integer('10'),
    'lg': Integer('10'),
    'ln': Real('e'),
    'lb': Integer('2'),
}

def parse_unit_string(units: str) -> UnitExpression:
    return _parse_unit_string_with_shift(units)


def _parse_unit_string_with_shift(units: str):
    units = units.strip(' ')
    for shift_op in ('@', 'after', 'from', 'since', 'ref'):
        if shift_op in units:
            rpos = units.rfind(shift_op)
            following = units[rpos + len(shift_op):].strip()
            if _is_literal(following):
                return Offset(_parse_unit_string(units[:rpos]), _parse_literal(following))
    return _parse_unit_string(units)


def _parse_unit_string(units: str) -> UnitExpression:
    units = units.replace('**', '^')
    if '(' in units or ')' in units:
        return _parse_unit_for_groups(units)
    else:
        return _parse_simple_unit_string(units)


def _parse_unit_for_groups(units: str) -> UnitExpression:
    # Clean up our groups
    strings = _decompose_bracket_pairs(units)
    strings = _parse_logs(strings)
    strings = _parse_leading_exponents(strings)
    strings = _parse_leading_operators(strings)
    expr, op = None, None
    for piece in strings:
        if expr is None:
            expr = piece if isinstance(piece, UnitExpression) else parse_unit_string(piece)
        elif piece[0] == "^":
            if op is not None:
                raise CNODCError(f"Operation cannot be followed by an exponentiation immediately")
            if expr is None:
                raise CNODCError(f"Exponentiation cannot be at the start")
            expr = Exponent(expr, int(piece[1:]))
        elif piece in UDUNITS_MULTIPLICATION:
            if op is not None:
                raise CNODCError(f"Operation cannot be followed by multiplication immediately")
            op = '*'
        elif piece in UDUNITS_DIVISION:
            if op is not None:
                raise CNODCError(f"Operation cannot be followed by a division immediately")
            op = '/'
        else:
            expr2 = piece if isinstance(piece, UnitExpression) else parse_unit_string(piece)
            if op is None or op == '*':
                expr = Product(expr, expr2)
                op = None
            elif op == '/':
                expr = Quotient(expr, expr2)
                op = None
    return expr


def _parse_leading_operators(pieces: list[str]) -> list[str]:
    new_pieces = []
    for idx, piece in enumerate(pieces):
        new_start = None
        new_end = None
        if not isinstance(piece, UnitExpression):
            for op in UDUNITS_BOTH_OPS:
                if new_start is None and piece.startswith(op):
                    new_start = op
                    piece = piece[len(op):].strip()
                if new_end is None and piece.endswith(op):
                    new_end = op
                    piece = piece[:len(op) * -1]
                if new_start is None and new_end is None:
                    break
        if new_start is not None:
            new_pieces.append(new_start)
        new_pieces.append(piece)
        if new_end is not None:
            new_pieces.append(new_end)
    return new_pieces

def _parse_leading_exponents(pieces: list[str]) -> list[str]:
    new_pieces = []
    for piece in pieces:
        if isinstance(piece, str):
            for exp_op in UDUNITS_EXPONENTIATION:
                if piece.startswith(exp_op):
                    exp_int, following = _extract_leading_int(piece[len(exp_op):].strip())
                    new_pieces.append(f'^{exp_int}')
                    following = following.strip()
                    if following:
                        new_pieces.append(following)
                    break
            else:
                if piece[0] in UNICODE_EXPONENTS:
                    exp_int, following = _extract_leading_int(piece)
                    new_pieces.append(f'^{exp_int}')
                    following = following.strip()
                    if following:
                        new_pieces.append(following)
                else:
                    new_pieces.append(piece)
        else:
            new_pieces.append(piece)
    return new_pieces


def _parse_logs(pieces: list[str]) -> list[str]:
    new_pieces = []
    skip_next = False
    for idx in range(0, len(pieces) - 1):
        if skip_next:
            skip_next = False
            continue
        for log_str in UDUNITS_LOG:
            # Check for the log flag
            if pieces[idx].endswith(log_str):
                # Check if there was preceding text in the same line
                if pieces[idx] != log_str:
                    new_pieces.append(pieces[idx][:-1 * len(log_str)].strip())
                # the next piece is log argument:
                log_argument = pieces[idx+1].strip()
                if log_argument.startswith("re:"):
                    log_argument = log_argument[3:].strip()
                elif log_argument.startswith("re"):
                    log_argument = log_argument[2:].strip()
                skip_next = True
                new_pieces.append(Log(parse_unit_string(log_argument), UDUNITS_LOG[log_str]))
                break
        else:
            new_pieces.append(pieces[idx])
    if not skip_next:
        new_pieces.append(pieces[-1])
    return new_pieces


def _decompose_bracket_pairs(units: str) -> list[str]:
    """ Breaks down the outermost pairs of brackets into a list of expressions. """
    pieces = [""]
    depth = 0
    for pos, char in enumerate(units):
        # handle opening brackets
        if char == '(':
            if depth == 0:
                pieces.append("")
            else:
                pieces[-1] += char
            depth += 1
        # handle closing bracket
        elif char == ")":
            depth -= 1
            if depth > 0:
                pieces[-1] += char
            elif depth == 0:
                pieces.append("")
            if depth < 0:
                raise CNODCError(f"Parse error, `)` found without opening `(` at position {pos} in [{units}]")
        else:
            pieces[-1] += char
    if depth > 0:
        raise CNODCError(f"Unit string parse error, '(' found without closing ')' in [{units}]")
    return [p.strip() for p in pieces if p.strip()]


def _parse_simple_unit_string(units: str) -> UnitExpression:
    """ Parse a simple unit string into objects. """
    units = units.strip()
    expr = None
    op = None
    pieces = _decompose_simple_unit_string(units)
    for piece in pieces:
        # The first piece is just an expression
        if expr is None:
            expr = _parse_unit_for_exponents(piece)

        # If we have an expression, we need to look for operations
        elif piece in UDUNITS_DIVISION:
            op = '/'
        elif piece in UDUNITS_MULTIPLICATION:
            op = '*'

        # Or the next element
        else:
            expr2 = _parse_unit_for_exponents(piece)
            # Default operation is implicit multiplication
            if op == '*' or op is None:
                expr = Product(expr, expr2)
                op = None
            elif op == '/':
                if isinstance(expr2, SimpleUnit):
                    expr = Product(expr, Exponent(expr2, Integer("-1")))
                elif isinstance(expr2, Exponent):
                    if expr2.exponent.value[0] == "-":
                        expr = Product(expr, Exponent(expr2.base, Integer(expr2.exponent.value[1:])))
                    else:
                        expr = Product(expr, Exponent(expr2.base, Integer("-" + expr2.exponent.value.strip("+"))))
                else:
                    expr = Quotient(expr, expr2)
                op = None
    return expr


def _decompose_simple_unit_string(units: str):
    """ Convert a simple unit string (no brackets, shifts, logs, etc) into a list of values and operations """
    # avoid confusion with the multiplication sign
    units = units.replace('**', '^')
    pieces = []
    buffer = ''
    skip = 0
    for i in range(0, len(units)):
        # Skip when we're told to
        if skip > 0:
            skip -= 1
            continue

        # break on spaces
        if units[i] == ' ':
            if buffer != '':
                pieces.append(buffer)
                buffer = ''
            continue

        # separate out the operations into their own buffer pieces
        for op in UDUNITS_BOTH_OPS:
            if units[i:i+len(op)] == op:
                if buffer != '':
                    pieces.append(buffer)
                pieces.append(op)
                buffer = ''
                skip = len(op) - 1
                break
        else:
            if buffer and buffer.isdigit() and not units[i].isdigit():
                pieces.append(buffer)
                buffer = ''
            buffer += units[i]
    if buffer:
        pieces.append(buffer)
    return pieces


def _parse_unit_for_exponents(units: str) -> UnitExpression:
    """ Extracts the exponent (if present) from a unit string. """
    if not units:
        raise CNODCError(f"Empty unit string", 'UNITS', 2000)
    units = units.strip()
    if _is_literal(units):
        return _parse_literal(units)
    elif '**' in units:
        p = units.find('**')
        return _build_exponent(units[:p], units[p+2:])
    elif '^' in units:
        p = units.find('^')
        return _build_exponent(units[:p], units[p+1:])
    elif units[-1].isdigit() or units[-1] in '+-':
        exp_pos = len(units) - 2
        while exp_pos >= 0 and (units[exp_pos].isdigit() or units[exp_pos] in '+-'):
            exp_pos -= 1
            if units[exp_pos] in "+-":
                break
        return _build_exponent(units[:exp_pos+1], units[exp_pos+1:])
    else:
        return SimpleUnit(units)


def _build_exponent(unit_name, exponent=None):
    """ Creates an appropriate exponent object """
    unit_name = unit_name.strip()
    exponent = (exponent or '').strip()
    for x in UNICODE_EXPONENTS:
        exponent = exponent.replace(x, UNICODE_EXPONENTS[x])
    if exponent == '1':
        return SimpleUnit(unit_name)
    if not _is_integer_number(exponent):
        raise CNODCError(f'Invalid exponent, expected integer, got [{exponent}]', 'UNITS', 2001)
    return Exponent(SimpleUnit(unit_name), Integer(exponent))


def _extract_leading_int(s: str) -> tuple[str, str]:
    """ Find and extract the leading integer"""
    idx = 0
    exp_int = ''
    while idx < len(s) and (s[idx] in UNICODE_EXPONENTS or s[idx].isdigit() or (s[idx] in '+-' and idx == 0)):
        if s[idx] in UNICODE_EXPONENTS:
            exp_int += UNICODE_EXPONENTS[s[idx]]
        else:
            exp_int += s[idx]
        idx += 1
    return exp_int, s[idx:]


def _parse_literal(s: str) -> Literal:
    """ Convert an integer or decimal into an Integer or Real object """
    if '.' in s or 'e' in s or 'E' in s:
        return Real(s)
    else:
        return Integer(s)


def _is_literal(s: str):
    """ Check if s is an integer, decimal, or scientific notation """
    if 'E' in s:
        pieces = s.split('E', maxsplit=1)
        return _is_decimal_number(pieces[0]) and _is_integer_number(pieces[1])
    elif 'e' in s:
        pieces = s.split('e', maxsplit=1)
        return _is_decimal_number(pieces[0]) and _is_integer_number(pieces[1])
    else:
        return _is_decimal_number(s)
    # TODO: timestamps


def _is_decimal_number(s: str) -> bool:
    """ Check if s is a decimal number string """
    if s == '':
        return False
    if s.startswith("+") or s.startswith("-"):
        s = s[1:]
    if "." in s:
        pieces = s.split(".", maxsplit=1)
        return pieces[0].isdigit() and (pieces[1] == '' or pieces[1].isdigit())
    else:
        return s.isdigit()

def _is_integer_number(s: str) -> bool:
    """ Check if s is an integer number string"""
    if s == '':
        return False
    if s.startswith("+") or s.startswith("-"):
        if len(s) == 0:
            return False
        return s[1:].isdigit()
    return s.isdigit()



