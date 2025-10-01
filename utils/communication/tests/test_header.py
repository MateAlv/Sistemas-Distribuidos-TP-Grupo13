HEADER_HELLO: int = 1


def header_to_bytes(header: int) -> bytes:
    if not isinstance(header, int):
        raise TypeError(f"header debe ser int, no {type(header).__name__}")
    if not (0 <= header <= 255):
        raise ValueError(f"header fuera de rango [0,255]: {header}")
    return header.to_bytes(1, byteorder='big')

def header_from_bytes(data: bytes) -> int:
    return int.from_bytes(data, byteorder='big')


def test_header_to_bytes():
    assert header_to_bytes(0) == b'\x00'
    assert len(header_to_bytes(0)) == 1
    assert header_to_bytes(1) == b'\x01'
    assert len(header_to_bytes(1)) == 1
    assert header_to_bytes(255) == b'\xff'
    assert len(header_to_bytes(255)) == 1
    try:
        header_to_bytes(-1)
    except ValueError as e:
        assert str(e) == "header fuera de rango [0,255]: -1"
    try:
        header_to_bytes(256)
    except ValueError as e:
        assert str(e) == "header fuera de rango [0,255]: 256"
    try:
        header_to_bytes("string")
    except TypeError as e:
        assert str(e) == "header debe ser int, no str"

def test_header_from_bytes():
    assert header_from_bytes(b'\x00') == 0
    assert header_from_bytes(b'\x01') == 1
    assert header_from_bytes(b'\xff') == 255
    
    try:
        header_from_bytes(b'')
    except Exception as e:
        assert isinstance(e, ValueError) or isinstance(e, IndexError)
    try:
        header_from_bytes(b'\x00\x01')
    except Exception as e:
        assert isinstance(e, ValueError) or isinstance(e, IndexError)