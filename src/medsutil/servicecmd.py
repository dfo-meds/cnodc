import typing as t
import time
import socket
import select
import zrlog


class SocketTimeout(Exception):
    pass


def recv_with_end(clientsocket, buffer_size: int = 1024, timeout: float = 5):
    """Receive bytes until the end transmission flag is seen."""
    data = bytearray()
    _start = time.monotonic()
    while not (data and data[-1] == 4):
        if time.monotonic() - _start > timeout:
            raise SocketTimeout()
        try:
            data.extend(_recv_with_timeout(clientsocket, buffer_size, 0.25))
        except SocketTimeout:
            pass
    return data[:-1]


def _recv_with_timeout(sock, buffer_size: int = 1024, timeout: float = 0.25):
    sock.setblocking(0)
    ready = select.select([sock], [], [], timeout)
    if ready[0]:
        return sock.recv(buffer_size)
    raise SocketTimeout


def send_with_end(clientsocket, content, end_flag=b"\4"):
    """Send bytes and append the end transmission flag."""
    clientsocket.sendall(content + end_flag)


def send_command(port: int, cmd: bytes) -> bytes:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect(("127.0.0.1", port))
        send_with_end(sock, cmd)
        return recv_with_end(sock)


class ServiceCommandManager:

    def __init__(self, port: int, handle: t.Callable[[bytes, t.Any], bytes]):
        self._port: int = port
        self._socket: socket.socket | None = None
        self._handle = handle
        self._log = zrlog.get_logger("medsutil.scm")

    def setup(self):
        if self._socket is None:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.bind(("127.0.0.1", self._port))
            self._socket.listen()
            self._log.trace("Listening on port %s for local connections", self._port)

    def check(self):
        ready, _, _ = select.select([self._socket], [], [], 0.5)
        if ready:
            clientsocket, address = self._socket.accept()
            self._log.trace("Accepted connection from %s", address)
            try:
                send_with_end(clientsocket, self.handle(address, recv_with_end(clientsocket)))
            except SocketTimeout:
                ...
            clientsocket.close()

    def handle(self, address, message: bytes) -> bytes:
        return self._handle(message, address)

    def cleanup(self):
        if self._socket:
            self._socket.close()
            self._socket = None
            self._log.trace("Socket close on port %s", self._port)
