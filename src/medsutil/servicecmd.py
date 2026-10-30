""" Provides functions for sending commands over a socket.

    The transmission protocol is pretty simple here. It's designed to transmit
    ASCII-encoded data in chunks of any size. The final chunk must end in
    ASCII 0x04 (end transmission). Once a message has been sent from the client,
    it awaits a similarly encoded response from the server on the same socket.
    Only one message sent and returned is expected.

    The main use case here is to send commands via a command line tool to an
    actively running process so that you can give it instructions.
"""
import typing as t
import time
import socket
import select

import zrlog


class SocketTimeout(Exception):
    """ Raised when a socket times out for the connection. """
    pass


def recv_with_end(clientsocket: socket.socket, buffer_size: int = 1024, timeout: float = 5) -> bytearray:
    """Receive bytes until the end transmission flag (ASCII 0x04) is seen.
        :raises SocketTimeout: if timeout is reached.
    """
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


def _recv_with_timeout(sock: socket.socket, buffer_size: int = 1024, timeout: float = 0.25) -> bytes:
    """ Receives data from a socket, with a timeout. """
    sock.setblocking(0)
    ready = select.select([sock], [], [], timeout)
    if ready[0]:
        return sock.recv(buffer_size)
    raise SocketTimeout


def send_with_end(clientsocket: socket.socket, content: bytes | bytearray, _end_flag: bytes | bytearray = b"\4"):
    """Send bytes and append the end transmission flag."""
    clientsocket.sendall(content + _end_flag)


def send_command(port: int, cmd: bytes) -> bytes:
    """ Sends a command to the server on the given port and returns the server's response. """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect(("127.0.0.1", port))
        send_with_end(sock, cmd)
        return recv_with_end(sock)


class ServiceCommandManager:
    """ Runs a small server that waits for commands and replies to them.

        This server does not run on its own, it must have `check()` called
        to check for a new connection and handle the message.
    """

    def __init__(self, port: int, handle: t.Callable[[bytes, t.Any], bytes]):
        """
        :param port: The port to listen on.
        :param handle: A callable function that receives a command from an external source and returns the response.
        """
        self._port: int = port
        self._socket: socket.socket | None = None
        self._handle = handle
        self._log = zrlog.get_logger("medsutil.scm")

    def setup(self):
        """ Initializes the command manager by opening a socket and listening on it. """
        if self._socket is None:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.bind(("127.0.0.1", self._port))
            self._socket.listen()
            self._log.trace("Listening on port %s for local connections", self._port)

    def check(self, timeout: float = 0.5):
        """ Checks for a new connection, passes it to the handler, and returns the response. """
        if self._socket is not None:
            ready, _, _ = select.select([self._socket], [], [], timeout)
            if ready:
                clientsocket, address = self._socket.accept()
                self._log.trace("Accepted connection from %s", address)
                try:
                    send_with_end(clientsocket, self.handle(address, recv_with_end(clientsocket)))
                except SocketTimeout:
                    pass
                clientsocket.close()

    def handle(self, address: tuple, message: bytes) -> bytes:
        """ Wrapper around the handling callback. """
        return self._handle(message, address)

    def cleanup(self):
        """ Cleans up the service by closing the socket. """
        if self._socket:
            sock = self._socket
            self._socket = None
            sock.close()
            self._log.trace("Socket close on port %s", self._port)
