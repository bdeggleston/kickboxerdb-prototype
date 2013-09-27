from socket import error as socketerror
from gevent import socket


class Connection(object):

    class ClosedException(Exception):
        """ Called when the connection is closed """

    def __init__(self, sckt, timeout=None):
        assert isinstance(sckt, socket.socket)
        self.socket = sckt
        self.is_open = True
        self.timeout = timeout
        self.socket.settimeout(self.timeout)

    @classmethod
    def connect(cls, address, timeout=30.0):
        s = socket.socket()
        s.connect(address)
        return Connection(s, timeout=timeout)

    def set_timeout(self, timeout):
        self.socket.settimeout(timeout)

    def read(self, size):
        if not self.is_open:
            raise Connection.ClosedException

        if size < 1: return

        try:
            result = self.socket.recv(size)
        except socketerror:
            self.is_open = False
            self.close()
            raise Connection.ClosedException

        if not result:
            self.is_open = False
            raise Connection.ClosedException
        return result

    def read_byte(self):
        return self.read(1)[0]

    def write(self, *data):
        if not self.is_open:
            raise self.ClosedException
        self.socket.send(''.join(data))

    def close(self):
        self.socket.close()

