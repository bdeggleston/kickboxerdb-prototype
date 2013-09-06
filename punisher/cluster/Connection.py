class Connection(object):

    class ClosedException(Exception):
        """ Called when the connection is closed """

    def __init__(self, sckt):
        from socket import socket
        assert isinstance(sckt, socket)
        self.socket = sckt
        self.is_open = True

    @classmethod
    def connect(cls, address):
        from socket import socket
        s = socket()
        s.connect(address)
        return Connection(s, address)

    def read(self, size):
        if not self.is_open:
            raise self.ClosedException

        if size < 1: return
        result = self.socket.recv(size)
        if not result:
            self.is_open = False
            raise self.ClosedException
        return result

    def read_byte(self):
        return self.read(1)[0]

    def write(self, *data):
        if not self.is_open:
            raise self.ClosedException
        self.socket.send(''.join(data))

    def close(self):
        self.socket.close()

