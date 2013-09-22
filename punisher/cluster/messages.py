from collections import namedtuple
from datetime import datetime
import inspect
import struct
import uuid

import msgpack

from punisher.cluster.connection import Connection
from punisher.utils import serialize_timestamp


class Message(object):
    """
    Base class of messages sent between peers

    wire format is as follows:
    [message type (4b)][message size (4b)][message body]
    message body is a list of values, serialized by msgpack

    the message body values and order are determined by the
    __init__ method args, the idea being that the other machine
    can deserialize the message and pass the results directly
    into the appropriate constructor. However, this means that
    the class attribute names must match the contructor arg names,
    `self` is ignored

    ie:
    self.value = value # this works
    self.value = v # this won't work
    """

    __message_type__ = None
    __arg_spec__ = {}
    __klass_map__ = {}

    @staticmethod
    def _uuid_bytes(v):
        return v.bytes if isinstance(v, uuid.UUID) else v

    @classmethod
    def _get_argspec(cls):
        if cls.__message_type__ not in Message.__arg_spec__:
            args = inspect.getargspec(cls.__init__).args
            arg_spec = [a for a in args if a != 'self']
            Message.__arg_spec__[cls.__message_type__] = arg_spec
        return Message.__arg_spec__[cls.__message_type__]

    def __init__(self, sender_id, message_id=None):
        super(Message, self).__init__()
        self.sender_id = Message._uuid_bytes(sender_id)
        self.message_id = Message._uuid_bytes(message_id or uuid.uuid1())

    def __repr__(self):
        args = inspect.getargspec(self.__init__).args
        def maybe_get_uuid(k):
            v = getattr(self, k)
            if isinstance(v, basestring):
                if len(v) == 16:
                    try:
                        return str(uuid.UUID(bytes=v))
                    except Exception:
                        pass
            return v
        message_data = [maybe_get_uuid(a) for a in args if a != 'self']
        return '<{} {}>'.format(self.__class__.__name__, message_data)

    @property
    def sender(self):
        return uuid.UUID(bytes=self.sender_id)

    def send(self, conn):
        """
        sends this message's bytes over the given socket
        :type conn: Connection
        """
        assert self.__message_type__ is not None
        arg_spec = self._get_argspec()
        message_data = [getattr(self, a) for a in arg_spec]
        message_body = msgpack.dumps(message_data)
        conn.write(
            struct.pack('!2I', self.__message_type__, len(message_body)),
            message_body
        )

    @classmethod
    def read(cls, conn):
        """
        reads a message from the given socket

        :type conn: Connection
        :rtype: Message
        """
        # don't read from subclasses
        assert cls == Message
        if not Message.__klass_map__:
            def discover(baseklass):
                assert isinstance(baseklass, type)
                assert issubclass(baseklass, Message)
                for klass in baseklass.__subclasses__():
                    if klass.__message_type__ is not None:
                        Message.__klass_map__[klass.__message_type__] = klass
                    discover(klass)
            discover(Message)

        message_type, message_size = struct.unpack('!2I', conn.read(8))
        message_args = msgpack.loads(conn.read(message_size))
        message = Message.__klass_map__[message_type](*message_args)
        return message


# ----------- startup and connection -----------

class NoopMessage(Message):
    __message_type__ = 0

class ConnectionRequest(Message):
    __message_type__ = 101

    def __init__(self, sender_id, sender_address, token, sender_name=None, message_id=None):
        super(ConnectionRequest, self).__init__(sender_id, message_id)
        self.sender_address = tuple(sender_address)
        self.sender_name = sender_name
        self.token = str(token) if token is not None else None


class ConnectionAcceptedResponse(Message):
    __message_type__ = 102

    def __init__(self, sender_id, token, name, message_id=None):
        super(ConnectionAcceptedResponse, self).__init__(sender_id, message_id)
        self.token = token
        self.name = name


class ConnectionRefusedResponse(Message):
    __message_type__ = 103

    def __init__(self, sender_id, reason, message_id=None):
        super(ConnectionRefusedResponse, self).__init__(sender_id, message_id)
        self.reason = reason


class DiscoverPeersRequest(Message):
    """ asks for a list of peer addresses and ids """
    __message_type__ = 201


class DiscoverPeersResponse(Message):
    """
    includes data about all known peers

    peers will be a tuple of this format:
        (<(address, port)>, <node_id>, <token>, <name>)

    """
    __message_type__ = 202

    PeerData = namedtuple('PeerData', ['address', 'node_id', 'token', 'name'])

    def __init__(self, sender_id, peers_list, message_id=None):
        super(DiscoverPeersResponse, self).__init__(sender_id, message_id)
        self.peers_list = []

        #post process peers list data
        for peer in peers_list:
            address, node_id, token, name = peer
            self.peers_list.append(DiscoverPeersResponse.PeerData(
                tuple(address),
                Message._uuid_bytes(node_id),
                str(token) if token else  None,
                name
            ))

    def get_peer_data(self):
        """ returns peer data with the node_id as a UUID """
        return [
            DiscoverPeersResponse.PeerData(
                tuple(p.address),
                uuid.UUID(bytes=p.node_id),
                long(p.token) if p.token else None,
                p.name
            )
            for p in self.peers_list
        ]


class PingRequest(Message):
    __message_type__ = 210


class PingResponse(Message):
    __message_type__ = 211


# ----------- query execution -----------

class RetrievalDigestRequest(Message):
    __message_type__ = 301

    def __init__(self, sender_id, instruction, key, args, message_id=None):
        super(RetrievalDigestRequest, self).__init__(sender_id, message_id)
        self.instruction = instruction
        self.key = key
        self.args = args


class RetrievalDigestResponse(Message):
    __message_type__ = 302

    def __init__(self, sender_id, digest, message_id=None):
        super(RetrievalDigestResponse, self).__init__(sender_id, message_id)
        self.digest = digest


class RetrievalValueRequest(Message):
    __message_type__ = 303

    def __init__(self, sender_id, instruction, key, args, message_id=None):
        super(RetrievalValueRequest, self).__init__(sender_id, message_id)
        self.instruction = instruction
        self.key = key
        self.args = args


class RetrievalValueResponse(Message):
    __message_type__ = 304

    def __init__(self, sender_id, data, message_id=None):
        super(RetrievalValueResponse, self).__init__(sender_id, message_id)
        self.data = data


class UnknownKeyResponse(Message):
    __message_type__ = 305


class MutationOperationRequest(Message):
    __message_type__ = 306

    def __init__(self, sender_id, instruction, key, args, timestamp=None, message_id=None):
        super(MutationOperationRequest, self).__init__(sender_id, message_id)
        self.instruction = instruction
        self.key = key
        self.args = args
        self.timestamp = timestamp
        if isinstance(timestamp, datetime):
            self.timestamp = serialize_timestamp(self.timestamp)


class MutationOperationResponse(Message):
    __message_type__ = 307

    def __init__(self, sender_id, result, message_id=None):
        super(MutationOperationResponse, self).__init__(sender_id, message_id)
        self.result = result


# ----------- node initialization / data migration -----------

class DataMigrateRequest(Message):
    __message_type__ = 701

    def __init__(self, sender_id, from_token, max_token, size=100, message_id=None):
        """
        :param from_token: the token to start streaming data from
        :param size: the number of keys to send
        """
        super(DataMigrateRequest, self).__init__(sender_id, message_id)
        self.from_token = str(from_token)
        self.max_token = str(max_token)
        self.size = size

    @property
    def from_token_long(self):
        return long(self.from_token)

    @property
    def max_token_long(self):
        return long(self.max_token)


class DataMigrationResponse(Message):
    __message_type__ = 702

    def __init__(self, sender_id, data, message_id=None):
        """
        :param data: the requested data
        """
        super(DataMigrationResponse, self).__init__(sender_id, message_id)
        self.data = data


class RetireKeyRangeRequest(Message):
    """
    indicates that the node may remove keys in the given range, if
    they are no longer owned or replicated by it
    """
    __message_type__ = 703

    def __init__(self, sender_id, start_token, stop_token, message_id=None):
        super(RetireKeyRangeRequest, self).__init__(sender_id, message_id)
        self.start_token = str(start_token)
        self.stop_token = str(stop_token)

    @property
    def start_token_long(self):
        return long(self.start_token)

    @property
    def stop_token_long(self):
        return long(self.stop_token)


class RetireKeyRangeResponse(Message):
    __message_type__ = 704


class StreamRequest(Message):
    """
    requests the destination node to stream keys replicated
    by the sending node to the sending node
    """
    __message_type__ = 705


class StreamResponse(Message):
    __message_type__ = 706


class StreamDataRequest(Message):
    """ sends 1 or more key/value pairs """
    __message_type__ = 707


class StreamDataResponse(Message):
    __message_type__ = 708


class StreamCompleteRequest(Message):
    """ sent when a node has finished streaming data to another node """
    __message_type__ = 709


class StreamCompleteResponse(Message):
    __message_type__ = 710


# ----------- token discovery / communication -----------

class AnnounceTokenRequest(Message):
    __message_type__ = 801

    def __init__(self, sender_id, token, message_id=None):
        super(AnnounceTokenRequest, self).__init__(sender_id, message_id)
        self.token = token


class AnnounceTokenResponse(Message):
    __message_type__ = 802


class RequestTokenRequest(Message):
    __message_type__ = 803

    def __init__(self, sender_id, message_id=None):
        super(RequestTokenRequest, self).__init__(sender_id, message_id)


class RequestTokenResponse(Message):
    __message_type__ = 804

    def __init__(self, sender_id, token, message_id=None):
        super(RequestTokenResponse, self).__init__(sender_id, message_id)
        self.token = token


class ChangedTokenRequest(Message):
    __message_type__ = 805

    def __init__(self, sender_id, node_id, new_token, message_id=None):
        super(ChangedTokenRequest, self).__init__(sender_id, message_id)
        self.node_id = self._uuid_bytes(node_id)
        self.new_token = str(new_token)

    @property
    def node_uuid(self):
        return uuid.UUID(bytes=self.node_id)

    @property
    def new_token_long(self):
        return long(self.new_token)


class ChangedTokenResponse(Message):
    __message_type__ = 806


# ----------- cluster admin (deprecated?) -----------

class JoinClusterRequest(Message):
    """ sent to the cluster when a node wants to join the active cluster and process requests """
    __message_type__ = 6


class JoinClusterResponse(Message):
    __message_type__ = 7


class DisconnectionRequest(Message):
    """ sent to the cluster when a node is temporarily going offline """
    __message_type__ = 8


class DisconnectionResponse(Message):
    __message_type__ = 9


class LeaveClusterRequest(Message):
    """ sent to the cluster when a node leaving the operational cluster, but remaining connected """
    __message_type__ = 10


class LeaveClusterResponse(Message):
    __message_type__ = 11


# ----------- error responses -----------

class ErrorResponse(Message):
    __message_type__ = 999

    def __init__(self, sender_id, reason, message_id=None):
        super(ErrorResponse, self).__init__(sender_id, message_id)
        self.reason = reason



