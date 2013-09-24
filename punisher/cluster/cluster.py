from collections import namedtuple
from datetime import datetime
from hashlib import md5
import pickle
import struct

from blist import sortedset
import gevent
from gevent.queue import Queue
from gevent.pool import Pool

from punisher.cluster import messages
from punisher.cluster.connection import Connection
from punisher.cluster.node.base import BaseNode
from punisher.cluster.node.local import LocalNode
from punisher.cluster.node.remote import RemoteNode


class _TokenContainer(object):
    def __init__(self, token):
        self.token = token


class Cluster(object):
    """
    Maintains the local view of the cluster, and coordinates client requests, and
    is responsible for handling replication and routing

    replication_factor of 0 will mirror all data to all nodes
    """

    class Status(object):
        INITIALIZING    = 0
        STREAMING       = 1
        NORMAL          = 2

    class ConsistencyLevel(object):
        ONE     = 1
        QUORUM  = 2
        ALL     = 3

    default_read_consistency = ConsistencyLevel.QUORUM
    default_write_consistency = ConsistencyLevel.QUORUM

    def __init__(self,
                 local_node,
                 partitioner,
                 seed_peers=None,
                 status=Status.INITIALIZING,
                 replication_factor=3):
        """
        :param local_node:
        :type local_node:
        :param partitioner:
        :type partitioner: punisher.partitioner.base.BasePartitioner
        :param seed_peers:
        :type seed_peers:
        :param status:
        :type status:
        :param replication_factor:
        :type replication_factor:
        """
        super(Cluster, self).__init__()
        self.partitioner = partitioner
        self.seed_peers = seed_peers or []
        self.replication_factor = max(0, replication_factor)

        assert isinstance(local_node, LocalNode)
        self.local_node = local_node
        self.nodes = {self.local_node.node_id: self.local_node}

        # this cluster's view of the token ring
        self.token_ring = None

        self.is_online = False
        self.status = status

        # initializer bookkeeping

        # the key that this node has been initialized to
        # initializing currently happens sequentially, ordered
        # by token, so this can be used to quickly determine
        # whether this node has a given key, or where to restart
        # initialization from, if there was an interruption
        self._initialization_history = {}

        # keeps info about nodes that are currently streaming data
        # to this node
        self._streaming_info = {}
        self._streaming_nodes = set()

        # the greenlet running the initialization
        self._initializer = None

        # this cluster's view of the token ring
        # as it relates to streaming data to populate
        # it's own data store on node join
        self._initializer_ring = None

    def __contains__(self, item):
        return item in self.nodes

    def __len__(self):
        return len(self.nodes)

    @property
    def node_id(self):
        return self.local_node.node_id

    @property
    def token(self):
        return self.local_node.token

    @property
    def name(self):
        return self.local_node.name

    @property
    def store(self):
        """ :rtype: punisher.store.base.BaseStore """
        return self.local_node.store

    @property
    def is_initializing(self):
        return self.status == Cluster.Status.INITIALIZING

    @property
    def is_streaming(self):
        return self.status == Cluster.Status.STREAMING

    @property
    def is_normal(self):
        return self.status == Cluster.Status.NORMAL

    # ------------- server start/stop -------------

    def start(self):
        #TODO: check that existing peers are still up
        if not [n for n in self.nodes.values() if isinstance(n, RemoteNode)]:
            self.connect_to_seeds()
        else:
            self.discover_peers()
        self.is_online = True
        self._refresh_ring()

        # migrate data from existing nodes
        # if this node is initializing
        if self.is_initializing:
            self._initializer = gevent.spawn(self._initialize_data)

    def stop(self):
        self.is_online = False
        for node in self.nodes.values():
            if isinstance(node, LocalNode): continue
            node.stop()

    def kill(self):
        self.stop()

    # ------------- node administration -------------

    def add_node(self, node_id, address, token, name=None):
        """
        :param node_id:
        :param address:
        :param token:
        :param name:

        :rtype: RemoteNode
        """
        if node_id in self.nodes:
            return self.nodes[node_id]
        #setdefault is threadsafe
        node = self.nodes.setdefault(
            node_id, RemoteNode(
                address,
                token=long(token),
                node_id=node_id,
                name=name,
                local_node=self.local_node
            )
        )
        node.connect()
        self.discover_peers(only=node)
        self._refresh_ring()
        return node

    def remove_node(self, node_id):
        node = self.nodes.pop(node_id, None)
        self._refresh_ring()
        return node

    def get_node(self, node_id):
        """ :rtype: RemoteNode """
        return self.nodes.get(node_id)

    def get_peers(self):
        return [p for p in self.nodes.values() if not isinstance(p, LocalNode)]

    def discover_peers(self, only=None):
        """ finds the other nodes in the cluster """
        request = messages.DiscoverPeersRequest(self.node_id)

        if only is not None and not isinstance(only, (list, tuple)):
            only = [only]

        for peer in self.nodes.values():
            if isinstance(peer, LocalNode):
                continue
            assert isinstance(peer, RemoteNode)
            if only and peer not in only:
                continue
            response = peer.send_message(request)
            if not isinstance(response, messages.DiscoverPeersResponse):
                continue
            assert isinstance(response, messages.DiscoverPeersResponse)
            for entry in response.get_peer_data():
                if entry.node_id == self.node_id:
                    continue
                new_peer = self.add_node(entry.node_id, entry.address, entry.token, entry.name)
                new_peer.connect()

    def connect_to_seeds(self):
        for address in self.seed_peers:
            try:
                conn = Connection.connect(address)
                messages.ConnectionRequest(
                    self.local_node.node_id,
                    self.local_node.address,
                    self.local_node.token,
                    sender_name=self.local_node.name
                ).send(conn)
                response = messages.Message.read(conn)

                assert isinstance(response, messages.ConnectionAcceptedResponse)
                assert response.token is not None

                peer = self.add_node(
                    response.sender,
                    address,
                    response.token,
                    name=response.name
                )
                peer.add_conn(conn)
                return peer

            except Connection.ClosedException:
                pass
            # except AssertionError:
            #     pass

    def _refresh_ring(self):
        """ builds a view of the token ring """
        self.token_ring = sortedset(self.nodes.values(), key=lambda n: n.token)
        if self.is_initializing:
            # if this is the only node, set it to normal
            # there are no nodes to stream data from
            if len(self.nodes) == 1:
                self.status = Cluster.Status.NORMAL
                self._initializer_ring = None
                return

            stream_nodes = [n for n in self.nodes.values() if n.node_id != self.node_id]
            self._initializer_ring = sortedset(stream_nodes, key=lambda n: n.token)

        else:
            self._initializer_ring = None

    def get_token_range(self):
        """ find the range of tokens that this cluster's node owns or replicates """
        idx = [n.node_id for n in self.token_ring].index(self.node_id)
        if len(self.token_ring) <= self.replication_factor:
            return 0, self.partitioner.max_token

        max_token = self.token_ring[(idx + 1) % len(self.token_ring)].token - 1
        min_token = self.token_ring[(idx - (self.replication_factor - 1)) % len(self.token_ring)].token
        return min_token, max_token

    def _initialize_data(self):
        """ handles populating this node with data when it joins an existing cluster """
        idx = self.token_ring.index(self.local_node)
        stream_from = list(self.token_ring[(idx - 1 - self.replication_factor): (idx - 1)])
        stream_from += list(self.token_ring[idx + 1: idx + self.replication_factor])

        for node in stream_from:
            self._request_streamed_data(node)

    def change_token(self, node_id, token, alert_cluster=True):
        """
        Changes this node's token and starts the pulls in the new key ranges
        :param node_id:
        :param token:
        :param alert_cluster: indicates that the other nodes in the cluster
            should be notified of the change
        """
        node_id = node_id or self.node_id
        node = self.nodes[node_id]

        if token == node.token:
            return

        old_min, old_max = self.get_token_range()
        old_ring = self.token_ring
        node.token = token
        self._refresh_ring()
        new_min, new_max = self.get_token_range()

        # alert other nodes of the change
        if alert_cluster:
            for node in self.nodes:
                if node.node_id == self.node_id: continue
                node.send_message(messages.ChangedTokenRequest(self.node_id, node.node_id, token))

        # bail out if we don't need to do anything
        if old_min == new_min and old_max == new_max:
            return

        # otherwise, stream data in from surrounding nodes

    def stream_to_node(self, node_id):
        """
        streams data contained on the local node to the given remote
        node

        :param node_id: the id of the remote node to stream data to
        :type node_id: UUID
        """
        node = self.nodes[node_id]
        for key in self.store.all_keys():
            if node in self.get_nodes_for_key(key):
                response = node.send_message(messages.StreamDataRequest(
                    self.node_id,
                    [pickle.dumps((key, self.store.get_raw_value(key)))]
                ))
                assert isinstance(response, messages.StreamDataResponse)

        response = node.send_message(messages.StreamCompleteRequest(
            self.node_id,
        ))
        assert isinstance(response, messages.StreamCompleteResponse)

    def _request_streamed_data(self, node):
        """
        requests a node to stream data to the requesting node
        :param node:
        """
        if node.node_id == self.node_id: return
        self._streaming_nodes.add(node.node_id)
        self.status = Cluster.Status.STREAMING
        response = node.send_message(messages.StreamRequest(self.node_id))
        assert isinstance(response, messages.StreamResponse)

    def _end_streaming(self, node_id):
        """
        handles a notification that a node is finished streaming data to this node
        :param node_id:
        """
        self._streaming_nodes.remove(node_id)
        if len(self._streaming_nodes) == 0:
            self.status = Cluster.Status.NORMAL

    def _receive_streamed_values(self, data):
        for d in data:
            key, val = pickle.loads(d)
            self.store.set_and_reconcile_raw_value(key, val)

    # ------------- request handling -------------

    def get_nodes_for_token(self, token, ring=None):
        """

        :param token:
        :param ring:
        :return:
        """
        ring = ring or self.token_ring
        # bisect returns the the insertion index for
        # the given token, which is always 1 higher
        # than the owning node, so we subtract 1 here,
        # and wrap the value to the length of the ring
        idx = (ring.bisect(_TokenContainer(token)) - 1) % len(ring)
        return [ring[(idx + i) % len(ring)] for i in range(self.replication_factor)]

    def get_nodes_for_key(self, key):
        """
        returns the owner and replica nodes for the given token

        :param key:
        :return:
        """
        if self.replication_factor == 0:
            return self.nodes.values()

        token = self.partitioner.get_key_token(key)
        return self.get_nodes_for_token(token)

    def _finalize_retrieval(self, instruction, key, args, gpool, greenlets):
        """
        finalizes the retrieval, repairing any discrepancies in data

        :param instruction:
        :param key:
        :param args:
        :param gpool: greenlet pool
        :type gpool: Pool
        :param greenlets:
        :type greenlets: list of Greenlet
        """
        gpool.join(timeout=10)

        # do we want to do anything with the exception? (g.exception)
        result_map = {g.node.node_id: g.value for g in greenlets}
        instructions = getattr(self.store, 'resolve_{}_instructions'.format(instruction))(key, args, result_map)
        for node_id, instruction_set in instructions.items():
            node = self.nodes[node_id]
            for instr in instruction_set:
                node.execute_mutation_instruction(instr.instruction, instr.key, instr.args, instr.timestamp)

    def execute_retrieval_instruction(self, instruction, key, args, consistency=None, synchronous=False):
        """
        executes a retrieval instruction against the cluster, and performs any
        reconciliation needed

        :param instruction:
        :param key:
        :param args:
        :return:gg
        """
        results = Queue()
        nodes = self.get_nodes_for_key(key)
        response_timeout = 10.0

        def _execute(node):
            if node.node_id == self.local_node.node_id and self.is_initializing:
                raise NotImplementedError(
                    'performing queries against intializing nodes is not yet supported'
                )
            result = node.execute_retrieval_instruction(instruction, key, args)
            results.put(result)
            return result
        pool = Pool(50)
        greenlets = []
        for node in nodes:
            greenlet = pool.spawn(_execute, node)
            greenlet.node = node
            greenlets.append(greenlet)
        consistency = self.default_read_consistency if consistency is None else consistency

        num_replies = {
            Cluster.ConsistencyLevel.ONE: 1,
            Cluster.ConsistencyLevel.QUORUM: (len(nodes) / 2) + 1,
            Cluster.ConsistencyLevel.ALL: len(nodes)
        }[consistency]

        values = [results.get(timeout=response_timeout) for _ in range(num_replies)]
        # resolve any differences
        result = getattr(self.store, 'resolve_{}'.format(instruction))(key, args, values)

        # spin up a greenlet to resolve any differences
        reconciler = gevent.spawn(self._finalize_retrieval, instruction, key, args, pool, greenlets)
        if synchronous:
            reconciler.join()

        return result.data

    def _finalize_mutation(self, instruction, key, args, timestamp, gpool, greenlets):
        """

        :param instruction:
        :param key:
        :param args:
        :param timestamp:
        :param gpool:
        :param greenlets:
        :type greenlets: list of Greenlet
        """
        #TODO: distribute hints for nonresponsive nodes locally and to other nodes
        gpool.join(timeout=10)

    def execute_mutation_instruction(self, instruction, key, args, timestamp=None, consistency=None, synchronous=False):
        """

        :param instruction:
        :param key:
        :param args:
        :param timestamp:
        :param consistency:
        :return:
        """
        timestamp = timestamp or datetime.utcnow()
        response_timeout = 10.0

        results = Queue()
        nodes = self.get_nodes_for_key(key)

        def _execute(node):
            if node.node_id == self.local_node.node_id and self.is_initializing:
                raise NotImplementedError(
                    'performing queries against intializing nodes is not yet supported'
                )
            result = node.execute_mutation_instruction(instruction, key, args, timestamp)
            results.put(result)
            return result

        pool = Pool(50)
        greenlets = []
        for node in nodes:
            greenlet = pool.spawn(_execute, node)
            greenlet.node = node
            greenlets.append(greenlet)
        consistency = self.default_read_consistency if consistency is None else consistency

        num_replies = {
            Cluster.ConsistencyLevel.ONE: 1,
            Cluster.ConsistencyLevel.QUORUM: (len(nodes) / 2) + 1,
            Cluster.ConsistencyLevel.ALL: len(nodes)
        }[consistency]

        values = [results.get(timeout=response_timeout) for _ in range(num_replies)]
        # resolve any differences
        result = getattr(self.store, 'resolve_{}'.format(instruction))(key, args, timestamp, values)

        # spin up a greenlet to resolve any differences
        reconciler = gevent.spawn(self._finalize_mutation, instruction, key, args, timestamp, pool, greenlets)
        if synchronous:
            reconciler.join()

        return result.data

