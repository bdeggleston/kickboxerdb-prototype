from datetime import datetime
from hashlib import md5
import struct

from blist import sortedset
import gevent
from gevent.queue import Queue
from gevent.pool import Pool

from punisher.cluster import messages
from punisher.cluster.Connection import Connection
from punisher.cluster.LocalNode import LocalNode
from punisher.cluster.RemoteNode import RemoteNode


class _TokenContainer(object):
    def __init__(self, token):
        self.token = token


class Cluster(object):
    """
    Maintains the local view of the cluster, and coordinates client requests, and
    is responsible for handling replication and routing

    replication_factor of 0 will mirror all data to all nodes
    """

    class ConsistencyLevel(object):
        ONE     = 1
        QUORUM  = 2
        ALL     = 3

    default_read_consistency = ConsistencyLevel.QUORUM
    default_write_consistency = ConsistencyLevel.QUORUM

    def __init__(self, local_node, seed_peers=None, replication_factor=3):
        super(Cluster, self).__init__()
        self.seed_peers = seed_peers or []
        self.replication_factor = max(0, replication_factor)

        assert isinstance(local_node, LocalNode)
        self.local_node = local_node
        self.nodes = {self.local_node.node_id: self.local_node}

        # cluster token data
        self.min_token = None
        self.max_token = None
        self.token_ring = None

        self.is_online = False

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
        return self.local_node.store

    def start(self):
        #TODO: check that existing peers are still up
        if not [n for n in self.nodes.values() if isinstance(n, RemoteNode)]:
            self.connect_to_seeds()

        self.get_peers()
        self.discover_peers()
        self.is_online = True

        self._refresh_ring()

    def stop(self):
        self.is_online = False
        for node in self.nodes.values():
            if isinstance(node, LocalNode): continue
            node.stop()

    def kill(self):
        self.stop()

    def add_node(self, node_id, address, token, name=None):
        """
        :param node_id:
        :param address:
        :param token:
        :param name:

        :rtype: RemoteNode
        """
        #setdefault is threadsafe
        node = self.nodes.setdefault(
            node_id, RemoteNode(
                address,
                token=token,
                node_id=node_id,
                name=name,
                local_node=self.local_node
            )
        )
        node.connect()
        if self.is_online:
            self._refresh_ring()
        return node

    def remove_node(self, node_id):
        node = self.nodes.pop(node_id, None)
        if self.is_online:
            self._refresh_ring()
        return node

    def get_node(self, node_id):
        """ :rtype: RemoteNode """
        return self.nodes.get(node_id)

    def get_peers(self):
        return [p for p in self.nodes.values() if not isinstance(p, LocalNode)]

    def discover_peers(self):
        """ finds the other nodes in the cluster """
        request = messages.DiscoverPeersRequest(self.node_id)
        for peer in self.nodes.values():
            if isinstance(peer, LocalNode):
                continue
            assert isinstance(peer, RemoteNode)
            response = peer.send_message(request)
            if not isinstance(response, messages.DiscoverPeersResponse):
                continue
            assert isinstance(response, messages.DiscoverPeersResponse)
            for entry in response.get_peer_data():
                if entry.node_id == self.node_id:
                    continue
                new_peer = self.add_node(entry.node_id, entry.address, entry.token, entry.name)
                new_peer.connect()

    def _refresh_ring(self):
        self.token_ring = sortedset(self.nodes.values(), key=lambda n: n.token)

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
            except AssertionError:
                pass

    def get_nodes_for_key(self, key):
        """
        returns the owner and replica nodes for the given token

        :param key:
        :return:
        """
        if self.replication_factor == 0:
            return self.nodes.values()
        ring = self.token_ring

        hsh = md5(key)
        u1, u2 = struct.unpack('!QQ', hsh.digest())
        token = (u1 << 64) | u2

        # bisect returns the the insertion index for
        # the given token, which is always 1 higher
        # than the owning node, so we subtract 1 here,
        # and wrap the value to the length of the ring
        idx = (ring.bisect(_TokenContainer(token)) - 1) % len(ring)
        return [ring[(idx + i) % len(ring)] for i in range(self.replication_factor)]

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

    def execute_retrieval_instruction(self, instruction, key, args, consistency=None):
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

        def _execute(node):
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

        values = [results.get() for _ in range(num_replies)]
        # resolve any differences
        result = getattr(self.store, 'resolve_{}'.format(instruction))(key, args, values)

        # spin up a greenlet to resolve any differences
        gevent.spawn(self._finalize_retrieval, instruction, key, args, pool, greenlets)

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

    def execute_mutation_instruction(self, instruction, key, args, timestamp=None, consistency=None):
        """

        :param instruction:
        :param key:
        :param args:
        :param timestamp:
        :param consistency:
        :return:
        """
        timestamp = timestamp or datetime.utcnow()

        #TODO... everything

