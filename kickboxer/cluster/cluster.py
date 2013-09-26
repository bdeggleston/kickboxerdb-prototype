from datetime import datetime
import pickle

from blist import sortedset
import gevent
from gevent.queue import Queue
from gevent.pool import Pool

from kickboxer.cluster import messages
from kickboxer.cluster.connection import Connection
from kickboxer.cluster.node.local import LocalNode
from kickboxer.cluster.node.remote import RemoteNode


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
        :type partitioner: kickboxer.partitioner.base.BasePartitioner
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

        # the set of node ids currently streaming data to this
        # node, if any
        self._streaming_nodes = set()

        # this cluster's view of the token ring
        # before the last token change, to help
        # coordinate reads while it's streaming
        self._previous_ring = None

    def __contains__(self, item):
        return item in self.nodes

    def __len__(self):
        return len(self.nodes)

    def __repr__(self):
        return '<Cluster name={} token={}>'.format(self.name, self.token)

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
        """ :rtype: kickboxer.store.base.BaseStore """
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
            self.join_cluster()

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

    def _refresh_ring(self):
        """ builds a view of the token ring """
        self.token_ring = sortedset(self.nodes.values(), key=lambda n: n.token)
        if self.is_initializing:
            # if this is the only node, set it to normal
            # there are no nodes to stream data from
            if len(self.nodes) == 1:
                self.status = Cluster.Status.NORMAL
                self._previous_ring = None
                return

            stream_nodes = [n for n in self.nodes.values() if n.node_id != self.node_id]
            self._previous_ring = sortedset(stream_nodes, key=lambda n: n.token)

        else:
            self._previous_ring = None

    def get_token_range(self):
        """ find the range of tokens that this cluster's node owns or replicates """
        idx = [n.node_id for n in self.token_ring].index(self.node_id)
        if len(self.token_ring) <= self.replication_factor:
            return 0, self.partitioner.max_token

        max_token = self.token_ring[(idx + 1) % len(self.token_ring)].token - 1
        min_token = self.token_ring[(idx - (self.replication_factor - 1)) % len(self.token_ring)].token
        return min_token, max_token

    def join_cluster(self):
        """
        called when a node is first added to the cluster

        When changing the token ring from this:
        N0      N1      N2      N3      N4      N5      N6      N7      N8      N9
        [00    ][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]

        to this:
        N0  N10 N1      N2      N3      N4      N5      N6      N7      N8      N9
        [00][05][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
        |--|->

        N10 should stream data from the node to it's left, since it's taking control
        of a portion of it's previous token space
        """
        ring = [n.node_id for n in self.token_ring]
        idx = ring.index(self.node_id)
        from_node = self.nodes[ring[(idx - 1) % len(ring)]]
        self._request_streamed_data(from_node)

    def change_token(self, token, node_id=None, alert_cluster=True):
        """
        Changes the given node's token and initiates streaming from new replica nodes

        When changing the token ring from this:
        N0      N1      N2      N3      N4      N5      N6      N7      N8      N9
        [00    ][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
                 --> --> --> --> --> --> --> --> --> --> -->|
        to this:
        N0              N2      N3      N4      N5      N6  N1* N7      N8      N9
        [00            ][20    ][30    ][40    ][50    ][60][65][70    ][80    ][90    ]
                <-------|------|                        |--|->
                |------|----------->

        N0 should now control N1's old tokens, and N1 should control half of N6's tokens

        After the token has been changed, each node should check if the node to it's left
        has changed. If it has, it should stream data from the left. If the node to the right
        has changed, then it should stream data from the right

        There is also

        If a node starts streaming in data as soon as it knows it's token space changes, there
        will be a race condition that may prevent the correct data being streamed to the node
        if the node doing the streaming is not aware of the token when it receives the request.

        :param token: the new token
        :param node_id: the id of the node to move. If it's None, the local node will be moved
        :param alert_cluster: indicates that the other nodes in the cluster
            should be notified of the change
        """
        node_id = node_id or self.node_id
        node = self.nodes[node_id]

        if token == node.token:
            return

        old_ring = [n.node_id for n in self.token_ring]
        node.token = token
        self._refresh_ring()
        new_ring = [n.node_id for n in self.token_ring]

        # alert other nodes of the change
        if alert_cluster:
            for node in self.nodes.values():
                if node.node_id == self.node_id: continue
                node.send_message(messages.ChangedTokenRequest(self.node_id, node.node_id, token))

        # determine which, if any, node to stream data from
        old_idx = old_ring.index(self.node_id)
        new_idx = new_ring.index(self.node_id)

        def _stream_from_src(src_node):
            # guarantee that the src node is already
            # aware of the token change by blocking
            # until it has acknowledged the token change
            response = src_node.send_message(
                messages.ChangedTokenRequest(
                    self.node_id,
                    node.node_id,
                    token
                )
            )
            assert isinstance(response, messages.ChangedTokenResponse)
            self._request_streamed_data(src_node)

        def _get_offset_nodes(offset):
            old_node = old_ring[(old_idx + offset) % len(old_ring)]
            new_node = new_ring[(new_idx + offset) % len(new_ring)]
            return old_node, new_node

        old_left, new_left = _get_offset_nodes(-1)
        if old_left != new_left:
            src_node = self.nodes[new_left]
            _stream_from_src(src_node)

        old_right, new_right = _get_offset_nodes(1)
        if old_right != new_right:
            src_node = self.nodes[new_right]
            _stream_from_src(src_node)

    def remove_node(self, node_id=None, alert_cluster=True):
        """
        removes the given node from the token ring

        removing N1
        N0      N1      N2      N3      N4      N5      N6      N7      N8      N9
        [0     ][10    ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
                |xxxxxx|
        to this:
        N0              N2      N3      N4      N5      N6      N7      N8      N9
        [0             ][20    ][30    ][40    ][50    ][60    ][70    ][80    ][90    ]
                 <------|------|

        N0 should now control N1's old tokens and  N0 should stream data from N2

        After the node is removed from the ring, each node should check if the node to
        it's right has changed, if it has, it should stream data from it. If the node
        to it's left has changed, it should not stream data from that node, since it
        was already replicating the token space that the new node was responsible for

        :param node_id:
        :param alert_cluster: indicates that the other nodes in the cluster
            should be notified of the change
        """
        node_id = node_id or self.node_id
        try:
            node = self.nodes[node_id]
        except KeyError:
            return

        def _alert_cluster():
            if alert_cluster:
                for dst_node in self.nodes.values():
                    if dst_node.node_id == self.node_id: continue
                    dst_node.send_message(messages.RemoveNodeRequest(self.node_id, node.node_id))

        if node_id == self.node_id:
            _alert_cluster()
            return

        old_ring = [n.node_id for n in self.token_ring]
        self.nodes.pop(node.node_id)
        self._refresh_ring()
        new_ring = [n.node_id for n in self.token_ring]

        _alert_cluster()

        # determine which, if any, node to stream data from
        old_idx = old_ring.index(self.node_id)
        new_idx = new_ring.index(self.node_id)

        def _stream_from_src(src_node):
            # guarantee that the src node is already
            # aware of the removed node by blocking
            # until it has acknowledged the token change
            response = src_node.send_message(
                messages.RemoveNodeRequest(
                    self.node_id,
                    node.node_id
                )
            )
            assert isinstance(response, messages.RemoveNodeResponse)
            self._request_streamed_data(src_node)

        def _get_offset_nodes(offset):
            old_node = old_ring[(old_idx + offset) % len(old_ring)]
            new_node = new_ring[(new_idx + offset) % len(new_ring)]
            return old_node, new_node

        old_right, new_right = _get_offset_nodes(1)
        if old_right != new_right:
            src_node = self.nodes[new_right]
            _stream_from_src(src_node)

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
            if node.node_id == self.local_node.node_id and self.is_initializing or self.is_streaming:
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
            if node.node_id == self.local_node.node_id and self.is_initializing or self.is_streaming:
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

