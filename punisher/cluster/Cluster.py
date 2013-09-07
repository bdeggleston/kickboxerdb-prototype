import gevent

from punisher.cluster import messages
from punisher.cluster.Connection import Connection
from punisher.cluster.LocalNode import LocalNode
from punisher.cluster.RemoteNode import RemoteNode

__author__ = 'bdeggleston'


class Cluster(object):
    """
    Maintains the local view of the cluster, and coordinates client requests, and
    is responsible for handling replication and routing
    """

    def __init__(self, local_node, seed_peers=None, replication_factor=3):
        super(Cluster, self).__init__()
        self.seed_peers = seed_peers or []
        self.replication_factor = replication_factor

        assert isinstance(local_node, LocalNode)
        self.local_node = local_node
        self.nodes = {self.local_node.token: self.local_node}

        # cluster token data
        self.min_token = None
        self.max_token = None

        self.is_online = False

        # self.nanny = None

    def __contains__(self, item):
        return item in self.nodes

    def __len__(self):
        return len(self.nodes)

    # def _nanny(self):
    #     """ background thread that checks the status of other nodes """
    #     import ipdb; ipdb.set_trace()
    #     print 'nanny'
    #     gevent.sleep(0.1)

    def start(self):
        #TODO: connect to peers
        if self.nodes:
            #TODO: check that existing peers are still up
            pass
        else:
            self.connect_to_seeds()

        self.is_online = True
        # self.nanny = gevent.spawn(self._nanny)

    def stop(self):
        # TODO: disconnect from peers
        self.is_online = False

    def kill(self):
        # TODO: kill connections to peers
        self.is_online = False

    def add_node(self, node_id, address, token, name=None):
        """
        :param node_id:
        :param address:
        :param token:
        :param name:

        :rtype: RemoteNode
        """
        #setdefault is threadsafe
        return self.nodes.setdefault(
            node_id, RemoteNode(
                address,
                token=token,
                node_id=node_id,
                name=name,
                local_node=self.local_node
            )
        )

    def remove_node(self, node_id):
        return self.nodes.pop(node_id, None)

    def get_node(self, node_id):
        return self.nodes.get(node_id)

    def _refresh_ring(self):
        pass

    def connect_to_seeds(self):
        for address in self.seed_peers:
            try:
                conn = Connection.connect(address)
                messages.ConnectionRequest(
                    self.local_node.node_id,
                    self.local_node.address,
                    sender_name=self.local_node.name
                ).send(conn)
                response = messages.Message.read(conn)
                assert isinstance(response, messages.ConnectionAcceptedResponse)
                if response.sender not in self:
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

    def execute_retrieval_instruction(self, instruction, key, args):
        pass

    def execute_mutation_instruction(self, instruction, key, args, timestamp):
        pass
