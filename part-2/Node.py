import asyncio
import json
import logging
from collections import namedtuple
from hashlib import md5

log = logging.getLogger(__name__)
log.setLevel(logging.ERROR)

stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.DEBUG)

log.addHandler(stdout_handler)

Block = namedtuple('Block', 'block_index prev_hash merkel_root timestamp hash')


# Node is the neighbouring peers/seeds as seen from peer
# It is used to organize all information regarding the connected peer
class Node:
    encode_format = 'utf-8'
    max_size = 8192

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.id = md5("{}:{}".format(self.ip, self.port).encode()).hexdigest()
        self.writer = None
        self.reader = None

    def __eq__(self, other):
        if isinstance(other, Node):
            return self.id == other.id
        elif isinstance(other, (tuple, list)) and len(other) == 2:
            return self.ip == other[0] and self.port == other[1]
        elif isinstance(other, str):
            return self.id == other
        return False

    def __repr__(self):
        s = dict()
        s['ip'] = self.ip
        s['port'] = self.port
        s['id'] = self.id
        return str(s)

    @staticmethod
    def encode_data(data):
        return (json.dumps(data) + '\0').encode(Node.encode_format)

    @staticmethod
    def decode_data(data):
        def loads(expr):
            try:
                return json.loads(expr)
            except Exception as e:
                log.info("exception in decode_data:{0}".format(e))
                return dict()

        return list(map(loads, data.decode(Node.encode_format).strip('\0').split('\0')))

    # send message
    async def write(self, message):
        self.writer.write(Node.encode_data(message))
        try:
            await asyncio.wait_for(self.writer.drain(), timeout=2)
        except Exception as e:
            log.debug("Exception in write:{}".format(e))

    # receive message
    async def read(self):
        try:
            data = await self.reader.read(Node.max_size)
            if not data:
                return None
            return Node.decode_data(data)
        except Exception as e:
            log.debug("Exception in write:{}".format(e))
            return None


# Neighbouring Peer node
class PeerNode(Node):
    def __init__(self, ip, port, con_to_me, con_to_me_ip=None, con_to_me_port=None):
        super().__init__(ip, port)
        self.delayed_liveness_reply = 0
        self.connected_to_me = con_to_me
        if self.connected_to_me is False:
            self.connected_ip = ip
            self.connected_port = port
        else:
            self.connected_ip = con_to_me_ip
            self.connected_port = con_to_me_port
        if self.connected_ip is None or self.connected_port is None:
            log.critical("No port socket on Node")

    def is_connected_to_me(self):
        return self.connected_to_me

    async def send_block(self, block_no, prev_hash, merkel_root, timestamp):
        message = {'message_type': 'block_message',
                   'block_no': block_no,
                   'timestamp': timestamp,
                   'prev_hash': prev_hash,
                   'merkel_root': merkel_root
                   }
        await self.write(message)

    async def send_block_history_request(self):
        message = {'message_type': 'Block_History_Request'}
        await self.write(message)

    async def send_block_history(self, block_history):
        message = {'message_type': 'Block_History_Reply',
                   'block_history': block_history}
        await self.write(message)

    async def send_registration_request(self, ip, port):
        try:
            self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(self.ip, self.port), timeout=2)
        except asyncio.TimeoutError:
            log.debug("write operation cancelled due to timeout")
        except ConnectionRefusedError:
            log.debug("connection refused with {}:{}".format(self.ip, self.port))
            return None

        msg = {'message_type': 'Registration_Request', 'SELF_IP': ip,
               'SELF_PORT': port}
        await self.write(msg)

    async def send_recent_block(self, block_history):
        maximum_pos = max(block_history.keys())
        recent_blocks = block_history[maximum_pos]
        msg = {'message_type': 'Recent_Blocks_Response',
               'recent_blocks': recent_blocks,
               'block_no': maximum_pos}
        await self.write(msg)

    async def send_recent_block_request(self):
        msg = {'message_type': 'Recent_Blocks_Request'}
        await self.write(msg)

    async def send_peer_list_request(self):
        msg = {'message_type': 'Peer_List_Request'}
        await self.write(msg)
        data = (await self.read())[0]
        return data['content']


class NodeList:
    def __init__(self):
        self.list = list()

    def __len__(self):
        return len(self.list)

    def __getitem__(self, position):
        return self.list[position]

    def append(self, node):
        self.list.append(node)

    def __bool__(self):
        return bool(self.list)

    def get_node_by_id(self, node_id):
        if node_id in self:
            node_pos = self.list.index(node_id)
            return self.list[node_pos]
        else:
            return None

    def remove(self, peer_node):
        self.list.remove(peer_node)


# List of PeerNodes
class PeerNodeList(NodeList):
    def __init__(self):
        super(PeerNodeList, self).__init__()

    def create_node(self, *argv):
        node = PeerNode(*argv)
        self.append(node)
        return node

    async def block_broadcast(self, block_no, prev_hash, merkel_root, timestamp):
        t = [peer_node.send_block(block_no, prev_hash, merkel_root, timestamp) for peer_node in self]
        await asyncio.gather(*t)

    def remove_peer_node(self, peer_node):
        try:
            peer_node.writer.close()
        except AttributeError:
            log.debug("NoneType object in writer")
        self.remove(peer_node)
