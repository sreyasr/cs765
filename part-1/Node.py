import asyncio
import datetime
import json
import logging
from hashlib import md5

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.CRITICAL)

log.addHandler(stdout_handler)


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

    async def connect_to(self, ip, port):
        try:
            self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(self.ip, self.port), timeout=2)
        except asyncio.TimeoutError:
            log.debug("write operation cancelled due to timeout")
        except ConnectionRefusedError:
            log.debug("connection refused with {}:{}".format(self.ip, self.port))
            return None

        msg = {'message_type': 'Establish_Peer_Connection', 'SELF_IP': ip,
               'SELF_PORT': port}
        await self.write(msg)

        return True

    async def send_liveness_reply(self, my_ip, my_port, timestamp):
        reply = {'message_type': 'Liveness_Reply', 'timestamp': timestamp, 'SENDER_IP': self.ip,
                 'SENDER_PORT': self.port,
                 'SELF_IP': my_ip, 'SELF_PORT': my_port}
        await self.write(reply)

    async def send_liveness_request(self, my_ip, my_port):
        message = {'message_type': 'Liveness_Request', 'timestamp': str(datetime.datetime.now()),
                   'SELF_IP': my_ip, 'SELF_PORT': my_port}
        await self.write(message)

    async def send_gossip_message(self, my_ip, my_port, content):
        message = {'message_type': 'Gossip_Message', 'timestamp': str(datetime.datetime.now()),
                   'SELF_IP': my_ip,
                   'SELF_PORT': my_port,
                   'message_content': content}
        await self.write(message)


# Neighbouring seed node
class SeedNode(Node):
    def __init__(self, ip, port):
        super().__init__(ip, port)

    async def connect(self):
        try:
            self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(self.ip, self.port), timeout=3)
            return 0
        except ConnectionRefusedError:
            log.debug("SeedNode Refused Connection: %r:%r" % (self.ip, self.port))
            return None

    async def send_registration_request(self, ip, port):
        message = {
            'message_type': 'Registration_Request',
            'SELF_IP': ip,
            'SELF_PORT': port
        }
        await self.write(message)

    async def set_peer_dead(self, dead_ip, dead_port, timestamp, self_ip, self_port):
        msg = {'message_type': 'Dead_Peer_Info', 'DEAD_IP': dead_ip, 'DEAD_PORT': dead_port,
               'timestamp': timestamp, 'SELF_IP': self_ip, 'SELF_PORT': self_port}
        log.info("Sending dead peer {}:{}".format(dead_ip, dead_port))
        await self.write(msg)

    async def send_peer_list_request(self):
        message = {'message_type': 'Peer_List_Request'}
        await self.write(message)
        data = await self.read()
        if data is None:
            return []
        else:
            data = data[0]
            return [tuple(x) for x in data['content']]


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
        log.debug("NodeList bool: len(list):%s, bool(list):%s" % (len(self.list), bool(self.list)))
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

    async def gossip_broadcast(self, my_ip, my_port, content):
        t = [peer_node.send_gossip_message(my_ip, my_port, content) for peer_node in self]
        await asyncio.gather(*t)

    async def liveness_request_broadcast(self, my_ip, my_port):
        t = [peer_node.send_liveness_request(my_ip, my_port) for peer_node in self]
        await asyncio.gather(*t)

    async def gossip_forward_broadcast(self, my_ip, my_port, content, received_peer_node):
        t = [peer_node.send_gossip_message(my_ip, my_port, content) for peer_node in self if
             peer_node != received_peer_node.id]
        await asyncio.gather(*t)

    def remove_peer_node(self, peer_node):
        try:
            peer_node.writer.close()
        except AttributeError:
            log.debug("NoneType object in writer")
        self.remove(peer_node)


# list of SeedNodes
class SeedNodeList(NodeList):
    def __init__(self):
        super(SeedNodeList, self).__init__()

    async def set_peer_dead(self, dead_ip, dead_port, timestamp, self_ip, self_port):
        await asyncio.gather(
            *[seed_node.set_peer_dead(dead_ip, dead_port, timestamp, self_ip, self_port) for seed_node in self])
