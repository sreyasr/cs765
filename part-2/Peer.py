import argparse
import random
import time
from collections import deque
from datetime import datetime, timedelta
from hashlib import sha256

import numpy

import Node
from Node import *

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.CRITICAL)

log.addHandler(stdout_handler)


def get_hash(key):
    return sha256(key.encode()).hexdigest()[:4]


def get_block(block_index, prev_hash, merkel_root, timestamp):
    return Block(block_index, prev_hash, merkel_root, timestamp,
                 get_hash("%s:%s:%s" % (prev_hash, merkel_root, timestamp)))


def exp_rand_var():
    _inter_arrival_time = 60
    _node_hash_power = 0.2
    _global_lambda = 1 / _inter_arrival_time
    _lambda = _node_hash_power * _global_lambda / 100
    _waiting_time = numpy.random.exponential(scale=1.0, size=None)
    return _waiting_time


class MessageList:
    def __init__(self):
        self.dict = dict()

    def __getitem__(self, item):
        hash_item = md5(item.encode()).hexdigest()
        return self.dict[hash_item]

    def __setitem__(self, key, value):
        hash_key = md5(key.encode()).hexdigest()
        self.dict[hash_key] = value

    def __len__(self):
        return len(self.dict)

    def get(self, item, default=None):
        hash_item = md5(item.encode()).hexdigest()
        return self.dict.get(hash_item, default) or list()

    def __contains__(self, item):
        hash_item = md5(item.encode()).hexdigest()
        return hash_item in self.dict


genesis_block = Block(0, "0000", "0000", "27858", "9e1c")


#  class peer to store information of peer
class Peer:
    max_data = 8192
    encode_format = 'utf-8'

    def __init__(self, ip="localhost", port=0, output_file=None, input_file="config.txt"):
        self.ip = ip
        self.port = port
        self.total_peer_list = []  # all peer list received from seed
        # total seed obtained from file will be used to send liveness_check and gossip; d[address] = reader,writer)
        self.total_seed_list = list()
        self.peer_list = []

        self.file_name = input_file  # file from seed will be acquired
        self.message_list = MessageList()  # map hash to a list of address whom received the message

        self.main_server_event = asyncio.Event()
        self.peer_node_list = PeerNodeList()
        self.output_file = output_file

        self.block_chain_history = {0: [genesis_block]}
        self.mining_time = 100
        self.new_block = False
        self.block_queue = list()
        self.pending_queue = deque()
        self.blocks_configured = False
        self.recent_blocks_event = asyncio.Event()
        self.pending_queue_event = asyncio.Event()
        self.block_height = 0

        if output_file is not None:
            open(output_file, "w").close()

    # function to print to screen and file
    def flush_out(self, text):
        if self.output_file is not None:
            with open(self.output_file, 'a') as f:
                f.write(text + "\n")
        print(text)

    def append_block(self, block):
        (self.block_chain_history.get(block.block_index, None) or list()).append(block)

    async def mine_block(self):
        await self.pending_queue_event.wait()
        while True:
            for _ in range(self.mining_time):
                await asyncio.sleep(1)
                if self.new_block:
                    break
            block_no = len(self.block_chain_history)
            block = get_block(block_no, self.block_chain_history[block_no - 1][0].hash, "0000", str(int(time.time())))
            (self.block_chain_history.get(block.block_index, None) or list()).insert(0, block)

    def is_block_valid(self, block: Block):
        block_no = block.block_index
        lm = self.block_chain_history[block_no]
        i: Block
        for i in lm:
            if isinstance(i, Block):
                if i.hash == block.prev_hash:
                    return True
            else:
                log.debug("ERROR on block validity")
        return False

    # detect the incoming message received and act accordingly
    def process(self, peer_node, data):
        if data['message_type'] == 'block_message':
            block = Block(data['prev_hash'], data['timestamp'], data['merkel_root'])
            self.pending_queue.append(block)
        elif data['message_type'] == 'block_history_request':
            peer_node.send_block_history(self.block_chain_history)
        elif data['message_type'] == 'recent_block_request':
            peer_node.send_recent_block(self.block_chain_history)
        elif data['message_type'] == 'block_history_reply':
            block_history = data['block_history']
            for key, value in block_history.items():
                block_no = int(key)
                blocks = list(map(lambda x: Block(block_no, *x[1:]), value))
                self.block_chain_history[block_no] = blocks
        elif data['message_type'] == 'Recent_Blocks_Response':
            recent_blocks = data['recent_blocks']
            block_no = data['block_no']
            blocks = map(lambda x: Block(block_no, *x[1:]), recent_blocks)
            self.block_chain_history[block_no] = list(blocks)

    # handle the peer messages: liveness request, liveness reply etc...
    async def handle_peer_messages(self, peer_node):
        while True:
            data = await peer_node.read()
            if data is None:
                break
            for message in data:
                self.process(peer_node, message)
        peer_node.writer.close()

    # The main server that will be used to receive all the connections
    async def start_main_server(self):
        async def main_server(reader, writer):
            data = await reader.read(Peer.max_data)
            data = Node.decode_data(data)
            if data is None:
                writer.close()
                return
            data = data[0]
            con_ip = writer.get_extra_info('peername')[0]
            con_port = writer.get_extra_info('peername')[1]
            ip = data['SELF_IP']
            port = data['SELF_PORT']
            peer_node = self.peer_node_list.create_node(ip, port, True, con_ip, con_port)
            peer_node.writer = writer
            peer_node.reader = reader

            if data['message_type'] == 'Registration_Request':
                peer_address = data['SELF_IP'], data['SELF_PORT']
                if peer_address not in self.total_peer_list:
                    self.total_peer_list.append(peer_address)

                await self.handle_peer_messages(peer_node)

        async def run_server(host, port):
            server = await asyncio.start_server(main_server, host, port)
            self.ip, self.port = server.sockets[0].getsockname()
            self.main_server_event.set()
            return server

        return await run_server(self.ip, self.port)

    async def consume_block_queue(self):
        await self.pending_queue_event.wait()
        while True:
            req_list = []
            del_list = []
            for i in self.block_queue:
                if i[0] > datetime.now():
                    del_list.append(i)
                else:
                    req_list.append(i)
            for block_meta in del_list:
                self.append_block(block_meta[1])
            self.block_queue = req_list
            await asyncio.sleep(1)

    # return list of seeds from file
    def get_seed_from_file(self):
        file_name = self.file_name
        seed_list = []
        with open(file_name, "r") as f:
            for line in f.readlines():
                address = line.split(",")
                ip = address[0]
                port = int(address[1])
                address = (ip, port)
                seed_list.append(address)
        self.total_seed_list = list(set(seed_list))
        return self.total_seed_list

    # return the entire list of peer received from seeds
    async def get_peer_from_seed(self, seed_list):
        peer_list = []
        for address in seed_list:
            peer_node = PeerNode(address[0], address[1], False)
            await peer_node.send_registration_request(self.ip, self.port)
            peer_list.append(peer_node)
        return peer_list

    async def establish_peer_connection(self, peer_list):
        self.peer_list = list(set(peer_list))
        self.total_peer_list = list(set(self.total_peer_list + self.peer_list))

        await asyncio.gather(*[self.handle_peer_messages(i) for i in self.peer_list])

    async def initial_set_up(self):
        while len(self.peer_list) == 0:
            await asyncio.sleep(3)
        if len(self.peer_list) == 0:
            return
        peer_node = self.peer_list[0]
        await peer_node.send_recent_block_request()
        await self.recent_blocks_event.wait()
        peer_node.send_recent_block_request()

    async def process_pending_queue(self):
        while self.blocks_configured is False:
            await asyncio.sleep(2)

        while True:
            block = self.pending_queue.popleft()
            if self.is_block_valid(block):
                x = datetime.now() + timedelta(seconds=int(exp_rand_var())), block
                self.block_queue.append(x)
                raise NotImplementedError

    # The entry point to all functions
    async def start(self):
        task1 = self.start_main_server()
        # prepare to start to main server, reserve it's ip and port
        main_server = await task1
        await self.main_server_event.wait()
        # get the seed from the file config.txt
        self.total_seed_list = self.get_seed_from_file()
        # find required number of seeds = floor(total seeds/2) + 1
        required_seed_number = int(len(self.total_seed_list) / 2) + 1
        # get a random list of required number seeds
        seed_list = random.sample(
            self.total_seed_list, required_seed_number)
        # get peer from the seeds
        peer_list = await self.get_peer_from_seed(seed_list)

        await asyncio.gather(main_server.serve_forever(), self.establish_peer_connection(peer_list),
                             self.consume_block_queue(), self.process_pending_queue())


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", "--ip", type=str, default="localhost", help="IP address of the peer")
    parser.add_argument("-port", "--port", type=int, default=0, help="PORT address of the peer")
    parser.add_argument("-in", "--input", type=str, default="config.txt",
                        help="File containing the list of Peer address")
    parser.add_argument("-out", "--out", type=str, help="File to write the output of Peer.py")
    args = parser.parse_args()
    output_file = args.out or "output_peer:%s:%s.txt" % (args.ip, args.port)
    peer = Peer(ip=args.ip, port=args.port, output_file=output_file, input_file=args.input)
    await peer.start()


if __name__ == "__main__":
    asyncio.run(main())
