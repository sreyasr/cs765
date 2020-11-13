import argparse
import time
from collections import deque
from datetime import datetime, timedelta
from hashlib import sha3_224

import numpy

import Node
from Node import *

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.DEBUG)

log.addHandler(stdout_handler)


def get_hash(key):
    return sha3_224(key.encode()).hexdigest()[-4:]


def get_block(block_index, prev_hash, merkel_root, timestamp):
    return Block(block_index, prev_hash, merkel_root, timestamp,
                 get_hash("%s:%s:%s" % (prev_hash, merkel_root, timestamp)))


def exp_rand_var():
    _inter_arrival_time = 6
    _node_hash_power = 33
    _global_lambda = 1 / _inter_arrival_time
    _lambda = _node_hash_power * _global_lambda / 100
    _waiting_time = numpy.random.exponential(scale=1.0, size=None)
    return _waiting_time


genesis_block = Block(0, "0000", "0000", "110569", "9e1c")


#  class peer to store information of peer
class Peer:
    max_data = 8192
    encode_format = 'utf-8'

    def __init__(self, ip="localhost", port=0, output_file=None, input_file="config.txt"):
        self.ip = ip
        self.port = port

        self.file_name = input_file  # file from seed will be acquired

        self.main_server_event = asyncio.Event()
        self.peer_node_list = PeerNodeList()
        self.output_file = output_file

        self.block_chain_history = {0: [genesis_block]}
        self.mining_time = 6
        self.new_mining_block = False
        self.block_queue = list()
        self.pending_queue = deque()
        self.recent_blocks_event = asyncio.Event()
        self.pending_queue_event = asyncio.Event()
        self.initial_set_up_event = asyncio.Event()
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
        self.block_chain_history[block.block_index] = (self.block_chain_history.get(block.block_index, None) or list())
        self.block_chain_history[block.block_index].append(block)

    async def mine_block(self):
        await self.pending_queue_event.wait()
        log.debug("Pending queue event set: Start mining")
        while True:
            for _ in range(self.mining_time):
                await asyncio.sleep(1)
                if self.new_mining_block:
                    break
            block_no = len(self.block_chain_history)
            log.debug("CHAIN: %s" % self.block_chain_history)
            block = get_block(block_no, self.block_chain_history[block_no - 1][0].hash, "0000", str(int(time.time())))
            self.block_chain_history[block.block_index] = self.block_chain_history.get(block.block_index,
                                                                                       None) or list()
            self.block_chain_history[block.block_index].insert(0, block)
            log.info("new block: %s" % block)
            await self.peer_node_list.block_broadcast(block_no, block.prev_hash, block.merkel_root, block.timestamp)

    def is_block_valid(self, block: Block):
        block_no = block.block_index
        if block_no - 1 not in self.block_chain_history.keys():
            return False
        for cur_block in self.block_chain_history.get(block_no, None) or list():
            if cur_block.hash == block.index:
                return False
        for prev_block in self.block_chain_history[block_no - 1]:
            if prev_block.hash == block.prev_hash:
                if int(prev_block.timestamp) < int(block.timestamp) + 3600 or int(prev_block.timestamp) < int(
                        block.timestamp) - 3600:
                    return True
                elif prev_block.block_index == 0:
                    return True
                else:
                    return False
        return False

    # detect the incoming message received and act accordingly
    async def process(self, peer_node, data):
        if data['message_type'] == 'block_message':
            block = get_block(data['block_no'], data['prev_hash'], data['timestamp'], data['merkel_root'])
            self.pending_queue.append(block)
        elif data['message_type'] == 'Block_History_Request':
            await peer_node.send_block_history(self.block_chain_history)
        elif data['message_type'] == 'Recent_Blocks_Request':
            await peer_node.send_recent_block(self.block_chain_history)
        elif data['message_type'] == 'block_history_reply':
            block_history = data['block_history']
            for key, value in block_history.items():
                block_no = int(key)
                blocks = list(map(lambda x: Block(block_no, *x[1:]), value))
                self.block_chain_history[block_no] = blocks
            self.pending_queue_event.set()
        elif data['message_type'] == 'Recent_Blocks_Response':
            recent_blocks = data['recent_blocks']
            block_no = data['block_no']
            blocks = map(lambda x: Block(block_no, *x[1:]), recent_blocks)
            self.block_chain_history[block_no] = list(blocks)
            self.recent_blocks_event.set()

    # handle the peer messages: liveness request, liveness reply etc...
    async def handle_peer_messages(self, peer_node):
        while True:
            data = await peer_node.read()
            if data is None:
                break
            for message in data:
                log.info("message:\n%s" % message)
                await self.process(peer_node, message)
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
            log.info("Connection from: %s %s" % (ip, port))
            if data['message_type'] == 'Registration_Request':
                await self.handle_peer_messages(peer_node)

        async def run_server(host, port):
            log.info("Starting main server on %s %s" % (host, port))
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
            prev_len = len(self.block_chain_history)
            for block_meta in del_list:
                self.append_block(block_meta[1])
            new_len = len(self.block_chain_history)
            if new_len > prev_len:
                self.new_mining_block = True
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
        return list(set(seed_list))

    # return the entire list of peer received from seeds
    async def get_peer_from_seed(self, seed_list):
        for address in seed_list:
            peer_node = self.peer_node_list.create_node(address[0], address[1], False)
            await peer_node.send_registration_request(self.ip, self.port)
        log.info("Completed Registration")
        self.initial_set_up_event.set()

    async def establish_peer_connection(self):
        await asyncio.gather(*[self.handle_peer_messages(i) for i in self.peer_node_list])

    async def initial_set_up(self):
        await self.initial_set_up_event.wait()
        log.info("Initial set up wait is over")
        if len(self.peer_node_list) == 0:
            return
        peer_node = self.peer_node_list[0]
        log.info("Sending Block Request")
        await peer_node.send_recent_block_request()
        await self.recent_blocks_event.wait()
        log.info("Sending Block History Request")
        await peer_node.send_block_history_request()

    async def process_pending_queue(self):
        await self.pending_queue_event.wait()
        log.info("processing pending queue...")
        while True:
            if len(self.pending_queue) == 0:
                await asyncio.sleep(1)
                continue
            block = self.pending_queue.popleft()
            if self.is_block_valid(block):
                x = datetime.now() + timedelta(seconds=int(exp_rand_var())), block
                for i in range(len(self.block_queue)):
                    if self.block_queue[i][1].prev_hash == block.prev_hash:
                        self.block_queue[i] = x
                        return
                self.block_queue.append(x)

    # The entry point to all functions
    async def start(self):
        task1 = self.start_main_server()
        # prepare to start to main server, reserve it's ip and port
        main_server = await task1
        await self.main_server_event.wait()
        # get the seed from the file config.txt
        seed_list = self.get_seed_from_file()
        if len(seed_list) == 0:
            self.recent_blocks_event.set()
            self.pending_queue_event.set()
        # get peer from the seeds
        await self.get_peer_from_seed(seed_list)

        await asyncio.gather(main_server.serve_forever(), self.establish_peer_connection(),
                             self.consume_block_queue(), self.process_pending_queue(), self.mine_block(),
                             self.initial_set_up())


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
