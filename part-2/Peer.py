import argparse
import time
from collections import deque
from hashlib import sha3_224

import numpy

import Node
from Node import *

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.DEBUG)

log.addHandler(stdout_handler)


def get_hash(key):
    return sha3_224(key.encode()).hexdigest()[-4:]


def get_block(block_index, prev_hash, merkel_root, timestamp):
    return Block(block_index, prev_hash, merkel_root, timestamp,
                 get_hash("%s:%s:%s" % (prev_hash, merkel_root, timestamp)))


inter_arrival_time = 6
node_hash_power = 33


def exp_rand_var():
    global_lambda = 1 / inter_arrival_time
    _lambda = node_hash_power * global_lambda / 100
    waiting_time = numpy.random.exponential(scale=1.0 / _lambda, size=None)
    return waiting_time


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
        self.block_queue = list()
        self.pending_queue = deque()
        self.recent_blocks_event = asyncio.Event()
        self.pending_queue_event = asyncio.Event()
        self.initial_set_up_event = asyncio.Event()
        self.new_mining_block_event = asyncio.Event()
        self.block_height = 0

        if output_file is not None:
            open(output_file, "w").close()

    def append_block(self, block):
        log.info("Block_Added: {}".format(block))
        self.block_chain_history[block.block_index] = (self.block_chain_history.get(block.block_index, None) or list())
        self.block_chain_history[block.block_index].append(block)

    async def mine_block(self):
        await self.pending_queue_event.wait()
        log.debug("Pending queue event set: Start mining")
        while True:
            try:
                await asyncio.wait_for(self.new_mining_block_event.wait(), timeout=exp_rand_var())
                log.debug("new block received. Mining Discontinued")
                await asyncio.sleep(1)
                self.new_mining_block_event.clear()
            except asyncio.TimeoutError:
                block_no = len(self.block_chain_history)
                block = get_block(block_no, self.block_chain_history[block_no - 1][0].hash, "0000",
                                  str(int(time.time())))
                self.block_chain_history[block.block_index] = self.block_chain_history.get(block.block_index,
                                                                                           None) or list()
                self.block_chain_history[block.block_index].insert(0, block)
                await self.peer_node_list.block_broadcast(block_no, block.prev_hash, block.merkel_root, block.timestamp)
                log.info("New_Block_Created: {}".format(block))

    def is_block_valid(self, block: Block):
        if block.merkel_root == "-1":
            return False
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
            block = get_block(data['block_no'], data['prev_hash'], data['merkel_root'], data['timestamp'])
            if block.block_index == max(self.block_chain_history.keys()) + 1:
                self.new_mining_block_event.set()
            self.pending_queue.append(block)
        elif data['message_type'] == 'Block_History_Request':
            await peer_node.send_block_history(self.block_chain_history)
        elif data['message_type'] == 'Recent_Blocks_Request':
            await peer_node.send_recent_block(self.block_chain_history)
        elif data['message_type'] == 'Block_History_Reply':
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
            log.debug("new message: %s" % data)
            for message in data:
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
            log.debug("Connection from: %s %s" % (ip, port))
            if data['message_type'] == 'Registration_Request':
                log.debug("Registration success from %s %s" % (ip, port))
                await self.handle_peer_messages(peer_node)

        async def run_server(host, port):
            log.debug("Starting main server on %s %s" % (host, port))
            server = await asyncio.start_server(main_server, host, port)
            self.ip, self.port = server.sockets[0].getsockname()
            self.main_server_event.set()
            return server

        return await run_server(self.ip, self.port)

    # return list of seeds from file
    def get_seed_from_file(self):
        file_name = self.file_name
        seed_list = []
        with open(file_name, "r") as f:
            global inter_arrival_time
            inter_arrival_time = float(f.readline().split()[1])
            global node_hash_power
            node_hash_power = float(f.readline().split()[1])
            for line in f.readlines():
                address = line.split(",")
                ip = address[0]
                port = int(address[1])
                address = (ip, port)
                seed_list.append(address)
        if len(seed_list) == 0:
            self.pending_queue_event.set()
        return seed_list

    # return the entire list of peer received from seeds
    async def get_peer_from_seed(self, seed_list):
        for address in seed_list:
            peer_node = self.peer_node_list.create_node(address[0], address[1], False)
            await peer_node.send_registration_request(self.ip, self.port)
            await peer_node.write({'message_type': 'HELLO'})
        log.debug("Completed Registration")
        self.initial_set_up_event.set()

    async def establish_peer_connection(self):
        await asyncio.gather(*[self.handle_peer_messages(i) for i in self.peer_node_list])

    async def initial_set_up(self):
        await self.initial_set_up_event.wait()
        if len(self.peer_node_list) == 0:
            return
        peer_node = self.peer_node_list[0]
        await peer_node.send_recent_block_request()
        log.debug("Completed recent block request")
        await self.recent_blocks_event.wait()
        await peer_node.send_block_history_request()

    async def process_pending_queue(self):
        await self.pending_queue_event.wait()
        log.debug("processing pending queue...")
        while True:
            if len(self.pending_queue) == 0:
                await asyncio.sleep(0.1)
                continue
            block = self.pending_queue.popleft()
            if self.is_block_valid(block):
                log.debug("Received_Valid_Block: {}".format(block))
                self.append_block(block)
            else:
                log.debug("Received_Invalid_Block: {}".format(block))

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
                             self.process_pending_queue(), self.mine_block(), self.initial_set_up())


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", "--ip", type=str, default="localhost", help="IP address of the peer")
    parser.add_argument("-port", "--port", type=int, default=0, help="PORT address of the peer")
    parser.add_argument("-in", "--input", type=str, default="config.txt",
                        help="File containing the list of Peer address")
    parser.add_argument("-out", "--out", type=str, help="File to write the output of Peer.py")
    args = parser.parse_args()
    output_file = args.out or "output_peer:%s:%s.txt" % (args.ip, args.port)
    file_handler = logging.FileHandler(output_file, mode='w')
    file_handler.setLevel(logging.DEBUG)
    log.addHandler(file_handler)
    peer = Peer(ip=args.ip, port=args.port, output_file=output_file, input_file=args.input)
    await peer.start()


if __name__ == "__main__":
    asyncio.run(main())
