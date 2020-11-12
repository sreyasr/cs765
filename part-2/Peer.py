import argparse
import calendar
import random
import sys
import time
from datetime import timedelta, datetime
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


def get_block(prev_hash, merkel_root, timestamp):
    return Block(prev_hash, merkel_root, timestamp, get_hash("%s:%s:%s" % (prev_hash, merkel_root, timestamp)))


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

        self.block_chain_history = [[Block("0000", "0000", "27858", "9e1c")]]
        self.mining_time = 100
        self.new_block = False
        self.block_queue = list()

        if output_file is not None:
            open(output_file, "w").close()

    # function to print to screen and file
    def flush_out(self, text):
        if self.output_file is not None:
            with open(self.output_file, 'a') as f:
                f.write(text + "\n")
        print(text)

    def append_block(self):
        prev_block = self.block_chain_history[-1][0]
        prev_hash = prev_block.prev_hash
        merkel_root = md5(str(random.randint(0, int(10e7))).encode()).hexdigest()[:4]
        timestamp = calendar.timegm(time.gmtime())
        x = [get_block(prev_hash, merkel_root, timestamp)]
        self.block_chain_history.append(x)

    async def mine_block(self):
        while len(self.block_chain_history) == 0:
            await asyncio.sleep(1)
            break
        while True:
            for _ in range(self.mining_time):
                await asyncio.sleep(1)
                if self.new_block:
                    break
            self.append_block()

    def is_block_valid(self, block_no, block: Block):
        if block_no < 1 or block_no > len(self.block_chain_history):
            return False
        lm = self.block_chain_history[block_no - 1]
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
            if self.is_block_valid(data['block_no'], block):
                block_meta = datetime.now() + timedelta(seconds=int(exp_rand_var())), data['block_no'], block
                self.block_queue.append(block_meta)
        elif data['message_type'] == 'block_no_request':
            peer_node.block_no_reply(len(self.block_chain_history), self.block_chain_history[-1])
        elif data['message_type'] == 'block_history_request':
            peer_node.send_block_history(self.block_chain_history)

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

                message = dict()
                message['message_type'] = 'Registration_Response'
                message['peer_list'] = self.total_peer_list + [(self.ip, self.port)]
                message['block_chain'] = self.block_chain_history[-1]
                message['block_no'] = len(self.block_chain_history) - 1

                writer.write(Node.encode_data(message))
                await writer.drain()

            await self.handle_peer_messages(peer_node)

        async def run_server(host, port):
            server = await asyncio.start_server(main_server, host, port)
            self.ip, self.port = server.sockets[0].getsockname()
            self.main_server_event.set()
            return server

        return await run_server(self.ip, self.port)

    async def consume_block_queue(self):
        while True:
            req_list = []
            del_list = []
            for i in self.block_queue:
                if i[0] > datetime.now():
                    del_list.append(i)
                else:
                    req_list.append(i)
            for i in del_list:
                block_meta = i
                if block_meta[1] == len(self.block_chain_history):
                    x = [get_block(block_meta[2].prev_hash, block_meta[2].merkel_root, block_meta[2].timestamp)]
                    self.block_chain_history[block_meta[1]].append(x)
                else:
                    x = get_block(block_meta[2].prev_hash, block_meta[2].merkel_root, block_meta[2].timestamp)
                    self.block_chain_history[block_meta[1]].append(x)
                await self.peer_node_list.block_broadcast(block_meta[1], block_meta[2].prev_hash,
                                                          block_meta[2].merkel_root, block_meta[2].timestamp)
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
        last_blocks = []
        peer_list = []
        block_nos = []
        for i in seed_list:
            ip = i[0]
            port = i[1]
            try:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(ip, port),
                                                        timeout=3)
            except ConnectionRefusedError:
                log.debug("PeerNode Refused Connection: %r:%r" % (self.ip, self.port))
                continue
            message = {
                'message_type': 'Registration_Request',
                'SELF_IP': ip,
                'SELF_PORT': port
            }
            writer.write(Node.encode_data(message))
            await writer.drain()
            data = await reader.read(Node.max_size)
            data = Node.decode_data(data)[0]

            peer_list.extend(data["peer_list"])
            last_blocks.append(data['block_chain'])
            block_nos.append(data['block_no'])

        for i in range(len(last_blocks)):
            pass
        self.total_peer_list.extend(peer_list)

        if peer_list:
            self.total_peer_list = list(set(self.total_peer_list) - {(self.ip, self.port)})
            print("Peers received from seed: %s" % self.total_peer_list)
            return self.total_peer_list
        else:
            self.flush_out("Can't connect to any Peers exiting....")
            sys.exit()

    async def establish_peer_connection(self, peer_list):
        t = list()
        for address in peer_list:
            try:
                peer_node = PeerNode(address[0], address[1], False)
                t.append(peer_node)
                self.peer_node_list.append(peer_node)
            except:
                pass
        g1 = await asyncio.gather(*[i.connect_to(self.ip, self.port) for i in t])

        # setting ConnectionRefusedError as dead
        failed_peer_node = [t[i] for i in range(len(t)) if g1[i] is None]
        timestamp = str(datetime.now())
        for peer_node in failed_peer_node:
            self.flush_out("Dead Node:%s:%s:%s:%s:%s" % (
                peer_node.ip, peer_node.port, timestamp, self.ip, self.port))

        [self.peer_node_list.remove_peer_node(peer_node) for peer_node in failed_peer_node]
        await asyncio.gather(*[self.handle_peer_messages(i) for i in t if i not in failed_peer_node])

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
        await self.get_peer_from_seed(seed_list)
        # choose the peers that are going to be connected
        peer_list = random.sample(
            self.total_peer_list, min(4, len(self.total_peer_list)))

        await asyncio.gather(main_server.serve_forever(), self.establish_peer_connection(peer_list),
                             self.consume_block_queue())


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
