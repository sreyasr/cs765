import argparse
import random
import sys
from collections import deque

from Node import *

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.CRITICAL)

log.addHandler(stdout_handler)


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

        self.message_queue = deque()  # queue to store messages which will be consumed by consume_message()
        self.file_name = input_file  # file from seed will be acquired
        self.message_list = MessageList()  # map hash to a list of address whom received the message

        self.main_server_event = asyncio.Event()
        self.peer_node_list = PeerNodeList()
        self.seed_node_list = SeedNodeList()
        self.output_file = output_file

        if output_file is not None:
            open(output_file, "w").close()

    # function to print to screen and file
    def flush_out(self, text):
        if self.output_file is not None:
            with open(self.output_file, 'a') as f:
                f.write(text + "\n")
        print(text)

    # detect the incoming message received and act accordingly
    def process(self, peer_node, data):
        if data['message_type'] == 'Liveness_Request':
            log.info("Received Liveness Request")
            cmd = ('Liveness_Reply', peer_node, data['timestamp'])
            self.message_queue.append(cmd)
        elif data['message_type'] == 'Gossip_Message':
            log.info("Received Gossip message")
            content = data['message_content']
            if content in self.message_list:
                return
            self.flush_out("%s:%s" % (datetime.datetime.now(), content))
            self.message_list[content] = self.message_list.get(content).append(peer_node)
            cmd = ('Gossip_Forward', peer_node, content)
            self.message_queue.append(cmd)
        elif data['message_type'] == 'Liveness_Reply':
            log.info("Received Liveness Reply")
            peer_node.delayed_liveness_reply = 0

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
            if data['message_type'] == 'Establish_Peer_Connection':
                log.info("Establishing Peer Connection from: {}:{}".format(
                    data['SELF_IP'], data['SELF_PORT']))
            con_ip = writer.get_extra_info('peername')[0]
            con_port = writer.get_extra_info('peername')[1]
            ip = data['SELF_IP']
            port = data['SELF_PORT']
            peer_node = self.peer_node_list.create_node(ip, port, True, con_ip, con_port)
            peer_node.writer = writer
            peer_node.reader = reader

            await self.handle_peer_messages(peer_node)

        async def run_server(host, port):
            log.debug("Peer.start_main_server.runserver begin")
            server = await asyncio.start_server(main_server, host, port)
            log.debug("Peer.start_main_server.runserver started main_server")
            self.ip, self.port = server.sockets[0].getsockname()
            log.debug("Changed ip to:{0}:{1}".format(self.ip, self.port))
            self.main_server_event.set()
            return server

        log.debug("Peer.start_main_server:going to await runserver")

        return await run_server(self.ip, self.port)

    # send the messages on message_queue
    async def consume_message(self):
        while True:
            while self.message_queue:
                message = self.message_queue.popleft()

                #  gossip message
                if message[0] == "Gossip_Message":
                    log.info("Sending gossip messages")
                    self.message_list[message[1]] = [peer_node for peer_node in self.peer_node_list]
                    await self.peer_node_list.gossip_broadcast(self.ip, self.port, message[1])

                #  check for liveness request
                elif message[0] == "Liveness_Request":
                    log.info("sending liveness request to all peers")
                    await self.peer_node_list.liveness_request_broadcast(self.ip, self.port)

                #  gossip forwarding
                elif message[0] == "Gossip_Forward":
                    log.info("Forwarding gossip messages")
                    self.message_list[message[2]] = [peer_node for peer_node in self.peer_node_list]
                    await self.peer_node_list.gossip_forward_broadcast(self.ip, self.port, message[2], message[1])

                #  liveness reply
                elif message[0] == "Liveness_Reply":
                    peer_node = message[1]
                    await peer_node.send_liveness_reply(self.ip, self.port, message[2])

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
        for i in seed_list:
            seed_node = SeedNode(i[0], i[1])
            ret = await seed_node.connect()
            if ret is None:
                continue
            self.seed_node_list.append(seed_node)
            await seed_node.send_registration_request(self.ip, self.port)
            peer_list = await seed_node.send_peer_list_request()
            self.total_peer_list.extend(peer_list)
        if self.seed_node_list:
            self.total_peer_list = list(set(self.total_peer_list) - {(self.ip, self.port)})
            print("Peers received from seed: %s" % self.total_peer_list)
            return self.total_peer_list
        else:
            self.flush_out("Can't connect to any Seeds exiting....")
            log.debug("bool:%r len:%r" % (bool(self.seed_node_list), len(self.seed_node_list)))
            sys.exit()

    async def establish_peer_connection(self, peer_list):
        t = list()
        for address in peer_list:
            log.debug("establish_peer_connection started peer_server")
            try:
                peer_node = PeerNode(address[0], address[1], False)
                t.append(peer_node)
                self.peer_node_list.append(peer_node)
            except Exception as e:
                log.debug("establish_peer_connection exception: {}".format(e))

        g1 = await asyncio.gather(*[i.connect_to(self.ip, self.port) for i in t])

        # setting ConnectionRefusedError as dead
        failed_peer_node = [t[i] for i in range(len(t)) if g1[i] is None]
        timestamp = str(datetime.datetime.now())
        for peer_node in failed_peer_node:
            self.flush_out("Dead Node:%s:%s:%s:%s:%s" % (
                peer_node.ip, peer_node.port, timestamp, self.ip, self.port))
        await asyncio.gather(
            *[self.seed_node_list.set_peer_dead(i.ip, i.port, timestamp, self.ip, self.port) for i in failed_peer_node])

        [self.peer_node_list.remove_peer_node(peer_node) for peer_node in failed_peer_node]
        await asyncio.gather(*[self.handle_peer_messages(i) for i in t if i not in failed_peer_node])

    async def message_generator(self):
        await asyncio.gather(self.liveness_check(), self.gossip_generator())

    async def gossip_generator(self):

        async def generate_gossip_message(msg_number='0'):
            content = "Message-%s" % msg_number
            return "%s:%s:%s" % (self.ip, self.port, content)

        while True:
            if len(self.peer_node_list) > 0:
                break
            await asyncio.sleep(2)
        for i in range(10):
            cont = await generate_gossip_message(str(i))
            self.message_queue.append(("Gossip_Message", cont))
            await asyncio.sleep(5)

    async def liveness_check(self):
        async def set_peer_dead(dead_ip, dead_port):
            await self.seed_node_list.set_peer_dead(dead_ip, dead_port, timestamp, self.ip, self.port)

        while True:
            if len(self.peer_node_list) > 0:
                break
            await asyncio.sleep(10)

        while True:
            for peer_node in self.peer_node_list:
                peer_node.delayed_liveness_reply += 1
            log.info("liveness_check:Going to send liveness request")
            self.message_queue.append(('Liveness_Request', 0))
            await asyncio.sleep(13)
            for peer_node in self.peer_node_list:
                if peer_node.delayed_liveness_reply > 2:
                    timestamp = str(datetime.datetime.now())
                    self.flush_out("Dead Node:%s:%s:%s:%s:%s" % (
                        peer_node.ip, peer_node.port, timestamp, self.ip, self.port))
                    await set_peer_dead(peer_node.ip, peer_node.port)
                    self.peer_node_list.remove_peer_node(peer_node)

    # The entry point to all functions
    async def start(self):
        log.debug("starting Peer.start")
        task1 = self.start_main_server()
        log.debug("going to await task1")
        # prepare to start to main server, reserve it's ip and port
        main_server = await task1
        log.debug("Running concurrently with task1")
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

        log.info("Main Server IP: {}:{}".format(self.ip, self.port))

        await asyncio.gather(main_server.serve_forever(), self.establish_peer_connection(peer_list),
                             self.consume_message(),
                             self.message_generator())


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", "--ip", type=str, default="localhost", help="IP address of the peer")
    parser.add_argument("-port", "--port", type=int, default=0, help="PORT address of the peer")
    parser.add_argument("-in", "--input", type=str, default="config.txt",
                        help="File containing the list of seed address")
    parser.add_argument("-out", "--out", type=str, help="File to write the output of Peer.py")
    args = parser.parse_args()
    output_file = args.out or "output_peer:%s:%s.txt" % (args.ip, args.port)
    peer = Peer(ip=args.ip, port=args.port, output_file=output_file, input_file=args.input)
    await peer.start()


if __name__ == "__main__":
    asyncio.run(main())
