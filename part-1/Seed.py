#!/usr/bin/python3
import argparse
import asyncio
import json
import logging
import socket

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.CRITICAL)

log.addHandler(stdout_handler)


class Seed:
    encode_format = 'utf-8'

    def __init__(self, ip="localhost", port=0, output_file=None):
        self.ip = ip
        self.max_data = 8192
        self.port = port
        self.peer_list = []
        self.address = (ip, port)
        self.output_file = output_file
        if output_file is not None:
            open(output_file, "w").close()

    def flush_out(self, text):
        if self.output_file is not None:
            with open(self.output_file, 'a') as f:
                f.write(text + "\n")
        print(text)

    @staticmethod
    def get_self_ip():
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip

    @staticmethod
    def encode_data(data):
        return (json.dumps(data) + '\0').encode(Seed.encode_format)

    @staticmethod
    def decode_data(data):
        def loads(expr):
            try:
                return json.loads(expr)
            except Exception as e:
                log.info("exception in decode_data:{0}".format(e))
                return dict()

        return list(map(loads, data.decode(Seed.encode_format).strip('\0').split('\0')))

    async def start_server(self):
        async def main_server(reader, writer):
            while True:
                data = await reader.read(self.max_data)
                if not data:
                    break
                all_data = self.decode_data(data)
                for data in all_data:
                    if not data:
                        break
                    elif data['message_type'] == 'Registration_Request':
                        log.info("Registration_Request from: {}:{}".format(
                            data['SELF_IP'], data['SELF_PORT']))
                        self.flush_out("Registration Request:%s:%s" % (data['SELF_IP'], data['SELF_PORT']))
                        peer_address = data['SELF_IP'], data['SELF_PORT']
                        if peer_address not in self.peer_list:
                            self.peer_list.append(peer_address)
                    elif data['message_type'] == 'Peer_List_Request':
                        log.debug("Peer_List_Request")
                        message = dict()
                        message['message_type'] = 'Peer_List_Response'
                        message['content'] = self.peer_list
                        writer.write(Seed.encode_data(message))
                        await writer.drain()
                    elif data['message_type'] == 'Dead_Peer_Info':
                        dead_peer_address = (data['DEAD_IP'], data['DEAD_PORT'])
                        self.flush_out("Dead Node:%s:%s:%s:%s:%s" % (
                            data['DEAD_IP'], data['DEAD_PORT'], data['timestamp'], data['SELF_IP'], data['SELF_PORT']))
                        while dead_peer_address in self.peer_list:
                            self.peer_list.remove(dead_peer_address)
            writer.close()

        async def run_server(host, port):
            server = await asyncio.start_server(main_server, host, port)
            self.address = server.sockets[0].getsockname()
            self.ip, self.port = self.address
            log.info("bind to: %s:%s" % (self.ip, self.port))
            await server.serve_forever()

        await run_server(self.ip, self.port)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", "--ip", type=str, default="localhost", help="IP address of the seed")
    parser.add_argument("-port", "--port", type=int, default=0, help="PORT address of the seed")
    parser.add_argument("-out", "--out", type=str, help="File to write the output of Seed.py")
    args = parser.parse_args()
    output_file = args.out or "output_seed:%s:%s" % (args.ip, args.port)
    seed = Seed(ip=args.ip, port=args.port, output_file=output_file)
    await seed.start_server()


if __name__ == '__main__':
    asyncio.run(main())
