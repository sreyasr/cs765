from Peer import Peer, log
import logging
import time
import asyncio
import argparse

class MaliciousPeer(Peer):
    def __init__(self, ip="localhost", port=0, output_file=None, input_file="config.txt", flood_peer=1):
        super().__init__(ip, port, output_file, input_file)
        self.flood_num = flood_peer

    async def generate_invalid_block(self):
        await self.pending_queue_event.wait()
        log.debug("Pending queue event set: Start mining")
        peer_node_list = self.peer_node_list[:self.flood_num]
        while True:
            block_no = len(self.block_chain_history)
            prev_hash=self.block_chain_history[block_no - 1][0].hash
            merkel_root = "-1"
            timestamp = str(int(time.time()))
            t = [peer_node.send_block(block_no, prev_hash, merkel_root, timestamp) for peer_node in peer_node_list]
            await asyncio.gather(*t)

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
                             self.process_pending_queue(), self.mine_block(), self.initial_set_up(), self.generate_invalid_block())


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", "--ip", type=str, default="localhost", help="IP address of the peer")
    parser.add_argument("-port", "--port", type=int, default=0, help="PORT address of the peer")
    parser.add_argument("-in", "--input", type=str, default="config.txt",
                        help="File containing the list of Peer address")
    parser.add_argument("-out", "--out", type=str, help="File to write the output of Peer.py")
    parser.add_argument("-f", "--flood", type=int, help="Number of nodes to flood")
    args = parser.parse_args()
    output_file = args.out or "output_peer:%s:%s.txt" % (args.ip, args.port)
    file_handler = logging.FileHandler(output_file, mode='w')
    file_handler.setLevel(logging.DEBUG)
    log.addHandler(file_handler)
    peer = MaliciousPeer(ip=args.ip, port=args.port, output_file=output_file, input_file=args.input, flood_peer=args.flood or 1)
    await peer.start()


if __name__ == "__main__":
    asyncio.run(main())
