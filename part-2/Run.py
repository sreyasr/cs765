import argparse
import multiprocessing
import time
import subprocess
import signal
import os

def helper(ip, port, input):
    print("python3", "Peer.py", "--ip", "127.0.0.1", "--port", str(port), "-in", input)
    return subprocess.Popen(["python3", "Peer.py", "--ip", "127.0.0.1", "--port", str(port), "-in", input])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-in", "--input", type=str, default="config.txt",
                        help="File containing the list of Peer address")
    parser.add_argument("-n", "--number", type=int, default=9)
    args = parser.parse_args()
    file_name = args.input
    seed_list = []
    with open(file_name, "r") as f:
        inter_arrival_time = float(f.readline().split()[1])
        node_hash_power = float(f.readline().split()[1])
        for line in f.readlines():
            address = line.split(",")
            ip = address[0]
            port = int(address[1])
            address = (ip, port)
            seed_list.append(address)
    proc_list = []
    for i in range(args.number):
        p = helper(seed_list[i][0], seed_list[i][1], "config%s.txt" % (i+1))
        proc_list.append(p)
        time.sleep(1)
    time.sleep(600)
    for pro in proc_list:
        os.killpg(os.getpgid(pro.pid), signal.SIGTERM)



main()