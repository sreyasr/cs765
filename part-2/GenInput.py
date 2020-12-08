import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-in", "--input", type=str, default="config.txt",
                        help="File containing the list of Peer address")
    parser.add_argument("-n", "--number", type=int, default=4)
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

    print(seed_list)

    for i in range(args.number):
        with open("config%s.txt" % (i+1), "w") as f:
            f.write("inter_arrival_time %s\n" % inter_arrival_time)
            f.write("node_hash_power %s\n" % node_hash_power)
            for j in range(i):
                f.write("%s,%s\n" %(seed_list[j][0], seed_list[j][1]))


main()