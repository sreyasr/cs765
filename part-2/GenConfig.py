import subprocess
ip = "127.0.0.1"
initial_port = 44300
num_nodes = 9
inter_arrival_time = 2
node_hash_power = 7.44
filename = "config.txt"

with open(filename, "w") as f:
    f.write("inter_arrival_time %s\n" % inter_arrival_time)
    f.write("node_hash_power %s\n" % node_hash_power)
    for i in range(num_nodes):
        f.write("%s,%s\n" % (ip, initial_port + i))

subprocess.run(["python3", "GenInput.py", "-n", str(num_nodes)])