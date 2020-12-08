import sys
import subprocess

subprocess.run(["python3", "MaliciousPeer.py"] + sys.argv[1:], timeout=600)

print(sys.argv)
