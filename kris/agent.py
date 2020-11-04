import datetime
import os
import shutil
import subprocess
import sys


print("Job is running agent:", sys.argv)

local_rank = sys.argv[1]
archive = sys.argv[2]
executable = sys.argv[3]
nargs = int(sys.argv[4])
name = sys.argv[5]
args = sys.argv[6:6 + nargs] + [local_rank]

job_id = datetime.datetime.isoformat(
        datetime.datetime.now()).replace(":", "-")
job_id += "_" + name
job_id += "_" + "".join(c for c in local_rank if c in "0123456789")

job_path = f"/home/jovyan/.kris/jobs/{job_id}"
if not os.path.exists(job_path):
    shutil.unpack_archive(archive, job_path)
os.chdir(job_path)

command = ["python3", executable] + args
print("Agent is running script:", command)
subprocess.run(command)
