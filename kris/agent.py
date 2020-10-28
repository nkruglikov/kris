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
args = sys.argv[5:5 + nargs] + [local_rank]

try:
    file_location = os.environ["OMPI_FILE_LOCATION"]
    job_id = file_location.split("/")[2]
except Exception:
    job_id = datetime.datetime.isoformat(
            datetime.datetime.now()).replace(":", "-")

job_path = f"/home/jovyan/.kris/jobs/{job_id}"
if not os.path.exists(job_path):
    shutil.unpack_archive(archive, job_path)
os.chdir(job_path)

command = ["python", executable] + args
print("Agent is running script:", command)
subprocess.run(command)
