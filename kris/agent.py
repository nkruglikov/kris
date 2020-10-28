import datetime
import os
import shutil
import subprocess
import sys


print("Job is running agent:", sys.argv)

archive = sys.argv[1]
executable = sys.argv[2]
nargs = int(sys.argv[3])
args = sys.argv[4:4 + nargs]

try:
    file_location = os.environ["OMPI_FILE_LOCATION"]
    job_id = file_location.split("/")[2]
except Exception:
    job_id = datetime.datetime.isoformat(
            datetime.datetime.now()).replace(":", "-")

job_path = f"/home/jovyan/.kris/jobs/{job_id}"
shutil.unpack_archive(archive, job_path)
os.chdir(job_path)

command = ["python", executable] + args
print("Agent is running script:", command)
subprocess.run(command)
