import os
import shutil
import subprocess
import sys


print("Job is running agent:", sys.argv)

archive = sys.argv[1]
executable = sys.argv[2]
nargs = int(sys.argv[3])
args = sys.argv[4:4 + nargs]

archive_path = os.path.dirname(archive)
shutil.unpack_archive(archive, archive_path)
os.chdir(archive_path)

command = ["python", executable] + args
print("Agent is running script:", command)
subprocess.run(command)
