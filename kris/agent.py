import os
import shutil
import subprocess
import sys


print("Running", sys.argv)

archive = sys.argv[1]
executable = sys.argv[2]
args = sys.argv[3:]

archive_path = os.path.dirname(archive)
shutil.unpack_archive(archive, archive_path)
os.chdir(archive_path)
subprocess.run(["python", executable] + args)
