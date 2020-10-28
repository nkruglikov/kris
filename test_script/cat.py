import sys


inp_file = sys.argv[1]
out_file = sys.argv[2]

with open(inp_file) as inp:
    content = inp.read()
print(content)
with open(out_file, "w") as out:
    out.write(content)
