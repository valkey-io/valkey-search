import json
import sys
import shlex
import os

if len(sys.argv) < 3:
  sys.stderr.write(f"Usage: {sys.argv[0]} <db_path> <cc_file_path>\n")
  sys.exit(1)

db_path = sys.argv[1]
cc_file = sys.argv[2]

abs_cc_file = os.path.abspath(cc_file)
container_root = os.getcwd()
rel_cc_file = os.path.relpath(abs_cc_file, container_root)

if not os.path.exists(db_path):
  sys.stderr.write(f"Error: {db_path} not found\n")
  sys.exit(1)

with open(db_path) as f:
  db = json.load(f)

for entry in db:
  db_dir = entry['directory']
  db_root = os.path.dirname(db_dir)
  
  entry_file = os.path.abspath(os.path.join(db_dir, entry['file']))
  rel_entry_file = os.path.relpath(entry_file, db_root)
  
  if rel_entry_file == rel_cc_file:
    if 'arguments' in entry:
      args = entry['arguments']
    elif 'command' in entry:
      args = shlex.split(entry['command'])
    else:
      continue
    
    translated_args = []
    for arg in args:
      translated_args.append(arg.replace(db_root, container_root))
      
    filtered_args = []
    skip = 0
    for i, arg in enumerate(translated_args):
      if i == 0:
        continue # skip compiler
      if skip > 0:
        skip -= 1
        continue
      if arg == '-c':
        skip = 1
        continue
      if arg == '-o':
        skip = 1
        continue
      if arg.startswith('-o'):
        continue
      if os.path.abspath(arg) == abs_cc_file:
        continue
      filtered_args.append(arg)
    
    print(' '.join(filtered_args))
    sys.exit(0)

sys.stderr.write(f"Error: No compilation command found for {cc_file} in {db_path}\n")
sys.exit(1)
