import redis
import time
import sys
import subprocess

server_pid = sys.argv[1]

r = redis.Redis(host='localhost', port=60099)

# Wait for server to be ready
while True:
    try:
        if r.ping():
            info = r.info()
            if not info.get('loading', 0):
                break
    except Exception as e:
        pass
    time.sleep(0.5)

print("Valkey-server is ready!")

try:
    r.execute_command('FT.CREATE', 'bench_idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:', 'SCHEMA', 'title', 'TEXT', 'body', 'TEXT')
except Exception as e:
    print(f"FT.CREATE error (may already exist): {e}")

docs = []
with open('integration/benchmarks/rax/dataset/documents.txt', 'r') as f:
    for i, line in enumerate(f):
        line = line.strip()
        if not line: continue
        parts = line.split('\t')
        if len(parts) >= 2:
            docs.append((str(i), parts[0], parts[1]))

print(f"Loaded {len(docs)} documents into python.")

# Start perf record on all threads of PID
perf_proc = subprocess.Popen(['perf', 'record', '-F', '999', '-p', server_pid, '-g', '-o', '/tmp/perf_ingest.data'])

time.sleep(1)

start = time.time()
for doc_id, title, body in docs:
    r.hset(f"doc:{doc_id}", mapping={'title': title, 'body': body})
elapsed = time.time() - start

print(f"Ingestion finished in {elapsed:.2f}s | Throughput: {len(docs)/elapsed:.1f} docs/s")

# Stop perf record cleanly
subprocess.run(['kill', '-INT', str(perf_proc.pid)])
perf_proc.wait()
