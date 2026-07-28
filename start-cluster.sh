#!/bin/bash
# Start a 3-shard + 3-replica cluster for overload testing
SERVER=/home/karsubba/valkey/src/valkey-server
CLI=/home/karsubba/valkey/src/valkey-cli
MODULE=/home/karsubba/valkey-search-local-priority/.build-release/libsearch.so
export LD_LIBRARY_PATH=/home/karsubba/.local/GCCStandalone/lib64

pkill -f "valkey-server.*638" 2>/dev/null
sleep 1

for port in 6380 6381 6382 6383 6384 6385; do
  mkdir -p /tmp/cluster-$port
  rm -f /tmp/cluster-$port/nodes.conf /tmp/cluster-$port/server.log /tmp/cluster-$port/dump.rdb
  $SERVER --port $port --daemonize yes \
    --logfile /tmp/cluster-$port/server.log \
    --dir /tmp/cluster-$port \
    --save "" \
    --cluster-enabled yes \
    --cluster-config-file /tmp/cluster-$port/nodes.conf \
    --cluster-node-timeout 5000 \
    --loadmodule $MODULE \
    --loglevel notice
done
sleep 2

echo "=== Nodes ==="
for port in 6380 6381 6382 6383 6384 6385; do
  echo -n "  $port: "; $CLI -p $port PING
done

echo ""
echo "=== Creating cluster (3 primaries, 1 replica each) ==="
echo "yes" | $CLI --cluster create \
  127.0.0.1:6380 127.0.0.1:6381 127.0.0.1:6382 \
  127.0.0.1:6383 127.0.0.1:6384 127.0.0.1:6385 \
  --cluster-replicas 1

sleep 3
echo ""
echo "=== Cluster state ==="
$CLI -p 6380 CLUSTER INFO | grep -E "cluster_state|cluster_size|cluster_known"
echo ""
echo "=== Done. Cluster ready on ports 6380-6385 ==="
