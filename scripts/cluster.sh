#!/usr/bin/env bash
# Start/stop a local N-node cluster:  scripts/cluster.sh start [N] | stop | status
# Client ports 6381.., raft ports 7381.., data in ./data/nK, logs in ./data/nK.log
set -euo pipefail
cd "$(dirname "$0")/.."
N=${2:-3}
BASE_C=${BASE_C:-6381}; BASE_R=${BASE_R:-7381}
nodes=""
for i in $(seq 1 "$N"); do nodes+="127.0.0.1:$((BASE_C+i-1)):$((BASE_R+i-1)),"; done
nodes=${nodes%,}
case "${1:-}" in
  start)
    mkdir -p data
    for i in $(seq 1 "$N"); do
      nohup build/raft-kv --id "$i" --nodes "$nodes" --dir "data/n$i" ${EXTRA:-} >> "data/n$i.log" 2>&1 &
      echo $! > "data/n$i.pid"
    done
    echo "started $N nodes; try: redis-cli -c -p $BASE_C SET hello world" ;;
  stop)
    for f in data/n*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; done ;;
  status)
    for i in $(seq 1 "$N"); do
      echo "node $i: $(redis-cli -p $((BASE_C+i-1)) INFO 2>/dev/null | grep -E '^(role|term|commit_index|keys):' | tr '\r\n' ' ')"
    done ;;
  *) echo "usage: $0 start|stop|status [N]"; exit 2 ;;
esac
