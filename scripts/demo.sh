#!/usr/bin/env bash
# Exercise a running GoMQTT broker using the standard Mosquitto command-line
# clients, which is also a quick proof that the broker speaks real MQTT rather
# than a private dialect.
#
#   make run          # in one terminal
#   ./scripts/demo.sh # in another
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-1883}"
ADMIN="${ADMIN:-http://localhost:8080}"

for binary in mosquitto_pub mosquitto_sub; do
  command -v "$binary" >/dev/null 2>&1 || {
    echo "$binary is not installed; install mosquitto-clients"; exit 1; }
done

echo "==> subscribing to demo/# in the background"
mosquitto_sub -h "$HOST" -p "$PORT" -t 'demo/#' -v &
SUB_PID=$!
trap 'kill $SUB_PID 2>/dev/null || true' EXIT
sleep 1

echo "==> QoS 0"
mosquitto_pub -h "$HOST" -p "$PORT" -t demo/qos0 -m "hello at qos 0" -q 0

echo "==> QoS 1 (acknowledged)"
mosquitto_pub -h "$HOST" -p "$PORT" -t demo/qos1 -m "hello at qos 1" -q 1

echo "==> retained: published now, delivered to whoever subscribes later"
mosquitto_pub -h "$HOST" -p "$PORT" -t demo/retained -m "stored state" -r

echo "==> wildcards"
mosquitto_pub -h "$HOST" -p "$PORT" -t demo/devices/1/temp -m "21.5"
mosquitto_pub -h "$HOST" -p "$PORT" -t demo/devices/2/temp -m "19.8"

sleep 1
echo
echo "==> a late subscriber receives the retained message immediately"
timeout 3 mosquitto_sub -h "$HOST" -p "$PORT" -t demo/retained -C 1 -v || true

echo
echo "==> clearing the retained message with a zero-length retained publish"
mosquitto_pub -h "$HOST" -p "$PORT" -t demo/retained -m "" -r
sleep 1
echo "    a late subscriber now receives nothing:"
timeout 2 mosquitto_sub -h "$HOST" -p "$PORT" -t demo/retained -C 1 -v || echo "    (nothing, as expected)"

echo
echo "==> broker stats"
curl -s "$ADMIN/api/v1/stats" | python3 -m json.tool 2>/dev/null || echo "    (admin API not reachable at $ADMIN)"

echo
echo "==> connected clients"
curl -s "$ADMIN/api/v1/clients" | python3 -m json.tool 2>/dev/null || true
