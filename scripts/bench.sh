#!/usr/bin/env bash
# Benchmark GoMQTT against Eclipse Mosquitto and print a comparison table.
#
#   ./scripts/bench.sh
#   DURATION=15s ./scripts/bench.sh
#
# Both brokers are measured by the same load generator over loopback TCP, so the
# client overhead is identical and cancels out.
#
# Methodology, and why it is not "publish as fast as possible":
#
# An unthrottled run saturates the load generator as well as the broker. What it
# then reports is queueing delay inside the measurement harness — multi-second
# "latencies" and delivery ratios in the teens — which says nothing about either
# broker. So this script does two separate things:
#
#   1. Latency, at a fixed offered load both brokers sustain comfortably. This
#      is the number that means something: how quickly does a message get from
#      publisher to subscriber when the broker is not the bottleneck.
#
#   2. Throughput, as a saturation ladder. The offered rate is stepped up and
#      the highest rate at which the broker still delivers ≥99% of messages with
#      a p99 under the latency budget is reported. A throughput figure without a
#      delivery and latency constraint attached is not a throughput figure.
set -euo pipefail

cd "$(dirname "$0")/.."

DURATION="${DURATION:-10s}"
WARMUP="${WARMUP:-3s}"
LADDER_DURATION="${LADDER_DURATION:-6s}"
LADDER_WARMUP="${LADDER_WARMUP:-2s}"
GOMQTT_PORT="${GOMQTT_PORT:-11883}"
MOSQUITTO_PORT="${MOSQUITTO_PORT:-11884}"
RESULTS_DIR="${RESULTS_DIR:-bench/results}"

# Pass criteria for a ladder step.
MIN_DELIVERY="${MIN_DELIVERY:-0.99}"
MAX_P99_MS="${MAX_P99_MS:-50}"

rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

echo "==> building"
go build -o ./bin/gomqtt ./cmd
go build -o ./bin/loadgen ./cmd/loadgen

cleanup() {
  [ -n "${GOMQTT_PID:-}" ] && kill "$GOMQTT_PID" 2>/dev/null || true
  [ -n "${MOSQ_PID:-}" ] && kill "$MOSQ_PID" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

echo "==> starting gomqtt on :$GOMQTT_PORT"
GOMQTT_ADDRESS=":$GOMQTT_PORT" GOMQTT_ADMIN_ENABLED=false GOMQTT_LOG_LEVEL=error \
  ./bin/gomqtt &
GOMQTT_PID=$!
sleep 2

HAVE_MOSQUITTO=0
if command -v mosquitto >/dev/null 2>&1; then
  HAVE_MOSQUITTO=1
  echo "==> starting mosquitto on :$MOSQUITTO_PORT"
  MOSQ_CONF="$(mktemp)"
  # Mosquitto 2.x refuses remote connections unless a listener and
  # allow_anonymous are set explicitly. Persistence is off and the in-flight
  # window matches GoMQTT's default, so neither broker is paying a cost the
  # other is not.
  cat > "$MOSQ_CONF" <<EOF
listener $MOSQUITTO_PORT 127.0.0.1
allow_anonymous true
persistence false
max_inflight_messages 32
max_queued_messages 1000
EOF
  mosquitto -c "$MOSQ_CONF" > /dev/null 2>&1 &
  MOSQ_PID=$!
  sleep 2
else
  echo "==> mosquitto is not installed; measuring gomqtt only"
fi

# run <addr> <label> <pubs> <subs> <rate> <qos> <payload> <duration> <warmup> <out>
run_one() {
  ./bin/loadgen \
    -addr "$1" -label "$2" \
    -publishers "$3" -subscribers "$4" \
    -rate "$5" -qos "$6" -payload "$7" \
    -duration "$8" -warmup "$9" \
    -json > "${10}"
}

json_field() { python3 -c "import json,sys;print(json.load(open(sys.argv[1]))[sys.argv[2]])" "$1" "$2"; }

# ---------------------------------------------------------------------------
# Part 1 — latency at a fixed, comfortable offered load
# ---------------------------------------------------------------------------

# label|publishers|subscribers|rate|qos|payload
LATENCY_SCENARIOS=(
  "1 pub -> 1 sub, QoS 0|1|1|2000|0|64"
  "10 pub -> 10 sub, QoS 0|10|10|10000|0|64"
  "10 pub -> 10 sub, QoS 1|10|10|10000|1|64"
  "1 pub -> 50 sub fan-out, QoS 0|1|50|1000|0|64"
  "10 pub -> 10 sub, QoS 0, 1KB payload|10|10|10000|0|1024"
)

for scenario in "${LATENCY_SCENARIOS[@]}"; do
  IFS='|' read -r name pubs subs rate qos payload <<< "$scenario"
  slug="$(echo "$name" | tr ' ,>-' '____')"

  echo "==> latency: $name (gomqtt)"
  run_one "127.0.0.1:$GOMQTT_PORT" "$name" "$pubs" "$subs" "$rate" "$qos" "$payload" \
    "$DURATION" "$WARMUP" "$RESULTS_DIR/lat-gomqtt-$slug.json"

  if [ "$HAVE_MOSQUITTO" = "1" ]; then
    echo "==> latency: $name (mosquitto)"
    run_one "127.0.0.1:$MOSQUITTO_PORT" "$name" "$pubs" "$subs" "$rate" "$qos" "$payload" \
      "$DURATION" "$WARMUP" "$RESULTS_DIR/lat-mosquitto-$slug.json"
  fi
done

# ---------------------------------------------------------------------------
# Part 2 — saturation ladder
# ---------------------------------------------------------------------------

RATES=(10000 25000 50000 75000 100000 150000)

ladder() {
  local addr="$1" broker="$2" name="$3" pubs="$4" subs="$5" qos="$6" payload="$7" slug="$8"
  local best=0 best_file=""

  for rate in "${RATES[@]}"; do
    local out="$RESULTS_DIR/ladder-$broker-$slug-$rate.json"
    run_one "$addr" "$name" "$pubs" "$subs" "$rate" "$qos" "$payload" \
      "$LADDER_DURATION" "$LADDER_WARMUP" "$out"

    local delivery p99
    delivery="$(json_field "$out" delivery_ratio)"
    p99="$(json_field "$out" latency_p99_ms)"

    local pass
    pass="$(python3 -c "print(1 if $delivery >= $MIN_DELIVERY and $p99 <= $MAX_P99_MS else 0)")"
    printf '    %-10s rate %-7s delivery %-8.4f p99 %-8.2f %s\n' \
      "$broker" "$rate" "$delivery" "$p99" "$([ "$pass" = 1 ] && echo PASS || echo FAIL)"

    if [ "$pass" = "1" ]; then
      best="$rate"
      best_file="$out"
    else
      break
    fi
  done

  if [ -n "$best_file" ]; then
    cp "$best_file" "$RESULTS_DIR/max-$broker-$slug.json"
  fi
  echo "$best"
}

LADDER_SCENARIOS=(
  "10 pub -> 10 sub, QoS 0|10|10|0|64"
  "10 pub -> 10 sub, QoS 1|10|10|1|64"
)

for scenario in "${LADDER_SCENARIOS[@]}"; do
  IFS='|' read -r name pubs subs qos payload <<< "$scenario"
  slug="$(echo "$name" | tr ' ,>-' '____')"

  echo "==> saturation ladder: $name"
  ladder "127.0.0.1:$GOMQTT_PORT" gomqtt "$name" "$pubs" "$subs" "$qos" "$payload" "$slug" > /dev/null

  if [ "$HAVE_MOSQUITTO" = "1" ]; then
    ladder "127.0.0.1:$MOSQUITTO_PORT" mosquitto "$name" "$pubs" "$subs" "$qos" "$payload" "$slug" > /dev/null
  fi
done

echo
echo "==> results"
python3 scripts/bench_report.py "$RESULTS_DIR" | tee "$RESULTS_DIR/report.md"
