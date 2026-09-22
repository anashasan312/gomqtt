# GoMQTT

An MQTT 3.1.1 broker written from scratch in Go — binary packet codec, raw TCP,
per-client goroutines, no MQTT library.

QoS 0 and QoS 1 with acknowledgement-based retransmission, wildcard
subscriptions (`+`, `#`) backed by a topic trie, retained messages, and
persistent sessions that queue for offline clients.

Load-tested head to head against Eclipse Mosquitto 2.0.18, and verified against
it for wire compatibility in both directions.

Built on **DDD + Clean Architecture** with **Google Wire** for dependency
injection.

---

## Quickstart

```bash
make deps    # resolve dependencies and write go.sum — run this first
make run     # broker on :1883, admin API and metrics on :8080
make demo    # exercise it with mosquitto_pub / mosquitto_sub
```

Or the full stack with Prometheus and Grafana:

```bash
docker compose -f deployment/docker-compose.yml up --build
```

| | |
|---|---|
| MQTT | `127.0.0.1:1883` |
| Admin API | http://localhost:8080/api/v1/stats |
| Metrics | http://localhost:8080/metrics |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin / admin) |

`make help` lists every target.

---

## Benchmarks against Mosquitto

Both brokers measured by the same load generator (`cmd/loadgen`) over loopback
TCP, so client overhead is identical and cancels out.

### Latency at a fixed offered load

The offered rate is held well below saturation for both brokers, so what is
measured is delivery latency rather than queueing.

| Scenario | Offered load | Broker | p50 | p95 | p99 | Delivery |
|---|---:|---|---:|---:|---:|---:|
| 1 pub → 1 sub, QoS 0 | 2.0k msg/s | **GoMQTT** | 0.09 ms | 0.18 ms | 0.25 ms | 100.0% |
| | | Mosquitto | 0.08 ms | 0.17 ms | 0.28 ms | 100.0% |
| 10 pub → 10 sub, QoS 0 | 10.0k msg/s | **GoMQTT** | 0.56 ms | 1.70 ms | 5.07 ms | 100.0% |
| | | Mosquitto | 0.98 ms | 2.49 ms | 3.34 ms | 100.0% |
| 10 pub → 10 sub, QoS 1 | 10.0k msg/s | **GoMQTT** | 0.89 ms | 3.28 ms | **5.59 ms** | 100.0% |
| | | Mosquitto | 0.45 ms | 13.64 ms | 25.93 ms | 100.0% |
| 1 pub → 50 sub fan-out, QoS 0 | 1.0k msg/s | **GoMQTT** | **0.37 ms** | 0.68 ms | 1.03 ms | 100.0% |
| | | Mosquitto | 9.51 ms | 18.47 ms | 20.11 ms | 100.0% |
| 10 pub → 10 sub, QoS 0, 1 KB | 10.0k msg/s | **GoMQTT** | 0.64 ms | 3.93 ms | 27.92 ms | 99.5% |
| | | Mosquitto | 1.13 ms | 2.66 ms | 3.63 ms | 100.0% |

### Maximum sustained throughput

The highest offered rate at which the broker still delivered ≥99% of messages
with a p99 under 50 ms. Delivered counts the fan-out: 10 subscribers means every
publish becomes ten deliveries.

| Scenario | Broker | Max sustained publish rate | Delivered | p99 |
|---|---|---:|---:|---:|
| 10 pub → 10 sub, QoS 0 | **GoMQTT** | 13.5k msg/s | 134.4k msg/s | 13.96 ms |
| | Mosquitto | **16.8k msg/s** | **167.9k msg/s** | 39.82 ms |
| 10 pub → 10 sub, QoS 1 | **GoMQTT** | 9.9k msg/s | 99.3k msg/s | 14.47 ms |
| | Mosquitto | **13.1k msg/s** | **131.2k msg/s** | 49.71 ms |

### Reading these honestly

**Where GoMQTT wins.** Fan-out is the standout: at 1 → 50 subscribers its
median latency is 0.37 ms against Mosquitto's 9.51 ms, roughly 26× better. That
is the topic trie plus per-client writer goroutines doing exactly what they were
designed for — one index lookup resolves the whole subscriber set, and fifty
deliveries are fifty channel sends rather than fifty sequential socket writes on
one thread. QoS 1 tail latency is the other clear win: p99 of 5.59 ms against
25.93 ms.

**Where Mosquitto wins.** Its raw throughput ceiling is about 25% higher on both
QoS levels, and it handles 1 KB payloads with a much tighter tail (p99 3.63 ms
against 27.92 ms). GoMQTT is copying payloads more than it needs to on the
publish path, and at larger sizes that shows.

**How they fail differs, and that is a design choice rather than an accident.**
Past its ceiling GoMQTT sheds load: its outbound queue per client is bounded, so
a client that cannot keep up loses messages and the broker's memory stays flat.
Mosquitto buffers further and its latency climbs instead. At 25k msg/s GoMQTT
delivered 82.5% at a p99 of 55 ms while Mosquitto delivered 100% at a p99 of
62 ms. Neither is right in general — shedding is right when stale telemetry is
worthless, buffering is right when every message matters — but GoMQTT's choice
is deliberate and is why `messages_dropped_total` is a first-class metric.

**Caveats, because a benchmark without them is marketing.**

- Measured on 2 logical CPUs (Intel Xeon @ 2.10 GHz), Linux 6.18, Go 1.24.7,
  Mosquitto 2.0.18. With only two cores the load generator competes with the
  broker for CPU, which compresses the ceiling for both.
- Loopback only. This measures broker behaviour, not network behaviour.
- At QoS 1 the load generator's publishers wait for each PUBACK, so the achieved
  publish rate is bounded by round-trip time rather than by the broker. The
  comparison stays fair — the same client drives both — but the QoS 1 ceilings
  are a property of the harness as much as of the brokers.
- Single runs, not repeated trials with confidence intervals.

Reproduce with `make bench`. Raw JSON lands in `bench/results/`, the table in
`bench/RESULTS.md`.

---

## Verified against Mosquitto

Everything else in this project tests GoMQTT against itself: the same codec
encodes and decodes, so a misreading of the specification would be applied
consistently in both directions and every test would still pass.

`make test-interop` breaks that circularity in both directions:

| | |
|---|---|
| `mosquitto_pub` → GoMQTT | an independent client publishes into our broker, QoS 0 and 1 |
| `mosquitto_sub` → GoMQTT | an independent client subscribes with wildcards and receives |
| `mosquitto_sub` → GoMQTT | retained delivery on subscribe |
| our client → Mosquitto | our CONNECT, PUBLISH, SUBSCRIBE and PUBACK against a real broker |
| our client → Mosquitto | retained messages |
| our client → Mosquitto | persistent session with offline queueing |

All six pass. That is what makes the wire format trustworthy rather than merely
self-consistent.

---

## Why it is built this way

### Dependencies point inwards

```
cmd/                      process lifecycle, listeners, ordered shutdown
 └── pkg/
     ├── api/             admin HTTP handlers and middleware
     ├── application/     services — orchestration, no protocol bytes
     ├── domain/          aggregates, value objects, ports  ← nothing above depends on
     ├── contracts/       admin API DTOs
     ├── hydrator/        service results → contracts
     ├── infrastructure/  packet codec, TCP transport, in-memory stores, Prometheus
     ├── client/          a small MQTT client, for tests and the load generator
     └── di/              the composition root (Wire)
```

The domain imports nothing from the layers around it. It has never heard of
TCP, of Gin, or of Prometheus. It declares the interfaces it needs —
`ISessionStore`, `IClientRegistry`, `IRetainedStore`, `ISubscriptionIndex`,
`metrics.Recorder`, `Authenticator` — and `pkg/di` is the one file that decides
which concrete type satisfies each.

### The trie is fast; the spec is readable; a test proves they agree

`TopicFilter.Matches` in `pkg/domain/message_aggregate/value_objects` is the
reference implementation of §4.7 — direct, level by level, written to be
*obviously* right rather than fast. The trie in
`pkg/infrastructure/persistence/memory` is the index the delivery path actually
uses.

`TestIndex_AgreesWithTheSpecImplementation` generates thousands of filter/topic
pairs and asserts the two never disagree — 2,829 pairs on the last run. Keeping
the readable definition and the fast index as separate artefacts, with a
property test between them, is what makes it safe to optimise the index later.

### Business rules live on the aggregates

`Session` owns the QoS 1 in-flight window, packet identifier allocation and the
offline queue together, because they change together: acknowledging a message
frees an identifier, which may let a queued message move into the window.
Splitting them across aggregates would make that sequence racy.

`Client.Disconnect` returns whether the will must be published, rather than
leaving the caller to decide. §3.1.2.5 gives one correct answer, and three
different code paths disconnect a client — a caller that re-derived the rule
would eventually get one of them wrong.

### SOLID, concretely

**Single responsibility.** `packet` knows bytes. `transport` knows sockets and
goroutine lifetimes. `publishing` knows what a delivery means. That split is why
the whole QoS 1 flow is unit-tested with an in-memory `PacketSink` and no
network at all.

**Open/closed.** `Authenticator` is an interface with two implementations;
adding LDAP or JWT means a new type and one line in `pkg/di/adapters.go`, and
the connection service does not change.

**Liskov.** Every store carries a compile-time assertion that it satisfies its
port, so a signature drift breaks the build next to the code that has to change.

**Interface segregation.** The admin service depends on a three-method
`CounterSource`, not on the whole publishing service — it wants to count
messages, not to be able to publish them. Configuration is the same idea:
`GetListenerConfig(cfg)` rather than passing `*AppConfig` everywhere.

**Dependency inversion.** Interfaces are declared by the consumer in
`pkg/domain/persistence`, implemented in `pkg/infrastructure`, and named
together only in `pkg/di`.

---

## Protocol coverage

| Feature | Status |
|---|---|
| CONNECT / CONNACK, all five return codes | ✅ |
| PUBLISH / PUBACK, QoS 0 and QoS 1 | ✅ |
| SUBSCRIBE / SUBACK, per-filter return codes | ✅ |
| UNSUBSCRIBE / UNSUBACK | ✅ |
| PINGREQ / PINGRESP, keep-alive enforcement at 1.5× | ✅ |
| DISCONNECT, with will suppression | ✅ |
| Wildcards `+` and `#`, including `$SYS` exclusion | ✅ |
| Retained messages, including clear-by-empty-payload | ✅ |
| Persistent sessions, offline queueing, resume | ✅ |
| Last will and testament | ✅ |
| Session takeover on duplicate client id | ✅ |
| QoS 1 retransmission with DUP, on timeout and on reconnect | ✅ |
| QoS 2 (PUBREC / PUBREL / PUBCOMP) | ❌ granted as QoS 1 in SUBACK; a QoS 2 PUBLISH closes the connection |
| TLS, WebSockets, MQTT 5.0 | ❌ not implemented |

QoS 2 is refused rather than silently downgraded on the publish path. Treating a
QoS 2 PUBLISH as QoS 1 would tell a publisher its exactly-once guarantee was
honoured when it was not, which is worse than refusing it.

---

## How it works

### One connection, two goroutines

A reader blocks on the socket; a writer blocks on a channel. One goroutine doing
both would mean a client that sends nothing delays every message the broker
wants to push to it — and a subscriber that never speaks is the normal case.

One writer per connection is also what keeps the stream valid: two goroutines
writing to one TCP connection interleave their bytes and corrupt it
irrecoverably. `packet.Writer` is documented as not safe for concurrent use, and
the single writer goroutine is the reason it does not need to be.

### Back-pressure is a bounded queue, not a block

`Connection.Send` never blocks. A full outbound queue returns
`ErrClientQueueFull` and the publishing service counts a drop. If it blocked,
one subscriber that stopped reading would stall the publisher goroutine
delivering to it — and since a publisher fans out to many subscribers in
sequence, one dead client would stop delivery to every other client.

### The QoS 1 flow

A QoS 1 delivery takes a packet identifier from the subscriber's session,
records the message in the in-flight window, and sends. The PUBACK clears it and
immediately promotes a queued message into the freed slot, which is what makes
the window act as flow control rather than a cap that silently drops everything
past it.

Unacknowledged messages are retransmitted with DUP set in two situations, which
are different failures and are handled separately: a dropped PUBACK on a live
connection (`RetransmitExpired`, on a timer) and a client that went away
entirely (`ResumeSession`, on reconnect).

### Session takeover

§3.1.4 requires a second CONNECT with an existing client identifier to displace
the first. The subtle part is the teardown: the old connection's cleanup runs
concurrently with the new connection's setup, and both refer to the same client
identifier. `IClientRegistry.Unregister` is identity-checked, and its answer
decides whether the disconnecting connection may touch the session at all —
without that check, the old connection's teardown deletes the session the new
one just created, and the reconnected client sits there subscribed to nothing
with no error anywhere.

That bug was real in this codebase, caught by
`TestSessionTakeover_ClosesTheOlderConnection`.

---

## Admin API

Read-only, on the private port.

| | |
|---|---|
| `GET /api/v1/stats` | counters, uptime, message totals |
| `GET /api/v1/clients` | connected clients with per-client session depth |
| `GET /api/v1/subscriptions` | every subscription, and whether its QoS was downgraded |
| `GET /api/v1/retained` | every retained topic, with payload size |
| `GET /health` | liveness |
| `GET /metrics` | Prometheus |

Read-only by design: everything an operator needs to *change* about a running
broker is already expressible in MQTT itself, and a mutating endpoint would be a
second, unaudited way to do what the protocol already does.

Metrics carry bounded labels only — packet type, QoS, and drop or disconnect
reasons from a closed set. No label ever carries a client identifier or a topic,
because a broker with 50,000 clients and a topic per device would otherwise
produce a cardinality that takes the Prometheus server down long before it told
anyone anything.

---

## Configuration

`config/config.local.yaml` documents every setting; `GOMQTT_`-prefixed
environment variables override it, and credentials only ever come from the
environment.

```bash
GOMQTT_ADDRESS=":1883"
GOMQTT_MAX_CONNECTIONS=50000
GOMQTT_ALLOW_ANONYMOUS=false
GOMQTT_USERS="alice:secret,bob:hunter2"
GOMQTT_MAX_INFLIGHT=64
GOMQTT_LOG_LEVEL=info
```

Configuration is validated at startup, so a bad value fails at boot rather than
at 3am when the first client of that shape connects — including the case of
`allow_anonymous: false` with no users configured, which would otherwise refuse
every client silently.

---

## Testing

```bash
make test            # unit + end-to-end, race detector, no dependencies
make test-interop    # against real Mosquitto
make fuzz            # fuzz the packet decoder
make bench           # head-to-head benchmark
make bench-trie      # subscription trie micro-benchmark
```

The default suite needs nothing installed. That is deliberate — a suite that
requires a broker to be running is a suite people stop running.

Worth reading as documentation of intent:

- `TestIndex_AgreesWithTheSpecImplementation` — the trie against the spec, over thousands of generated pairs
- `FuzzDecode` — the decoder never panics; ~1M executions clean on the last run
- `TestDecode_RejectsMalformedInput` — 19 hostile byte sequences, each rejected
- `TestSessionTakeover_ClosesTheOlderConnection` — the race described above
- `TestWill_IsSuppressedOnACleanDisconnect` — and its abnormal-disconnect twin
- `TestRetained_IsClearedByAnEmptyRetainedPublish` — the clear-by-empty rule
- `TestKeepAlive_DisconnectsASilentClient` — probed via the broker's own connection count, because any client-side probe would reset the keep-alive it is trying to test

---

## Design decisions, and what they cost

**A trie for subscriptions, a flat map for retained messages.** The asymmetry
comes from the access pattern. Subscriptions are read once per published
message, so they get an index; retained messages are *written* by exact topic on
every retained publish and read by filter only on subscribe, so writes are O(1)
and the rare read takes a linear scan. That scan is the known scaling limit, and
it is called out here rather than discovered later.

**Payloads are copied on decode.** The decoded slice aliases a connection read
buffer that is reused for the next packet, while a message may be queued for an
offline client or retained indefinitely. The copy is the price of not handing
subscribers a buffer that changes under them.

**Everything is in memory.** Sessions do not survive a broker restart. The
`ISessionStore` port exists precisely so a disk-backed implementation is a new
package plus one binding in `pkg/di`, but it is not written.

**One RWMutex over the trie.** Matching takes the read lock and is the
overwhelming majority of traffic, so publishers proceed in parallel. A sharded
lock would scale further and is not worth the complexity until a profile says
the lock is the bottleneck.

**The client shares the broker's codec.** Convenient and dependency-free, but it
means a codec bug could make client and broker agree while both disagree with
the standard. That is exactly why the interop suite exists.

---

## Project layout

```
cmd/
  main.go                       lifecycle, signals, ordered shutdown
  server/                       listeners, background loops, admin routes
  loadgen/                      the benchmark tool
pkg/
  domain/
    client_aggregate/           Client root, client id, keep-alive, protocol version
    session_aggregate/          Session root: subscriptions, in-flight window, offline queue
    message_aggregate/          Message root, topic name and filter, QoS
    subscription_aggregate/     Subscription entity
    persistence/                the ports
    metrics/                    the observability port
  application/
    services/                   service interfaces and the PacketSink seam
    connection/                 CONNECT, session resolution, takeover, teardown
    publishing/                 routing, QoS flows, retained delivery, resume
    subscribing/                SUBSCRIBE and UNSUBSCRIBE
    admin/                      read-only introspection
  infrastructure/
    mqtt/packet/                the MQTT 3.1.1 wire format
    transport/                  TCP listener and per-client goroutines
    persistence/memory/         topic trie, session store, retained store, registry
    metrics/prometheus/         the metrics implementation
    config/                     configuration and its narrow accessors
  client/                       a small MQTT client for tests and the load generator
  api/, contracts/, hydrator/, common/, di/
test/
  conformance/                  end-to-end over real TCP
  interop/                      against Eclipse Mosquitto
deployment/                     Dockerfile, compose, Prometheus, Grafana
bench/RESULTS.md                the generated benchmark tables
```

---

## License

MIT
