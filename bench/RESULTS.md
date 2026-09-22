
### Latency at a fixed offered load

Both brokers are given the same message rate, well below saturation, so what is measured is delivery latency rather than queueing.

| Scenario | Offered load | Broker | p50 | p95 | p99 | Delivery |
|---|---:|---|---:|---:|---:|---:|
| 1 pub to 1 sub, QoS 0 | 2.0k msg/s | **GoMQTT** | 0.09 ms | 0.18 ms | 0.25 ms | 100.0% |
|  |  | Mosquitto | 0.08 ms | 0.17 ms | 0.28 ms | 100.0% |
| 10 pub to 10 sub, QoS 0 | 10.0k msg/s | **GoMQTT** | 0.56 ms | 1.70 ms | 5.07 ms | 100.0% |
|  |  | Mosquitto | 0.98 ms | 2.49 ms | 3.34 ms | 100.0% |
| 10 pub to 10 sub, QoS 1 | 10.0k msg/s | **GoMQTT** | 0.89 ms | 3.28 ms | 5.59 ms | 100.0% |
|  |  | Mosquitto | 0.45 ms | 13.64 ms | 25.93 ms | 100.0% |
| 1 pub to 50 sub fan-out, QoS 0 | 1.0k msg/s | **GoMQTT** | 0.37 ms | 0.68 ms | 1.03 ms | 100.0% |
|  |  | Mosquitto | 9.51 ms | 18.47 ms | 20.11 ms | 100.0% |
| 10 pub to 10 sub, QoS 0, 1KB | 10.0k msg/s | **GoMQTT** | 0.64 ms | 3.93 ms | 27.92 ms | 99.5% |
|  |  | Mosquitto | 1.13 ms | 2.66 ms | 3.63 ms | 100.0% |

### Maximum sustained throughput

The highest offered rate at which the broker still delivered ≥99% of messages with a p99 under 50 ms.

| Scenario | Broker | Max sustained publish rate | Delivered (fan-out) | p99 |
|---|---|---:|---:|---:|
| 10 pub to 10 sub, QoS 0 | **GoMQTT** | 13.5k msg/s | 134.4k msg/s | 13.96 ms |
|  | Mosquitto | 16.8k msg/s | 167.9k msg/s | 39.82 ms |
| 10 pub to 10 sub, QoS 1 | **GoMQTT** | 9.9k msg/s | 99.3k msg/s | 14.47 ms |
|  | Mosquitto | 13.1k msg/s | 131.2k msg/s | 49.71 ms |

Measured on:

- Linux 6.18.44-fc-v37, x86_64
- 2 logical CPUs
- Both brokers on loopback, load generator in a separate process

