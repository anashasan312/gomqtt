// Command loadgen measures an MQTT broker's throughput and end-to-end latency.
//
// It is broker-agnostic on purpose: it speaks plain MQTT 3.1.1 over TCP, so the
// same binary measures GoMQTT and Mosquitto, and the comparison in the README
// is two runs of one tool rather than two tools whose overheads differ.
//
// Latency is measured by stamping a monotonic timestamp into each payload and
// reading it back in the subscriber. Publishers and subscribers run in one
// process, so both stamps come from the same clock and the measurement needs no
// clock synchronisation — which is also its main limitation: this measures
// broker latency on loopback, not network latency.
//
//	go run ./cmd/loadgen -addr 127.0.0.1:1883 -publishers 10 -subscribers 10 \
//	  -rate 1000 -duration 10s -qos 0 -payload 64
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anashasan/gomqtt/pkg/client"
)

// timestampBytes is the width of the monotonic stamp at the head of a payload.
const timestampBytes = 8

// Config is one benchmark run.
type Config struct {
	Address     string
	Publishers  int
	Subscribers int
	// Rate is the target messages per second across all publishers. Zero means
	// as fast as possible, which measures saturation throughput rather than
	// latency under a controlled load.
	Rate        int
	Duration    time.Duration
	QoS         byte
	PayloadSize int
	TopicPrefix string
	// Warmup is discarded from the results, so connection setup and the Go
	// runtime's first-allocation costs do not show up as broker latency.
	Warmup time.Duration
	JSON   bool
	Label  string
}

// Result is what one run measured.
type Result struct {
	Label       string  `json:"label"`
	Broker      string  `json:"broker"`
	Publishers  int     `json:"publishers"`
	Subscribers int     `json:"subscribers"`
	QoS         int     `json:"qos"`
	PayloadSize int     `json:"payload_bytes"`
	TargetRate  int     `json:"target_rate_msgs_per_sec"`
	DurationSec float64 `json:"duration_sec"`

	Published         uint64  `json:"published"`
	Received          uint64  `json:"received"`
	Expected          uint64  `json:"expected"`
	DeliveryRatio     float64 `json:"delivery_ratio"`
	PublishThroughput float64 `json:"publish_throughput_msgs_per_sec"`
	DeliverThroughput float64 `json:"deliver_throughput_msgs_per_sec"`

	LatencyP50Ms  float64 `json:"latency_p50_ms"`
	LatencyP95Ms  float64 `json:"latency_p95_ms"`
	LatencyP99Ms  float64 `json:"latency_p99_ms"`
	LatencyMaxMs  float64 `json:"latency_max_ms"`
	LatencyMeanMs float64 `json:"latency_mean_ms"`
	Samples       int     `json:"latency_samples"`
}

func main() {
	cfg := Config{}

	flag.StringVar(&cfg.Address, "addr", "127.0.0.1:1883", "broker address")
	flag.IntVar(&cfg.Publishers, "publishers", 10, "number of publishing clients")
	flag.IntVar(&cfg.Subscribers, "subscribers", 10, "number of subscribing clients")
	flag.IntVar(&cfg.Rate, "rate", 0, "target messages per second in total; 0 means unthrottled")
	flag.DurationVar(&cfg.Duration, "duration", 10*time.Second, "measurement duration")
	qos := flag.Int("qos", 0, "publish QoS (0 or 1)")
	flag.IntVar(&cfg.PayloadSize, "payload", 64, "payload size in bytes (minimum 8)")
	flag.StringVar(&cfg.TopicPrefix, "topic", "bench", "topic prefix")
	flag.DurationVar(&cfg.Warmup, "warmup", 2*time.Second, "warmup period, excluded from the results")
	flag.BoolVar(&cfg.JSON, "json", false, "emit the result as JSON")
	flag.StringVar(&cfg.Label, "label", "", "label for this run, used in the report")
	flag.Parse()

	cfg.QoS = byte(*qos)
	if cfg.PayloadSize < timestampBytes {
		cfg.PayloadSize = timestampBytes
	}
	if cfg.Label == "" {
		cfg.Label = cfg.Address
	}

	result, err := run(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
		os.Exit(1)
	}

	if cfg.JSON {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		_ = encoder.Encode(result)
		return
	}
	printReport(result)
}

// latencyRecorder accumulates latency samples for one subscriber.
//
// Deliberately lock-free: each subscriber owns its own recorder and only its
// own read goroutine touches it. A single shared recorder behind a mutex
// becomes the contended hot spot at high message rates, and the queueing delay
// it introduces lands in the very numbers the tool is supposed to be measuring.
//
// Samples are kept and sorted at the end rather than summarised online, because
// percentiles are the whole point: a mean hides exactly the tail that decides
// whether a broker is usable, and a streaming quantile estimator would add
// approximation error to a measurement whose credibility is its selling point.
type latencyRecorder struct {
	samples []time.Duration
}

// newLatencyRecorder builds a recorder with room to grow without reallocating
// mid-measurement.
func newLatencyRecorder() *latencyRecorder {
	return &latencyRecorder{samples: make([]time.Duration, 0, 1<<16)}
}

// record adds a sample.
func (r *latencyRecorder) record(d time.Duration) {
	r.samples = append(r.samples, d)
}

// mergeRecorders combines every subscriber's samples. Called after the
// subscribers have stopped, so no lock is needed here either.
func mergeRecorders(recorders []*latencyRecorder) *latencyRecorder {
	total := 0
	for _, r := range recorders {
		total += len(r.samples)
	}

	merged := &latencyRecorder{samples: make([]time.Duration, 0, total)}
	for _, r := range recorders {
		merged.samples = append(merged.samples, r.samples...)
	}
	return merged
}

// percentiles returns p50, p95, p99, max and mean.
func (r *latencyRecorder) percentiles() (p50, p95, p99, max, mean time.Duration, count int) {
	if len(r.samples) == 0 {
		return 0, 0, 0, 0, 0, 0
	}

	sorted := append([]time.Duration(nil), r.samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, s := range sorted {
		total += s
	}

	at := func(q float64) time.Duration {
		idx := int(float64(len(sorted)-1) * q)
		return sorted[idx]
	}

	return at(0.50), at(0.95), at(0.99),
		sorted[len(sorted)-1],
		total / time.Duration(len(sorted)),
		len(sorted)
}

// run executes one benchmark.
func run(cfg Config) (Result, error) {
	var (
		published atomic.Uint64
		received  atomic.Uint64
		// measureFrom is the instant the measurement window opens, in
		// nanoseconds. Zero means the window has not opened yet.
		//
		// A message is counted by the time it was *published*, read back out of
		// its own payload, rather than by when it arrived. Counting arrivals
		// would include messages published during warmup that landed just after
		// the window opened, which is how a delivery ratio ends up above 1.0 and
		// the whole measurement stops being defensible.
		measureFrom atomic.Int64
	)

	recorders := make([]*latencyRecorder, cfg.Subscribers)

	// Subscribers connect first. A subscription that is not yet registered when
	// the first publication arrives loses it — correct MQTT behaviour, and it
	// would show up as a delivery shortfall that has nothing to do with the
	// broker's performance.
	subscribers := make([]*client.Client, 0, cfg.Subscribers)
	for i := 0; i < cfg.Subscribers; i++ {
		recorder := newLatencyRecorder()
		recorders[i] = recorder

		sub := client.New(client.Options{
			Address:      cfg.Address,
			ClientID:     fmt.Sprintf("loadgen-sub-%d-%d", os.Getpid(), i),
			CleanSession: true,
			KeepAlive:    60,
			OnMessage: func(m client.Message) {
				now := time.Now().UnixNano()

				from := measureFrom.Load()
				if from == 0 || len(m.Payload) < timestampBytes {
					return
				}
				sentAt := int64(binary.BigEndian.Uint64(m.Payload[:timestampBytes]))
				if sentAt < from {
					// Published during warmup; not this window's business.
					return
				}

				received.Add(1)
				recorder.record(time.Duration(now - sentAt))
			},
		})
		if err := sub.Connect(); err != nil {
			return Result{}, fmt.Errorf("subscriber %d: %w", i, err)
		}
		defer sub.Close()

		if _, err := sub.Subscribe(cfg.TopicPrefix+"/#", cfg.QoS); err != nil {
			return Result{}, fmt.Errorf("subscriber %d: %w", i, err)
		}
		subscribers = append(subscribers, sub)
	}

	publishers := make([]*client.Client, 0, cfg.Publishers)
	for i := 0; i < cfg.Publishers; i++ {
		pub := client.New(client.Options{
			Address:      cfg.Address,
			ClientID:     fmt.Sprintf("loadgen-pub-%d-%d", os.Getpid(), i),
			CleanSession: true,
			KeepAlive:    60,
			AckTimeout:   30 * time.Second,
		})
		if err := pub.Connect(); err != nil {
			return Result{}, fmt.Errorf("publisher %d: %w", i, err)
		}
		defer pub.Close()
		publishers = append(publishers, pub)
	}

	payload := make([]byte, cfg.PayloadSize)
	for i := timestampBytes; i < len(payload); i++ {
		payload[i] = byte('a' + i%26)
	}

	// Per-publisher interval, so the aggregate rate is the configured one
	// however many publishers there are.
	var interval time.Duration
	if cfg.Rate > 0 {
		interval = time.Duration(float64(time.Second) / (float64(cfg.Rate) / float64(cfg.Publishers)))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i, pub := range publishers {
		wg.Add(1)
		go func(idx int, c *client.Client) {
			defer wg.Done()
			publishLoop(c, cfg, idx, payload, interval, stop, &published)
		}(i, pub)
	}

	// Warmup: connections settle, the runtime warms, and none of it is counted.
	time.Sleep(cfg.Warmup)

	start := time.Now()
	measureFrom.Store(start.UnixNano())
	publishedAtStart := published.Load()

	time.Sleep(cfg.Duration)

	// The window closes here, and publishing stops, so every message counted
	// below was published inside it.
	windowEnd := time.Now()
	elapsed := windowEnd.Sub(start)
	close(stop)
	wg.Wait()

	publishedDuring := published.Load() - publishedAtStart

	// A settle period before reading the receive count: messages published in
	// the last moments of the window are still in flight, and counting
	// immediately would understate delivery for reasons that have nothing to do
	// with the broker.
	time.Sleep(1500 * time.Millisecond)
	receivedDuring := received.Load()

	// Subscribers are closed before their samples are read, so the merge and
	// the sort cannot race a receive goroutine still appending.
	for _, sub := range subscribers {
		sub.Close()
	}
	p50, p95, p99, max, mean, samples := mergeRecorders(recorders).percentiles()

	expected := publishedDuring * uint64(len(subscribers))
	ratio := 0.0
	if expected > 0 {
		ratio = float64(receivedDuring) / float64(expected)
	}

	return Result{
		Label:       cfg.Label,
		Broker:      cfg.Address,
		Publishers:  cfg.Publishers,
		Subscribers: cfg.Subscribers,
		QoS:         int(cfg.QoS),
		PayloadSize: cfg.PayloadSize,
		TargetRate:  cfg.Rate,
		DurationSec: elapsed.Seconds(),

		Published:         publishedDuring,
		Received:          receivedDuring,
		Expected:          expected,
		DeliveryRatio:     ratio,
		PublishThroughput: float64(publishedDuring) / elapsed.Seconds(),
		DeliverThroughput: float64(receivedDuring) / elapsed.Seconds(),

		LatencyP50Ms:  msOf(p50),
		LatencyP95Ms:  msOf(p95),
		LatencyP99Ms:  msOf(p99),
		LatencyMaxMs:  msOf(max),
		LatencyMeanMs: msOf(mean),
		Samples:       samples,
	}, nil
}

// publishLoop publishes until stopped.
func publishLoop(
	c *client.Client,
	cfg Config,
	index int,
	payload []byte,
	interval time.Duration,
	stop <-chan struct{},
	published *atomic.Uint64,
) {
	topic := fmt.Sprintf("%s/%d", cfg.TopicPrefix, index)

	// Each publisher owns its payload buffer: they all write a timestamp into
	// the first eight bytes, and sharing one buffer would have them overwriting
	// each other's stamps and producing nonsense latencies.
	buf := append([]byte(nil), payload...)

	var ticker *time.Ticker
	if interval > 0 {
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	}

	for {
		select {
		case <-stop:
			return
		default:
		}

		if ticker != nil {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}

		binary.BigEndian.PutUint64(buf[:timestampBytes], uint64(time.Now().UnixNano()))

		if err := c.Publish(topic, buf, cfg.QoS, false); err != nil {
			// A publish failure at saturation is information, not a reason to
			// abort: the run reports the delivery ratio, and a broker that
			// refuses work under load should be visible in the numbers rather
			// than crashing the tool.
			return
		}
		published.Add(1)
	}
}

// msOf renders a duration in milliseconds.
func msOf(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / 1e6
}

// printReport writes a human-readable summary.
func printReport(r Result) {
	fmt.Printf("\n  %s\n", r.Label)
	fmt.Printf("  %s\n", divider())
	fmt.Printf("  broker            %s\n", r.Broker)
	fmt.Printf("  publishers        %d\n", r.Publishers)
	fmt.Printf("  subscribers       %d\n", r.Subscribers)
	fmt.Printf("  qos               %d\n", r.QoS)
	fmt.Printf("  payload           %d bytes\n", r.PayloadSize)
	if r.TargetRate > 0 {
		fmt.Printf("  target rate       %d msg/s\n", r.TargetRate)
	} else {
		fmt.Printf("  target rate       unthrottled\n")
	}
	fmt.Printf("  duration          %.1fs\n", r.DurationSec)
	fmt.Printf("  %s\n", divider())
	fmt.Printf("  published         %d (%.0f msg/s)\n", r.Published, r.PublishThroughput)
	fmt.Printf("  delivered         %d (%.0f msg/s)\n", r.Received, r.DeliverThroughput)
	fmt.Printf("  delivery ratio    %.4f\n", r.DeliveryRatio)
	fmt.Printf("  %s\n", divider())
	fmt.Printf("  latency p50       %.3f ms\n", r.LatencyP50Ms)
	fmt.Printf("  latency p95       %.3f ms\n", r.LatencyP95Ms)
	fmt.Printf("  latency p99       %.3f ms\n", r.LatencyP99Ms)
	fmt.Printf("  latency max       %.3f ms\n", r.LatencyMaxMs)
	fmt.Printf("  latency mean      %.3f ms\n", r.LatencyMeanMs)
	fmt.Printf("  samples           %d\n", r.Samples)
	fmt.Println()
}

// divider renders a rule for the report.
func divider() string {
	const width = 40
	out := make([]byte, width)
	for i := range out {
		out[i] = '-'
	}
	return string(out)
}
