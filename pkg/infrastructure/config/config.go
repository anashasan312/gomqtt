// Package config loads and validates broker configuration.
//
// Configuration is read from a YAML file and then overlaid with environment
// variables, in that order. The file holds the shape and the sensible defaults
// so a new contributor can read one document and understand every knob; the
// environment holds whatever a deployment must change, above all secrets, which
// never belong in a committed file.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/anashasan/gomqtt/pkg/common/errors"
)

// EnvPrefix namespaces every environment override.
const EnvPrefix = "GOMQTT_"

// AppConfig is the root configuration document.
type AppConfig struct {
	Env     string        `yaml:"env"`
	Broker  BrokerConfig  `yaml:"broker"`
	Admin   AdminConfig   `yaml:"admin"`
	Logger  LoggerConfig  `yaml:"logger"`
	Session SessionConfig `yaml:"session"`
	Limits  LimitsConfig  `yaml:"limits"`
	Auth    AuthConfig    `yaml:"auth"`
}

// BrokerConfig configures the MQTT TCP listener.
type BrokerConfig struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
	// Address is the MQTT bind address. 1883 is the IANA-assigned MQTT port.
	Address string `yaml:"address"`
	// MaxConnections caps concurrent clients. Zero means unlimited.
	MaxConnections int `yaml:"max_connections"`
	// ConnectTimeout is how long a new socket has to send its CONNECT before
	// it is dropped, which is what stops an idle-socket flood.
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	// WriteTimeout bounds a single socket write.
	WriteTimeout time.Duration `yaml:"write_timeout"`
	// ShutdownTimeout bounds how long a graceful stop waits for clients.
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	// OutboundQueueSize is how many packets may await writing per client.
	OutboundQueueSize int `yaml:"outbound_queue_size"`
	// ReadBufferSize sizes each connection's read buffer.
	ReadBufferSize int `yaml:"read_buffer_size"`
}

// AdminConfig configures the HTTP admin and metrics listener.
//
// A separate port from MQTT, so a deployment can expose the broker to devices
// while keeping metrics and introspection on an internal network.
type AdminConfig struct {
	Enabled bool   `yaml:"enabled"`
	Address string `yaml:"address"`
}

// LoggerConfig configures the structured logger.
type LoggerConfig struct {
	Level string `yaml:"level"`
}

// SessionConfig bounds per-session memory.
type SessionConfig struct {
	// MaxInflight is the QoS 1 flow-control window per client.
	MaxInflight int `yaml:"max_inflight"`
	// MaxQueued is how many messages are held for an offline client.
	MaxQueued int `yaml:"max_queued"`
	// MaxSubscriptions caps the filters one session may hold.
	MaxSubscriptions int `yaml:"max_subscriptions"`
	// RetransmitInterval is how often unacknowledged QoS 1 messages are resent
	// to a still-connected client.
	RetransmitInterval time.Duration `yaml:"retransmit_interval"`
	// RetransmitTimeout is how long a message may go unacknowledged before it
	// is resent.
	RetransmitTimeout time.Duration `yaml:"retransmit_timeout"`
	// ResumeBatchSize caps how many queued messages are drained per reconnect.
	ResumeBatchSize int `yaml:"resume_batch_size"`
}

// LimitsConfig bounds what a client may send.
type LimitsConfig struct {
	// MaxPacketSize bounds one inbound control packet.
	MaxPacketSize int `yaml:"max_packet_size"`
	// MaxPayloadSize bounds a published payload.
	MaxPayloadSize int `yaml:"max_payload_size"`
	// MaxKeepAlive caps what a client may request. Zero means no cap.
	MaxKeepAlive uint16 `yaml:"max_keep_alive"`
}

// AuthConfig configures client authentication.
type AuthConfig struct {
	// AllowAnonymous permits a CONNECT with no username.
	//
	// True by default, which is what an MQTT broker on a trusted network
	// normally wants — and it is stated plainly here rather than buried, so
	// anyone deploying it to a less trusted network can see what they are
	// turning off.
	AllowAnonymous bool `yaml:"allow_anonymous"`
	// Users maps a username to a password for the static authenticator.
	// Populate it from the environment, never from a committed file.
	Users map[string]string `yaml:"users"`
}

// Default returns a fully populated configuration.
//
// Every field has a working default so the binary runs with no file and no
// environment at all. A config system that needs twenty variables before it
// will start is a config system people work around.
func Default() *AppConfig {
	return &AppConfig{
		Env: "local",
		Broker: BrokerConfig{
			Name:              "gomqtt",
			Version:           "1.0.0",
			Address:           ":1883",
			MaxConnections:    10000,
			ConnectTimeout:    10 * time.Second,
			WriteTimeout:      10 * time.Second,
			ShutdownTimeout:   15 * time.Second,
			OutboundQueueSize: 256,
			ReadBufferSize:    4096,
		},
		Admin: AdminConfig{
			Enabled: true,
			Address: ":8080",
		},
		Logger: LoggerConfig{Level: "info"},
		Session: SessionConfig{
			MaxInflight:        32,
			MaxQueued:          1000,
			MaxSubscriptions:   512,
			RetransmitInterval: 5 * time.Second,
			RetransmitTimeout:  30 * time.Second,
			ResumeBatchSize:    64,
		},
		Limits: LimitsConfig{
			MaxPacketSize:  1 << 20,
			MaxPayloadSize: 1 << 20,
			MaxKeepAlive:   0,
		},
		Auth: AuthConfig{
			AllowAnonymous: true,
			Users:          map[string]string{},
		},
	}
}

// Load reads configuration from an optional YAML file and the environment.
//
// A missing path is not an error: it means "run on defaults plus environment",
// which is what a container deployment wants.
func Load(path string) (*AppConfig, error) {
	cfg := Default()

	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, errors.Internal("config_read_failed", "failed to read config file "+path, err)
		}
		if err := yaml.Unmarshal(raw, cfg); err != nil {
			return nil, errors.Internal("config_parse_failed", "failed to parse config file "+path, err)
		}
	}

	cfg.applyEnv()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// applyEnv overlays environment variables.
//
// Only the values a deployment genuinely varies are exposed. Mapping every
// field automatically would mean a typo in an env var silently changes
// behaviour nobody intended to make configurable.
func (c *AppConfig) applyEnv() {
	envString(&c.Env, "ENV")
	envString(&c.Logger.Level, "LOG_LEVEL")
	envString(&c.Broker.Address, "ADDRESS")
	envString(&c.Admin.Address, "ADMIN_ADDRESS")
	envBool(&c.Admin.Enabled, "ADMIN_ENABLED")
	envInt(&c.Broker.MaxConnections, "MAX_CONNECTIONS")
	envInt(&c.Session.MaxInflight, "MAX_INFLIGHT")
	envInt(&c.Session.MaxQueued, "MAX_QUEUED")
	envInt(&c.Limits.MaxPacketSize, "MAX_PACKET_SIZE")
	envBool(&c.Auth.AllowAnonymous, "ALLOW_ANONYMOUS")

	// Credentials come from the environment as "user:pass,user2:pass2" so they
	// never have to live in a file that might be committed.
	if raw := os.Getenv(EnvPrefix + "USERS"); raw != "" {
		users := make(map[string]string)
		for _, pair := range strings.Split(raw, ",") {
			name, password, ok := strings.Cut(strings.TrimSpace(pair), ":")
			if ok && name != "" {
				users[name] = password
			}
		}
		if len(users) > 0 {
			c.Auth.Users = users
		}
	}
}

// Validate rejects a configuration that cannot produce a working broker.
//
// This runs at startup so a bad value fails loudly at boot rather than at 3am
// when the first client of that shape connects.
func (c *AppConfig) Validate() error {
	if c.Broker.Address == "" {
		return errors.Invalid("invalid_config", "broker.address must not be empty")
	}
	if c.Admin.Enabled && c.Admin.Address == "" {
		return errors.Invalid("invalid_config", "admin.address must not be empty when the admin API is enabled")
	}
	if c.Admin.Enabled && c.Admin.Address == c.Broker.Address {
		return errors.Invalid("invalid_config", "broker.address and admin.address must differ")
	}
	if c.Session.MaxInflight <= 0 {
		return errors.Invalid("invalid_config", "session.max_inflight must be greater than 0")
	}
	if c.Session.MaxQueued < 0 {
		return errors.Invalid("invalid_config", "session.max_queued must not be negative")
	}
	if c.Limits.MaxPacketSize <= 0 {
		return errors.Invalid("invalid_config", "limits.max_packet_size must be greater than 0")
	}
	// A broker that rejects anonymous clients but has no credentials configured
	// would refuse every connection, which is almost certainly a mistake rather
	// than an intent.
	if !c.Auth.AllowAnonymous && len(c.Auth.Users) == 0 {
		return errors.Invalid(
			"invalid_config",
			"auth.allow_anonymous is false but no users are configured; the broker would refuse every client",
		)
	}
	return nil
}

// IsProduction reports whether the process runs in a production-like
// environment, which switches Gin out of debug mode.
func (c *AppConfig) IsProduction() bool {
	switch strings.ToLower(c.Env) {
	case "prod", "production", "stg", "staging":
		return true
	default:
		return false
	}
}

func envString(target *string, key string) {
	if v := os.Getenv(EnvPrefix + key); v != "" {
		*target = v
	}
}

func envInt(target *int, key string) {
	raw := os.Getenv(EnvPrefix + key)
	if raw == "" {
		return
	}
	if v, err := strconv.Atoi(raw); err == nil {
		*target = v
	}
}

func envBool(target *bool, key string) {
	raw := os.Getenv(EnvPrefix + key)
	if raw == "" {
		return
	}
	if v, err := strconv.ParseBool(raw); err == nil {
		*target = v
	}
}
