package config

import (
	"github.com/anashasan/gomqtt/pkg/application/connection"
	"github.com/anashasan/gomqtt/pkg/application/publishing"
	sessionAgg "github.com/anashasan/gomqtt/pkg/domain/session_aggregate"
	"github.com/anashasan/gomqtt/pkg/infrastructure/transport"
)

// This file holds the narrow accessors Wire uses to hand each component exactly
// the slice of configuration it needs.
//
// Passing *AppConfig everywhere would be simpler to write and much worse to
// live with: every constructor would depend on the whole document, a change to
// an unrelated section would recompile and re-test half the project, and a unit
// test for the publishing service would have to build a listener config to
// construct one. This is interface segregation applied to configuration.

// Named string types for the scalar settings.
//
// Wire resolves providers by type, so two accessors that both returned a plain
// string would be indistinguishable to it and the graph would fail to build.
// Naming each one also makes call sites self-documenting.
type (
	// LogLevel is the minimum level the logger emits.
	LogLevel string
	// BrokerVersion is the build version reported by the health endpoint.
	BrokerVersion string
	// AdminAddress is the HTTP admin bind address.
	AdminAddress string
)

// GetLogLevel extracts the logger level.
func GetLogLevel(cfg *AppConfig) LogLevel { return LogLevel(cfg.Logger.Level) }

// GetBrokerVersion extracts the version for the health endpoint.
func GetBrokerVersion(cfg *AppConfig) BrokerVersion { return BrokerVersion(cfg.Broker.Version) }

// GetAdminAddress extracts the admin listener address.
func GetAdminAddress(cfg *AppConfig) AdminAddress { return AdminAddress(cfg.Admin.Address) }

// GetListenerConfig extracts the TCP listener settings.
func GetListenerConfig(cfg *AppConfig) transport.ListenerConfig {
	return transport.ListenerConfig{
		Address:         cfg.Broker.Address,
		MaxConnections:  cfg.Broker.MaxConnections,
		ShutdownTimeout: cfg.Broker.ShutdownTimeout,
		Connection: transport.ConnectionConfig{
			OutboundQueueSize: cfg.Broker.OutboundQueueSize,
			ReadBufferSize:    cfg.Broker.ReadBufferSize,
			WriteTimeout:      cfg.Broker.WriteTimeout,
			MaxPacketSize:     cfg.Limits.MaxPacketSize,
			ConnectTimeout:    cfg.Broker.ConnectTimeout,
			MaxPayloadBytes:   cfg.Limits.MaxPayloadSize,
		},
	}
}

// GetConnectionConfig extracts the connection service settings.
func GetConnectionConfig(cfg *AppConfig) connection.Config {
	return connection.Config{
		SessionLimits: sessionAgg.Limits{
			MaxInflight:      cfg.Session.MaxInflight,
			MaxQueued:        cfg.Session.MaxQueued,
			MaxSubscriptions: cfg.Session.MaxSubscriptions,
		},
		MaxKeepAlive:    cfg.Limits.MaxKeepAlive,
		AllowAnonymous:  cfg.Auth.AllowAnonymous,
		MaxPayloadBytes: cfg.Limits.MaxPayloadSize,
	}
}

// GetPublishingConfig extracts the delivery settings.
func GetPublishingConfig(cfg *AppConfig) publishing.Config {
	return publishing.Config{ResumeBatchSize: cfg.Session.ResumeBatchSize}
}

// GetAuthConfig extracts the authentication settings.
func GetAuthConfig(cfg *AppConfig) AuthConfig { return cfg.Auth }
