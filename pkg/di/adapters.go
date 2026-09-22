package di

import (
	prom "github.com/prometheus/client_golang/prometheus"

	h "github.com/anashasan/gomqtt/pkg/api/handlers"
	connApp "github.com/anashasan/gomqtt/pkg/application/connection"
	svc "github.com/anashasan/gomqtt/pkg/application/services"
	"github.com/anashasan/gomqtt/pkg/common/logger"
	"github.com/anashasan/gomqtt/pkg/infrastructure/config"
	"github.com/anashasan/gomqtt/pkg/infrastructure/transport"
)

// This file holds the thin adapters that let Wire connect two components whose
// signatures are correct on their own but do not line up by type.
//
// They live in di rather than being worked around by loosening a constructor's
// parameter types, because the alternative — making every constructor take a
// plain string so the container can wire it — would push a container concern
// into code that has nothing to do with containers.

// provideRegisterer narrows the concrete registry to the interface the metrics
// recorder actually needs.
func provideRegisterer(registry *prom.Registry) prom.Registerer { return registry }

// provideSlogLogger adapts the named LogLevel to the logger's plain string
// parameter, keeping the logger package free of any config import.
func provideSlogLogger(level config.LogLevel) *logger.SlogLogger {
	return logger.NewSlogLogger(string(level))
}

// provideSupportHandler adapts the named BrokerVersion to the support handler.
func provideSupportHandler(version config.BrokerVersion) *h.SupportHandler {
	return h.NewSupportHandler(string(version))
}

// provideHandlers groups the three services a connection drives.
//
// The transport takes them as one struct rather than three parameters, so
// adding a fourth service later is a field rather than a change to every
// constructor between here and the connection.
func provideHandlers(
	connection svc.IConnectionService,
	subscription svc.ISubscriptionService,
	publishing svc.IPublishingService,
) transport.Handlers {
	return transport.Handlers{
		Connection:   connection,
		Subscription: subscription,
		Publishing:   publishing,
	}
}

// provideAuthenticator selects the authentication strategy from configuration.
//
// This is where the Authenticator port pays off: adding an LDAP or JWT
// authenticator means a new type and one more case here, and neither the
// connection service nor anything above it changes.
func provideAuthenticator(cfg config.AuthConfig) connApp.Authenticator {
	if len(cfg.Users) == 0 && cfg.AllowAnonymous {
		return connApp.NewAllowAllAuthenticator()
	}
	return connApp.NewStaticAuthenticator(cfg.Users, cfg.AllowAnonymous)
}
