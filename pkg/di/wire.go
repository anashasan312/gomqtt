//go:build wireinject
// +build wireinject

// Package di is the composition root.
//
// It is the only package that knows which concrete type satisfies which
// interface. Every other package depends on interfaces alone, which is what
// makes dependency inversion real here rather than decorative: replacing the
// in-memory session store with a disk-backed one is a new package plus one
// binding in this file, and no service changes.
//
// The structure follows a consistent shape:
//
//	ProvideX — a Wire injector returning one concrete type
//	xSet     — a ProviderSet binding that concrete type to its interface
//
// Grouping a provider with its wire.Bind means a consumer asks for the
// interface and never has to know, or repeat, which implementation satisfies it.
//
// After editing this file run `make wire` to regenerate wire_gen.go.
package di

import (
	"github.com/google/wire"
	prom "github.com/prometheus/client_golang/prometheus"

	h "github.com/anashasan/gomqtt/pkg/api/handlers"
	adminApp "github.com/anashasan/gomqtt/pkg/application/admin"
	connApp "github.com/anashasan/gomqtt/pkg/application/connection"
	pubApp "github.com/anashasan/gomqtt/pkg/application/publishing"
	svc "github.com/anashasan/gomqtt/pkg/application/services"
	subApp "github.com/anashasan/gomqtt/pkg/application/subscribing"
	"github.com/anashasan/gomqtt/pkg/common/clock"
	"github.com/anashasan/gomqtt/pkg/common/logger"
	"github.com/anashasan/gomqtt/pkg/common/uid"
	"github.com/anashasan/gomqtt/pkg/domain/metrics"
	iPersist "github.com/anashasan/gomqtt/pkg/domain/persistence"
	"github.com/anashasan/gomqtt/pkg/infrastructure/config"
	promInfra "github.com/anashasan/gomqtt/pkg/infrastructure/metrics/prometheus"
	"github.com/anashasan/gomqtt/pkg/infrastructure/persistence/memory"
	"github.com/anashasan/gomqtt/pkg/infrastructure/transport"
)

// region — shared kernel

// ProvideClock provides the wall clock.
func ProvideClock() clock.Clock {
	wire.Build(clockSet)
	return nil
}

var clockSet = wire.NewSet(
	clock.NewSystemClock,
	wire.Bind(new(clock.Clock), new(*clock.SystemClock)),
)

// ProvideUIDGenerator provides the identifier generator.
func ProvideUIDGenerator() uid.Generator {
	wire.Build(uidSet)
	return nil
}

var uidSet = wire.NewSet(
	uid.NewRandomGenerator,
	wire.Bind(new(uid.Generator), new(*uid.RandomGenerator)),
)

// ProvideLogger provides the structured logger.
func ProvideLogger(cfg *config.AppConfig) logger.Logger {
	wire.Build(loggerSet)
	return nil
}

var loggerSet = wire.NewSet(
	config.GetLogLevel,
	provideSlogLogger,
	wire.Bind(new(logger.Logger), new(*logger.SlogLogger)),
)

// endregion

// region — metrics

// ProvideMetricsRegistry provides the Prometheus registry.
func ProvideMetricsRegistry() *prom.Registry {
	wire.Build(promInfra.NewRegistry)
	return nil
}

// ProvideMetricsRecorder provides the metrics port implementation.
func ProvideMetricsRecorder(registry *prom.Registry) metrics.Recorder {
	wire.Build(metricsSet)
	return nil
}

// metricsSet binds the recorder and narrows *prom.Registry to the
// prom.Registerer the recorder actually needs — it only registers collectors,
// so it should not be handed a type that can also gather and reset them.
var metricsSet = wire.NewSet(
	provideRegisterer,
	promInfra.NewRecorder,
	wire.Bind(new(metrics.Recorder), new(*promInfra.Recorder)),
)

// endregion

// region — persistence

var sessionStoreSet = wire.NewSet(
	memory.NewSessionStore,
	wire.Bind(new(iPersist.ISessionStore), new(*memory.SessionStore)),
)

var clientRegistrySet = wire.NewSet(
	memory.NewClientRegistry,
	wire.Bind(new(iPersist.IClientRegistry), new(*memory.ClientRegistry)),
)

var retainedStoreSet = wire.NewSet(
	memory.NewRetainedStore,
	wire.Bind(new(iPersist.IRetainedStore), new(*memory.RetainedStore)),
)

var subscriptionIndexSet = wire.NewSet(
	memory.NewSubscriptionIndex,
	wire.Bind(new(iPersist.ISubscriptionIndex), new(*memory.SubscriptionIndex)),
)

// endregion

// region — application services

var publishingSet = wire.NewSet(
	config.GetPublishingConfig,
	pubApp.NewPublishingService,
	wire.Bind(new(svc.IPublishingService), new(*pubApp.PublishingService)),
	// The admin API wants only three counters, so it binds to the narrow
	// CounterSource rather than to the whole publishing service.
	wire.Bind(new(adminApp.CounterSource), new(*pubApp.PublishingService)),
)

var connectionSet = wire.NewSet(
	config.GetConnectionConfig,
	config.GetAuthConfig,
	provideAuthenticator,
	connApp.NewConnectionService,
	wire.Bind(new(svc.IConnectionService), new(*connApp.ConnectionService)),
)

var subscribingSet = wire.NewSet(
	subApp.NewSubscriptionService,
	wire.Bind(new(svc.ISubscriptionService), new(*subApp.SubscriptionService)),
)

var adminSet = wire.NewSet(
	adminApp.NewAdminService,
	wire.Bind(new(svc.IAdminService), new(*adminApp.AdminService)),
)

// brokerSet is every store and service the broker needs, in one set.
//
// The stores are listed here rather than in each injector because they must be
// *singletons* across the whole graph: the listener and the admin API have to
// see the same subscription index, or the admin API would report an empty
// broker. Wire builds one instance per set per injector, so grouping them is
// what guarantees the sharing.
var brokerSet = wire.NewSet(
	sessionStoreSet,
	clientRegistrySet,
	retainedStoreSet,
	subscriptionIndexSet,

	publishingSet,
	connectionSet,
	subscribingSet,
	adminSet,

	clockSet,
	uidSet,
	loggerSet,
)

// endregion

// region — injectors

// Broker is everything a running broker is made of.
//
// Returned as one struct so the whole graph is built by a single injector call
// and the components that must share a store genuinely do.
type Broker struct {
	Listener   *transport.Listener
	Handlers   *h.Handlers
	Publishing *pubApp.PublishingService
	Sessions   iPersist.ISessionStore
	Index      iPersist.ISubscriptionIndex
	Clients    iPersist.IClientRegistry
	Retained   iPersist.IRetainedStore
	Log        logger.Logger
}

// newBroker assembles the Broker struct. Wire calls it with the graph it built.
func newBroker(
	listener *transport.Listener,
	handlers *h.Handlers,
	publishing *pubApp.PublishingService,
	sessions iPersist.ISessionStore,
	index iPersist.ISubscriptionIndex,
	clients iPersist.IClientRegistry,
	retained iPersist.IRetainedStore,
	log logger.Logger,
) *Broker {
	return &Broker{
		Listener:   listener,
		Handlers:   handlers,
		Publishing: publishing,
		Sessions:   sessions,
		Index:      index,
		Clients:    clients,
		Retained:   retained,
		Log:        log,
	}
}

// InjectBroker builds the complete broker graph.
func InjectBroker(cfg *config.AppConfig, recorder metrics.Recorder) *Broker {
	wire.Build(
		newBroker,

		transport.NewListener,
		config.GetListenerConfig,
		provideHandlers,

		h.NewHandlers,
		h.NewAdminHandler,
		provideSupportHandler,
		config.GetBrokerVersion,

		brokerSet,
	)
	return nil
}

// endregion
