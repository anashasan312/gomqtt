// Package connection implements the client lifecycle: accepting a CONNECT,
// resolving the session it refers to, and tearing the client down.
package connection

import (
	"context"
	"time"

	"github.com/anashasan/gomqtt/pkg/application/services"
	"github.com/anashasan/gomqtt/pkg/common/clock"
	"github.com/anashasan/gomqtt/pkg/common/errors"
	"github.com/anashasan/gomqtt/pkg/common/logger"
	"github.com/anashasan/gomqtt/pkg/common/uid"
	clientAgg "github.com/anashasan/gomqtt/pkg/domain/client_aggregate"
	clientVO "github.com/anashasan/gomqtt/pkg/domain/client_aggregate/value_objects"
	msgAgg "github.com/anashasan/gomqtt/pkg/domain/message_aggregate"
	msgVO "github.com/anashasan/gomqtt/pkg/domain/message_aggregate/value_objects"
	"github.com/anashasan/gomqtt/pkg/domain/metrics"
	"github.com/anashasan/gomqtt/pkg/domain/persistence"
	sessionAgg "github.com/anashasan/gomqtt/pkg/domain/session_aggregate"
	"github.com/anashasan/gomqtt/pkg/infrastructure/mqtt/packet"
)

var _ services.IConnectionService = (*ConnectionService)(nil)

// Authenticator decides whether a client may connect.
//
// A port rather than a concrete check, so a deployment can bind a password
// file, an LDAP lookup or a JWT verifier without any of this package changing.
// The default binding allows everyone, which is what an MQTT broker on a
// trusted network normally wants and is stated plainly in the config rather
// than hidden.
type Authenticator interface {
	// Authenticate reports whether the credentials are acceptable. It returns
	// the CONNACK return code to send when they are not.
	Authenticate(ctx context.Context, clientID clientVO.ClientID, username string, password []byte) (bool, packet.ConnectReturnCode)
}

// AllowAllAuthenticator accepts every client.
type AllowAllAuthenticator struct{}

// NewAllowAllAuthenticator builds the permissive authenticator.
func NewAllowAllAuthenticator() *AllowAllAuthenticator { return &AllowAllAuthenticator{} }

// Authenticate accepts unconditionally.
func (AllowAllAuthenticator) Authenticate(
	context.Context, clientVO.ClientID, string, []byte,
) (bool, packet.ConnectReturnCode) {
	return true, packet.ConnectAccepted
}

// Config tunes connection handling.
type Config struct {
	// SessionLimits bounds each session's memory.
	SessionLimits sessionAgg.Limits
	// MaxKeepAlive caps what a client may request. Zero means no cap.
	//
	// A client asking for a 65535-second keep-alive is asking the broker to hold
	// a dead connection's memory for eighteen hours, so a deployment can pull
	// that ceiling down.
	MaxKeepAlive uint16
	// AllowAnonymous permits a CONNECT with no username.
	AllowAnonymous bool
	// MaxPayloadBytes caps a will payload.
	MaxPayloadBytes int
}

// ConnectionService accepts and tears down clients.
type ConnectionService struct {
	sessions   persistence.ISessionStore
	clients    persistence.IClientRegistry
	index      persistence.ISubscriptionIndex
	publishing services.IPublishingService
	auth       Authenticator
	ids        uid.Generator
	clock      clock.Clock
	metrics    metrics.Recorder
	log        logger.Logger
	cfg        Config
}

// NewConnectionService builds a ConnectionService.
func NewConnectionService(
	sessions persistence.ISessionStore,
	clients persistence.IClientRegistry,
	index persistence.ISubscriptionIndex,
	publishing services.IPublishingService,
	auth Authenticator,
	ids uid.Generator,
	clk clock.Clock,
	recorder metrics.Recorder,
	log logger.Logger,
	cfg Config,
) *ConnectionService {
	return &ConnectionService{
		sessions:   sessions,
		clients:    clients,
		index:      index,
		publishing: publishing,
		auth:       auth,
		ids:        ids,
		clock:      clk,
		metrics:    recorder,
		log:        log,
		cfg:        cfg,
	}
}

// Connect validates a CONNECT and accepts or refuses the client.
//
// A refusal returns a ConnectResult carrying the return code rather than an
// error. §3.1.4 requires the broker to send a CONNACK explaining the refusal
// and *then* close, so "rejected" is a normal outcome this method must be able
// to express — a bare error would tempt the caller into closing the socket
// silently, which leaves the client with no idea why.
func (s *ConnectionService) Connect(
	ctx context.Context,
	req *packet.Connect,
	remoteAddr string,
	sink services.PacketSink,
) (services.ConnectResult, error) {
	// §3.1.2.1–2: protocol first, before any other field is trusted.
	protocol, err := clientVO.NewProtocolVersion(req.ProtocolName, req.ProtocolLevel)
	if err != nil {
		s.metrics.RecordConnectionRejected("unsupported_protocol_version")
		return services.ConnectResult{ReturnCode: packet.ConnectUnacceptableProtocolVer}, nil
	}

	clientID, assigned, err := s.resolveClientID(req)
	if err != nil {
		s.metrics.RecordConnectionRejected("identifier_rejected")
		return services.ConnectResult{ReturnCode: packet.ConnectIdentifierRejected}, nil
	}

	if !s.cfg.AllowAnonymous && !req.HasUsername {
		s.metrics.RecordConnectionRejected("anonymous_not_allowed")
		return services.ConnectResult{ReturnCode: packet.ConnectNotAuthorized}, nil
	}

	authorised, returnCode := s.auth.Authenticate(ctx, clientID, req.Username, req.Password)
	if !authorised {
		s.metrics.RecordConnectionRejected("not_authorized")
		return services.ConnectResult{ReturnCode: returnCode}, nil
	}

	will, err := s.buildWill(req)
	if err != nil {
		// A malformed will topic is the client's error, and §3.1.4 has no
		// return code for it, so the connection is refused as identifier
		// rejected rather than accepted with a will that can never be published.
		s.metrics.RecordConnectionRejected("invalid_will_topic")
		return services.ConnectResult{ReturnCode: packet.ConnectIdentifierRejected}, nil
	}

	keepAlive := clientVO.NewKeepAlive(s.capKeepAlive(req.KeepAlive))
	now := s.clock.Now()

	client, err := clientAgg.NewClient(clientAgg.NewClientParams{
		ID:           clientID,
		Protocol:     protocol,
		KeepAlive:    keepAlive,
		CleanSession: req.CleanSession,
		Will:         will,
		Username:     req.Username,
		RemoteAddr:   remoteAddr,
		AssignedID:   assigned,
		Now:          now,
	})
	if err != nil {
		return services.ConnectResult{ReturnCode: packet.ConnectIdentifierRejected}, nil
	}

	sessionPresent := s.resolveSession(clientID, req.CleanSession, now)

	// §3.1.4: a second connection with the same identifier takes over, and the
	// first is disconnected. Registering and displacing happen inside the
	// registry under one lock, so there is no window in which both are live.
	displaced, err := s.clients.Register(client)
	if err != nil {
		return services.ConnectResult{}, err
	}

	// Registering the new sink returns the one it displaced, which the caller
	// closes. Doing the swap here rather than looking the old sink up
	// separately is what keeps the takeover atomic.
	displacedSink := s.publishing.RegisterSink(clientID, sink)

	if displaced != nil {
		s.log.Warn(ctx, "client identifier taken over by a new connection",
			logger.F("client_id", clientID.String()),
			logger.F("previous_addr", displaced.RemoteAddr()),
			logger.F("new_addr", remoteAddr),
		)
	}
	s.metrics.RecordConnection()
	s.metrics.SetConnectedClients(s.clients.Count())
	s.metrics.SetSessionCount(s.sessions.Count())

	s.log.Info(ctx, "client connected",
		logger.F("client_id", clientID.String()),
		logger.F("remote_addr", remoteAddr),
		logger.F("clean_session", req.CleanSession),
		logger.F("keep_alive", keepAlive.Seconds()),
		logger.F("session_present", sessionPresent),
	)

	return services.ConnectResult{
		Client:         client,
		ReturnCode:     packet.ConnectAccepted,
		SessionPresent: sessionPresent,
		Displaced:      displacedSink,
	}, nil
}

// resolveClientID applies §3.1.3.1.
//
// An empty identifier is legal only with clean session 1, in which case the
// broker assigns one. With clean session 0 it must be refused: the identifier
// is the key the session is stored under, so a persistent session with no
// identifier could never be found again.
func (s *ConnectionService) resolveClientID(req *packet.Connect) (clientVO.ClientID, bool, error) {
	if req.ClientID != "" {
		id, err := clientVO.NewClientID(req.ClientID)
		return id, false, err
	}

	if !req.CleanSession {
		return "", false, errors.Invalid(
			"identifier_rejected",
			"an empty client id requires clean session 1",
		)
	}

	id, err := clientVO.NewClientID("auto-" + s.ids.New())
	return id, true, err
}

// resolveSession applies §3.1.2.4 and reports the CONNACK session-present flag.
func (s *ConnectionService) resolveSession(
	clientID clientVO.ClientID,
	cleanSession bool,
	now time.Time,
) bool {
	existing, found := s.sessions.Get(clientID)

	if cleanSession {
		// Clean session discards whatever was stored, including the
		// subscriptions held in the index — which the session itself does not
		// own, so they must be cleared explicitly or the client would keep
		// receiving messages for filters it no longer holds.
		if found {
			s.index.UnsubscribeAll(clientID)
			s.sessions.Delete(clientID)
		}
		s.sessions.Put(sessionAgg.NewSession(clientID, true, s.cfg.SessionLimits, now))
		return false
	}

	if !found {
		s.sessions.Put(sessionAgg.NewSession(clientID, false, s.cfg.SessionLimits, now))
		return false
	}

	// A stored session created with clean session 1 is not resumable: the
	// client asked last time for it to be thrown away.
	if existing.IsClean() {
		s.index.UnsubscribeAll(clientID)
		s.sessions.Delete(clientID)
		s.sessions.Put(sessionAgg.NewSession(clientID, false, s.cfg.SessionLimits, now))
		return false
	}

	_ = s.sessions.Update(clientID, func(session *sessionAgg.Session) error {
		session.Resume(now)
		return nil
	})
	return true
}

// capKeepAlive applies the configured ceiling.
func (s *ConnectionService) capKeepAlive(requested uint16) uint16 {
	if s.cfg.MaxKeepAlive == 0 || requested == 0 {
		return requested
	}
	if requested > s.cfg.MaxKeepAlive {
		return s.cfg.MaxKeepAlive
	}
	return requested
}

// buildWill turns the CONNECT's will fields into a domain Message.
func (s *ConnectionService) buildWill(req *packet.Connect) (*msgAgg.Message, error) {
	if req.Will == nil {
		return nil, nil
	}

	topic, err := msgVO.NewTopicName(req.Will.Topic)
	if err != nil {
		return nil, err
	}
	qos, err := msgVO.NewQoS(req.Will.QoS)
	if err != nil {
		return nil, err
	}

	// The will is built at QoS 0 with no packet identifier and re-levelled at
	// publication time: it is stored for the life of the connection and cannot
	// hold an identifier allocated now, which would be stale by the time it is
	// used.
	return msgAgg.NewMessage(msgAgg.NewMessageParams{
		Topic:           topic,
		Payload:         req.Will.Payload,
		QoS:             qos.Downgrade(msgVO.MaxSupportedQoS),
		Retain:          req.Will.Retain,
		Now:             s.clock.Now(),
		MaxPayloadBytes: s.cfg.MaxPayloadBytes,
	})
}

// Disconnect tears a client down.
//
// The order matters: the will is published while the client's subscriptions
// still exist, so a client subscribed to its own will topic — a real pattern
// for peer monitoring — still receives it.
func (s *ConnectionService) Disconnect(
	ctx context.Context,
	client *clientAgg.Client,
	reason clientAgg.DisconnectReason,
) error {
	publishWill := client.Disconnect(reason)
	clientID := client.ID()

	// Unregister is identity-checked, and its answer decides everything below.
	//
	// When a client reconnects, the old connection's teardown races the new
	// connection's setup. Both refer to the same client identifier, and session
	// state is keyed by that identifier — so a teardown that cleaned up
	// unconditionally would delete the session and the subscriptions the new
	// connection had just installed, and the reconnected client would sit there
	// subscribed to nothing, receiving nothing, with no error anywhere.
	//
	// A false answer means this connection was already superseded, so its
	// session state is not its to tear down.
	stillCurrent := s.clients.Unregister(clientID, client)

	if publishWill {
		// Published even when superseded: a subscriber watching a device's
		// status topic should learn that *that connection* went away, and this
		// matches what Mosquitto does on takeover.
		if err := s.publishWill(ctx, client); err != nil {
			s.log.Error(ctx, "failed to publish will message", err,
				logger.F("client_id", clientID.String()))
		}
	}

	if !stillCurrent {
		s.metrics.RecordDisconnection(reason.String())
		s.log.Info(ctx, "superseded connection closed; its session now belongs to the newer connection",
			logger.F("client_id", clientID.String()),
			logger.F("reason", reason.String()),
		)
		return nil
	}

	// §3.1.2.4: a clean session is destroyed on disconnect; a persistent one is
	// kept, with its subscriptions and its queued messages, for the client to
	// resume.
	if client.CleanSession() {
		s.index.UnsubscribeAll(clientID)
		s.sessions.Delete(clientID)
	} else {
		_ = s.sessions.Update(clientID, func(session *sessionAgg.Session) error {
			session.Disconnect(s.clock.Now())
			return nil
		})
	}

	s.metrics.RecordDisconnection(reason.String())
	s.metrics.SetConnectedClients(s.clients.Count())
	s.metrics.SetSessionCount(s.sessions.Count())
	s.metrics.SetSubscriptionCount(s.index.SubscriptionCount())

	s.log.Info(ctx, "client disconnected",
		logger.F("client_id", clientID.String()),
		logger.F("reason", reason.String()),
		logger.F("clean_session", client.CleanSession()),
	)
	return nil
}

// publishWill delivers the client's last-will message (§3.1.2.5).
func (s *ConnectionService) publishWill(ctx context.Context, client *clientAgg.Client) error {
	will := client.Will()
	if will == nil {
		return nil
	}

	s.log.Info(ctx, "publishing will message",
		logger.F("client_id", client.ID().String()),
		logger.F("topic", will.Topic().String()),
	)
	return s.publishing.Publish(ctx, will)
}
