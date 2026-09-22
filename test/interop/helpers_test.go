//go:build interop

package interop

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anashasan/gomqtt/pkg/infrastructure/transport"
)

// netListen binds an ephemeral port so the caller can learn a free port number.
func netListen() (net.Listener, error) {
	return net.Listen("tcp", "127.0.0.1:0")
}

// splitHostPort splits an address.
func splitHostPort(addr string) (string, string, error) {
	return net.SplitHostPort(addr)
}

// mustAddr takes the address from startGoMQTT's two return values.
func mustAddr(addr string, _ *transport.Listener) string { return addr }

// mustSplitPort extracts the port from a host:port address.
func mustSplitPort(t *testing.T, addr string) string {
	t.Helper()

	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	return port
}

// waitForPort blocks until something is accepting on addr.
//
// Necessary because an external process takes an unpredictable moment to bind,
// and a fixed sleep would be either slow or flaky depending on the machine.
func waitForPort(t *testing.T, addr string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("nothing is listening on %s", addr)
}
