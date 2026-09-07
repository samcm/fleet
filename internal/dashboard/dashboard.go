// Package dashboard serves the fleet status wall: a static page for a screen
// on the wall, and the daemon's snapshot behind it, on a TCP address a browser
// can reach. The daemon itself only listens on a unix socket.
package dashboard

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"net"
	"net/http"
	"time"

	"github.com/samcm/fleet/internal/fleet"
)

//go:embed static
var static embed.FS

// Handler serves the page and answers /api/dashboard from the daemon.
func Handler(client *fleet.Client) http.Handler {
	site, err := fs.Sub(static, "static")
	if err != nil {
		panic(err) // the embedded tree is fixed at build time
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServerFS(site))
	mux.HandleFunc("GET /api/dashboard", func(w http.ResponseWriter, r *http.Request) {
		b, err := client.Dashboard(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(b)
	})

	return mux
}

// Serve answers on ln until ctx ends, then closes the listener.
func Serve(ctx context.Context, ln net.Listener, client *fleet.Client) error {
	srv := &http.Server{Handler: Handler(client), ReadHeaderTimeout: 10 * time.Second}

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = srv.Shutdown(shutdownCtx)
	}()

	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}
