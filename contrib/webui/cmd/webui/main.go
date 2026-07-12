// Command webui runs the chronos-go task-management console.
//
//	go run ./cmd/webui --db 15
//
// It binds 127.0.0.1:8080 by default and opens your browser. To expose it
// remotely, set --addr 0.0.0.0:8080 and put it behind an authenticating
// reverse proxy — the console performs destructive actions (run/delete) and
// ships no authentication of its own.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
	"github.com/kenshin579/chronos-go/contrib/webui"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "Redis address")
	db := flag.Int("db", 0, "Redis logical database")
	noOpen := flag.Bool("no-open", false, "do not open a browser on start")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, DB: *db})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("cannot reach Redis at %s (db %d): %v", *redisAddr, *db, err)
	}
	defer rdb.Close()

	srv := &http.Server{Addr: *addr, Handler: webui.Handler(chronos.NewInspector(rdb))}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("cannot listen on %s: %v", *addr, err)
	}

	go func() {
		log.Printf("chronos-go console on http://%s (redis %s db %d)", *addr, *redisAddr, *db)
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	if !*noOpen {
		openBrowser("http://" + *addr)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// openBrowser best-effort opens url in the default browser; failures are logged.
func openBrowser(url string) {
	var cmd string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		cmd = "open"
	case "windows":
		cmd, args = "rundll32", []string{"url.dll,FileProtocolHandler"}
	default:
		cmd = "xdg-open"
	}
	if err := exec.Command(cmd, append(args, url)...).Start(); err != nil {
		log.Printf("could not open browser (%v); visit %s manually", err, url)
	}
}
