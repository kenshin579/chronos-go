// Command webui runs the chronos-go task-management console.
//
//	go run ./cmd/webui --db 15                                   # standalone (default)
//	go run ./cmd/webui --cluster --redis n1:7000,n2:7001         # Redis Cluster
//
// It binds 127.0.0.1:8080 by default and opens your browser. To expose it
// remotely, set --addr 0.0.0.0:8080 and put it behind an authenticating
// reverse proxy — the console performs destructive actions (run/delete) and
// ships no authentication of its own.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
	"github.com/kenshin579/chronos-go/contrib/webui"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	standalone := flag.Bool("standalone", false, "connect to a standalone Redis (default)")
	cluster := flag.Bool("cluster", false, "connect to a Redis Cluster (--redis takes comma-separated seed nodes)")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "Redis address (comma-separated for --cluster)")
	db := flag.Int("db", 0, "Redis logical database (standalone only; cluster has only DB 0)")
	noOpen := flag.Bool("no-open", false, "do not open a browser on start")
	flag.Parse()

	rdb, err := buildClient(*standalone, *cluster, *redisAddr, *db)
	if err != nil {
		log.Fatalf("webui: %v", err)
	}
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("cannot reach Redis at %s: %v", *redisAddr, err)
	}
	defer rdb.Close()

	conn := fmt.Sprintf("standalone %s db%d", *redisAddr, *db)
	if *cluster {
		conn = fmt.Sprintf("cluster (%d seed)", len(splitAddrs(*redisAddr)))
	}
	srv := &http.Server{Addr: *addr, Handler: webui.Handler(chronos.NewInspector(rdb), webui.WithConnInfo(conn))}

	ln, lerr := net.Listen("tcp", *addr)
	if lerr != nil {
		log.Fatalf("cannot listen on %s: %v", *addr, lerr)
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

// buildClient creates the Redis client for the chosen mode. Mirrors
// cmd/chronos's buildClient — the two commands live in different Go modules,
// so the few lines are duplicated rather than shared.
func buildClient(standalone, cluster bool, addr string, db int) (redis.UniversalClient, error) {
	if standalone && cluster {
		return nil, errors.New("--standalone and --cluster are mutually exclusive")
	}
	if cluster {
		if db != 0 {
			return nil, errors.New("--db is not supported with --cluster: Redis Cluster has only database 0")
		}
		addrs := splitAddrs(addr)
		if len(addrs) == 0 {
			return nil, errors.New("--cluster requires at least one seed address in --redis")
		}
		return redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs}), nil
	}
	return redis.NewClient(&redis.Options{Addr: addr, DB: db}), nil
}

// splitAddrs splits a comma-separated address list, trimming whitespace and
// dropping empty entries.
func splitAddrs(s string) []string {
	parts := strings.Split(s, ",")
	addrs := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			addrs = append(addrs, p)
		}
	}
	return addrs
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
