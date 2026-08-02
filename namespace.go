package chronos

import (
	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
)

// Namespace scopes every Redis key chronos-go writes under a prefix, so two
// applications can share one Redis database without colliding.
//
// Sharing matters more than it looks: the scheduler leader lock is a single
// global key. Two deployments on one database contend for it, and the loser
// stops firing its schedules with no error logged anywhere. A namespace gives
// each deployment its own lock.
//
// Derive all four types from one Namespace. Configuring a prefix on some of
// them and not others is the failure this type exists to prevent — a Client
// writing under one prefix while a Server reads another produces tasks that are
// never processed and never reported.
//
//	ns := chronos.NewNamespace(rdb, "inspireme")
//	client := ns.NewClient()
//	srv := ns.NewServer(chronos.ServerConfig{Queues: map[string]int{"default": 1}})
//
// The four package-level constructors (NewClient, NewServer, NewScheduler,
// NewInspector) are equivalent to a namespace with the default prefix
// "chronos", which is the layout chronos-go has always written.
//
// Changing the prefix of a running deployment requires draining in-flight tasks
// first: a task's unique-lock key is stored in its body, so a task enqueued
// under one prefix cannot have its lock released under another.
type Namespace struct {
	client redis.UniversalClient
	keys   base.Keys
}

// NewNamespace returns a Namespace scoping all keys under prefix.
//
// It panics if the prefix is invalid — braces break the cluster hash tag and
// glob metacharacters break schedule scanning, both silently. See the package
// docs for the accepted form.
func NewNamespace(r redis.UniversalClient, prefix string) *Namespace {
	return &Namespace{client: r, keys: base.NewKeys(prefix)}
}

// Prefix returns the normalized prefix.
func (ns *Namespace) Prefix() string { return ns.keys.Prefix() }

// NewClient returns a Client scoped to this namespace.
func (ns *Namespace) NewClient() *Client {
	return &Client{rdb: rdb.NewRDB(ns.client, ns.keys)}
}

// NewInspector returns an Inspector scoped to this namespace.
func (ns *Namespace) NewInspector() *Inspector {
	return &Inspector{rdb: rdb.NewRDB(ns.client, ns.keys)}
}

// NewServer returns a Server scoped to this namespace.
func (ns *Namespace) NewServer(cfg ServerConfig) *Server {
	return newServer(ns.client, cfg, ns.keys)
}

// NewScheduler returns a Scheduler scoped to this namespace.
func (ns *Namespace) NewScheduler(cfg SchedulerConfig) *Scheduler {
	return newScheduler(ns.client, cfg, ns.keys)
}
