package soak

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kenshin579/chronos-go"
)

// ---- 태스크 인자 (실패 여부는 payload로 결정적 제어) ----

type taskArgs struct {
	Seq      int  `json:"seq"`
	FailOnce bool `json:"fail_once"` // 첫 시도만 실패 → 재시도에서 성공
	Fail     bool `json:"fail"`      // 항상 실패
}

func (taskArgs) Kind() string { return "soak:task" }

type chainArgs struct {
	Seq  int `json:"seq"`
	Link int `json:"link"`
}

func (chainArgs) Kind() string { return "soak:chain" }

type groupArgs struct {
	Seq    int `json:"seq"`
	Member int `json:"member"`
}

func (groupArgs) Kind() string { return "soak:group" }

type cbArgs struct {
	Seq int `json:"seq"`
}

func (cbArgs) Kind() string { return "soak:cb" }

type schedArgs struct{}

func (schedArgs) Kind() string { return "soak:sched" }

type uniqueArgs struct {
	Batch int `json:"batch"`
}

func (uniqueArgs) Kind() string { return "soak:unique" }

// ---- 혼합 비율 (결정적 — 난수 없음) ----

type variant int

const (
	varNormal         variant = iota
	varNormalDelayed          // 성공 + 5~15s 지연
	varNormalRetained         // 성공 + WithRetention(30s)
	varFailOnce               // 1회 실패 후 성공 (retry ZSET 순환)
	varDeadLetter             // 항상 실패, MaxRetry=1 → archived
	varDiscard                // 항상 실패 + discard (즉시 삭제)
)

// pickVariant maps a sequence number onto the spec's mix:
// fail-once 10%, dead-letter 5%, discard 5%, delayed 10%p,
// retained 16%p (~20% of the success share), plain success the rest.
func pickVariant(seq int) variant {
	switch m := seq % 100; {
	case m < 10:
		return varFailOnce
	case m < 15:
		return varDeadLetter
	case m < 20:
		return varDiscard
	case m < 30:
		return varNormalDelayed
	case m < 46:
		return varNormalRetained
	default:
		return varNormal
	}
}

// pickQueue splits 3:1 to match the server's queue weights.
func pickQueue(seq int) string {
	if seq%4 < 3 {
		return "soak-a"
	}
	return "soak-b"
}

// ---- 워크로드 ----

// Config parameterizes the workload. Rate is the base tasks/sec fed through
// pickVariant; chains/groups/unique batches ride on fixed 10s tickers.
type Config struct {
	Rate int
}

// Workload owns the mux (handlers), the load generators and the completion
// counter the Sampler reads.
type Workload struct {
	cfg       Config
	client    *chronos.Client
	insp      *chronos.Inspector
	completed atomic.Int64
	enqueued  atomic.Int64 // baseLoad enqueue 성공 수 (유효 rate 진단용)
	seenOnce  sync.Map     // task ID → struct{}{} (fail-once 1회 실패 판정)
	// 참고: recoverer 오회수로 fail-once가 이중 전달되면 completed가 논리 태스크
	// 1개당 2회 집계될 수 있다(드묾, 맵은 결국 비워져 누수 없음). Throughput은
	// 진단용이라 판정에는 영향 없다.
}

func NewWorkload(client *chronos.Client, insp *chronos.Inspector, cfg Config) *Workload {
	if cfg.Rate < 1 {
		cfg.Rate = 1 // 0 나눗셈·NewTicker 패닉 방지 (최소 1 task/s)
	}
	return &Workload{cfg: cfg, client: client, insp: insp}
}

// Completed returns the shared completion counter (for the Sampler).
func (w *Workload) Completed() *atomic.Int64 { return &w.completed }

// Enqueued reports how many baseLoad tasks were actually accepted — compared
// against the nominal rate x duration it shows whether enqueue kept up.
// Ticker specials (unique/chain/group) are excluded.
func (w *Workload) Enqueued() int64 { return w.enqueued.Load() }

var errSoakFail = errors.New("soak: deliberate failure")

// Mux registers every handler kind. work simulates 1~5ms of effort.
func (w *Workload) Mux() *chronos.Mux {
	mux := chronos.NewMux()
	work := func(seq int) {
		time.Sleep(time.Duration(1+seq%5) * time.Millisecond)
	}
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[taskArgs]) error {
		work(t.Args.Seq)
		if t.Args.Fail {
			return errSoakFail
		}
		if t.Args.FailOnce {
			if _, seen := w.seenOnce.LoadOrStore(t.ID(), struct{}{}); !seen {
				return errSoakFail // 첫 시도만 실패
			}
			w.seenOnce.Delete(t.ID()) // 재시도 성공 — 맵 누적 방지
		}
		w.completed.Add(1)
		return nil
	})
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[chainArgs]) error {
		work(t.Args.Seq)
		w.completed.Add(1)
		return nil
	})
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[groupArgs]) error {
		work(t.Args.Seq)
		w.completed.Add(1)
		return nil
	})
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[cbArgs]) error {
		w.completed.Add(1)
		return nil
	})
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[schedArgs]) error {
		w.completed.Add(1)
		return nil
	})
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[uniqueArgs]) error {
		work(t.Args.Batch)
		w.completed.Add(1)
		return nil
	})
	return mux
}

// Run blocks generating load until ctx is done. Enqueue errors are logged and
// counted, never fatal — a soak must survive transient hiccups.
func (w *Workload) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); w.baseLoad(ctx) }()
	go func() { defer wg.Done(); w.tickers(ctx) }()
	go func() { defer wg.Done(); w.pauseToggler(ctx) }()
	wg.Wait()
}

// baseLoad enqueues cfg.Rate tasks/sec through the variant mix.
func (w *Workload) baseLoad(ctx context.Context) {
	interval := time.Second / time.Duration(w.cfg.Rate)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	seq := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			w.enqueueOne(ctx, seq)
			seq++
		}
	}
}

func (w *Workload) enqueueOne(ctx context.Context, seq int) {
	args := taskArgs{Seq: seq}
	opts := []chronos.Option{chronos.WithQueue(pickQueue(seq))}
	switch pickVariant(seq) {
	case varFailOnce:
		args.FailOnce = true
	case varDeadLetter:
		args.Fail = true
		opts = append(opts, chronos.WithMaxRetry(1))
	case varDiscard:
		args.Fail = true
		opts = append(opts, chronos.WithMaxRetry(0), chronos.WithDeadLetterDiscard())
	case varNormalDelayed:
		opts = append(opts, chronos.WithProcessIn(time.Duration(5+seq%11)*time.Second))
	case varNormalRetained:
		opts = append(opts, chronos.WithRetention(30*time.Second))
	}
	if _, err := chronos.Enqueue(ctx, w.client, args, opts...); err != nil {
		if ctx.Err() == nil {
			log.Printf("soak: enqueue seq=%d: %v", seq, err)
		}
		return
	}
	w.enqueued.Add(1)
}

// tickers drives the 10s specials: unique batch, chain, group.
func (w *Workload) tickers(ctx context.Context) {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	batch := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			// unique: 동일 payload 5개 → 1개만 수락 (4개 ErrDuplicateTask).
			for i := 0; i < 5; i++ {
				_, err := chronos.Enqueue(ctx, w.client, uniqueArgs{Batch: batch},
					chronos.WithQueue("soak-a"), chronos.WithUnique(30*time.Second))
				if err != nil && !errors.Is(err, chronos.ErrDuplicateTask) && ctx.Err() == nil {
					log.Printf("soak: unique enqueue: %v", err)
				}
			}
			// chain 3링크 → 팬아웃→팬인→후속 워크플로(단일+그룹+단일 스테이지).
			wf := chronos.NewChain().
				Then(chainArgs{Seq: batch, Link: 0}, chronos.WithQueue("soak-a")).
				ThenGroup(chronos.NewGroup().
					Add(groupArgs{Seq: batch, Member: 0}, chronos.WithQueue("soak-a")).
					Add(groupArgs{Seq: batch, Member: 1}, chronos.WithQueue("soak-b")).
					OnComplete(cbArgs{Seq: batch}, chronos.WithQueue("soak-a"))).
				Then(chainArgs{Seq: batch, Link: 2}, chronos.WithQueue("soak-a"))
			if _, err := wf.Enqueue(ctx, w.client); err != nil && ctx.Err() == nil {
				log.Printf("soak: workflow enqueue: %v", err)
			}
			// group 3멤버 + 콜백.
			g := chronos.NewGroup().
				Add(groupArgs{Seq: batch, Member: 0}, chronos.WithQueue("soak-a")).
				Add(groupArgs{Seq: batch, Member: 1}, chronos.WithQueue("soak-b")).
				Add(groupArgs{Seq: batch, Member: 2}, chronos.WithQueue("soak-a")).
				OnComplete(cbArgs{Seq: batch}, chronos.WithQueue("soak-a"))
			if _, err := g.Enqueue(ctx, w.client); err != nil && ctx.Err() == nil {
				log.Printf("soak: group enqueue: %v", err)
			}
			batch++
		}
	}
}

// pauseToggler pauses soak-b for 30s every 2 minutes (backlog then drain).
func (w *Workload) pauseToggler(ctx context.Context) {
	tick := time.NewTicker(2 * time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := w.insp.PauseQueue(ctx, "soak-b"); err != nil && ctx.Err() == nil {
				log.Printf("soak: pause: %v", err)
				continue
			}
			select {
			case <-ctx.Done():
				// 종료 중에도 paused로 남기지 않는다(배수 후 판정 안정).
				_ = w.insp.ResumeQueue(context.WithoutCancel(ctx), "soak-b")
				return
			case <-time.After(30 * time.Second):
			}
			if err := w.insp.ResumeQueue(ctx, "soak-b"); err != nil {
				if ctx.Err() != nil {
					// 종료와 겹침 — paused로 남기지 않는다(배수 후 판정 안정).
					_ = w.insp.ResumeQueue(context.WithoutCancel(ctx), "soak-b")
					return
				}
				log.Printf("soak: resume: %v", err)
			}
		}
	}
}

// SchedArgs is what the interval schedule enqueues (exported for cmd/soak).
func SchedArgs() schedArgs { return schedArgs{} }
