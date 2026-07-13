package base

import (
	"strings"
	"testing"
)

func TestEncodeDecodeMessage_RoundTrip(t *testing.T) {
	msg := &TaskMessage{
		ID:      "task-1",
		Kind:    "email:send",
		Payload: []byte(`{"user_id":"u1"}`),
		Queue:   "default",
		State:   StatePending,
	}

	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.ID != msg.ID || got.Kind != msg.Kind || got.Queue != msg.Queue {
		t.Errorf("round trip mismatch: got %+v want %+v", got, msg)
	}
	if string(got.Payload) != string(msg.Payload) {
		t.Errorf("payload = %q, want %q", got.Payload, msg.Payload)
	}
	if got.State != StatePending {
		t.Errorf("state = %v, want StatePending", got.State)
	}
}

func TestTaskMessage_M2Fields_RoundTrip(t *testing.T) {
	msg := &TaskMessage{
		ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default",
		Retried: 3, MaxRetry: 25, NoArchive: true,
	}
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Retried != 3 || got.MaxRetry != 25 || !got.NoArchive {
		t.Errorf("m2 fields round trip mismatch: %+v", got)
	}
}

func TestTaskMessage_UniqueKey_RoundTrip(t *testing.T) {
	msg := &TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default",
		UniqueKey: "chronos:{default}:unique:k:deadbeef"}
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.UniqueKey != msg.UniqueKey {
		t.Errorf("UniqueKey = %q, want %q", got.UniqueKey, msg.UniqueKey)
	}
}

func TestTaskMessage_RetentionRoundTrips(t *testing.T) {
	msg := &TaskMessage{ID: "t2", Kind: "k", Queue: "default", Retention: 3600, CompletedAt: 1700000000}
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Retention != 3600 || got.CompletedAt != 1700000000 {
		t.Errorf("Retention=%d CompletedAt=%d, want 3600/1700000000", got.Retention, got.CompletedAt)
	}
}

func TestCompletedKey(t *testing.T) {
	if got, want := CompletedKey("q1"), "chronos:{q1}:completed"; got != want {
		t.Errorf("CompletedKey = %q, want %q", got, want)
	}
}

func TestTaskState_String(t *testing.T) {
	if StateActive.String() != "active" {
		t.Errorf("StateActive.String() = %q, want %q", StateActive.String(), "active")
	}
}

func TestTaskMessage_LastErrRoundTrips(t *testing.T) {
	msg := &TaskMessage{ID: "t1", Kind: "k", Queue: "default", LastErr: "boom: timeout"}
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.LastErr != "boom: timeout" {
		t.Errorf("LastErr = %q, want %q", got.LastErr, "boom: timeout")
	}
}

func TestTaskMessage_ChainRoundTrips(t *testing.T) {
	msg := &TaskMessage{
		ID: "ch:0", Kind: "a", Queue: "default",
		ChainID: "ch", ChainIndex: 0,
		Chain: []ChainLink{
			{Kind: "b", Payload: []byte(`{"n":2}`), Queue: "low", MaxRetry: 5, Retention: 60, Delay: 3},
			{Kind: "c", Payload: []byte(`{"n":3}`), Queue: "default", MaxRetry: 25},
		},
	}
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ChainID != "ch" || got.ChainIndex != 0 || len(got.Chain) != 2 {
		t.Fatalf("chain fields lost: %+v", got)
	}
	l := got.Chain[0]
	if l.Kind != "b" || l.Queue != "low" || l.MaxRetry != 5 || l.Retention != 60 || l.Delay != 3 {
		t.Errorf("link[0] = %+v", l)
	}
}

func TestTaskMessage_GroupRoundTrips(t *testing.T) {
	msg := &TaskMessage{
		ID: "g:m0", Kind: "a", Queue: "default",
		GroupID: "g", GroupQueue: "cbq",
		GroupCallback: &ChainLink{Kind: "cb", Payload: []byte(`{"b":1}`), Queue: "cbq", MaxRetry: 25, Delay: 2},
	}
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.GroupID != "g" || got.GroupQueue != "cbq" || got.GroupCallback == nil {
		t.Fatalf("group fields lost: %+v", got)
	}
	if got.GroupCallback.Kind != "cb" || got.GroupCallback.Delay != 2 {
		t.Errorf("callback = %+v", got.GroupCallback)
	}
}

func TestMessageResultFieldsRoundTrip(t *testing.T) {
	in := &TaskMessage{
		ID: "t1", Kind: "k", Queue: "q",
		Result:       []byte(`{"path":"s3://out"}`),
		PrevResult:   []byte(`{"n":1}`),
		GroupResults: [][]byte{[]byte(`{"a":1}`), nil, []byte(`{"b":2}`)},
		GroupIndex:   2,
		GroupSize:    3,
	}
	b, err := EncodeMessage(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := DecodeMessage(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(out.Result) != string(in.Result) || string(out.PrevResult) != string(in.PrevResult) {
		t.Errorf("result fields lost: %+v", out)
	}
	if len(out.GroupResults) != 3 || out.GroupResults[1] != nil ||
		string(out.GroupResults[2]) != `{"b":2}` {
		t.Errorf("group results wrong: %v", out.GroupResults)
	}
	if out.GroupIndex != 2 || out.GroupSize != 3 {
		t.Errorf("group index/size wrong: %+v", out)
	}
	// 빈 필드는 직렬화에서 생략(기존 메시지와 하위호환).
	empty, _ := EncodeMessage(&TaskMessage{ID: "t2", Kind: "k", Queue: "q"})
	for _, field := range []string{"result", "prev_result", "group_results", "group_size"} {
		if strings.Contains(string(empty), `"`+field+`"`) {
			t.Errorf("empty message must omit %q", field)
		}
	}
}

func TestGroupKey(t *testing.T) {
	if got, want := GroupKey("cbq", "g1"), "chronos:{cbq}:group:g1"; got != want {
		t.Errorf("GroupKey = %q, want %q", got, want)
	}
}

func TestGlobalKeys(t *testing.T) {
	if PausedKey() != "chronos:paused" {
		t.Errorf("PausedKey = %q", PausedKey())
	}
	if SchedulesKey() != "chronos:schedules" {
		t.Errorf("SchedulesKey = %q", SchedulesKey())
	}
}
