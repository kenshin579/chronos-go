package base

import "testing"

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

func TestTaskState_String(t *testing.T) {
	if StateActive.String() != "active" {
		t.Errorf("StateActive.String() = %q, want %q", StateActive.String(), "active")
	}
}
