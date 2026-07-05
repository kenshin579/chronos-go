package chronos

import "encoding/json"

// encodeArgs serializes task args to the payload stored in Redis. JSON is the
// default codec (chosen for redis-cli debuggability); a pluggable Marshaler is
// a later enhancement.
func encodeArgs[T TaskArgs](args T) ([]byte, error) {
	return json.Marshal(args)
}

// decodeArgs deserializes a payload into a task args value.
func decodeArgs[T TaskArgs](payload []byte) (T, error) {
	var args T
	err := json.Unmarshal(payload, &args)
	return args, err
}
