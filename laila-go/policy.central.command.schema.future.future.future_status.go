package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// policy/central/command/schema/future/future/future_status.py --
// enumeration of future lifecycle status codes.
//
// These string values are the canonical wire representation -- they are
// sent verbatim through RPC frames (see
// LAILA_IDENTIFIABLE_POLICY_.Get_future_status_) and rendered in log
// records, so renaming a member is a breaking change for any peer or log
// consumer that hard-codes the value.

// FutureStatus is the lifecycle state of every laila future (a ``str``
// enum: the Go value *is* the wire string).
//
//   - UNKNOWN:      status could not be determined (newly-created
//     RemoteFuture handles before the first poll).
//   - NOT_STARTED:  constructed and registered, task not yet executing.
//   - RUNNING:      the task is actively executing.
//   - POLL_TIMEOUT: a status poll timed out without a definitive answer.
//   - FINISHED:     completed successfully; ``result`` is set.
//   - ERROR:        the task raised; ``exception`` is set.
//   - CANCELLED:    cancelled before completion.
type FutureStatus string

// FutureStatus members.
const (
	FutureStatus_UNKNOWN      FutureStatus = "unknown"
	FutureStatus_NOT_STARTED  FutureStatus = "not_started"
	FutureStatus_RUNNING      FutureStatus = "running"
	FutureStatus_POLL_TIMEOUT FutureStatus = "poll_timeout"
	FutureStatus_FINISHED     FutureStatus = "finished"
	FutureStatus_ERROR        FutureStatus = "error"
	FutureStatus_CANCELLED    FutureStatus = "cancelled"
)

// FutureStatus_members_ is ``list(FutureStatus)`` in declaration order.
var FutureStatus_members_ = []FutureStatus{
	FutureStatus_UNKNOWN, FutureStatus_NOT_STARTED, FutureStatus_RUNNING,
	FutureStatus_POLL_TIMEOUT, FutureStatus_FINISHED, FutureStatus_ERROR,
	FutureStatus_CANCELLED,
}

var future_status_names_ = map[FutureStatus]string{
	FutureStatus_UNKNOWN:      "UNKNOWN",
	FutureStatus_NOT_STARTED:  "NOT_STARTED",
	FutureStatus_RUNNING:      "RUNNING",
	FutureStatus_POLL_TIMEOUT: "POLL_TIMEOUT",
	FutureStatus_FINISHED:     "FINISHED",
	FutureStatus_ERROR:        "ERROR",
	FutureStatus_CANCELLED:    "CANCELLED",
}

// Name is ``member.name``.
func (s FutureStatus) Name() string { return future_status_names_[s] }

// Value is ``member.value`` (the wire string).
func (s FutureStatus) Value() string { return string(s) }

// String is ``str(member)`` -> ``FutureStatus.RUNNING`` (Python 3.11+
// ``Enum.__str__`` applies to ``str`` mixins too). The wire/JSON form is
// the *value* (``json.dumps(FutureStatus.RUNNING)`` -> ``"running"``):
// the py codecs serialize named string kinds by value, never via String.
func (s FutureStatus) String() string {
	if name, ok := future_status_names_[s]; ok {
		return "FutureStatus." + name
	}
	return string(s)
}

// Repr is ``repr(member)`` -> ``<FutureStatus.RUNNING: 'running'>``.
func (s FutureStatus) Repr() string {
	return "<FutureStatus." + s.Name() + ": " + py.Repr_str(string(s)) + ">"
}

// Valid reports whether s is a declared member.
func (s FutureStatus) Valid() bool { _, ok := future_status_names_[s]; return ok }

// FutureStatus_by_name is ``FutureStatus[name]``; KeyError for unknown names.
func FutureStatus_by_name(name string) (FutureStatus, error) {
	for _, m := range FutureStatus_members_ {
		if m.Name() == name {
			return m, nil
		}
	}
	return "", py.KeyError.New(name)
}

// FutureStatus_of is ``FutureStatus(value)``: accepts a member or its wire
// string; ValueError otherwise.
func FutureStatus_of(value any) (FutureStatus, error) {
	switch v := value.(type) {
	case FutureStatus:
		if v.Valid() {
			return v, nil
		}
	case string:
		if FutureStatus(v).Valid() {
			return FutureStatus(v), nil
		}
	}
	return "", py.ValueError.Newf("%s is not a valid FutureStatus", py.Repr(value))
}

// ``FutureStatus.__reduce_ex__`` -> ``(FutureStatus, (value,))``.
func init() {
	py.Register_enum(FutureStatus_NOT_STARTED, "laila.policy.central.command.schema.future.future.future_status.FutureStatus",
		func(m any) any { return m.(FutureStatus).Value() },
		func(v any) (any, error) {
			if s := FutureStatus(py.Str(v)); s.Valid() {
				return s, nil
			}
			return nil, py.ValueError.Newf("%s is not a valid FutureStatus", py.Repr(v))
		})
}
