package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// policy/central/command/taskforce/status.py -- lifecycle status
// enumeration for task-forces.
//
// Defines the canonical state machine used by every concrete task-force in
// laila. The members are ``str``-valued so they serialize cleanly in JSON /
// YAML / Pydantic dumps and are stable across releases (rename them and you
// break wire compatibility).

// TaskForceStatus is the lifecycle state of a LAILA_IDENTIFIABLE_TASK_FORCE_.
//
// The valid transitions are:
//
//	NOT_STARTED --start()--> RUNNING
//	RUNNING     --pause()--> PAUSED
//	PAUSED      --start()--> RUNNING
//	RUNNING     --shutdown()--> STOPPED
//	PAUSED      --shutdown()--> STOPPED
//	*           --(crash)--> CRASHED
//
//   - NOT_STARTED: constructed but Start has not yet been called.
//     Model_post_init auto-transitions out of this state for most
//     subclasses.
//   - RUNNING: actively dispatching submitted tasks.
//   - PAUSED: quiesced via Pause. Backend resources are still allocated
//     but no new work is dispatched. Transition back to RUNNING via Start.
//   - STOPPED: terminal: backend resources released by Shutdown. The
//     task-force cannot be restarted; construct a new one.
//   - CRASHED: terminal: an unrecoverable backend error was observed. Set
//     by the subclass on observed worker failures.
type TaskForceStatus string

// TaskForceStatus members.
const (
	TaskForceStatus_NOT_STARTED TaskForceStatus = "not_started"
	TaskForceStatus_RUNNING     TaskForceStatus = "running"
	TaskForceStatus_PAUSED      TaskForceStatus = "paused"
	TaskForceStatus_STOPPED     TaskForceStatus = "stopped"
	TaskForceStatus_CRASHED     TaskForceStatus = "crashed"
)

// TaskForceStatus_members_ is ``list(TaskForceStatus)`` in declaration order.
var TaskForceStatus_members_ = []TaskForceStatus{
	TaskForceStatus_NOT_STARTED, TaskForceStatus_RUNNING, TaskForceStatus_PAUSED,
	TaskForceStatus_STOPPED, TaskForceStatus_CRASHED,
}

var task_force_status_names_ = map[TaskForceStatus]string{
	TaskForceStatus_NOT_STARTED: "NOT_STARTED",
	TaskForceStatus_RUNNING:     "RUNNING",
	TaskForceStatus_PAUSED:      "PAUSED",
	TaskForceStatus_STOPPED:     "STOPPED",
	TaskForceStatus_CRASHED:     "CRASHED",
}

// Name is ``member.name``.
func (s TaskForceStatus) Name() string { return task_force_status_names_[s] }

// Value is ``member.value`` (the wire string).
func (s TaskForceStatus) Value() string { return string(s) }

// String is ``str(member)``; for a ``str`` enum that is the value itself.
func (s TaskForceStatus) String() string { return string(s) }

// Repr is ``repr(member)`` -> ``<TaskForceStatus.RUNNING: 'running'>``.
func (s TaskForceStatus) Repr() string {
	return "<TaskForceStatus." + s.Name() + ": " + py.Repr_str(string(s)) + ">"
}

// Valid reports whether s is a declared member.
func (s TaskForceStatus) Valid() bool { _, ok := task_force_status_names_[s]; return ok }

// TaskForceStatus_by_name is ``TaskForceStatus[name]``; KeyError for
// unknown names.
func TaskForceStatus_by_name(name string) (TaskForceStatus, error) {
	for _, m := range TaskForceStatus_members_ {
		if m.Name() == name {
			return m, nil
		}
	}
	return "", py.KeyError.New(name)
}

// TaskForceStatus_of is ``TaskForceStatus(value)``: accepts a member or its
// wire string; ValueError otherwise.
func TaskForceStatus_of(value any) (TaskForceStatus, error) {
	switch v := value.(type) {
	case TaskForceStatus:
		if v.Valid() {
			return v, nil
		}
	case string:
		if TaskForceStatus(v).Valid() {
			return TaskForceStatus(v), nil
		}
	}
	return "", py.ValueError.Newf("%s is not a valid TaskForceStatus", py.Repr(value))
}

// ``TaskForceStatus.__reduce_ex__`` -> ``(TaskForceStatus, (value,))``.
func init() {
	py.Register_enum(TaskForceStatus_members_[0], "laila.policy.central.command.taskforce.status.TaskForceStatus",
		func(m any) any { return m.(TaskForceStatus).Value() },
		func(v any) (any, error) {
			if s := TaskForceStatus(py.Str(v)); s.Valid() {
				return s, nil
			}
			return nil, py.ValueError.Newf("%s is not a valid TaskForceStatus", py.Repr(v))
		})
}
