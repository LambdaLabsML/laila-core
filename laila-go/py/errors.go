// Package py is the Python-semantics compatibility layer used by the laila
// Go port. It knows nothing about laila; it reproduces the parts of the
// Python runtime and standard library that the verbatim translation relies
// on (exception hierarchy, dict/list/tuple/set value model, repr/str, json,
// signature binding, threading, contextvars, asyncio, concurrent.futures,
// pydantic model mechanics, logging and the serialization codecs).
package py

import (
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
)

// Type is a Python exception class. It implements error so that
// errors.Is(err, py.KeyError) works through the Python MRO.
type Type struct {
	Name string
	Base *Type
}

// types_by_name is the exception-class namespace, so an exception that
// crossed a process/wire boundary as ``(type name, message)`` can be
// rebuilt as an instance of the same class.
var (
	types_by_name_mu sync.RWMutex
	types_by_name    = map[string]*Type{}
)

// NewType declares a new exception class deriving from base. The first
// class declared under a name wins the name lookup (Type_by_name).
func NewType(name string, base *Type) *Type {
	t := &Type{Name: name, Base: base}
	types_by_name_mu.Lock()
	if _, dup := types_by_name[name]; !dup {
		types_by_name[name] = t
	}
	types_by_name_mu.Unlock()
	return t
}

// Type_by_name resolves an exception class by its name (nil when unknown).
func Type_by_name(name string) *Type {
	types_by_name_mu.RLock()
	defer types_by_name_mu.RUnlock()
	return types_by_name[name]
}

// Error implements error for use as an errors.Is target.
func (t *Type) Error() string { return t.Name }

// Is reports whether t is target or a subclass of it (issubclass).
func (t *Type) Is(target error) bool {
	tt, ok := target.(*Type)
	if !ok {
		return false
	}
	for c := t; c != nil; c = c.Base {
		if c == tt {
			return true
		}
	}
	return false
}

// Issubclass mirrors issubclass(t, base) for exception types.
func (t *Type) Issubclass(base *Type) bool { return t.Is(base) }

// New builds an exception instance: Type.New(args...) == Type(*args).
func (t *Type) New(args ...any) *Error { return &Error{Type: t, Args: args} }

// Newf builds an exception with a formatted single message argument.
func (t *Type) Newf(format string, a ...any) *Error {
	return &Error{Type: t, Args: []any{fmt.Sprintf(format, a...)}}
}

// Error is a Python exception instance.
type Error struct {
	Type  *Type
	Args  []any
	Attrs map[string]any // extra attributes set by subclasses (e.g. errno, filename)
	Cause error          // raise ... from cause  (__cause__)
	Ctx   error          // implicit __context__
}

// Error returns str(exc) with Python's rules.
func (e *Error) Error() string {
	if e == nil {
		return "None"
	}
	if e.Type.Is(KeyError) && len(e.Args) == 1 {
		return Repr(e.Args[0])
	}
	switch len(e.Args) {
	case 0:
		return ""
	case 1:
		return Str(e.Args[0])
	default:
		return Repr(Tuple(e.Args))
	}
}

// Repr returns repr(exc): Type('arg1', 'arg2').
func (e *Error) Repr() string {
	parts := make([]string, len(e.Args))
	for i, a := range e.Args {
		parts[i] = Repr(a)
	}
	return e.Type.Name + "(" + strings.Join(parts, ", ") + ")"
}

// Is lets errors.Is match either an exception class or an identical instance.
func (e *Error) Is(target error) bool {
	switch t := target.(type) {
	case *Type:
		return e.Type.Is(t)
	case *Error:
		return e == t
	}
	return false
}

// Unwrap exposes the explicit cause for errors.Is/As chains.
func (e *Error) Unwrap() error { return e.Cause }

// Pytype returns type(exc).__name__.
func (e *Error) Pytype() string { return e.Type.Name }

// Arg returns Args[i] or nil.
func (e *Error) Arg(i int) any {
	if i < len(e.Args) {
		return e.Args[i]
	}
	return nil
}

// Attr returns a subclass attribute (errno, filename, ...).
func (e *Error) Attr(name string) any {
	if e.Attrs == nil {
		return nil
	}
	return e.Attrs[name]
}

// With sets an attribute and returns the error (fluent).
func (e *Error) With(name string, v any) *Error {
	if e.Attrs == nil {
		e.Attrs = map[string]any{}
	}
	e.Attrs[name] = v
	return e
}

// From sets __cause__ (raise exc from cause).
func (e *Error) From(cause error) *Error { e.Cause = cause; return e }

// The Python exception hierarchy (subset used by laila and its tests).
var (
	BaseException     = NewType("BaseException", nil)
	Exception         = NewType("Exception", BaseException)
	SystemExit        = NewType("SystemExit", BaseException)
	KeyboardInterrupt = NewType("KeyboardInterrupt", BaseException)
	GeneratorExit     = NewType("GeneratorExit", BaseException)
	CancelledError    = NewType("CancelledError", BaseException) // asyncio.CancelledError

	StopIteration      = NewType("StopIteration", Exception)
	StopAsyncIteration = NewType("StopAsyncIteration", Exception)
	ArithmeticError    = NewType("ArithmeticError", Exception)
	OverflowError      = NewType("OverflowError", ArithmeticError)
	ZeroDivisionError  = NewType("ZeroDivisionError", ArithmeticError)
	AssertionError     = NewType("AssertionError", Exception)
	AttributeError     = NewType("AttributeError", Exception)
	BufferError        = NewType("BufferError", Exception)
	EOFError           = NewType("EOFError", Exception)
	ImportError        = NewType("ImportError", Exception)
	ModuleNotFoundError = NewType("ModuleNotFoundError", ImportError)
	LookupError        = NewType("LookupError", Exception)
	IndexError         = NewType("IndexError", LookupError)
	KeyError           = NewType("KeyError", LookupError)
	MemoryError        = NewType("MemoryError", Exception)
	NameError          = NewType("NameError", Exception)
	OSError            = NewType("OSError", Exception)
	BlockingIOError    = NewType("BlockingIOError", OSError)
	ChildProcessError  = NewType("ChildProcessError", OSError)
	ConnectionError    = NewType("ConnectionError", OSError)
	BrokenPipeError    = NewType("BrokenPipeError", ConnectionError)
	ConnectionAbortedError = NewType("ConnectionAbortedError", ConnectionError)
	ConnectionRefusedError = NewType("ConnectionRefusedError", ConnectionError)
	ConnectionResetError   = NewType("ConnectionResetError", ConnectionError)
	FileExistsError    = NewType("FileExistsError", OSError)
	FileNotFoundError  = NewType("FileNotFoundError", OSError)
	InterruptedError   = NewType("InterruptedError", OSError)
	IsADirectoryError  = NewType("IsADirectoryError", OSError)
	NotADirectoryError = NewType("NotADirectoryError", OSError)
	PermissionError    = NewType("PermissionError", OSError)
	ProcessLookupError = NewType("ProcessLookupError", OSError)
	TimeoutError       = NewType("TimeoutError", OSError) // also asyncio/concurrent TimeoutError (3.11+)
	ReferenceError     = NewType("ReferenceError", Exception)
	RuntimeError       = NewType("RuntimeError", Exception)
	NotImplementedError = NewType("NotImplementedError", RuntimeError)
	RecursionError     = NewType("RecursionError", RuntimeError)
	BrokenExecutor     = NewType("BrokenExecutor", RuntimeError)     // concurrent.futures
	BrokenProcessPool  = NewType("BrokenProcessPool", BrokenExecutor) // concurrent.futures.process
	InvalidStateError  = NewType("InvalidStateError", Exception)      // asyncio / concurrent.futures
	SyntaxError        = NewType("SyntaxError", Exception)
	SystemError        = NewType("SystemError", Exception)
	TypeError          = NewType("TypeError", Exception)
	ValueError         = NewType("ValueError", Exception)
	UnicodeError       = NewType("UnicodeError", ValueError)
	UnicodeDecodeError = NewType("UnicodeDecodeError", UnicodeError)
	UnicodeEncodeError = NewType("UnicodeEncodeError", UnicodeError)
	JSONDecodeError    = NewType("JSONDecodeError", ValueError) // json.JSONDecodeError
	ValidationError    = NewType("ValidationError", ValueError) // pydantic.ValidationError
	InvalidToken       = NewType("InvalidToken", Exception)     // cryptography.fernet.InvalidToken
	UnpicklingError    = NewType("UnpicklingError", Exception)  // pickle.UnpicklingError
	PicklingError      = NewType("PicklingError", Exception)    // pickle.PicklingError
	QueueEmpty         = NewType("Empty", Exception)            // queue.Empty
	QueueFull          = NewType("Full", Exception)             // queue.Full
	Sqlite3Error       = NewType("sqlite3.Error", Exception)    // sqlite3.Error
	DatabaseError      = NewType("DatabaseError", Sqlite3Error) // sqlite3.DatabaseError
	OperationalError   = NewType("OperationalError", DatabaseError)
	IntegrityError     = NewType("IntegrityError", DatabaseError)
	ProgrammingError   = NewType("ProgrammingError", DatabaseError)
	Warning            = NewType("Warning", Exception)
	UserWarning        = NewType("UserWarning", Warning)
	DeprecationWarning = NewType("DeprecationWarning", Warning)
	RuntimeWarning     = NewType("RuntimeWarning", Warning)
)

// Raise is shorthand for t.New(args...) returned as error.
func Raise(t *Type, args ...any) error { return t.New(args...) }

// Raisef is shorthand for t.Newf(format, a...) returned as error.
func Raisef(t *Type, format string, a ...any) error { return t.Newf(format, a...) }

// AsError returns the *Error inside err (walking wrapped causes), or nil.
func AsError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return nil
}

// Typename returns type(exc).__name__ for any error. Foreign Go errors are
// reported as their classified Python type (see FromGo).
func Typename(err error) string {
	if e := AsError(err); e != nil {
		return e.Type.Name
	}
	if err == nil {
		return "NoneType"
	}
	return FromGo(err).Type.Name
}

// IsInstance mirrors isinstance(exc, t).
func IsInstance(err error, t *Type) bool { return err != nil && errors.Is(err, t) }

// Format_exception is ``"".join(traceback.format_exception(type(exc), exc,
// exc.__traceback__))``: a "Traceback (most recent call last):" header,
// the captured Go stack when the error carries one under the
// ``__traceback__`` attribute (see With_traceback), then ``Type: message``.
// Chained causes (``raise ... from``) are rendered first, like CPython.
func Format_exception(err error) string {
	if err == nil {
		return "NoneType: None\n"
	}
	var sb strings.Builder
	if e := AsError(err); e != nil && e.Cause != nil {
		sb.WriteString(Format_exception(e.Cause))
		sb.WriteString("\nThe above exception was the direct cause of the following exception:\n\n")
	}
	sb.WriteString("Traceback (most recent call last):\n")
	if e := AsError(err); e != nil {
		if tb, ok := e.Attrs["__traceback__"]; ok && tb != nil {
			s := Str(tb)
			sb.WriteString(s)
			if !strings.HasSuffix(s, "\n") {
				sb.WriteString("\n")
			}
		}
	}
	sb.WriteString(Typename(err))
	if msg := err.Error(); msg != "" {
		sb.WriteString(": ")
		sb.WriteString(msg)
	}
	sb.WriteString("\n")
	return sb.String()
}

// With_traceback attaches the current goroutine stack to err as its
// ``__traceback__`` (the point where a Python exception would have been
// raised). Returns err unchanged when it is not a py.Error.
func With_traceback(err error) error {
	if e := AsError(err); e != nil {
		if _, ok := e.Attrs["__traceback__"]; !ok {
			e.With("__traceback__", string(debug.Stack()))
		}
	}
	return err
}

// FromGo converts an arbitrary Go error into a Python exception, classifying
// the common standard-library errors the way CPython would.
func FromGo(err error) *Error {
	if err == nil {
		return nil
	}
	if e := AsError(err); e != nil {
		return e
	}
	switch {
	case errors.Is(err, os.ErrNotExist):
		return FileNotFoundError.New(2, "No such file or directory").With("errno", 2).From(err)
	case errors.Is(err, os.ErrExist):
		return FileExistsError.New(17, "File exists").With("errno", 17).From(err)
	case errors.Is(err, os.ErrPermission):
		return PermissionError.New(13, "Permission denied").With("errno", 13).From(err)
	case errors.Is(err, os.ErrDeadlineExceeded):
		return TimeoutError.New(err.Error()).From(err)
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "connection refused"):
		return ConnectionRefusedError.New(111, "Connection refused").With("errno", 111).From(err)
	case strings.Contains(msg, "connection reset"):
		return ConnectionResetError.New(104, "Connection reset by peer").With("errno", 104).From(err)
	case strings.Contains(msg, "broken pipe"):
		return BrokenPipeError.New(32, "Broken pipe").With("errno", 32).From(err)
	case strings.Contains(msg, "i/o timeout"), strings.Contains(msg, "deadline exceeded"):
		return TimeoutError.New(msg).From(err)
	case strings.Contains(msg, "use of closed network connection"):
		return OSError.New(msg).From(err)
	}
	return RuntimeError.New(msg).From(err)
}

// Recover converts a panic into a Python exception and stores it in *err.
// Use as: defer py.Recover(&err). A panic carrying a py error is kept as-is
// (it was a "raise" that could not be returned); anything else becomes a
// RuntimeError, like an uncaught Exception inside a Python task runner.
func Recover(err *error) {
	if r := recover(); r != nil {
		*err = PanicToError(r)
	}
}

// PanicToError converts a recovered panic value into an error.
func PanicToError(r any) error {
	switch v := r.(type) {
	case *Error:
		return v
	case error:
		return FromGo(v)
	default:
		return RuntimeError.New(fmt.Sprint(v))
	}
}

// Must panics with the given error (used for programming errors that Python
// would surface as an exception escaping a constructor).
func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// Check panics if err != nil.
func Check(err error) {
	if err != nil {
		panic(err)
	}
}
