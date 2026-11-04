package py

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Python logging levels.
const (
	CRITICAL = 50
	ERROR    = 40
	WARNING  = 30
	INFO     = 20
	DEBUG    = 10
	NOTSET   = 0
)

var levelNames = map[int]string{CRITICAL: "CRITICAL", ERROR: "ERROR", WARNING: "WARNING", INFO: "INFO", DEBUG: "DEBUG", NOTSET: "NOTSET"}

// Get_level_name is logging.getLevelName(level).
func Get_level_name(level int) string {
	if n, ok := levelNames[level]; ok {
		return n
	}
	return fmt.Sprintf("Level %d", level)
}

// Level_from_name parses "INFO"/"debug"/...; returns ok=false when unknown.
func Level_from_name(name string) (int, bool) {
	for lv, n := range levelNames {
		if strings.EqualFold(n, name) {
			return lv, true
		}
	}
	return 0, false
}

// LogRecord is logging.LogRecord.
type LogRecord struct {
	Name       string
	Levelno    int
	Levelname  string
	Msg        string
	Args       []any
	Created    float64
	Thread     int64
	ThreadName string
	Process    int
	Exc_info   error
	Extra      Kw
	message    string
}

// Get_message is record.getMessage(): msg % args.
func (r *LogRecord) Get_message() string {
	if r.message != "" {
		return r.message
	}
	if len(r.Args) == 0 {
		r.message = r.Msg
	} else {
		r.message = Percent_format(r.Msg, r.Args...)
	}
	return r.message
}

// Handler is logging.Handler.
type Handler interface {
	Handle(r *LogRecord)
	Set_level(level int)
	Level() int
	Set_formatter(f *Formatter)
	Close()
}

// BaseHandler supplies the common Handler state.
type BaseHandler struct {
	mu        sync.Mutex
	level     int
	formatter *Formatter
}

// Set_level is handler.setLevel(level).
func (h *BaseHandler) Set_level(level int) { h.mu.Lock(); h.level = level; h.mu.Unlock() }

// Level is handler.level.
func (h *BaseHandler) Level() int { h.mu.Lock(); defer h.mu.Unlock(); return h.level }

// Set_formatter is handler.setFormatter(f).
func (h *BaseHandler) Set_formatter(f *Formatter) { h.mu.Lock(); h.formatter = f; h.mu.Unlock() }

// Format applies the handler's formatter (or the default one).
func (h *BaseHandler) Format(r *LogRecord) string {
	h.mu.Lock()
	f := h.formatter
	h.mu.Unlock()
	if f == nil {
		f = defaultFormatter
	}
	return f.Format(r)
}

// Close is a no-op by default.
func (h *BaseHandler) Close() {}

// NullHandler is logging.NullHandler.
type NullHandler struct{ BaseHandler }

// Handle discards the record.
func (h *NullHandler) Handle(*LogRecord) {}

// StreamHandler is logging.StreamHandler (stderr by default).
type StreamHandler struct {
	BaseHandler
	W io.Writer
}

// New_StreamHandler creates a handler writing to w (nil -> stderr).
func New_StreamHandler(w io.Writer) *StreamHandler {
	if w == nil {
		w = os.Stderr
	}
	return &StreamHandler{W: w}
}

// Handle writes the formatted record plus newline.
func (h *StreamHandler) Handle(r *LogRecord) {
	if r.Levelno < h.Level() {
		return
	}
	line := h.Format(r)
	h.mu.Lock()
	_, _ = io.WriteString(h.W, line+"\n")
	h.mu.Unlock()
}

// FileHandler is logging.FileHandler.
type FileHandler struct {
	StreamHandler
	f *os.File
}

// New_FileHandler opens path for appending.
func New_FileHandler(path string) (*FileHandler, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, FromGo(err)
	}
	return &FileHandler{StreamHandler: StreamHandler{W: f}, f: f}, nil
}

// Close closes the file.
func (h *FileHandler) Close() { _ = h.f.Close() }

// FuncHandler adapts a function into a Handler (for custom sinks).
type FuncHandler struct {
	BaseHandler
	Fn func(r *LogRecord)
}

// Handle calls Fn.
func (h *FuncHandler) Handle(r *LogRecord) {
	if r.Levelno < h.Level() {
		return
	}
	h.Fn(r)
}

// Formatter is logging.Formatter with %-style attribute placeholders.
type Formatter struct {
	Fmt     string
	Datefmt string
}

var defaultFormatter = &Formatter{Fmt: "%(message)s"}

// New_Formatter creates logging.Formatter(fmt, datefmt).
func New_Formatter(format string, datefmt ...string) *Formatter {
	f := &Formatter{Fmt: format}
	if len(datefmt) > 0 {
		f.Datefmt = datefmt[0]
	}
	if f.Fmt == "" {
		f.Fmt = "%(message)s"
	}
	return f
}

var fmtRe = regexp.MustCompile(`%\((\w+)\)([-+ 0#]*\d*(?:\.\d+)?)([sdfr])`)

// Format_time is formatter.formatTime(record).
func (f *Formatter) Format_time(r *LogRecord) string {
	t := time.Unix(0, int64(r.Created*1e9))
	if f.Datefmt != "" {
		return Strftime(t, f.Datefmt)
	}
	return t.Format("2006-01-02 15:04:05") + fmt.Sprintf(",%03d", t.Nanosecond()/1e6)
}

// Format is formatter.format(record).
func (f *Formatter) Format(r *LogRecord) string {
	msg := r.Get_message()
	out := fmtRe.ReplaceAllStringFunc(f.Fmt, func(m string) string {
		sub := fmtRe.FindStringSubmatch(m)
		name, flags, conv := sub[1], sub[2], sub[3]
		var v any
		switch name {
		case "message":
			v = msg
		case "msg":
			v = r.Msg
		case "name":
			v = r.Name
		case "levelname":
			v = r.Levelname
		case "levelno":
			v = r.Levelno
		case "asctime":
			v = f.Format_time(r)
		case "created":
			v = r.Created
		case "msecs":
			v = (r.Created - float64(int64(r.Created))) * 1000
		case "thread":
			v = r.Thread
		case "threadName":
			v = r.ThreadName
		case "process":
			v = r.Process
		case "processName":
			v = "MainProcess"
		default:
			if r.Extra != nil {
				v = r.Extra[name]
			}
		}
		return fmt.Sprintf("%"+flags+goVerb(conv, v), v)
	})
	if r.Exc_info != nil {
		out += "\n" + Typename(r.Exc_info) + ": " + r.Exc_info.Error()
	}
	return out
}

func goVerb(conv string, v any) string {
	switch conv {
	case "d":
		return "d"
	case "f":
		return "f"
	case "r":
		return "s"
	}
	if _, ok := v.(string); ok {
		return "s"
	}
	return "v"
}

// Percent_format is Python's `msg % args` for %s/%d/%r/%f with widths.
func Percent_format(msg string, args ...any) string {
	var sb strings.Builder
	ai := 0
	for i := 0; i < len(msg); i++ {
		c := msg[i]
		if c != '%' {
			sb.WriteByte(c)
			continue
		}
		if i+1 < len(msg) && msg[i+1] == '%' {
			sb.WriteByte('%')
			i++
			continue
		}
		j := i + 1
		for j < len(msg) && strings.IndexByte("-+ 0#.0123456789", msg[j]) >= 0 {
			j++
		}
		if j >= len(msg) {
			sb.WriteString(msg[i:])
			break
		}
		flags := msg[i+1 : j]
		conv := msg[j]
		var a any
		if ai < len(args) {
			a = args[ai]
			ai++
		}
		switch conv {
		case 's':
			sb.WriteString(fmt.Sprintf("%"+flags+"s", Str(a)))
		case 'r':
			sb.WriteString(fmt.Sprintf("%"+flags+"s", Repr(a)))
		case 'd', 'i':
			if n, ok := toInt64(a); ok {
				sb.WriteString(fmt.Sprintf("%"+flags+"d", n))
			} else {
				sb.WriteString(Str(a))
			}
		case 'f', 'g', 'e':
			switch x := a.(type) {
			case float64:
				sb.WriteString(fmt.Sprintf("%"+flags+string(conv), x))
			case float32:
				sb.WriteString(fmt.Sprintf("%"+flags+string(conv), float64(x)))
			default:
				if n, ok := toInt64(a); ok {
					sb.WriteString(fmt.Sprintf("%"+flags+string(conv), float64(n)))
				} else {
					sb.WriteString(Str(a))
				}
			}
		case 'x':
			if n, ok := toInt64(a); ok {
				sb.WriteString(fmt.Sprintf("%"+flags+"x", n))
			}
		default:
			sb.WriteString(msg[i : j+1])
		}
		i = j
	}
	return sb.String()
}

// Logger is logging.Logger (hierarchical by dotted name).
type Logger struct {
	mu        sync.Mutex
	Name      string
	level     int
	handlers  []Handler
	Propagate bool
	parent    *Logger
	disabled  bool
}

var (
	loggersMu  sync.Mutex
	loggers    = map[string]*Logger{}
	rootLogger = &Logger{Name: "root", level: WARNING, Propagate: false}
)

// Get_logger is logging.getLogger(name); "" returns the root logger.
func Get_logger(name string) *Logger {
	if name == "" || name == "root" {
		return rootLogger
	}
	loggersMu.Lock()
	defer loggersMu.Unlock()
	return getLoggerLocked(name)
}

func getLoggerLocked(name string) *Logger {
	if l, ok := loggers[name]; ok {
		return l
	}
	l := &Logger{Name: name, level: NOTSET, Propagate: true}
	loggers[name] = l
	// parent = closest ancestor (created on demand, like PlaceHolders)
	l.parent = rootLogger
	if i := strings.LastIndexByte(name, '.'); i > 0 {
		l.parent = getLoggerLocked(name[:i])
	}
	// adopt existing descendants whose parent was above us
	for n, c := range loggers {
		if c != l && strings.HasPrefix(n, name+".") && !strings.HasPrefix(c.parent.Name+".", name+".") {
			c.parent = l
		}
	}
	return l
}

// Set_level is logger.setLevel(level).
func (l *Logger) Set_level(level int) { l.mu.Lock(); l.level = level; l.mu.Unlock() }

// Level is logger.level.
func (l *Logger) Level() int { l.mu.Lock(); defer l.mu.Unlock(); return l.level }

// Get_effective_level walks up to the first non-NOTSET level.
func (l *Logger) Get_effective_level() int {
	for c := l; c != nil; c = c.parent {
		if lv := c.Level(); lv != NOTSET {
			return lv
		}
	}
	return NOTSET
}

// Is_enabled_for is logger.isEnabledFor(level).
func (l *Logger) Is_enabled_for(level int) bool {
	l.mu.Lock()
	disabled := l.disabled
	l.mu.Unlock()
	return !disabled && level >= l.Get_effective_level()
}

// Add_handler is logger.addHandler(h) (idempotent).
func (l *Logger) Add_handler(h Handler) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, x := range l.handlers {
		if x == h {
			return
		}
	}
	l.handlers = append(l.handlers, h)
}

// Remove_handler is logger.removeHandler(h).
func (l *Logger) Remove_handler(h Handler) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, x := range l.handlers {
		if x == h {
			l.handlers = append(l.handlers[:i], l.handlers[i+1:]...)
			return
		}
	}
}

// Handlers is logger.handlers.
func (l *Logger) Handlers() []Handler {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Handler(nil), l.handlers...)
}

// Has_handlers is logger.hasHandlers().
func (l *Logger) Has_handlers() bool {
	for c := l; c != nil; c = c.parent {
		if len(c.Handlers()) > 0 {
			return true
		}
		if !c.Propagate {
			break
		}
	}
	return false
}

// Parent is logger.parent.
func (l *Logger) Parent() *Logger { return l.parent }

// Set_disabled is logger.disabled = flag.
func (l *Logger) Set_disabled(flag bool) { l.mu.Lock(); l.disabled = flag; l.mu.Unlock() }

// Make_record builds a LogRecord for this logger.
func (l *Logger) Make_record(level int, msg string, args []any, exc error, extra Kw) *LogRecord {
	t := Current_thread()
	return &LogRecord{
		Name: l.Name, Levelno: level, Levelname: Get_level_name(level), Msg: msg, Args: args,
		Created: Time(), Thread: t.Ident(), ThreadName: t.Name(), Process: os.Getpid(), Exc_info: exc, Extra: extra,
	}
}

// Handle dispatches a record up the hierarchy (logger.handle / callHandlers).
func (l *Logger) Handle(r *LogRecord) {
	found := 0
	for c := l; c != nil; c = c.parent {
		for _, h := range c.Handlers() {
			found++
			if r.Levelno >= h.Level() {
				h.Handle(r)
			}
		}
		if !c.Propagate {
			break
		}
	}
	if found == 0 && r.Levelno >= WARNING {
		lastResort.Handle(r)
	}
}

var lastResort = &StreamHandler{W: os.Stderr}

// Log is logger.log(level, msg, *args, exc_info=..., extra=...).
func (l *Logger) Log(level int, msg string, args ...any) {
	if !l.Is_enabled_for(level) {
		return
	}
	pos, kw := SplitKw(args)
	var exc error
	if e, ok := kw["exc_info"].(error); ok {
		exc = e
	}
	var extra Kw
	if e, ok := kw["extra"].(Kw); ok {
		extra = e
	}
	l.Handle(l.Make_record(level, msg, pos, exc, extra))
}

// Debug is logger.debug.
func (l *Logger) Debug(msg string, args ...any) { l.Log(DEBUG, msg, args...) }

// Info is logger.info.
func (l *Logger) Info(msg string, args ...any) { l.Log(INFO, msg, args...) }

// Warning is logger.warning.
func (l *Logger) Warning(msg string, args ...any) { l.Log(WARNING, msg, args...) }

// Error is logger.error.
func (l *Logger) Error(msg string, args ...any) { l.Log(ERROR, msg, args...) }

// Critical is logger.critical.
func (l *Logger) Critical(msg string, args ...any) { l.Log(CRITICAL, msg, args...) }

// Exception is logger.exception(msg, exc).
func (l *Logger) Exception(msg string, exc error, args ...any) {
	l.Log(ERROR, msg, append(args, Kw{"exc_info": exc})...)
}

// All_loggers returns every named logger (sorted), for tests.
func All_loggers() []*Logger {
	loggersMu.Lock()
	defer loggersMu.Unlock()
	out := make([]*Logger, 0, len(loggers))
	for _, l := range loggers {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Strftime implements the strftime directives Python code in laila uses.
func Strftime(t time.Time, layout string) string {
	var sb strings.Builder
	for i := 0; i < len(layout); i++ {
		if layout[i] != '%' || i+1 >= len(layout) {
			sb.WriteByte(layout[i])
			continue
		}
		i++
		switch layout[i] {
		case 'Y':
			sb.WriteString(fmt.Sprintf("%04d", t.Year()))
		case 'm':
			sb.WriteString(fmt.Sprintf("%02d", int(t.Month())))
		case 'd':
			sb.WriteString(fmt.Sprintf("%02d", t.Day()))
		case 'H':
			sb.WriteString(fmt.Sprintf("%02d", t.Hour()))
		case 'M':
			sb.WriteString(fmt.Sprintf("%02d", t.Minute()))
		case 'S':
			sb.WriteString(fmt.Sprintf("%02d", t.Second()))
		case 'f':
			sb.WriteString(fmt.Sprintf("%06d", t.Nanosecond()/1000))
		case 'z':
			sb.WriteString(t.Format("-0700"))
		case 'Z':
			sb.WriteString(t.Format("MST"))
		case 'j':
			sb.WriteString(fmt.Sprintf("%03d", t.YearDay()))
		case 'a':
			sb.WriteString(t.Format("Mon"))
		case 'A':
			sb.WriteString(t.Format("Monday"))
		case 'b':
			sb.WriteString(t.Format("Jan"))
		case 'B':
			sb.WriteString(t.Format("January"))
		case 'y':
			sb.WriteString(fmt.Sprintf("%02d", t.Year()%100))
		case 'p':
			sb.WriteString(t.Format("PM"))
		case 'I':
			sb.WriteString(t.Format("03"))
		case '%':
			sb.WriteByte('%')
		default:
			sb.WriteByte('%')
			sb.WriteByte(layout[i])
		}
	}
	return sb.String()
}

func init() {
	Logger_warning = func(format string, a ...any) {
		Get_logger("asyncio").Warning(fmt.Sprintf(format, a...))
	}
}
