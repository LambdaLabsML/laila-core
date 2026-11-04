package py

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

// JsonOpts mirrors the keyword arguments of json.dumps.
type JsonOpts struct {
	Sort_keys    bool
	Indent       *int
	Item_sep     string // default ", " (or "," when Indent is set)
	Key_sep      string // default ": "
	Ensure_ascii *bool  // default true
	Allow_nan    *bool  // default true
	Default      func(v any) (any, error)
}

// Json_dumps is json.dumps(v) with Python's default formatting.
func Json_dumps(v any) (string, error) { return Json_dumps_opts(v, JsonOpts{}) }

// Json_dumps_compact is json.dumps(v, separators=(",", ":")).
func Json_dumps_compact(v any, sortKeys bool) (string, error) {
	return Json_dumps_opts(v, JsonOpts{Item_sep: ",", Key_sep: ":", Sort_keys: sortKeys})
}

// Json_dumps_opts is json.dumps(v, **opts).
func Json_dumps_opts(v any, o JsonOpts) (string, error) {
	if o.Item_sep == "" {
		if o.Indent != nil {
			o.Item_sep = ","
		} else {
			o.Item_sep = ", "
		}
	}
	if o.Key_sep == "" {
		o.Key_sep = ": "
	}
	ensureASCII := o.Ensure_ascii == nil || *o.Ensure_ascii
	allowNaN := o.Allow_nan == nil || *o.Allow_nan
	w := &jsonWriter{o: o, ensureASCII: ensureASCII, allowNaN: allowNaN}
	if err := w.encode(v, 0); err != nil {
		return "", err
	}
	return w.sb.String(), nil
}

type jsonWriter struct {
	sb          strings.Builder
	o           JsonOpts
	ensureASCII bool
	allowNaN    bool
}

func (w *jsonWriter) newline(level int) {
	if w.o.Indent == nil {
		return
	}
	w.sb.WriteByte('\n')
	w.sb.WriteString(strings.Repeat(" ", *w.o.Indent*level))
}

func (w *jsonWriter) floatText(f float64) (string, error) {
	switch {
	case math.IsNaN(f):
		if !w.allowNaN {
			return "", ValueError.New("Out of range float values are not JSON compliant: nan")
		}
		return "NaN", nil
	case math.IsInf(f, 1):
		if !w.allowNaN {
			return "", ValueError.New("Out of range float values are not JSON compliant: inf")
		}
		return "Infinity", nil
	case math.IsInf(f, -1):
		if !w.allowNaN {
			return "", ValueError.New("Out of range float values are not JSON compliant: -inf")
		}
		return "-Infinity", nil
	}
	return Repr_float(f), nil
}

func (w *jsonWriter) keyText(k any) (string, error) {
	switch x := k.(type) {
	case string:
		return x, nil
	case nil:
		return "null", nil
	case bool:
		if x {
			return "true", nil
		}
		return "false", nil
	case float32:
		return w.floatText(float64(x))
	case float64:
		return w.floatText(x)
	}
	if hk, err := keyOf(k); err == nil && (hk.kind == hkInt || hk.kind == hkBig) {
		return Repr(k), nil
	}
	return "", TypeError.Newf("keys must be str, int, float, bool or None, not %s", Pytypename(k))
}

func (w *jsonWriter) writeString(s string) {
	w.sb.WriteByte('"')
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		i += size
		switch r {
		case '"':
			w.sb.WriteString(`\"`)
		case '\\':
			w.sb.WriteString(`\\`)
		case '\n':
			w.sb.WriteString(`\n`)
		case '\r':
			w.sb.WriteString(`\r`)
		case '\t':
			w.sb.WriteString(`\t`)
		case '\b':
			w.sb.WriteString(`\b`)
		case '\f':
			w.sb.WriteString(`\f`)
		default:
			switch {
			case r < 0x20:
				fmt.Fprintf(&w.sb, `\u%04x`, r)
			case r < 0x7f || !w.ensureASCII:
				w.sb.WriteRune(r)
			case r <= 0xffff:
				fmt.Fprintf(&w.sb, `\u%04x`, r)
			default:
				r -= 0x10000
				fmt.Fprintf(&w.sb, `\u%04x\u%04x`, 0xd800+(r>>10), 0xdc00+(r&0x3ff))
			}
		}
	}
	w.sb.WriteByte('"')
}

func (w *jsonWriter) encodeItems(items []Item, level int) error {
	if w.o.Sort_keys {
		keys := make([]any, len(items))
		for i, it := range items {
			keys[i] = it.Key
		}
		order, err := Sorted_index(keys)
		if err != nil {
			return err
		}
		sorted := make([]Item, len(items))
		for i, idx := range order {
			sorted[i] = items[idx]
		}
		items = sorted
	}
	if len(items) == 0 {
		w.sb.WriteString("{}")
		return nil
	}
	w.sb.WriteByte('{')
	w.newline(level + 1)
	for i, it := range items {
		if i > 0 {
			w.sb.WriteString(w.o.Item_sep)
			w.newline(level + 1)
		}
		k, err := w.keyText(it.Key)
		if err != nil {
			return err
		}
		w.writeString(k)
		w.sb.WriteString(w.o.Key_sep)
		if err := w.encode(it.Value, level+1); err != nil {
			return err
		}
	}
	w.newline(level)
	w.sb.WriteByte('}')
	return nil
}

func (w *jsonWriter) encodeList(items []any, level int) error {
	if len(items) == 0 {
		w.sb.WriteString("[]")
		return nil
	}
	w.sb.WriteByte('[')
	w.newline(level + 1)
	for i, it := range items {
		if i > 0 {
			w.sb.WriteString(w.o.Item_sep)
			w.newline(level + 1)
		}
		if err := w.encode(it, level+1); err != nil {
			return err
		}
	}
	w.newline(level)
	w.sb.WriteByte(']')
	return nil
}

func (w *jsonWriter) encode(v any, level int) error {
	switch x := v.(type) {
	case nil:
		w.sb.WriteString("null")
	case bool:
		if x {
			w.sb.WriteString("true")
		} else {
			w.sb.WriteString("false")
		}
	case string:
		w.writeString(x)
	case float32:
		t, err := w.floatText(float64(x))
		if err != nil {
			return err
		}
		w.sb.WriteString(t)
	case float64:
		t, err := w.floatText(x)
		if err != nil {
			return err
		}
		w.sb.WriteString(t)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, *big.Int:
		w.sb.WriteString(Repr(x))
	case *Dict:
		return w.encodeItems(x.Items(), level)
	case map[string]any:
		return w.encodeItems(DictFromMap(x).Items(), level)
	case Kw:
		return w.encodeItems(DictFromKw(x).Items(), level)
	case *List:
		return w.encodeList(x.Items(), level)
	case []any:
		return w.encodeList(x, level)
	case Tuple:
		return w.encodeList([]any(x), level)
	case []string:
		return w.encodeList(Iter(x), level)
	case []byte:
		return w.fallback(v, level)
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Ptr:
			// Optional[T] fields are *T in the port.
			if rv.IsNil() {
				w.sb.WriteString("null")
				return nil
			}
			if _, isObj := v.(interface{ Class_() *Class }); !isObj {
				return w.encode(rv.Elem().Interface(), level)
			}
		case reflect.String:
			// Named string kinds (``class X(str, Enum)``) serialize as
			// their value, exactly like a str subclass does in Python.
			w.writeString(rv.String())
			return nil
		case reflect.Bool:
			return w.encode(rv.Bool(), level)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return w.encode(rv.Int(), level)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return w.encode(rv.Uint(), level)
		case reflect.Float32, reflect.Float64:
			return w.encode(rv.Float(), level)
		case reflect.Slice, reflect.Array:
			return w.encodeList(Iter(v), level)
		case reflect.Map:
			if rv.Type().Key().Kind() == reflect.String {
				m := map[string]any{}
				for _, k := range rv.MapKeys() {
					m[k.String()] = rv.MapIndex(k).Interface()
				}
				return w.encodeItems(DictFromMap(m).Items(), level)
			}
		}
		return w.fallback(v, level)
	}
	return nil
}

func (w *jsonWriter) fallback(v any, level int) error {
	if w.o.Default != nil {
		alt, err := w.o.Default(v)
		if err != nil {
			return err
		}
		return w.encode(alt, level)
	}
	return TypeError.Newf("Object of type %s is not JSON serializable", Pytypename(v))
}

// ---------------------------------------------------------------------------
// reader
// ---------------------------------------------------------------------------

// Json_loads is json.loads(s): objects become *Dict (ordered), arrays *List,
// integers int64 (or *big.Int), reals float64; NaN/Infinity accepted.
func Json_loads(s string) (any, error) {
	p := &jsonParser{s: s}
	p.skipWS()
	v, err := p.value()
	if err != nil {
		return nil, err
	}
	p.skipWS()
	if p.i != len(p.s) {
		return nil, p.errorf("Extra data")
	}
	return v, nil
}

// Json_loads_bytes decodes UTF-8 bytes.
func Json_loads_bytes(b []byte) (any, error) { return Json_loads(string(b)) }

type jsonParser struct {
	s string
	i int
}

func (p *jsonParser) errorf(msg string) error {
	line, col := 1, 1
	for j := 0; j < p.i && j < len(p.s); j++ {
		if p.s[j] == '\n' {
			line++
			col = 1
		} else {
			col++
		}
	}
	return JSONDecodeError.Newf("%s: line %d column %d (char %d)", msg, line, col, p.i).
		With("msg", msg).With("pos", p.i).With("lineno", line).With("colno", col)
}

func (p *jsonParser) skipWS() {
	for p.i < len(p.s) {
		switch p.s[p.i] {
		case ' ', '\t', '\n', '\r':
			p.i++
		default:
			return
		}
	}
}

func (p *jsonParser) value() (any, error) {
	if p.i >= len(p.s) {
		return nil, p.errorf("Expecting value")
	}
	switch c := p.s[p.i]; {
	case c == '{':
		return p.object()
	case c == '[':
		return p.array()
	case c == '"':
		return p.str()
	case c == 'n' && strings.HasPrefix(p.s[p.i:], "null"):
		p.i += 4
		return nil, nil
	case c == 't' && strings.HasPrefix(p.s[p.i:], "true"):
		p.i += 4
		return true, nil
	case c == 'f' && strings.HasPrefix(p.s[p.i:], "false"):
		p.i += 5
		return false, nil
	case c == 'N' && strings.HasPrefix(p.s[p.i:], "NaN"):
		p.i += 3
		return NaN, nil
	case c == 'I' && strings.HasPrefix(p.s[p.i:], "Infinity"):
		p.i += 8
		return math.Inf(1), nil
	case c == '-' && strings.HasPrefix(p.s[p.i:], "-Infinity"):
		p.i += 9
		return math.Inf(-1), nil
	case c == '-' || (c >= '0' && c <= '9'):
		return p.number()
	}
	return nil, p.errorf("Expecting value")
}

func (p *jsonParser) number() (any, error) {
	start := p.i
	if p.s[p.i] == '-' {
		p.i++
	}
	if p.i >= len(p.s) {
		return nil, p.errorf("Expecting value")
	}
	if p.s[p.i] == '0' {
		p.i++
	} else if p.s[p.i] >= '1' && p.s[p.i] <= '9' {
		for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
			p.i++
		}
	} else {
		p.i = start
		return nil, p.errorf("Expecting value")
	}
	isFloat := false
	if p.i < len(p.s) && p.s[p.i] == '.' {
		j := p.i + 1
		if j < len(p.s) && p.s[j] >= '0' && p.s[j] <= '9' {
			isFloat = true
			p.i = j
			for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
				p.i++
			}
		}
	}
	if p.i < len(p.s) && (p.s[p.i] == 'e' || p.s[p.i] == 'E') {
		j := p.i + 1
		if j < len(p.s) && (p.s[j] == '+' || p.s[j] == '-') {
			j++
		}
		if j < len(p.s) && p.s[j] >= '0' && p.s[j] <= '9' {
			isFloat = true
			p.i = j
			for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
				p.i++
			}
		}
	}
	text := p.s[start:p.i]
	if isFloat {
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			// overflow -> inf like Python's float()
			if strings.HasPrefix(text, "-") {
				return math.Inf(-1), nil
			}
			return math.Inf(1), nil
		}
		return f, nil
	}
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		return n, nil
	}
	b, ok := new(big.Int).SetString(text, 10)
	if !ok {
		return nil, p.errorf("Expecting value")
	}
	return b, nil
}

func (p *jsonParser) str() (string, error) {
	// p.s[p.i] == '"'
	start := p.i
	p.i++
	var sb strings.Builder
	for {
		if p.i >= len(p.s) {
			p.i = start
			return "", p.errorf("Unterminated string starting at")
		}
		c := p.s[p.i]
		switch {
		case c == '"':
			p.i++
			return sb.String(), nil
		case c == '\\':
			p.i++
			if p.i >= len(p.s) {
				p.i = start
				return "", p.errorf("Unterminated string starting at")
			}
			e := p.s[p.i]
			p.i++
			switch e {
			case '"':
				sb.WriteByte('"')
			case '\\':
				sb.WriteByte('\\')
			case '/':
				sb.WriteByte('/')
			case 'b':
				sb.WriteByte('\b')
			case 'f':
				sb.WriteByte('\f')
			case 'n':
				sb.WriteByte('\n')
			case 'r':
				sb.WriteByte('\r')
			case 't':
				sb.WriteByte('\t')
			case 'u':
				r, err := p.hex4()
				if err != nil {
					return "", err
				}
				if r >= 0xd800 && r <= 0xdbff && strings.HasPrefix(p.s[p.i:], `\u`) {
					save := p.i
					p.i += 2
					r2, err := p.hex4()
					if err == nil && r2 >= 0xdc00 && r2 <= 0xdfff {
						r = 0x10000 + ((r - 0xd800) << 10) + (r2 - 0xdc00)
					} else {
						p.i = save
					}
				}
				sb.WriteRune(rune(r))
			default:
				p.i -= 2
				return "", p.errorf("Invalid \\escape")
			}
		case c < 0x20:
			return "", p.errorf("Invalid control character at")
		default:
			sb.WriteByte(c)
			p.i++
		}
	}
}

func (p *jsonParser) hex4() (int, error) {
	if p.i+4 > len(p.s) {
		return 0, p.errorf("Invalid \\uXXXX escape")
	}
	n, err := strconv.ParseUint(p.s[p.i:p.i+4], 16, 32)
	if err != nil {
		return 0, p.errorf("Invalid \\uXXXX escape")
	}
	p.i += 4
	return int(n), nil
}

func (p *jsonParser) array() (any, error) {
	p.i++ // [
	out := NewList()
	p.skipWS()
	if p.i < len(p.s) && p.s[p.i] == ']' {
		p.i++
		return out, nil
	}
	for {
		p.skipWS()
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		out.Append(v)
		p.skipWS()
		if p.i >= len(p.s) {
			return nil, p.errorf("Expecting ',' delimiter")
		}
		switch p.s[p.i] {
		case ',':
			comma := p.i
			p.i++
			p.skipWS()
			if p.i < len(p.s) && p.s[p.i] == ']' {
				p.i = comma
				return nil, p.errorf("Illegal trailing comma before end of array")
			}
		case ']':
			p.i++
			return out, nil
		default:
			return nil, p.errorf("Expecting ',' delimiter")
		}
	}
}

func (p *jsonParser) object() (any, error) {
	p.i++ // {
	out := NewDict()
	p.skipWS()
	if p.i < len(p.s) && p.s[p.i] == '}' {
		p.i++
		return out, nil
	}
	for {
		p.skipWS()
		if p.i >= len(p.s) || p.s[p.i] != '"' {
			return nil, p.errorf("Expecting property name enclosed in double quotes")
		}
		k, err := p.str()
		if err != nil {
			return nil, err
		}
		p.skipWS()
		if p.i >= len(p.s) || p.s[p.i] != ':' {
			return nil, p.errorf("Expecting ':' delimiter")
		}
		p.i++
		p.skipWS()
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		out.Set(k, v)
		p.skipWS()
		if p.i >= len(p.s) {
			return nil, p.errorf("Expecting ',' delimiter")
		}
		switch p.s[p.i] {
		case ',':
			comma := p.i
			p.i++
			p.skipWS()
			if p.i < len(p.s) && p.s[p.i] == '}' {
				p.i = comma
				return nil, p.errorf("Illegal trailing comma before end of object")
			}
		case '}':
			p.i++
			return out, nil
		default:
			return nil, p.errorf("Expecting ',' delimiter")
		}
	}
}
