package py

import (
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Reprer is implemented by objects with a custom __repr__.
type Reprer interface{ Repr() string }

// Stringer is implemented by objects with a custom __str__ (Go's fmt.Stringer).
type Stringer interface{ String() string }

// Repr_float formats a float64 exactly like Python's repr(float):
// shortest round-trip digits, exponent form when exp < -4 or exp >= 16,
// always a ".0" on integral values, "inf"/"-inf"/"nan".
func Repr_float(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case f == 0:
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}
	// shortest repr digits via 'e' format: d.ddddde±XX
	s := strconv.FormatFloat(f, 'e', -1, 64)
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}
	epos := strings.IndexByte(s, 'e')
	mant, expStr := s[:epos], s[epos+1:]
	exp, _ := strconv.Atoi(expStr)
	digits := strings.Replace(mant, ".", "", 1)
	// strip trailing zeros from digits (keep at least one)
	digits = strings.TrimRight(digits, "0")
	if digits == "" {
		digits = "0"
	}
	var out string
	if exp < -4 || exp >= 16 {
		// exponent form: d[.ddd]e±XX
		if len(digits) > 1 {
			out = digits[:1] + "." + digits[1:]
		} else {
			out = digits
		}
		sign := "+"
		if exp < 0 {
			sign = "-"
			exp = -exp
		}
		out += fmt.Sprintf("e%s%02d", sign, exp)
	} else if exp >= 0 {
		// fixed: integer part has exp+1 digits
		if len(digits) <= exp+1 {
			out = digits + strings.Repeat("0", exp+1-len(digits)) + ".0"
		} else {
			out = digits[:exp+1] + "." + digits[exp+1:]
		}
	} else {
		// 0.000ddd
		out = "0." + strings.Repeat("0", -exp-1) + digits
	}
	if neg {
		out = "-" + out
	}
	return out
}

// Repr_str formats a string exactly like Python's repr(str).
func Repr_str(s string) string {
	quote := byte('\'')
	if strings.IndexByte(s, '\'') >= 0 && strings.IndexByte(s, '"') < 0 {
		quote = '"'
	}
	var sb strings.Builder
	sb.WriteByte(quote)
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			sb.WriteString(fmt.Sprintf("\\x%02x", s[i]))
			i++
			continue
		}
		i += size
		switch {
		case r == rune(quote):
			sb.WriteByte('\\')
			sb.WriteByte(quote)
		case r == '\\':
			sb.WriteString("\\\\")
		case r == '\n':
			sb.WriteString("\\n")
		case r == '\r':
			sb.WriteString("\\r")
		case r == '\t':
			sb.WriteString("\\t")
		case r < 0x20 || r == 0x7f:
			sb.WriteString(fmt.Sprintf("\\x%02x", r))
		case r < 0x7f:
			sb.WriteRune(r)
		case unicode.IsPrint(r):
			sb.WriteRune(r)
		case r <= 0xff:
			sb.WriteString(fmt.Sprintf("\\x%02x", r))
		case r <= 0xffff:
			sb.WriteString(fmt.Sprintf("\\u%04x", r))
		default:
			sb.WriteString(fmt.Sprintf("\\U%08x", r))
		}
	}
	sb.WriteByte(quote)
	return sb.String()
}

// Repr_bytes formats bytes exactly like Python's repr(bytes).
func Repr_bytes(b []byte) string {
	quote := byte('\'')
	hasSingle, hasDouble := false, false
	for _, c := range b {
		if c == '\'' {
			hasSingle = true
		} else if c == '"' {
			hasDouble = true
		}
	}
	if hasSingle && !hasDouble {
		quote = '"'
	}
	var sb strings.Builder
	sb.WriteByte('b')
	sb.WriteByte(quote)
	for _, c := range b {
		switch {
		case c == quote:
			sb.WriteByte('\\')
			sb.WriteByte(c)
		case c == '\\':
			sb.WriteString("\\\\")
		case c == '\n':
			sb.WriteString("\\n")
		case c == '\r':
			sb.WriteString("\\r")
		case c == '\t':
			sb.WriteString("\\t")
		case c < 0x20 || c >= 0x7f:
			sb.WriteString(fmt.Sprintf("\\x%02x", c))
		default:
			sb.WriteByte(c)
		}
	}
	sb.WriteByte(quote)
	return sb.String()
}

// Repr is Python repr(v) over the value model.
func Repr(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case bool:
		if x {
			return "True"
		}
		return "False"
	case int:
		return strconv.Itoa(x)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint8:
		return strconv.FormatUint(uint64(x), 10)
	case uint16:
		return strconv.FormatUint(uint64(x), 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case *big.Int:
		return x.String()
	case float32:
		return Repr_float(float64(x))
	case float64:
		return Repr_float(x)
	case string:
		return Repr_str(x)
	case []byte:
		return Repr_bytes(x)
	case Tuple:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = Repr(e)
		}
		if len(x) == 1 {
			return "(" + parts[0] + ",)"
		}
		return "(" + strings.Join(parts, ", ") + ")"
	case []any:
		return Repr(NewList(x...))
	case *List:
		items := x.Items()
		parts := make([]string, len(items))
		for i, e := range items {
			parts[i] = Repr(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case *Dict:
		items := x.Items()
		parts := make([]string, len(items))
		for i, it := range items {
			parts[i] = Repr(it.Key) + ": " + Repr(it.Value)
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case map[string]any:
		return Repr(DictFromMap(x))
	case Kw:
		return Repr(DictFromKw(x))
	case *Set:
		items := x.Items()
		if len(items) == 0 {
			return "set()"
		}
		parts := make([]string, len(items))
		for i, e := range items {
			parts[i] = Repr(e)
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case FrozenSet:
		if x.Len() == 0 {
			return "frozenset()"
		}
		return "frozenset(" + Repr(x.Set) + ")"
	case []string:
		out := make([]any, len(x))
		for i, s := range x {
			out[i] = s
		}
		return Repr(NewList(out...))
	case Reprer:
		return x.Repr()
	case *Error:
		return x.Repr()
	case *Type:
		return "<class '" + x.Name + "'>"
	}
	if s, ok := v.(interface{ Class_() *Class }); ok {
		return fmt.Sprintf("<%s object at %#x>", s.Class_().Name, reflect.ValueOf(v).Pointer())
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		return Repr(Iter(v))
	case reflect.Ptr:
		if rv.IsNil() {
			return "None"
		}
		return fmt.Sprintf("<%s object at %#x>", reflect.TypeOf(v).Elem().Name(), rv.Pointer())
	}
	return fmt.Sprintf("%v", v)
}

// Repr_type is ``repr(type(v))`` -> ``<class 'name'>``.
func Repr_type(v any) string { return "<class '" + Pytypename(v) + "'>" }

// Str is Python str(v): strings verbatim, objects via __str__, else repr.
func Str(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case *Error:
		return x.Error()
	case Stringer:
		return x.String()
	case error:
		return x.Error()
	}
	return Repr(v)
}

// Format is Python's str.format for the common positional/named cases used
// in translated f-strings: "{}", "{0}", "{name}", "{x!r}", "{x:...}" is not
// supported beyond !r/!s conversions.
func Format(tpl string, args []any, kw Kw) string {
	var sb strings.Builder
	auto := 0
	for i := 0; i < len(tpl); i++ {
		c := tpl[i]
		if c == '{' {
			if i+1 < len(tpl) && tpl[i+1] == '{' {
				sb.WriteByte('{')
				i++
				continue
			}
			end := strings.IndexByte(tpl[i:], '}')
			if end < 0 {
				sb.WriteString(tpl[i:])
				break
			}
			field := tpl[i+1 : i+end]
			i += end
			conv := ""
			if p := strings.IndexByte(field, '!'); p >= 0 {
				conv = field[p+1:]
				field = field[:p]
			}
			var val any
			if field == "" {
				if auto < len(args) {
					val = args[auto]
				}
				auto++
			} else if n, err := strconv.Atoi(field); err == nil {
				if n < len(args) {
					val = args[n]
				}
			} else {
				val = kw[field]
			}
			if conv == "r" {
				sb.WriteString(Repr(val))
			} else {
				sb.WriteString(Str(val))
			}
			continue
		}
		if c == '}' && i+1 < len(tpl) && tpl[i+1] == '}' {
			sb.WriteByte('}')
			i++
			continue
		}
		sb.WriteByte(c)
	}
	return sb.String()
}
