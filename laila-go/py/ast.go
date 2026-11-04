package py

import (
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ast.literal_eval -- safe evaluation of Python literal source text.
//
// Supports the literal subset CPython's ast.literal_eval accepts: None /
// True / False / Ellipsis-less numbers (int, float, with an optional
// leading sign), str and bytes literals (all quote styles, escapes, the
// ``r`` / ``b`` / ``u`` prefixes), and nested tuples / lists / dicts /
// sets. Anything else (names, calls, operators other than a leading
// sign) is a ValueError, exactly like ``malformed node or string``.
//
// Produced values use the port's value model: nil, bool, int (or *big.Int
// on overflow), float64, string, []byte, Tuple, *List, *Dict, *Set.

// Literal_eval is ast.literal_eval(text).
func Literal_eval(text string) (any, error) {
	p := &literalParser{src: text}
	p.skipSpace()
	v, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.pos != len(p.src) {
		return nil, p.malformed()
	}
	return v, nil
}

type literalParser struct {
	src string
	pos int
}

func (p *literalParser) malformed() error {
	return ValueError.New("malformed node or string")
}

func (p *literalParser) skipSpace() {
	for p.pos < len(p.src) {
		switch p.src[p.pos] {
		case ' ', '\t', '\n', '\r', '\f', '\v':
			p.pos++
		default:
			return
		}
	}
}

func (p *literalParser) peek() byte {
	if p.pos >= len(p.src) {
		return 0
	}
	return p.src[p.pos]
}

func (p *literalParser) hasPrefix(s string) bool { return strings.HasPrefix(p.src[p.pos:], s) }

func isIdentChar(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func (p *literalParser) parseValue() (any, error) {
	p.skipSpace()
	c := p.peek()
	switch {
	case c == 0:
		return nil, p.malformed()
	case c == '(':
		return p.parseTuple()
	case c == '[':
		return p.parseList()
	case c == '{':
		return p.parseDictOrSet()
	case c == '\'' || c == '"':
		return p.parseString(false, false)
	case c == '-' || c == '+':
		p.pos++
		p.skipSpace()
		v, err := p.parseNumber()
		if err != nil {
			return nil, err
		}
		if c == '-' {
			switch x := v.(type) {
			case int:
				return -x, nil
			case *big.Int:
				return new(big.Int).Neg(x), nil
			case float64:
				return -x, nil
			}
		}
		return v, nil
	case c >= '0' && c <= '9' || c == '.':
		return p.parseNumber()
	}
	// identifiers: None / True / False / string prefixes
	start := p.pos
	for p.pos < len(p.src) && isIdentChar(p.src[p.pos]) {
		p.pos++
	}
	word := p.src[start:p.pos]
	switch word {
	case "None":
		return nil, nil
	case "True":
		return true, nil
	case "False":
		return false, nil
	}
	lw := strings.ToLower(word)
	if len(lw) <= 2 && (p.peek() == '\'' || p.peek() == '"') {
		isBytes := strings.Contains(lw, "b")
		isRaw := strings.Contains(lw, "r")
		valid := lw == "b" || lw == "r" || lw == "u" || lw == "br" || lw == "rb"
		if valid {
			return p.parseString(isBytes, isRaw)
		}
	}
	return nil, p.malformed()
}

func (p *literalParser) parseNumber() (any, error) {
	start := p.pos
	isFloat := false
	// prefixes: 0x 0o 0b
	if p.hasPrefix("0x") || p.hasPrefix("0X") || p.hasPrefix("0o") || p.hasPrefix("0O") || p.hasPrefix("0b") || p.hasPrefix("0B") {
		p.pos += 2
		for p.pos < len(p.src) && (isIdentChar(p.src[p.pos])) {
			p.pos++
		}
		txt := strings.ReplaceAll(p.src[start:p.pos], "_", "")
		n, ok := new(big.Int).SetString(txt, 0)
		if !ok {
			return nil, p.malformed()
		}
		return normInt(n), nil
	}
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		if c >= '0' && c <= '9' || c == '_' {
			p.pos++
			continue
		}
		if c == '.' {
			isFloat = true
			p.pos++
			continue
		}
		if c == 'e' || c == 'E' {
			isFloat = true
			p.pos++
			if p.peek() == '+' || p.peek() == '-' {
				p.pos++
			}
			continue
		}
		if c == 'j' || c == 'J' {
			return nil, ValueError.New("complex literals are not supported by the port")
		}
		break
	}
	txt := strings.ReplaceAll(p.src[start:p.pos], "_", "")
	if txt == "" || txt == "." {
		return nil, p.malformed()
	}
	if isFloat {
		f, err := strconv.ParseFloat(txt, 64)
		if err != nil {
			return nil, p.malformed()
		}
		return f, nil
	}
	n, ok := new(big.Int).SetString(txt, 10)
	if !ok {
		return nil, p.malformed()
	}
	return normInt(n), nil
}

func normInt(n *big.Int) any {
	if n.IsInt64() {
		v := n.Int64()
		if int64(int(v)) == v {
			return int(v)
		}
	}
	return n
}

// parseString parses one or more adjacent string literals (implicit
// concatenation), honouring triple quotes.
func (p *literalParser) parseString(isBytes, isRaw bool) (any, error) {
	var sb strings.Builder
	for {
		q := p.peek()
		if q != '\'' && q != '"' {
			break
		}
		triple := p.hasPrefix(string([]byte{q, q, q}))
		if triple {
			p.pos += 3
		} else {
			p.pos++
		}
		for {
			if p.pos >= len(p.src) {
				return nil, p.malformed()
			}
			c := p.src[p.pos]
			if c == q {
				if !triple {
					p.pos++
					break
				}
				if p.hasPrefix(string([]byte{q, q, q})) {
					p.pos += 3
					break
				}
				sb.WriteByte(c)
				p.pos++
				continue
			}
			if c == '\\' && !isRaw {
				p.pos++
				if p.pos >= len(p.src) {
					return nil, p.malformed()
				}
				if err := p.parseEscape(&sb, isBytes); err != nil {
					return nil, err
				}
				continue
			}
			if c == '\\' && isRaw {
				sb.WriteByte(c)
				p.pos++
				if p.pos < len(p.src) {
					sb.WriteByte(p.src[p.pos])
					p.pos++
				}
				continue
			}
			if !triple && c == '\n' {
				return nil, p.malformed()
			}
			sb.WriteByte(c)
			p.pos++
		}
		// implicit concatenation: skip whitespace and look for another
		// literal (with an optional prefix of the same kind)
		save := p.pos
		p.skipSpace()
		start := p.pos
		for p.pos < len(p.src) && isIdentChar(p.src[p.pos]) {
			p.pos++
		}
		prefix := strings.ToLower(p.src[start:p.pos])
		nq := p.peek()
		if (nq == '\'' || nq == '"') && (prefix == "" || prefix == "b" || prefix == "r" || prefix == "u" || prefix == "br" || prefix == "rb") {
			if strings.Contains(prefix, "b") != isBytes {
				return nil, p.malformed()
			}
			isRaw = strings.Contains(prefix, "r")
			continue
		}
		p.pos = save
		break
	}
	if isBytes {
		return []byte(sb.String()), nil
	}
	return sb.String(), nil
}

func (p *literalParser) parseEscape(sb *strings.Builder, isBytes bool) error {
	c := p.src[p.pos]
	p.pos++
	switch c {
	case '\n':
		return nil
	case '\\':
		sb.WriteByte('\\')
	case '\'':
		sb.WriteByte('\'')
	case '"':
		sb.WriteByte('"')
	case 'a':
		sb.WriteByte('\a')
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
	case 'v':
		sb.WriteByte('\v')
	case 'x':
		if p.pos+2 > len(p.src) {
			return p.malformed()
		}
		n, err := strconv.ParseUint(p.src[p.pos:p.pos+2], 16, 8)
		if err != nil {
			return p.malformed()
		}
		p.pos += 2
		if isBytes {
			sb.WriteByte(byte(n))
		} else {
			writeRune(sb, rune(n))
		}
	case 'u', 'U':
		if isBytes {
			sb.WriteByte('\\')
			sb.WriteByte(c)
			return nil
		}
		width := 4
		if c == 'U' {
			width = 8
		}
		if p.pos+width > len(p.src) {
			return p.malformed()
		}
		n, err := strconv.ParseUint(p.src[p.pos:p.pos+width], 16, 32)
		if err != nil {
			return p.malformed()
		}
		p.pos += width
		writeRune(sb, rune(n))
	case 'N':
		return ValueError.New("\\N{...} escapes are not supported by the port")
	default:
		if c >= '0' && c <= '7' {
			start := p.pos - 1
			for p.pos < len(p.src) && p.pos-start < 3 && p.src[p.pos] >= '0' && p.src[p.pos] <= '7' {
				p.pos++
			}
			n, _ := strconv.ParseUint(p.src[start:p.pos], 8, 32)
			if isBytes {
				sb.WriteByte(byte(n))
			} else {
				writeRune(sb, rune(n))
			}
			return nil
		}
		// unknown escape: kept verbatim (Python emits a warning)
		sb.WriteByte('\\')
		sb.WriteByte(c)
	}
	return nil
}

func writeRune(sb *strings.Builder, r rune) {
	var buf [utf8.UTFMax]byte
	n := utf8.EncodeRune(buf[:], r)
	sb.Write(buf[:n])
}

func (p *literalParser) parseTuple() (any, error) {
	p.pos++ // (
	out := Tuple{}
	p.skipSpace()
	if p.peek() == ')' {
		p.pos++
		return out, nil
	}
	first, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.peek() == ')' {
		// parenthesised expression, not a tuple
		p.pos++
		return first, nil
	}
	out = append(out, first)
	for {
		p.skipSpace()
		if p.peek() == ')' {
			p.pos++
			return out, nil
		}
		if p.peek() != ',' {
			return nil, p.malformed()
		}
		p.pos++
		p.skipSpace()
		if p.peek() == ')' {
			p.pos++
			return out, nil
		}
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
}

func (p *literalParser) parseList() (any, error) {
	p.pos++ // [
	out := NewList()
	for {
		p.skipSpace()
		if p.peek() == ']' {
			p.pos++
			return out, nil
		}
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		out.Append(v)
		p.skipSpace()
		switch p.peek() {
		case ',':
			p.pos++
		case ']':
			p.pos++
			return out, nil
		default:
			return nil, p.malformed()
		}
	}
}

func (p *literalParser) parseDictOrSet() (any, error) {
	p.pos++ // {
	p.skipSpace()
	if p.peek() == '}' {
		p.pos++
		return NewDict(), nil
	}
	first, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.peek() == ':' {
		d := NewDict()
		key := first
		for {
			p.pos++ // :
			v, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			d.Set(key, v)
			p.skipSpace()
			switch p.peek() {
			case ',':
				p.pos++
				p.skipSpace()
				if p.peek() == '}' {
					p.pos++
					return d, nil
				}
				key, err = p.parseValue()
				if err != nil {
					return nil, err
				}
				p.skipSpace()
				if p.peek() != ':' {
					return nil, p.malformed()
				}
			case '}':
				p.pos++
				return d, nil
			default:
				return nil, p.malformed()
			}
		}
	}
	s := NewSet(first)
	for {
		p.skipSpace()
		switch p.peek() {
		case ',':
			p.pos++
			p.skipSpace()
			if p.peek() == '}' {
				p.pos++
				return s, nil
			}
			v, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			s.Add(v)
		case '}':
			p.pos++
			return s, nil
		default:
			return nil, p.malformed()
		}
	}
}
