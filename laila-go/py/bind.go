package py

import (
	"fmt"
	"strings"
)

// ParamKind mirrors inspect.Parameter.kind.
type ParamKind uint8

const (
	POSITIONAL_ONLY ParamKind = iota
	POSITIONAL_OR_KEYWORD
	VAR_POSITIONAL
	KEYWORD_ONLY
	VAR_KEYWORD
)

// Param is one parameter of a Python signature.
type Param struct {
	Name       string
	Kind       ParamKind
	Default    any
	HasDefault bool
}

// Signature is a Python function signature used to bind (args, kwargs)
// exactly like CPython does, including its TypeError messages.
type Signature struct {
	Name   string
	Params []Param
}

// P declares a required positional-or-keyword parameter.
func P(name string) Param { return Param{Name: name, Kind: POSITIONAL_OR_KEYWORD} }

// PD declares a positional-or-keyword parameter with a default.
func PD(name string, def any) Param {
	return Param{Name: name, Kind: POSITIONAL_OR_KEYWORD, Default: def, HasDefault: true}
}

// PO declares a positional-only parameter (before "/").
func PO(name string) Param { return Param{Name: name, Kind: POSITIONAL_ONLY} }

// KW declares a required keyword-only parameter (after "*").
func KW(name string) Param { return Param{Name: name, Kind: KEYWORD_ONLY} }

// KWD declares a keyword-only parameter with a default.
func KWD(name string, def any) Param {
	return Param{Name: name, Kind: KEYWORD_ONLY, Default: def, HasDefault: true}
}

// VarArgs declares *args.
func VarArgs(name string) Param { return Param{Name: name, Kind: VAR_POSITIONAL} }

// VarKw declares **kwargs.
func VarKw(name string) Param { return Param{Name: name, Kind: VAR_KEYWORD} }

// Sig builds a signature.
func Sig(name string, params ...Param) *Signature { return &Signature{Name: name, Params: params} }

// Bound holds the result of binding: parameter name -> value, plus the
// collected *args (as Tuple) and **kwargs (as Kw) when declared.
type Bound struct {
	Args   map[string]any
	Extra  Tuple
	Kwargs Kw
}

// Get returns a bound value.
func (b *Bound) Get(name string) any { return b.Args[name] }

// Has reports whether name was bound (explicitly or via default).
func (b *Bound) Has(name string) bool { _, ok := b.Args[name]; return ok }

// Bind binds positional args and keyword args to the signature, applying
// defaults, with CPython's error texts.
func (s *Signature) Bind(args []any, kw Kw) (*Bound, error) {
	return s.bind(args, kw, false)
}

// Bind_partial is ``sig.bind_partial(*args, **kwargs)`` followed by
// ``bound.apply_defaults()``: like Bind, but missing parameters are not
// an error (they are simply absent from Args).
func (s *Signature) Bind_partial(args []any, kw Kw) (*Bound, error) {
	return s.bind(args, kw, true)
}

// Call_args is ``(*bound.args, **bound.kwargs)``: the positional and
// keyword arguments that re-invoke the function with the bound values.
func (b *Bound) Call_args(s *Signature) ([]any, Kw) {
	var args []any
	kw := Kw{}
	for _, p := range s.Params {
		switch p.Kind {
		case POSITIONAL_ONLY, POSITIONAL_OR_KEYWORD:
			if v, ok := b.Args[p.Name]; ok {
				args = append(args, v)
			}
		case VAR_POSITIONAL:
			args = append(args, []any(b.Extra)...)
		case KEYWORD_ONLY:
			if v, ok := b.Args[p.Name]; ok {
				kw[p.Name] = v
			}
		case VAR_KEYWORD:
			for k, v := range b.Kwargs {
				kw[k] = v
			}
		}
	}
	return args, kw
}

func (s *Signature) bind(args []any, kw Kw, partial bool) (*Bound, error) {
	b := &Bound{Args: map[string]any{}}
	kw = kw.Copy()
	var positional []Param
	var varPos, varKw *Param
	for i := range s.Params {
		p := &s.Params[i]
		switch p.Kind {
		case POSITIONAL_ONLY, POSITIONAL_OR_KEYWORD:
			positional = append(positional, *p)
		case VAR_POSITIONAL:
			varPos = p
		case VAR_KEYWORD:
			varKw = p
		}
	}
	// positional (overflow is reported after keywords, like CPython)
	for i, a := range args {
		if i < len(positional) {
			b.Args[positional[i].Name] = a
		} else {
			b.Extra = append(b.Extra, a)
		}
	}
	if varPos != nil && b.Extra == nil {
		b.Extra = Tuple{}
	}
	// keywords
	kwonlyGiven := 0
	for _, p := range s.Params {
		if p.Kind == VAR_POSITIONAL || p.Kind == VAR_KEYWORD {
			continue
		}
		v, ok := kw[p.Name]
		if !ok {
			continue
		}
		if p.Kind == POSITIONAL_ONLY {
			if varKw == nil {
				return nil, TypeError.Newf("%s() got some positional-only arguments passed as keyword arguments: '%s'", s.Name, p.Name)
			}
			continue
		}
		if _, dup := b.Args[p.Name]; dup {
			return nil, TypeError.Newf("%s() got multiple values for argument '%s'", s.Name, p.Name)
		}
		if p.Kind == KEYWORD_ONLY {
			kwonlyGiven++
		}
		b.Args[p.Name] = v
		delete(kw, p.Name)
	}
	if len(kw) > 0 {
		if varKw != nil {
			b.Kwargs = kw
		} else {
			name := ""
			for n := range kw {
				if name == "" || n < name {
					name = n
				}
			}
			return nil, TypeError.Newf("%s() got an unexpected keyword argument '%s'", s.Name, name)
		}
	} else if varKw != nil {
		b.Kwargs = Kw{}
	}
	if len(args) > len(positional) && varPos == nil {
		return nil, TypeError.New(s.tooMany(len(positional), len(args), kwonlyGiven))
	}
	// defaults / missing
	var missingPos, missingKw []string
	for _, p := range s.Params {
		if p.Kind == VAR_POSITIONAL || p.Kind == VAR_KEYWORD {
			continue
		}
		if _, ok := b.Args[p.Name]; ok {
			continue
		}
		if p.HasDefault {
			b.Args[p.Name] = p.Default
			continue
		}
		if p.Kind == KEYWORD_ONLY {
			missingKw = append(missingKw, p.Name)
		} else {
			missingPos = append(missingPos, p.Name)
		}
	}
	if partial {
		return b, nil
	}
	if len(missingPos) > 0 {
		return nil, TypeError.New(s.missing("positional", missingPos))
	}
	if len(missingKw) > 0 {
		return nil, TypeError.New(s.missing("keyword-only", missingKw))
	}
	return b, nil
}

func (s *Signature) missing(kind string, names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = "'" + n + "'"
	}
	plural := "argument"
	if len(names) > 1 {
		plural = "arguments"
	}
	var list string
	switch len(quoted) {
	case 1:
		list = quoted[0]
	case 2:
		list = quoted[0] + " and " + quoted[1]
	default:
		list = strings.Join(quoted[:len(quoted)-1], ", ") + ", and " + quoted[len(quoted)-1]
	}
	return fmt.Sprintf("%s() missing %d required %s %s: %s", s.Name, len(names), kind, plural, list)
}

func (s *Signature) tooMany(allowed, given int, kwonlyGiven int) string {
	required := 0
	for _, p := range s.Params {
		if (p.Kind == POSITIONAL_ONLY || p.Kind == POSITIONAL_OR_KEYWORD) && !p.HasDefault {
			required++
		}
	}
	var takes string
	if required == allowed {
		takes = fmt.Sprintf("takes %d positional argument", allowed)
	} else {
		takes = fmt.Sprintf("takes from %d to %d positional argument", required, allowed)
	}
	if allowed != 1 {
		takes += "s"
	}
	msg := fmt.Sprintf("%s() %s but %d", s.Name, takes, given)
	if n := kwonlyGiven; n > 0 {
		msg += fmt.Sprintf(" positional argument%s (and %d keyword-only argument%s)", plural(given), n, plural(n))
	}
	if given == 1 {
		msg += " was given"
	} else {
		msg += " were given"
	}
	return msg
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
