package py

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"testing"
)

type vecValue struct {
	T string          `json:"t"`
	V json.RawMessage `json:"v"`
}

type vecResult struct {
	Ok  *string `json:"ok"`
	Err string  `json:"err"`
	Msg string  `json:"msg"`
}

type vecFile struct {
	Python         string `json:"python"`
	PickleProtocol int    `json:"pickle_protocol"`
	Values         []struct {
		Value         vecValue             `json:"value"`
		Repr          string               `json:"repr"`
		Json          map[string]vecResult `json:"json"`
		MsgpackStrict vecResult            `json:"msgpack_strict"`
		MsgpackLax    vecResult            `json:"msgpack_lax"`
		Pickle        vecResult            `json:"pickle"`
	} `json:"values"`
	JsonLoads []struct {
		Text  string    `json:"text"`
		Value *vecValue `json:"value"`
		Repr  string    `json:"repr"`
		Err   string    `json:"err"`
		Msg   string    `json:"msg"`
	} `json:"json_loads"`
	EncodeLong []struct {
		N   string `json:"n"`
		Hex string `json:"hex"`
	} `json:"encode_long"`
	Quote []struct {
		S          string `json:"s"`
		Quote      string `json:"quote"`
		QuoteSlash string `json:"quote_slash"`
		Unquote    string `json:"unquote"`
	} `json:"quote"`
	Bind []struct {
		Fn     string              `json:"fn"`
		Args   []vecValue          `json:"args"`
		Kwargs map[string]vecValue `json:"kwargs"`
		Bound  map[string]vecValue `json:"bound"`
		Err    string              `json:"err"`
	} `json:"bind"`
	Fernet struct {
		Key   string `json:"key"`
		Cases []struct {
			Msg   string `json:"msg"`
			Time  int64  `json:"time"`
			Iv    string `json:"iv"`
			Token string `json:"token"`
			Ts    int64  `json:"ts"`
		} `json:"cases"`
	} `json:"fernet"`
	Npy []struct {
		Dtype string `json:"dtype"`
		Shape []int  `json:"shape"`
		Data  string `json:"data"`
		Npy   string `json:"npy"`
	} `json:"npy"`
	Uuid []struct {
		Name  string `json:"name"`
		Uuid5 string `json:"uuid5"`
		Uuid3 string `json:"uuid3"`
	} `json:"uuid"`
	Exceptions []struct {
		Type string     `json:"type"`
		Args []vecValue `json:"args"`
		Str  string     `json:"str"`
		Repr string     `json:"repr"`
	} `json:"exceptions"`
}

func loadVectors(t *testing.T) *vecFile {
	t.Helper()
	b, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Skipf("vectors not generated: %v", err)
	}
	var vf vecFile
	if err := json.Unmarshal(b, &vf); err != nil {
		t.Fatal(err)
	}
	return &vf
}

func decodeValue(t *testing.T, v vecValue) any {
	t.Helper()
	switch v.T {
	case "none":
		return nil
	case "bool":
		var b bool
		_ = json.Unmarshal(v.V, &b)
		return b
	case "int":
		var s string
		_ = json.Unmarshal(v.V, &s)
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
		b, _ := new(big.Int).SetString(s, 10)
		return b
	case "float":
		var s string
		_ = json.Unmarshal(v.V, &s)
		switch s {
		case "nan":
			return NaN
		case "inf":
			return math.Inf(1)
		case "-inf":
			return math.Inf(-1)
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Fatal(err)
		}
		return f
	case "str":
		var s string
		_ = json.Unmarshal(v.V, &s)
		return s
	case "bytes":
		var s string
		_ = json.Unmarshal(v.V, &s)
		b, _ := hex.DecodeString(s)
		if b == nil {
			b = []byte{}
		}
		return b
	case "list", "tuple", "set", "frozenset":
		var items []vecValue
		_ = json.Unmarshal(v.V, &items)
		out := make([]any, len(items))
		for i, it := range items {
			out[i] = decodeValue(t, it)
		}
		switch v.T {
		case "list":
			return NewList(out...)
		case "tuple":
			return Tuple(out)
		case "set":
			return NewSet(out...)
		default:
			return FrozenSet{NewSet(out...)}
		}
	case "dict":
		var pairs [][2]vecValue
		_ = json.Unmarshal(v.V, &pairs)
		d := NewDict()
		for _, p := range pairs {
			d.Set(decodeValue(t, p[0]), decodeValue(t, p[1]))
		}
		return d
	}
	t.Fatalf("unknown tag %s", v.T)
	return nil
}

func short(s string) string {
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}

func TestVectors_Repr(t *testing.T) {
	vf := loadVectors(t)
	for i, c := range vf.Values {
		v := decodeValue(t, c.Value)
		if c.Value.T == "set" || c.Value.T == "frozenset" {
			continue // Python set order is hash-dependent
		}
		if got := Repr(v); got != c.Repr {
			t.Errorf("[%d] repr mismatch\n got: %s\nwant: %s", i, short(got), short(c.Repr))
		}
	}
}

func TestVectors_JsonDumps(t *testing.T) {
	vf := loadVectors(t)
	indent := 2
	variants := map[string]JsonOpts{
		"default": {}, "compact": {Item_sep: ",", Key_sep: ":"}, "sorted": {Sort_keys: true},
		"indent2": {Indent: &indent}, "noascii": {Ensure_ascii: new(bool)},
	}
	for i, c := range vf.Values {
		v := decodeValue(t, c.Value)
		if c.Value.T == "set" || c.Value.T == "frozenset" {
			continue
		}
		for name, opts := range variants {
			want := c.Json[name]
			got, err := Json_dumps_opts(v, opts)
			if want.Ok == nil {
				if err == nil {
					t.Errorf("[%d] json %s: expected %s, got %s", i, name, want.Err, short(got))
				} else if Typename(err) != want.Err || err.Error() != want.Msg {
					t.Errorf("[%d] json %s: error mismatch got %s(%s) want %s(%s)", i, name, Typename(err), err.Error(), want.Err, want.Msg)
				}
				continue
			}
			if err != nil {
				t.Errorf("[%d] json %s: unexpected error %v", i, name, err)
				continue
			}
			if got != *want.Ok {
				t.Errorf("[%d] json %s mismatch\n got: %s\nwant: %s", i, name, short(got), short(*want.Ok))
			}
		}
	}
}

func TestVectors_JsonLoads(t *testing.T) {
	vf := loadVectors(t)
	for _, c := range vf.JsonLoads {
		got, err := Json_loads(c.Text)
		if c.Err != "" {
			if err == nil {
				t.Errorf("loads(%q): expected %s, got %s", c.Text, c.Err, Repr(got))
			} else if Typename(err) != c.Err || err.Error() != c.Msg {
				t.Errorf("loads(%q): got %s(%s) want %s(%s)", c.Text, Typename(err), err.Error(), c.Err, c.Msg)
			}
			continue
		}
		if err != nil {
			t.Errorf("loads(%q): unexpected error %v", c.Text, err)
			continue
		}
		if r := Repr(got); r != c.Repr {
			t.Errorf("loads(%q): repr %s want %s", c.Text, r, c.Repr)
		}
		if !Eq(got, decodeValue(t, *c.Value)) && !(c.Value.T == "float" && strings.Contains(c.Repr, "nan")) {
			t.Errorf("loads(%q): value mismatch", c.Text)
		}
	}
}

func TestVectors_Msgpack(t *testing.T) {
	vf := loadVectors(t)
	for i, c := range vf.Values {
		v := decodeValue(t, c.Value)
		if c.Value.T == "set" || c.Value.T == "frozenset" {
			continue
		}
		for name, want := range map[string]vecResult{"strict": c.MsgpackStrict, "lax": c.MsgpackLax} {
			got, err := Msgpack_packb(v, MsgpackOpts{Strict_types: name == "strict"})
			if want.Ok == nil {
				if err == nil {
					t.Errorf("[%d] msgpack %s: expected %s", i, name, want.Err)
				} else if Typename(err) != want.Err || err.Error() != want.Msg {
					t.Errorf("[%d] msgpack %s: got %s(%s) want %s(%s)", i, name, Typename(err), err.Error(), want.Err, want.Msg)
				}
				continue
			}
			if err != nil {
				t.Errorf("[%d] msgpack %s: unexpected %v", i, name, err)
				continue
			}
			if hex.EncodeToString(got) != *want.Ok {
				t.Errorf("[%d] msgpack %s mismatch\n got: %s\nwant: %s", i, name, short(hex.EncodeToString(got)), short(*want.Ok))
				continue
			}
			back, err := Msgpack_unpackb(got)
			if err != nil {
				if name == "lax" && tupleish(v) && IsInstance(err, TypeError) {
					continue // Python also fails: tuple keys become unhashable lists
				}
				t.Errorf("[%d] msgpack %s: unpack error %v", i, name, err)
				continue
			}
			if c.Value.T != "tuple" && !containsNaN(v) && !Eq(back, v) && !tupleish(v) {
				t.Errorf("[%d] msgpack %s: round trip mismatch %s vs %s", i, name, short(Repr(back)), short(Repr(v)))
			}
		}
	}
}

func tupleish(v any) bool { return strings.Contains(Repr(v), "(") }

func containsNaN(v any) bool { return strings.Contains(Repr(v), "nan") }

func TestVectors_Pickle(t *testing.T) {
	vf := loadVectors(t)
	if vf.PickleProtocol != pickleDefaultProtocol {
		t.Fatalf("python default protocol %d != port default %d", vf.PickleProtocol, pickleDefaultProtocol)
	}
	for i, c := range vf.Values {
		v := decodeValue(t, c.Value)
		want := c.Pickle
		got, err := Pickle_dumps(v)
		if want.Ok == nil {
			if err == nil {
				t.Errorf("[%d] pickle: expected %s", i, want.Err)
			}
			continue
		}
		if err != nil {
			t.Errorf("[%d] pickle: unexpected %v", i, err)
			continue
		}
		if c.Value.T != "set" && c.Value.T != "frozenset" && hex.EncodeToString(got) != *want.Ok {
			t.Errorf("[%d] pickle mismatch for %s\n got: %s\nwant: %s", i, short(c.Repr), short(hex.EncodeToString(got)), short(*want.Ok))
		}
		// our bytes decode back to the same value
		back, err := Pickle_loads(got)
		if err != nil {
			t.Errorf("[%d] pickle: loads(ours) error %v", i, err)
		} else if !containsNaN(v) && !Eq(back, v) {
			t.Errorf("[%d] pickle: round trip mismatch %s vs %s", i, short(Repr(back)), short(Repr(v)))
		}
		// python's bytes decode to the same value
		pb, _ := hex.DecodeString(*want.Ok)
		back, err = Pickle_loads(pb)
		if err != nil {
			t.Errorf("[%d] pickle: loads(python) error %v", i, err)
		} else if !containsNaN(v) && !Eq(back, v) {
			t.Errorf("[%d] pickle: python bytes decode mismatch %s vs %s", i, short(Repr(back)), short(Repr(v)))
		}
	}
}

func TestVectors_EncodeLong(t *testing.T) {
	vf := loadVectors(t)
	for _, c := range vf.EncodeLong {
		n, _ := new(big.Int).SetString(c.N, 10)
		if got := hex.EncodeToString(Encode_long(n)); got != c.Hex {
			t.Errorf("encode_long(%s) = %s want %s", c.N, got, c.Hex)
		}
		b, _ := hex.DecodeString(c.Hex)
		if Decode_long(b).Cmp(n) != 0 {
			t.Errorf("decode_long(%s) != %s", c.Hex, c.N)
		}
	}
}

func TestVectors_Quote(t *testing.T) {
	vf := loadVectors(t)
	for _, c := range vf.Quote {
		if got := Quote(c.S, ""); got != c.Quote {
			t.Errorf("quote(%q, safe='') = %q want %q", c.S, got, c.Quote)
		}
		if got := Quote(c.S, "/"); got != c.QuoteSlash {
			t.Errorf("quote(%q) = %q want %q", c.S, got, c.QuoteSlash)
		}
		if got := Unquote(c.Quote); got != c.Unquote {
			t.Errorf("unquote(%q) = %q want %q", c.Quote, got, c.Unquote)
		}
	}
}

func TestVectors_Bind(t *testing.T) {
	vf := loadVectors(t)
	sigs := map[string]*Signature{
		"f1": Sig("f1", P("a"), P("b"), PD("c", int64(3))),
		"f2": Sig("f2", P("a"), VarArgs("args"), KW("k"), VarKw("kw")),
		"f3": Sig("f3", PO("a"), PO("b"), P("c"), KW("d"), KWD("e", int64(5))),
	}
	for _, c := range vf.Bind {
		args := make([]any, len(c.Args))
		for i, a := range c.Args {
			args[i] = decodeValue(t, a)
		}
		kw := Kw{}
		for k, v := range c.Kwargs {
			kw[k] = decodeValue(t, v)
		}
		b, err := sigs[c.Fn].Bind(args, kw)
		if c.Err != "" {
			if err == nil {
				t.Errorf("%s%v%v: expected %q", c.Fn, args, kw, c.Err)
			} else if err.Error() != c.Err {
				t.Errorf("%s%v%v:\n got %q\nwant %q", c.Fn, args, kw, err.Error(), c.Err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s%v%v: unexpected %v", c.Fn, args, kw, err)
			continue
		}
		for name, want := range c.Bound {
			var got any
			switch {
			case name == "args" && c.Fn == "f2":
				got = b.Extra
			case name == "kw" && c.Fn == "f2":
				got = DictFromKw(b.Kwargs)
			default:
				got = b.Get(name)
			}
			if !Eq(got, decodeValue(t, want)) {
				t.Errorf("%s: bound %s = %s want %s", c.Fn, name, Repr(got), Repr(decodeValue(t, want)))
			}
		}
	}
}

func TestVectors_Fernet(t *testing.T) {
	vf := loadVectors(t)
	f, err := New_Fernet(vf.Fernet.Key)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range vf.Fernet.Cases {
		msg, _ := hex.DecodeString(c.Msg)
		iv, _ := hex.DecodeString(c.Iv)
		tok, err := f.Encrypt_at_time(msg, c.Time, iv)
		if err != nil {
			t.Fatal(err)
		}
		if string(tok) != c.Token {
			t.Errorf("fernet token mismatch for %q\n got %s\nwant %s", msg, tok, c.Token)
		}
		back, err := f.Decrypt_at_time(c.Token, -1, c.Time+10)
		if err != nil || string(back) != string(msg) {
			t.Errorf("fernet decrypt: %v %q", err, back)
		}
		if ts, _ := f.Extract_timestamp(c.Token); ts != c.Ts {
			t.Errorf("fernet timestamp %d want %d", ts, c.Ts)
		}
		if _, err := f.Decrypt_at_time(c.Token, 5, c.Time+100); !IsInstance(err, InvalidToken) {
			t.Errorf("fernet ttl: expected InvalidToken, got %v", err)
		}
	}
	if _, err := New_Fernet("short"); !IsInstance(err, ValueError) {
		t.Errorf("bad key: %v", err)
	}
}

func TestVectors_Npy(t *testing.T) {
	vf := loadVectors(t)
	for _, c := range vf.Npy {
		data, _ := hex.DecodeString(c.Data)
		if data == nil {
			data = []byte{}
		}
		a := &NDArray{Dtype: c.Dtype, Shape: c.Shape, Data: data}
		got, err := Npy_save(a)
		if err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(got) != c.Npy {
			t.Errorf("npy %s %v mismatch\n got %s\nwant %s", c.Dtype, c.Shape, short(hex.EncodeToString(got)), short(c.Npy))
		}
		want, _ := hex.DecodeString(c.Npy)
		back, err := Npy_load(want)
		if err != nil {
			t.Errorf("npy load: %v", err)
			continue
		}
		if !back.Eq(a) {
			t.Errorf("npy round trip mismatch: %s vs %s", back.Repr(), a.Repr())
		}
	}
}

func TestVectors_Uuid(t *testing.T) {
	vf := loadVectors(t)
	for _, c := range vf.Uuid {
		if got := Uuid5(NAMESPACE_DNS, c.Name).String(); got != c.Uuid5 {
			t.Errorf("uuid5(%q) = %s want %s", c.Name, got, c.Uuid5)
		}
		if got := Uuid3(NAMESPACE_DNS, c.Name).String(); got != c.Uuid3 {
			t.Errorf("uuid3(%q) = %s want %s", c.Name, got, c.Uuid3)
		}
	}
}

func TestVectors_Exceptions(t *testing.T) {
	vf := loadVectors(t)
	types := map[string]*Type{"KeyError": KeyError, "ValueError": ValueError, "TypeError": TypeError, "RuntimeError": RuntimeError}
	for _, c := range vf.Exceptions {
		args := make([]any, len(c.Args))
		for i, a := range c.Args {
			args[i] = decodeValue(t, a)
		}
		e := types[c.Type].New(args...)
		if e.Error() != c.Str {
			t.Errorf("str(%s%v) = %q want %q", c.Type, args, e.Error(), c.Str)
		}
		if e.Repr() != c.Repr {
			t.Errorf("repr(%s%v) = %q want %q", c.Type, args, e.Repr(), c.Repr)
		}
	}
}
