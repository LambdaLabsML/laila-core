package py

import (
	"encoding/binary"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

// pickle opcodes (subset used by protocols 0-5 for the value model)
const (
	opMARK            = '('
	opSTOP            = '.'
	opPOP             = '0'
	opPOP_MARK        = '1'
	opDUP             = '2'
	opFLOAT           = 'F'
	opINT             = 'I'
	opBININT          = 'J'
	opBININT1         = 'K'
	opLONG            = 'L'
	opBININT2         = 'M'
	opNONE            = 'N'
	opREDUCE          = 'R'
	opSTRING          = 'S'
	opBINSTRING       = 'T'
	opSHORT_BINSTRING = 'U'
	opUNICODE         = 'V'
	opBINUNICODE      = 'X'
	opAPPEND          = 'a'
	opBUILD           = 'b'
	opGLOBAL          = 'c'
	opDICT            = 'd'
	opEMPTY_DICT      = '}'
	opAPPENDS         = 'e'
	opGET             = 'g'
	opBINGET          = 'h'
	opINST            = 'i'
	opLONG_BINGET     = 'j'
	opLIST            = 'l'
	opEMPTY_LIST      = ']'
	opOBJ             = 'o'
	opPUT             = 'p'
	opBINPUT          = 'q'
	opLONG_BINPUT     = 'r'
	opSETITEM         = 's'
	opTUPLE           = 't'
	opEMPTY_TUPLE     = ')'
	opSETITEMS        = 'u'
	opBINFLOAT        = 'G'
	opPROTO           = 0x80
	opNEWOBJ          = 0x81
	opEXT1            = 0x82
	opEXT2            = 0x83
	opEXT4            = 0x84
	opTUPLE1          = 0x85
	opTUPLE2          = 0x86
	opTUPLE3          = 0x87
	opNEWTRUE         = 0x88
	opNEWFALSE        = 0x89
	opLONG1           = 0x8a
	opLONG4           = 0x8b
	opBINBYTES        = 'B'
	opSHORT_BINBYTES  = 'C'
	opSHORT_BINUNICODE = 0x8c
	opBINUNICODE8     = 0x8d
	opBINBYTES8       = 0x8e
	opEMPTY_SET       = 0x8f
	opADDITEMS        = 0x90
	opFROZENSET       = 0x91
	opNEWOBJ_EX       = 0x92
	opSTACK_GLOBAL    = 0x93
	opMEMOIZE         = 0x94
	opFRAME           = 0x95
	opBYTEARRAY8      = 0x96
	opNEXT_BUFFER     = 0x97
	opREADONLY_BUFFER = 0x98
)

const (
	pickleFrameSizeMin    = 4
	pickleFrameSizeTarget = 64 * 1024
	pickleBatchSize       = 1000
	pickleDefaultProtocol = 5
)

// Reducer is implemented by objects that can be pickled through
// __reduce__: returns (callable global "module.name", args Tuple).
type Reducer interface{ Reduce() (string, Tuple) }

// Pickle_dumps is pickle.dumps(obj) (protocol 5, the 3.14 default),
// byte-identical to CPython for the value model: framing (min 4 bytes,
// 64 KiB target), MEMOIZE after every str/bytes/container, 1000-item
// APPENDS/SETITEMS/ADDITEMS batches, APPEND/SETITEM for single items.
func Pickle_dumps(v any, protocol ...int) ([]byte, error) {
	proto := pickleDefaultProtocol
	if len(protocol) > 0 && protocol[0] >= 0 {
		proto = protocol[0]
	}
	if proto < 2 {
		return nil, ValueError.Newf("pickle protocol %d is not supported by the Go port (use >= 2)", proto)
	}
	p := &pickler{proto: proto, memo: map[memoKey]int{}}
	p.out = append(p.out, opPROTO, byte(proto))
	if proto >= 4 {
		p.framing = true
	}
	if err := p.save(v); err != nil {
		return nil, err
	}
	p.write(opSTOP)
	p.endFraming()
	return p.out, nil
}

type memoKey struct {
	ptr uintptr
	n   int
	k   byte
}

type pickler struct {
	proto   int
	out     []byte
	frame   []byte
	framing bool
	memo    map[memoKey]int
	memoLen int
}

func (p *pickler) write(b ...byte) {
	if p.framing {
		p.frame = append(p.frame, b...)
	} else {
		p.out = append(p.out, b...)
	}
}

func (p *pickler) writeBytes(b []byte) {
	if p.framing {
		p.frame = append(p.frame, b...)
	} else {
		p.out = append(p.out, b...)
	}
}

func (p *pickler) commitFrame(force bool) {
	if !p.framing {
		return
	}
	if len(p.frame) >= pickleFrameSizeTarget || force {
		if len(p.frame) >= pickleFrameSizeMin {
			p.out = append(p.out, opFRAME)
			p.out = binary.LittleEndian.AppendUint64(p.out, uint64(len(p.frame)))
		}
		p.out = append(p.out, p.frame...)
		p.frame = p.frame[:0]
	}
}

func (p *pickler) endFraming() {
	if p.framing && len(p.frame) > 0 {
		p.commitFrame(true)
	}
	p.framing = false
}

func (p *pickler) writeLarge(header []byte, payload []byte) {
	if p.framing {
		p.commitFrame(true)
	}
	p.out = append(p.out, header...)
	p.out = append(p.out, payload...)
}

func (p *pickler) put() {
	idx := p.memoLen
	p.memoLen++
	if p.proto >= 4 {
		p.write(opMEMOIZE)
	} else if idx < 256 {
		p.write(opBINPUT, byte(idx))
	} else {
		p.write(opLONG_BINPUT)
		p.frameOrOut(binary.LittleEndian.AppendUint32(nil, uint32(idx)))
	}
}

func (p *pickler) frameOrOut(b []byte) { p.writeBytes(b) }

func (p *pickler) get(idx int) {
	if idx < 256 {
		p.write(opBINGET, byte(idx))
	} else {
		p.write(opLONG_BINGET)
		p.writeBytes(binary.LittleEndian.AppendUint32(nil, uint32(idx)))
	}
}

func (p *pickler) memoize(k memoKey, ok bool) {
	if ok {
		p.memo[k] = p.memoLen
	}
	p.put()
}

func identity(v any) (memoKey, bool) {
	switch x := v.(type) {
	case *List:
		return memoKey{ptr: uintptr(unsafe.Pointer(x)), k: 1}, true
	case *Dict:
		return memoKey{ptr: uintptr(unsafe.Pointer(x)), k: 2}, true
	case *Set:
		return memoKey{ptr: uintptr(unsafe.Pointer(x)), k: 3}, true
	case FrozenSet:
		return memoKey{ptr: uintptr(unsafe.Pointer(x.Set)), k: 4}, true
	case Tuple:
		if len(x) == 0 {
			return memoKey{}, false
		}
		return memoKey{ptr: uintptr(unsafe.Pointer(&x[0])), n: len(x), k: 5}, true
	case []byte:
		if len(x) == 0 {
			return memoKey{}, false
		}
		return memoKey{ptr: uintptr(unsafe.Pointer(&x[0])), n: len(x), k: 6}, true
	case []any:
		if len(x) == 0 {
			return memoKey{}, false
		}
		return memoKey{ptr: uintptr(unsafe.Pointer(&x[0])), n: len(x), k: 7}, true
	case *NDArray:
		return memoKey{ptr: uintptr(unsafe.Pointer(x)), k: 8}, true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr && !rv.IsNil() {
		return memoKey{ptr: rv.Pointer(), k: 8}, true
	}
	return memoKey{}, false
}

func (p *pickler) save(v any) error {
	p.commitFrame(false)
	key, hasID := identity(v)
	if hasID {
		if idx, ok := p.memo[key]; ok {
			p.get(idx)
			return nil
		}
	}
	switch x := v.(type) {
	case nil:
		p.write(opNONE)
	case bool:
		if x {
			p.write(opNEWTRUE)
		} else {
			p.write(opNEWFALSE)
		}
	case int:
		p.saveInt(big.NewInt(int64(x)))
	case int8:
		p.saveInt(big.NewInt(int64(x)))
	case int16:
		p.saveInt(big.NewInt(int64(x)))
	case int32:
		p.saveInt(big.NewInt(int64(x)))
	case int64:
		p.saveInt(big.NewInt(x))
	case uint:
		p.saveInt(new(big.Int).SetUint64(uint64(x)))
	case uint8:
		p.saveInt(big.NewInt(int64(x)))
	case uint16:
		p.saveInt(big.NewInt(int64(x)))
	case uint32:
		p.saveInt(big.NewInt(int64(x)))
	case uint64:
		p.saveInt(new(big.Int).SetUint64(x))
	case *big.Int:
		p.saveInt(x)
	case float32:
		p.write(opBINFLOAT)
		p.writeBytes(binary.BigEndian.AppendUint64(nil, math.Float64bits(float64(x))))
	case float64:
		p.write(opBINFLOAT)
		p.writeBytes(binary.BigEndian.AppendUint64(nil, math.Float64bits(x)))
	case string:
		p.saveStr(x)
		p.memoize(memoKey{}, false)
	case []byte:
		p.saveBytes(x)
		p.memoize(key, hasID)
	case Tuple:
		return p.saveTuple(x, key, hasID)
	case []any:
		return p.saveList(x, key, hasID)
	case *List:
		return p.saveList(x.Items(), key, hasID)
	case *Dict:
		return p.saveDict(x.Items(), key, hasID)
	case Kw:
		return p.saveDict(DictFromKw(x).Items(), memoKey{}, false)
	case map[string]any:
		return p.saveDict(DictFromMap(x).Items(), memoKey{}, false)
	case *Set:
		return p.saveSet(x.Items(), key, hasID)
	case FrozenSet:
		return p.saveFrozenset(x.Items(), key, hasID)
	case []string:
		return p.saveList(Iter(x), memoKey{}, false)
	case Reducer:
		global, args := x.Reduce()
		return p.saveReduce(global, args, key, hasID)
	case *NDArray:
		return p.saveNDArray(x, key, hasID)
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice {
			return p.saveList(Iter(v), memoKey{}, false)
		}
		if cls := Classof(v); cls != nil {
			return p.saveObject(v, cls, key, hasID)
		}
		if e, ok := enum_pickle_of_(v); ok {
			return p.saveReduce(e.global, Tuple{e.value(v)}, memoKey{}, false)
		}
		return PicklingError.Newf("Can't pickle <class '%s'> object", Pytypename(v))
	}
	return nil
}

func (p *pickler) saveInt(n *big.Int) {
	if n.IsInt64() {
		x := n.Int64()
		switch {
		case x >= 0 && x <= 0xff:
			p.write(opBININT1, byte(x))
			return
		case x >= 0 && x <= 0xffff:
			p.write(opBININT2)
			p.writeBytes(binary.LittleEndian.AppendUint16(nil, uint16(x)))
			return
		case x >= -0x80000000 && x <= 0x7fffffff:
			p.write(opBININT)
			p.writeBytes(binary.LittleEndian.AppendUint32(nil, uint32(int32(x))))
			return
		}
	}
	enc := Encode_long(n)
	if len(enc) < 256 {
		p.write(opLONG1, byte(len(enc)))
	} else {
		p.write(opLONG4)
		p.writeBytes(binary.LittleEndian.AppendUint32(nil, uint32(len(enc))))
	}
	p.writeBytes(enc)
}

// Encode_long is pickle.encode_long: two's-complement little-endian,
// minimal length; 0 encodes as b"".
func Encode_long(n *big.Int) []byte {
	if n.Sign() == 0 {
		return nil
	}
	nbytes := n.BitLen()>>3 + 1 // BitLen is of |n|, like int.bit_length()
	mod := new(big.Int).Lsh(big.NewInt(1), uint(8*nbytes))
	val := new(big.Int).Mod(n, mod)
	be := val.Bytes()
	out := make([]byte, nbytes)
	for i := 0; i < len(be); i++ {
		out[i] = be[len(be)-1-i]
	}
	if n.Sign() < 0 && nbytes > 1 && out[nbytes-1] == 0xff && out[nbytes-2]&0x80 != 0 {
		out = out[:nbytes-1]
	}
	return out
}

// Decode_long is pickle.decode_long.
func Decode_long(b []byte) *big.Int {
	if len(b) == 0 {
		return new(big.Int)
	}
	be := make([]byte, len(b))
	for i := range b {
		be[i] = b[len(b)-1-i]
	}
	n := new(big.Int).SetBytes(be)
	if b[len(b)-1]&0x80 != 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
	}
	return n
}

func (p *pickler) saveStr(s string) {
	enc := []byte(s)
	n := len(enc)
	switch {
	case n <= 0xff && p.proto >= 4:
		p.write(opSHORT_BINUNICODE, byte(n))
		p.writeBytes(enc)
	case n > 0xffffffff && p.proto >= 4:
		p.writeLarge(append([]byte{opBINUNICODE8}, binary.LittleEndian.AppendUint64(nil, uint64(n))...), enc)
	case n >= pickleFrameSizeTarget:
		p.writeLarge(append([]byte{opBINUNICODE}, binary.LittleEndian.AppendUint32(nil, uint32(n))...), enc)
	default:
		p.write(opBINUNICODE)
		p.writeBytes(binary.LittleEndian.AppendUint32(nil, uint32(n)))
		p.writeBytes(enc)
	}
}

func (p *pickler) saveBytes(b []byte) {
	n := len(b)
	switch {
	case n <= 0xff:
		p.write(opSHORT_BINBYTES, byte(n))
		p.writeBytes(b)
	case n > 0xffffffff:
		p.writeLarge(append([]byte{opBINBYTES8}, binary.LittleEndian.AppendUint64(nil, uint64(n))...), b)
	case n >= pickleFrameSizeTarget:
		p.writeLarge(append([]byte{opBINBYTES}, binary.LittleEndian.AppendUint32(nil, uint32(n))...), b)
	default:
		p.write(opBINBYTES)
		p.writeBytes(binary.LittleEndian.AppendUint32(nil, uint32(n)))
		p.writeBytes(b)
	}
}

func (p *pickler) saveTuple(t Tuple, key memoKey, hasID bool) error {
	n := len(t)
	if n == 0 {
		p.write(opEMPTY_TUPLE)
		return nil
	}
	if n <= 3 {
		for _, e := range t {
			if err := p.save(e); err != nil {
				return err
			}
		}
		if idx, ok := p.memo[key]; ok && hasID {
			for i := 0; i < n; i++ {
				p.write(opPOP)
			}
			p.get(idx)
			return nil
		}
		p.write([]byte{0, opTUPLE1, opTUPLE2, opTUPLE3}[n])
		p.memoize(key, hasID)
		return nil
	}
	p.write(opMARK)
	for _, e := range t {
		if err := p.save(e); err != nil {
			return err
		}
	}
	if idx, ok := p.memo[key]; ok && hasID {
		p.write(opPOP_MARK)
		p.get(idx)
		return nil
	}
	p.write(opTUPLE)
	p.memoize(key, hasID)
	return nil
}

func (p *pickler) saveList(items []any, key memoKey, hasID bool) error {
	p.write(opEMPTY_LIST)
	p.memoize(key, hasID)
	for start := 0; start < len(items); start += pickleBatchSize {
		end := start + pickleBatchSize
		if end > len(items) {
			end = len(items)
		}
		batch := items[start:end]
		if len(batch) != 1 {
			p.write(opMARK)
			for _, e := range batch {
				if err := p.save(e); err != nil {
					return err
				}
			}
			p.write(opAPPENDS)
		} else {
			if err := p.save(batch[0]); err != nil {
				return err
			}
			p.write(opAPPEND)
		}
	}
	return nil
}

func (p *pickler) saveDict(items []Item, key memoKey, hasID bool) error {
	p.write(opEMPTY_DICT)
	p.memoize(key, hasID)
	for start := 0; start < len(items); start += pickleBatchSize {
		end := start + pickleBatchSize
		if end > len(items) {
			end = len(items)
		}
		batch := items[start:end]
		if len(batch) != 1 {
			p.write(opMARK)
			for _, it := range batch {
				if err := p.save(it.Key); err != nil {
					return err
				}
				if err := p.save(it.Value); err != nil {
					return err
				}
			}
			p.write(opSETITEMS)
		} else {
			if err := p.save(batch[0].Key); err != nil {
				return err
			}
			if err := p.save(batch[0].Value); err != nil {
				return err
			}
			p.write(opSETITEM)
		}
	}
	return nil
}

func (p *pickler) saveSet(items []any, key memoKey, hasID bool) error {
	p.write(opEMPTY_SET)
	p.memoize(key, hasID)
	for start := 0; start < len(items); start += pickleBatchSize {
		end := start + pickleBatchSize
		if end > len(items) {
			end = len(items)
		}
		p.write(opMARK)
		for _, e := range items[start:end] {
			if err := p.save(e); err != nil {
				return err
			}
		}
		p.write(opADDITEMS)
	}
	return nil
}

func (p *pickler) saveFrozenset(items []any, key memoKey, hasID bool) error {
	p.write(opMARK)
	for _, e := range items {
		if err := p.save(e); err != nil {
			return err
		}
	}
	if idx, ok := p.memo[key]; ok && hasID {
		p.write(opPOP_MARK)
		p.get(idx)
		return nil
	}
	p.write(opFROZENSET)
	p.memoize(key, hasID)
	return nil
}

func (p *pickler) saveGlobal(global string) error {
	dot := strings.LastIndexByte(global, '.')
	if dot < 0 {
		return PicklingError.Newf("Can't pickle global %q", global)
	}
	module, name := global[:dot], global[dot+1:]
	p.saveStr(module)
	p.memoize(memoKey{}, false)
	p.saveStr(name)
	p.memoize(memoKey{}, false)
	p.write(opSTACK_GLOBAL)
	p.memoize(memoKey{}, false)
	return nil
}

func (p *pickler) saveReduce(global string, args Tuple, key memoKey, hasID bool) error {
	if err := p.saveGlobal(global); err != nil {
		return err
	}
	if err := p.save(args); err != nil {
		return err
	}
	p.write(opREDUCE)
	p.memoize(key, hasID)
	return nil
}

// ---------------------------------------------------------------------------
// unpickler
// ---------------------------------------------------------------------------

// Global_constructor resolves "module.name" globals during unpickling;
// laila registers its picklable classes here. Builtins set/frozenset/
// bytearray/bytes/list/dict/tuple are built in.
var Global_constructor = map[string]func(args Tuple) (any, error){
	"builtins.set": func(args Tuple) (any, error) {
		if len(args) == 0 {
			return NewSet(), nil
		}
		return NewSet(Iter(args[0])...), nil
	},
	"builtins.frozenset": func(args Tuple) (any, error) {
		if len(args) == 0 {
			return FrozenSet{NewSet()}, nil
		}
		return FrozenSet{NewSet(Iter(args[0])...)}, nil
	},
	"builtins.bytearray": func(args Tuple) (any, error) {
		if len(args) == 0 {
			return []byte{}, nil
		}
		if b, ok := args[0].([]byte); ok {
			return b, nil
		}
		return nil, UnpicklingError.New("bytearray() unsupported args")
	},
	"builtins.bytes": func(args Tuple) (any, error) { return []byte{}, nil },
	"builtins.list": func(args Tuple) (any, error) {
		if len(args) == 0 {
			return NewList(), nil
		}
		return NewList(Iter(args[0])...), nil
	},
	"builtins.dict": func(args Tuple) (any, error) { return NewDict(), nil },
	"builtins.tuple": func(args Tuple) (any, error) {
		if len(args) == 0 {
			return Tuple{}, nil
		}
		return Tuple(Iter(args[0])), nil
	},
	"_codecs.encode": func(args Tuple) (any, error) {
		if len(args) == 2 {
			if s, ok := args[0].(string); ok {
				enc, _ := args[1].(string)
				if strings.EqualFold(enc, "latin1") || strings.EqualFold(enc, "latin-1") {
					out := make([]byte, 0, len(s))
					for _, r := range s {
						out = append(out, byte(r))
					}
					return out, nil
				}
				return []byte(s), nil
			}
		}
		return nil, UnpicklingError.New("codecs.encode unsupported args")
	},
}

type pyGlobal struct{ name string }

// Pickle_loads is pickle.loads(data) for protocols 0-5 over the value model.
func Pickle_loads(data []byte) (any, error) {
	u := &unpickler{b: data, memo: map[int]any{}}
	return u.load()
}

type unpickler struct {
	b     []byte
	i     int
	stack []any
	marks []int
	memo  map[int]any
	proto int
}

func (u *unpickler) read(n int) ([]byte, error) {
	if u.i+n > len(u.b) {
		return nil, UnpicklingError.New("pickle data was truncated")
	}
	out := u.b[u.i : u.i+n]
	u.i += n
	return out, nil
}

func (u *unpickler) readLine() ([]byte, error) {
	j := u.i
	for j < len(u.b) && u.b[j] != '\n' {
		j++
	}
	if j >= len(u.b) {
		return nil, UnpicklingError.New("pickle data was truncated")
	}
	out := u.b[u.i:j]
	u.i = j + 1
	return out, nil
}

func (u *unpickler) push(v any) { u.stack = append(u.stack, v) }

func (u *unpickler) pop() (any, error) {
	if len(u.stack) == 0 {
		return nil, UnpicklingError.New("unpickling stack underflow")
	}
	v := u.stack[len(u.stack)-1]
	u.stack = u.stack[:len(u.stack)-1]
	return v, nil
}

func (u *unpickler) popMark() ([]any, error) {
	if len(u.marks) == 0 {
		return nil, UnpicklingError.New("could not find MARK")
	}
	m := u.marks[len(u.marks)-1]
	u.marks = u.marks[:len(u.marks)-1]
	items := append([]any(nil), u.stack[m:]...)
	u.stack = u.stack[:m]
	return items, nil
}

func (u *unpickler) top() (any, error) {
	if len(u.stack) == 0 {
		return nil, UnpicklingError.New("unpickling stack underflow")
	}
	return u.stack[len(u.stack)-1], nil
}

func (u *unpickler) load() (any, error) {
	for {
		if u.i >= len(u.b) {
			return nil, UnpicklingError.New("pickle data was truncated")
		}
		op := u.b[u.i]
		u.i++
		switch op {
		case opPROTO:
			b, err := u.read(1)
			if err != nil {
				return nil, err
			}
			u.proto = int(b[0])
			if u.proto > 5 {
				return nil, ValueError.Newf("unsupported pickle protocol: %d", u.proto)
			}
		case opFRAME:
			if _, err := u.read(8); err != nil {
				return nil, err
			}
		case opSTOP:
			return u.pop()
		case opNONE:
			u.push(nil)
		case opNEWTRUE:
			u.push(true)
		case opNEWFALSE:
			u.push(false)
		case opBININT:
			b, err := u.read(4)
			if err != nil {
				return nil, err
			}
			u.push(int64(int32(binary.LittleEndian.Uint32(b))))
		case opBININT1:
			b, err := u.read(1)
			if err != nil {
				return nil, err
			}
			u.push(int64(b[0]))
		case opBININT2:
			b, err := u.read(2)
			if err != nil {
				return nil, err
			}
			u.push(int64(binary.LittleEndian.Uint16(b)))
		case opLONG1:
			nb, err := u.read(1)
			if err != nil {
				return nil, err
			}
			b, err := u.read(int(nb[0]))
			if err != nil {
				return nil, err
			}
			u.push(bigOrInt(Decode_long(b)))
		case opLONG4:
			nb, err := u.read(4)
			if err != nil {
				return nil, err
			}
			b, err := u.read(int(int32(binary.LittleEndian.Uint32(nb))))
			if err != nil {
				return nil, err
			}
			u.push(bigOrInt(Decode_long(b)))
		case opINT:
			line, err := u.readLine()
			if err != nil {
				return nil, err
			}
			s := string(line)
			switch s {
			case "00":
				u.push(false)
			case "01":
				u.push(true)
			default:
				n, ok := new(big.Int).SetString(s, 10)
				if !ok {
					return nil, UnpicklingError.Newf("invalid INT %q", s)
				}
				u.push(bigOrInt(n))
			}
		case opLONG:
			line, err := u.readLine()
			if err != nil {
				return nil, err
			}
			s := strings.TrimSuffix(string(line), "L")
			n, ok := new(big.Int).SetString(s, 10)
			if !ok {
				return nil, UnpicklingError.Newf("invalid LONG %q", s)
			}
			u.push(bigOrInt(n))
		case opBINFLOAT:
			b, err := u.read(8)
			if err != nil {
				return nil, err
			}
			u.push(math.Float64frombits(binary.BigEndian.Uint64(b)))
		case opFLOAT:
			line, err := u.readLine()
			if err != nil {
				return nil, err
			}
			f, err := strconv.ParseFloat(string(line), 64)
			if err != nil {
				return nil, UnpicklingError.New(err.Error())
			}
			u.push(f)
		case opSHORT_BINUNICODE:
			nb, err := u.read(1)
			if err != nil {
				return nil, err
			}
			b, err := u.read(int(nb[0]))
			if err != nil {
				return nil, err
			}
			u.push(string(b))
		case opBINUNICODE:
			nb, err := u.read(4)
			if err != nil {
				return nil, err
			}
			b, err := u.read(int(binary.LittleEndian.Uint32(nb)))
			if err != nil {
				return nil, err
			}
			u.push(string(b))
		case opBINUNICODE8:
			nb, err := u.read(8)
			if err != nil {
				return nil, err
			}
			b, err := u.read(int(binary.LittleEndian.Uint64(nb)))
			if err != nil {
				return nil, err
			}
			u.push(string(b))
		case opUNICODE:
			line, err := u.readLine()
			if err != nil {
				return nil, err
			}
			u.push(rawUnicodeUnescape(string(line)))
		case opSHORT_BINBYTES:
			nb, err := u.read(1)
			if err != nil {
				return nil, err
			}
			b, err := u.read(int(nb[0]))
			if err != nil {
				return nil, err
			}
			u.push(Bytes_clone(b))
		case opBINBYTES:
			nb, err := u.read(4)
			if err != nil {
				return nil, err
			}
			b, err := u.read(int(binary.LittleEndian.Uint32(nb)))
			if err != nil {
				return nil, err
			}
			u.push(Bytes_clone(b))
		case opBINBYTES8, opBYTEARRAY8:
			nb, err := u.read(8)
			if err != nil {
				return nil, err
			}
			b, err := u.read(int(binary.LittleEndian.Uint64(nb)))
			if err != nil {
				return nil, err
			}
			u.push(Bytes_clone(b))
		case opSHORT_BINSTRING:
			nb, err := u.read(1)
			if err != nil {
				return nil, err
			}
			b, err := u.read(int(nb[0]))
			if err != nil {
				return nil, err
			}
			u.push(latin1(b))
		case opBINSTRING:
			nb, err := u.read(4)
			if err != nil {
				return nil, err
			}
			b, err := u.read(int(int32(binary.LittleEndian.Uint32(nb))))
			if err != nil {
				return nil, err
			}
			u.push(latin1(b))
		case opSTRING:
			line, err := u.readLine()
			if err != nil {
				return nil, err
			}
			s := string(line)
			if len(s) >= 2 && (s[0] == '\'' || s[0] == '"') {
				s = s[1 : len(s)-1]
			}
			u.push(s)
		case opEMPTY_TUPLE:
			u.push(Tuple{})
		case opTUPLE1, opTUPLE2, opTUPLE3:
			n := int(op - opTUPLE1 + 1)
			if len(u.stack) < n {
				return nil, UnpicklingError.New("unpickling stack underflow")
			}
			t := Tuple(append([]any(nil), u.stack[len(u.stack)-n:]...))
			u.stack = u.stack[:len(u.stack)-n]
			u.push(t)
		case opTUPLE:
			items, err := u.popMark()
			if err != nil {
				return nil, err
			}
			u.push(Tuple(items))
		case opMARK:
			u.marks = append(u.marks, len(u.stack))
		case opPOP:
			if _, err := u.pop(); err != nil {
				return nil, err
			}
		case opPOP_MARK:
			if _, err := u.popMark(); err != nil {
				return nil, err
			}
		case opDUP:
			t, err := u.top()
			if err != nil {
				return nil, err
			}
			u.push(t)
		case opEMPTY_LIST:
			u.push(NewList())
		case opLIST:
			items, err := u.popMark()
			if err != nil {
				return nil, err
			}
			u.push(NewList(items...))
		case opAPPEND:
			v, err := u.pop()
			if err != nil {
				return nil, err
			}
			t, err := u.top()
			if err != nil {
				return nil, err
			}
			l, ok := t.(*List)
			if !ok {
				return nil, UnpicklingError.New("APPEND on non-list")
			}
			l.Append(v)
		case opAPPENDS:
			items, err := u.popMark()
			if err != nil {
				return nil, err
			}
			t, err := u.top()
			if err != nil {
				return nil, err
			}
			l, ok := t.(*List)
			if !ok {
				return nil, UnpicklingError.New("APPENDS on non-list")
			}
			for _, it := range items {
				l.Append(it)
			}
		case opEMPTY_DICT:
			u.push(NewDict())
		case opDICT:
			items, err := u.popMark()
			if err != nil {
				return nil, err
			}
			d := NewDict()
			for i := 0; i+1 < len(items); i += 2 {
				d.Set(items[i], items[i+1])
			}
			u.push(d)
		case opSETITEM:
			v, err := u.pop()
			if err != nil {
				return nil, err
			}
			k, err := u.pop()
			if err != nil {
				return nil, err
			}
			t, err := u.top()
			if err != nil {
				return nil, err
			}
			if err := setItem(t, k, v); err != nil {
				return nil, err
			}
		case opSETITEMS:
			items, err := u.popMark()
			if err != nil {
				return nil, err
			}
			t, err := u.top()
			if err != nil {
				return nil, err
			}
			for i := 0; i+1 < len(items); i += 2 {
				if err := setItem(t, items[i], items[i+1]); err != nil {
					return nil, err
				}
			}
		case opEMPTY_SET:
			u.push(NewSet())
		case opADDITEMS:
			items, err := u.popMark()
			if err != nil {
				return nil, err
			}
			t, err := u.top()
			if err != nil {
				return nil, err
			}
			s, ok := t.(*Set)
			if !ok {
				return nil, UnpicklingError.New("ADDITEMS on non-set")
			}
			for _, it := range items {
				s.Add(it)
			}
		case opFROZENSET:
			items, err := u.popMark()
			if err != nil {
				return nil, err
			}
			u.push(FrozenSet{NewSet(items...)})
		case opMEMOIZE:
			t, err := u.top()
			if err != nil {
				return nil, err
			}
			u.memo[len(u.memo)] = t
		case opBINPUT:
			b, err := u.read(1)
			if err != nil {
				return nil, err
			}
			t, err := u.top()
			if err != nil {
				return nil, err
			}
			u.memo[int(b[0])] = t
		case opLONG_BINPUT:
			b, err := u.read(4)
			if err != nil {
				return nil, err
			}
			t, err := u.top()
			if err != nil {
				return nil, err
			}
			u.memo[int(binary.LittleEndian.Uint32(b))] = t
		case opPUT:
			line, err := u.readLine()
			if err != nil {
				return nil, err
			}
			n, _ := strconv.Atoi(string(line))
			t, err := u.top()
			if err != nil {
				return nil, err
			}
			u.memo[n] = t
		case opBINGET:
			b, err := u.read(1)
			if err != nil {
				return nil, err
			}
			v, ok := u.memo[int(b[0])]
			if !ok {
				return nil, UnpicklingError.New("Memo value not found at index")
			}
			u.push(v)
		case opLONG_BINGET:
			b, err := u.read(4)
			if err != nil {
				return nil, err
			}
			v, ok := u.memo[int(binary.LittleEndian.Uint32(b))]
			if !ok {
				return nil, UnpicklingError.New("Memo value not found at index")
			}
			u.push(v)
		case opGET:
			line, err := u.readLine()
			if err != nil {
				return nil, err
			}
			n, _ := strconv.Atoi(string(line))
			v, ok := u.memo[n]
			if !ok {
				return nil, UnpicklingError.New("Memo value not found at index")
			}
			u.push(v)
		case opGLOBAL:
			mod, err := u.readLine()
			if err != nil {
				return nil, err
			}
			name, err := u.readLine()
			if err != nil {
				return nil, err
			}
			u.push(pyGlobal{string(mod) + "." + string(name)})
		case opSTACK_GLOBAL:
			name, err := u.pop()
			if err != nil {
				return nil, err
			}
			mod, err := u.pop()
			if err != nil {
				return nil, err
			}
			u.push(pyGlobal{Str(mod) + "." + Str(name)})
		case opREDUCE:
			args, err := u.pop()
			if err != nil {
				return nil, err
			}
			fn, err := u.pop()
			if err != nil {
				return nil, err
			}
			g, ok := fn.(pyGlobal)
			if !ok {
				return nil, UnpicklingError.New("REDUCE on non-global")
			}
			ctor, ok := Global_constructor[g.name]
			if !ok {
				return nil, UnpicklingError.Newf("Can't resolve global %q in the Go port", g.name)
			}
			t, _ := args.(Tuple)
			v, err := ctor(t)
			if err != nil {
				return nil, err
			}
			u.push(v)
		case opNEWOBJ:
			args, err := u.pop()
			if err != nil {
				return nil, err
			}
			cls, err := u.pop()
			if err != nil {
				return nil, err
			}
			g, ok := cls.(pyGlobal)
			if !ok {
				return nil, UnpicklingError.New("NEWOBJ on non-global")
			}
			t, _ := args.(Tuple)
			var v any
			if ctor, ok := Global_constructor[g.name]; ok {
				v, err = ctor(t)
			} else if obj, found, oerr := new_object_for_global_(g.name, t); found {
				v, err = obj, oerr
			} else {
				return nil, UnpicklingError.Newf("Can't resolve global %q in the Go port", g.name)
			}
			if err != nil {
				return nil, err
			}
			u.push(v)
		case opBUILD:
			state, err := u.pop()
			if err != nil {
				return nil, err
			}
			t, err := u.top()
			if err != nil {
				return nil, err
			}
			if s, ok := t.(interface{ Setstate(state any) error }); ok {
				if err := s.Setstate(state); err != nil {
					return nil, err
				}
				// a ``_reconstruct`` placeholder becomes the array it built
				u.stack[len(u.stack)-1] = unwrap_np_(t)
				if idx := u.memoIndexOf(t); idx >= 0 {
					u.memo[idx] = u.stack[len(u.stack)-1]
				}
			} else if Classof(t) != nil {
				if err := Object_setstate(t, state); err != nil {
					return nil, err
				}
			} else {
				return nil, UnpicklingError.Newf("BUILD on object without __setstate__: %s", Pytypename(t))
			}
		default:
			return nil, UnpicklingError.Newf("invalid load key, '%s'.", Repr_bytes([]byte{op})[2:3])
		}
	}
}

func bigOrInt(n *big.Int) any {
	if n.IsInt64() {
		return n.Int64()
	}
	return n
}

func latin1(b []byte) string {
	r := make([]rune, len(b))
	for i, c := range b {
		r[i] = rune(c)
	}
	return string(r)
}

// Decode_latin1 is ``bytes.decode("latin-1")``: every byte becomes the
// code point with the same value.
func Decode_latin1(b []byte) string { return latin1(b) }

// Encode_latin1 is ``str.encode("latin-1")``; code points above U+00FF
// are replaced by ``?`` (Python would raise UnicodeEncodeError, but the
// callers only ever round-trip Decode_latin1 output).
func Encode_latin1(s string) []byte {
	out := make([]byte, 0, len(s))
	for _, r := range s {
		if r > 0xFF {
			out = append(out, '?')
			continue
		}
		out = append(out, byte(r))
	}
	return out
}

func rawUnicodeUnescape(s string) string {
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+5 < len(s) && s[i+1] == 'u' {
			if n, err := strconv.ParseUint(s[i+2:i+6], 16, 32); err == nil {
				sb.WriteRune(rune(n))
				i += 5
				continue
			}
		}
		sb.WriteByte(s[i])
	}
	return sb.String()
}

func setItem(target, k, v any) error {
	switch t := target.(type) {
	case *Dict:
		if _, err := keyOf(k); err != nil {
			return err
		}
		t.Set(k, v)
		return nil
	case interface{ Setitem(k, v any) error }:
		return t.Setitem(k, v)
	}
	return UnpicklingError.Newf("SETITEM on %s", Pytypename(target))
}
