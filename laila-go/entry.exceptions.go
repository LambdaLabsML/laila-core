package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// entry/exceptions.py -- exceptions raised by the Entry subsystem.

// EntryNotBuiltError is raised when ``Entry.data`` is accessed on an
// unbuilt entry.
//
// An entry is *unbuilt* when it has a constitution attached but no
// payload yet. The user must explicitly materialise it (e.g. via
// ``laila.build(entry).wait()`` or ``laila.remember(...)``) before its
// data can be read.
var EntryNotBuiltError = py.NewType("EntryNotBuiltError", py.RuntimeError)
