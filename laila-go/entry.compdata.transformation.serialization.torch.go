package laila

import "github.com/amirzadehlambda/laila/laila-go/py"

// entry/compdata/transformation/serialization/torch.py -- PyTorch tensor
// serialisation.
//
// The Python module is optional (``pip install laila-core[torch]``) and the
// package exports ``TorchSerializer = None`` when torch is not importable.
// The Go port has no torch runtime, so it is permanently in that state:
// TorchSerializer_class is nil, no "torch" transformation is registered
// and tensors are never dispatched to a torch wrapper (CD_torchtensor is
// absent, exactly as in a torch-less Python environment).
var TorchSerializer_class *py.Class
