from .entry import Entry as Entry
from .entry import EntryIdentityView as EntryIdentityView
from .entry_state import EntryState
from .compdata.transformation import *

 
transformation_base64 = TransformationSequence (
    transformations = [
        Base64()
    ]
)

transformation_base64_compression = TransformationSequence (
    transformations = [
        Base64(),
        Zlib()
    ]
)

transformation_base64_compression_encryption = lambda key: TransformationSequence(
    transformations=[
        Base64(),
        Zlib(),
        FernetEncryption(key=key),
    ]
)

transformation_encryption = lambda key: TransformationSequence(
    transformations=[
        FernetEncryption(key=key),
    ]
)