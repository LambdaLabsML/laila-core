"""Entry sub-package: the :class:`Entry` data unit and ready-made transformation pipelines.

The :class:`Entry` class is the fundamental data unit of LAILA -- it
wraps any Python object (tensor, image, dict, dataset, etc.) with a
stable identity (UUID + optional evolution counter), an explicit
lifecycle state, and either a concrete payload or a
:class:`Constitution` describing how to materialize one.

This module also exposes a small library of pre-built
:class:`TransformationSequence` presets that pools commonly use to
serialize entries on disk / over the wire. Each preset is the
*forward* pipeline applied at memorize time; the inverse pipeline is
recorded in the entry's :class:`SimpleConstitution` so reading back
goes through the exact reverse sequence.

Available presets
-----------------
- :data:`transformation_base64` -- base64 only. Use when the pool only
  needs UTF-8-safe text (e.g. JSON files, REST APIs).
- :data:`transformation_base64_compression` -- base64 *of* zlib output.
  The default for textual / network-friendly storage where bandwidth or
  disk space matter.
- :data:`transformation_base64_compression_encryption` -- factory:
  ``transformation_base64_compression_encryption(key)`` returns a
  pipeline that compresses, encrypts (Fernet), then base64-encodes. Use
  when stored bytes must be both compact and confidential.
- :data:`transformation_encryption` -- factory:
  ``transformation_encryption(key)`` returns a Fernet-only pipeline.
  Suitable when the destination pool already handles its own binary
  framing (e.g. a SQL ``BYTEA`` column).
"""

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