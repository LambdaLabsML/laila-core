"""Per-modality transforms applied at load time.

Datasets built with the ``data_loading`` skill store raw encoded bytes
(PNG / JPEG / MP4 / WAV ...) to keep storage small; these helpers turn
them into tensors inside the loader. Each transform takes one realized
payload (``entry.data``) and returns the value that will be handed to
``collate_fn``.
"""

from __future__ import annotations

from typing import Any


def identity(value: Any) -> Any:
    """Return the payload untouched (default for every modality)."""
    return value


def image_bytes_to_tensor(image_bytes: bytes, *, mode: str = "RGB", scale: bool = True):
    """Decode encoded image bytes with Pillow into a ``C x H x W`` float tensor.

    ``scale=True`` divides by 255 so values land in ``[0, 1]``; pass
    ``scale=False`` to keep a ``uint8`` tensor.
    """
    import io

    import numpy as np
    import torch

    try:
        from PIL import Image
    except ImportError as exc:  # pragma: no cover - import guard
        raise ImportError(
            "image_bytes_to_tensor needs Pillow: pip install 'laila-core[torch]'"
        ) from exc

    image = Image.open(io.BytesIO(image_bytes)).convert(mode)
    array = np.array(image, dtype=np.uint8)
    if array.ndim == 2:
        array = array[:, :, None]
    tensor = torch.from_numpy(array).permute(2, 0, 1)
    return tensor.float() / 255.0 if scale else tensor


def numpy_to_tensor(array, *, dtype=None):
    """``np.ndarray`` -> ``torch.Tensor`` (shares memory when possible)."""
    import numpy as np
    import torch

    tensor = torch.from_numpy(np.ascontiguousarray(array))
    return tensor.to(dtype) if dtype is not None else tensor


def image_array_to_tensor(array, *, scale: bool = True):
    """``H x W x C`` uint8 array -> ``C x H x W`` tensor (float in [0, 1] by default)."""

    tensor = numpy_to_tensor(array)
    if tensor.ndim == 2:
        tensor = tensor[None]
    elif tensor.ndim == 3:
        tensor = tensor.permute(2, 0, 1)
    return tensor.float() / 255.0 if scale else tensor


__all__ = [
    "identity",
    "image_array_to_tensor",
    "image_bytes_to_tensor",
    "numpy_to_tensor",
]
