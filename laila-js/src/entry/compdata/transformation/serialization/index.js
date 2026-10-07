/**
 * Serializer transformations for various data formats.
 *
 * Each serializer is a ``_data_transformation`` whose ``forward`` turns a
 * value into bytes and whose ``backward`` does the inverse. They are typically
 * the *first* step in a transformation pipeline (the rest is encoding /
 * compression / encryption on top of the resulting bytes).
 *
 * Available serializers
 * ---------------------
 *
 * ================  ===============================================
 * Class             When to use
 * ================  ===============================================
 * PickleSerializer  Catch-all; works for arbitrary objects but
 *                   produces opaque, version-coupled blobs.
 *                   Preferred fall-back when no native serializer
 *                   fits.
 * MsgpackSerializer Compact, language-agnostic. Good for dicts /
 *                   lists of JSON-shaped values; faster and smaller
 *                   than pickle for those payloads.
 * NumpySerializer   Optimised for ``NDArray`` -- preserves dtype,
 *                   shape, and byte order.
 * TorchSerializer   Optimised for ``torch.Tensor``. Resolves to
 *                   ``null`` -- there is no torch runtime in JS
 *                   (same as Python without ``torch`` installed).
 * ================  ===============================================
 */
export { MsgpackSerializer } from "./msgpack.js";
export { NumpySerializer } from "./numpy.js";
export { PickleSerializer } from "./pickle.js";

export const TorchSerializer = null;
