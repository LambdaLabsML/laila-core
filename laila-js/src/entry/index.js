/**
 * Entry sub-package: the ``Entry`` data unit and ready-made transformation pipelines.
 *
 * Mirrors ``laila/entry/__init__.py``. The presets are the *forward*
 * pipeline applied at memorize time; the inverse pipeline is recorded in
 * the entry's ``SimpleConstitution`` so reading back goes through the
 * exact reverse sequence.
 *
 * Available presets
 * -----------------
 * - ``transformation_base64`` -- base64 only.
 * - ``transformation_base64_compression`` -- base64 *of* zlib output.
 * - ``transformation_base64_compression_encryption(key = null)`` -- factory:
 *   compresses, encrypts (Fernet), then base64-encodes.
 * - ``transformation_encryption(key = null)`` -- factory: Fernet-only pipeline.
 *
 * With ``key = null`` the Fernet step reads the process-wide key from
 * ``laila.args.encryption.key`` (alias ``laila.encryption_key``).
 */

export * from "./compdata/transformation/index.js";
export { Entry } from "./entry.js";
export { EntryIdentityView, EntryHolisticView } from "./entry_metadata.js";
export { EntryState } from "./entry_state.js";
export { EntryNotBuiltError } from "./exceptions.js";
export { ComputationalData } from "./compdata/index.js";
export { ComplexConstitution, Constitution, SimpleConstitution } from "./constitution/index.js";
export { build_by_scope, register_builder, BUILDER_MAP } from "./constitution/build_maps.js";

import { register } from "../_compat/lazy.js";
import * as _self from "./index.js";
import { TransformationSequence } from "./compdata/transformation/base.js";
import { Base64 } from "./compdata/transformation/base64/index.js";
import { Zlib } from "./compdata/transformation/compression/index.js";
import { FernetEncryption } from "./compdata/transformation/encryption/index.js";

export const transformation_base64 = new TransformationSequence({ transformations: [new Base64()] });

export const transformation_base64_compression = new TransformationSequence({
  transformations: [new Base64(), new Zlib()],
});

export const transformation_base64_compression_encryption = (key = null) =>
  new TransformationSequence({
    transformations: [new Base64(), new Zlib(), new FernetEncryption({ key })],
  });

export const transformation_encryption = (key = null) =>
  new TransformationSequence({
    transformations: [new FernetEncryption({ key })],
  });

// ``sys.modules["laila.entry"]`` for in-function imports (``from ...entry import transformation_base64``).
register("laila.entry", _self);
