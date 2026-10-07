/**
 * PyTorch tensor serialisation / deserialisation transformation.
 *
 * There is no torch runtime in JavaScript, so this module is the analogue of
 * the Python module when ``torch`` is **not** installed: importing it from the
 * package index is skipped (``TorchSerializer`` resolves to ``null`` there,
 * exactly like Python's ``except ModuleNotFoundError: TorchSerializer = None``).
 * The class is still provided so the recovery-code emitter stays
 * byte-identical for tooling; ``forward`` / ``backward`` raise
 * ``ModuleNotFoundError`` like ``import torch`` would.
 */
import { Field, define_fields } from "../../../../_compat/pydantic.js";
import { ModuleNotFoundError } from "../../../../_compat/errors.js";
import { emit_torch } from "../../../../_codecs/recovery_codes.js";
import { _data_transformation } from "../base.js";

/** Reversible PyTorch serialiser using ``torch.save`` / ``torch.load``. */
export class TorchSerializer extends _data_transformation {
  static {
    define_fields(this, { name: ["str", Field({ default: "torch" })] });
    _data_transformation.__init_subclass__(this);
  }

  /** Build backward_code dynamically after model creation. */
  model_post_init(_context) {
    super.model_post_init(_context);
    this.backward_code = emit_torch(this.backward_kwargs);
  }

  /** @param {any} _inp */
  forward(_inp) {
    throw new ModuleNotFoundError("No module named 'torch'");
  }

  /** @param {Uint8Array} _inp */
  backward(_inp) {
    throw new ModuleNotFoundError("No module named 'torch'");
  }
}
