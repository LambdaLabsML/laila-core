/**
 * Hint model used to guide memory routing decisions.
 *
 * A ``MemoryHint`` is a soft, declarative knob the caller can pass into a
 * memory operation to bias the ``PoolRouter``'s pool selection without
 * hard-coding a specific ``pool_id``. The router is free to ignore any field
 * it doesn't understand -- hints are advisory, never authoritative. (For an
 * authoritative selection, pass ``pool_id`` or ``pool_nickname`` directly to
 * ``memorize`` / ``remember`` / ``forget``.)
 */
import { register } from "../../../../_compat/lazy.js";
import { BaseModel, Field, define_fields } from "../../../../_compat/pydantic.js";

/**
 * Routing hints that bias which pools are searched or written to.
 *
 * All fields are optional. Each is a soft preference; the ``PoolRouter`` may
 * honor or ignore them based on the routing strategy currently in effect.
 */
export class MemoryHint extends BaseModel {
  static {
    define_fields(this, {
      pools: ["set[str] | None", Field({ default: null, description: "Which pools to search first." })],
      keywords: ["set[str] | None", Field({ default: null, description: "Keywords that help find the entry in memory." })],
      semantics: ["Any | None", Field({ default: null, description: "Vector semantics that help find the entry in memory." })],
      purpose: ["str | None", Field({ default: null, description: "If certain pools are uniquely used for a special purpose." })],
      affinity: ["float | None", Field({ default: null, ge: 0.0, le: 1.0, description: "Closeness to local/nearby memory caches (0–1)." })],
    });
  }
}

register("laila.policy.central.memory.hint.hint", { MemoryHint });
