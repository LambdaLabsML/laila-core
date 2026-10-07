/** Lightweight read-only views over Entry identity and state. */
import { PrivateAttr, define_private } from "../_compat/pydantic.js";
import { _LAILA_IDENTIFIABLE_OBJECT } from "../basics/definitions/identifiable_object.js";

/** Read-only projection exposing only the identity and state of an Entry. */
export class EntryIdentityView extends _LAILA_IDENTIFIABLE_OBJECT {
  static _DEFAULT_SCOPES = ["ENTRY"];

  static {
    define_private(this, { _state: PrivateAttr({}) });
  }
}

/** Read-only projection exposing identity, state, and constitution of an Entry. */
export class EntryHolisticView extends _LAILA_IDENTIFIABLE_OBJECT {
  static _DEFAULT_SCOPES = ["ENTRY"];

  static {
    define_private(this, {
      _state: PrivateAttr({}),
      _constitution: PrivateAttr({}),
    });
  }
}
