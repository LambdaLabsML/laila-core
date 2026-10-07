/**
 * Policy schema sub-package -- base class for every Laila policy.
 *
 * Houses ``_LAILA_IDENTIFIABLE_POLICY``, the abstract base that combines
 * ``_LAILA_CLI_CAPABLE_CLASS`` (4-tier parameter resolution from
 * ``laila.args``) with ``_LAILA_IDENTIFIABLE_OBJECT`` (UUID + global_id
 * machinery). Every concrete policy subclass -- including the default
 * ``DefaultPolicy`` -- inherits from this base.
 */

export { _LAILA_IDENTIFIABLE_POLICY } from "./base.js";
