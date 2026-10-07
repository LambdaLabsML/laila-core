/**
 * Policy module -- container for the policy schema and its central sub-systems.
 *
 * A *policy* is the top-level coordinator that owns three central
 * sub-systems:
 *
 * - ``central.memory`` (``_LAILA_IDENTIFIABLE_CENTRAL_MEMORY``) -- routes
 *   ``memorize`` / ``remember`` / ``forget`` calls to the appropriate
 *   ``Pool`` and applies serialization transformations.
 * - ``central.command`` (``_LAILA_IDENTIFIABLE_CENTRAL_COMMAND``) -- manages
 *   task-forces and the future bank; submits work asynchronously and tracks
 *   lifetime via ``Future`` objects.
 * - ``central.communication`` (``_LAILA_IDENTIFIABLE_COMMUNICATION``) --
 *   registers transport protocols (TCP/IP today) and the resulting peer
 *   registry of ``RemotePolicyProxy`` handles.
 *
 * A process can host any number of policies but only one is *active* at any
 * moment (see ``laila.activate_policy``). Policies are themselves
 * addressable by their ``global_id``, which is what enables peer-to-peer RPC:
 * a remote process sees a local policy as a proxy keyed by that gid.
 */

export { _LAILA_IDENTIFIABLE_POLICY } from "./schema/index.js";
export * as central from "./central/index.js";
export * as schema from "./schema/index.js";
