/**
 * Default implementations and compile-time constants for laila.
 *
 * Single source of truth for the "what does laila ship with by default?"
 * question. The aliases exported from this module name the *concrete* class
 * that backs each abstract role:
 *
 * ==========================  ==================================================
 * Default name                Concrete class
 * ==========================  ==================================================
 * DefaultTaskForce            ``PythonAsyncThreadPoolTaskForce``
 * DefaultCentralCommand       ``_LAILA_IDENTIFIABLE_CENTRAL_COMMAND``
 * DefaultCentralCommunication ``_LAILA_IDENTIFIABLE_COMMUNICATION``
 * DefaultTCPIPProtocol        ``_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL``
 * DefaultLoRaProtocol         ``_LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL`` (scaffold)
 * DefaultBluetoothProtocol    ``_LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL`` (scaffold)
 * DefaultCentralMemory        ``_LAILA_IDENTIFIABLE_CENTRAL_MEMORY``
 * DefaultPolicy               ``_LAILA_IDENTIFIABLE_POLICY``
 * DefaultPool                 ``_LAILA_IDENTIFIABLE_POOL``
 * DefaultPoolRouter           ``_LAILA_IDENTIFIABLE_POOL_ROUTER``
 * DefaultMultiBuffer          ``MultiBuffer``
 * ==========================  ==================================================
 *
 * Other constants:
 *
 * - ``AUTO_INITIALIZE_POLICY`` -- whether ``import laila`` should spin up a
 *   default policy automatically. Toggling this off is useful for unit-test
 *   rigs that want full control over policy lifecycle.
 * - ``LAILA_UNIVERSAL_NAMESPACE`` -- the UUID-5 namespace used as the *root*
 *   of laila's deterministic-id tree. Touch only if you know exactly what
 *   you're doing -- changing it invalidates every nickname-derived id ever
 *   produced.
 * - ``LAILA_DEFAULT_DIRECTORIES`` -- the on-disk layout under ``~/.laila``:
 *   per-pool storage under ``pools/``, log files under ``logs/``, key material
 *   under ``secrets/``, and non-memorizing query helpers (e.g. Manifest SQL
 *   indices) under ``indices/``. Each directory is created at import time if
 *   missing.
 *
 * The concrete-class aliases live in ``./defaults_classes.js`` (wired in once
 * the policy / data / communication packages are loaded) and are re-exported
 * here so ``import { DefaultPolicy } from ".../macros/defaults.js"`` reads
 * exactly like the Python import.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { register } from "../_compat/lazy.js";
import { NAMESPACE_DNS, uuid5 } from "../_compat/uuid.js";
import * as _self from "./defaults.js";

export * from "./defaults_classes.js";

// ``from .macros.defaults import DefaultX`` inside function bodies resolves
// through the lazy registry; the namespace is registered here so the module
// is importable by its Python dotted name as soon as it has been evaluated.
register("laila.macros.defaults", _self);

export const AUTO_INITIALIZE_POLICY = true;

// ============================================================
// DO NOT CHANGE THIS VALUE UNLESS YOU KNOW WHAT YOU ARE DOING
export const LAILA_UNIVERSAL_NAMESPACE = uuid5(NAMESPACE_DNS, "laila");
// ============================================================

const _DEFAULT_ROOT = path.join(os.homedir(), ".laila");

export const LAILA_DEFAULT_DIRECTORIES = {
  root: _DEFAULT_ROOT,
  pools: path.join(_DEFAULT_ROOT, "pools"),
  logs: path.join(_DEFAULT_ROOT, "logs"),
  secrets: path.join(_DEFAULT_ROOT, "secrets"),
  indices: path.join(_DEFAULT_ROOT, "indices"),
};

for (const _dir of Object.values(LAILA_DEFAULT_DIRECTORIES)) {
  fs.mkdirSync(_dir, { recursive: true });
}
