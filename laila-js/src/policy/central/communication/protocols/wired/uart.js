/**
 * UART / serial communication transport.
 *
 * Point-to-point framed JSON-RPC over an asynchronous serial line, built on
 * the ``_P2PStreamRPCProtocol`` carrier. Uses the ``serialport`` package
 * (Python: ``pyserial``) to open the port; the resulting duplex stream is
 * wrapped into an asyncio-style ``[reader, writer]`` pair so the serial
 * device participates as a stream.
 *
 * - ``protocol_name`` ``"uart"`` (aliases ``serial``)
 * - URI scheme ``serial:///dev/ttyUSB0`` (informational; the port comes from
 *   the ``port`` field)
 *
 * ``serialport`` is imported lazily inside ``_open_stream``; a missing
 * library or device raises a clear, actionable error.
 */
import { createRequire } from "node:module";
import * as asyncio from "../../../../../_compat/asyncio.js";
import { RuntimeError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { _P2PStreamRPCProtocol } from "../_carriers/p2p.js";
import { register_comm_protocol } from "../base.js";

const _require = createRequire(import.meta.url);

const _INSTALL_HINT = "The serial transport requires pyserial. Install it with `pip install laila-core[serial]` (or `pip install pyserial`).";

/**
 * Serial-line transport (UART).
 *
 * Parameters
 * ----------
 * port : str
 *     Serial device path (e.g. ``"/dev/ttyUSB0"``, ``"COM3"``).
 * baudrate : int, default ``115200``
 *     Line speed.
 */
export class _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL extends _P2PStreamRPCProtocol {
  static protocol_name = "uart";
  static _TOKEN_ALIASES = Object.freeze(new Set(["uart", "serial"]));

  static {
    define_fields(this, {
      port: ["str", Field({ default: "" })],
      baudrate: ["int", Field({ default: 115200 })],
    });
    define_private(this, {
      _serial: PrivateAttr({ default: null }),
      _read_transport: PrivateAttr({ default: null }),
    });
  }

  /** Accept ``"uart"`` / ``"serial"``. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``serial://`` and ``uart://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("serial://") || uri.startsWith("uart://");
  }

  _make_serial() {
    let serial;
    try {
      serial = _require("serialport");
    } catch (exc) {
      if (exc && (exc.code === "MODULE_NOT_FOUND" || exc.code === "ERR_MODULE_NOT_FOUND")) {
        const err = new RuntimeError(_INSTALL_HINT);
        err.__cause__ = exc;
        throw err;
      }
      throw exc;
    }
    if (!this.port) {
      throw new RuntimeError(`${this.constructor.name} requires a \`port\` (e.g. '/dev/ttyUSB0').`);
    }
    return new serial.SerialPort({ path: this.port, baudRate: this.baudrate, autoOpen: false });
  }

  async _open_stream() {
    const ser = this._make_serial();
    this._configure_serial(ser);
    this._serial = ser;
    await new Promise((res, rej) => ser.open((err) => (err ? rej(err) : res())));
    const [reader, writer] = asyncio._wrap_stream_socket(ser);
    this._read_transport = writer.transport;
    return [reader, writer];
  }

  /** Hook for subclasses (RS-485 direction control, etc.). */
  _configure_serial(_ser) {
    return null;
  }

  async _close_stream() {
    if (this._read_transport !== null && this._read_transport !== undefined) {
      try {
        this._read_transport.close();
      } catch {
        /* ignore */
      }
      this._read_transport = null;
    }
    await super._close_stream();
    if (this._serial !== null && this._serial !== undefined) {
      try {
        if (this._serial.isOpen) this._serial.close(() => {});
      } catch {
        /* ignore */
      }
      this._serial = null;
    }
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.uart", { _LAILA_IDENTIFIABLE_UART_COMM_PROTOCOL });
