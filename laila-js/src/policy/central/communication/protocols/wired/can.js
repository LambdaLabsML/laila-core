/**
 * CAN / CAN-FD communication transport.
 *
 * CAN is a multidrop bus with tiny frames (8 bytes classic, up to 64 for
 * CAN-FD), so JSON-RPC messages must be segmented. This transport uses
 * ISO-TP (ISO 15765-2) segmentation over a SocketCAN raw channel
 * (``socketcan`` package; Python: ``can-isotp`` over ``python-can``) to
 * expose a reliable, segmented stream and carries RPC on top of the
 * point-to-point stream carrier.
 *
 * - ``protocol_name`` ``"can"`` (aliases ``canfd`` / ``can-fd``)
 * - URI scheme ``can://<channel>``
 *
 * The SocketCAN binding is imported lazily; a missing library raises a
 * clear, actionable error. End-to-end verification uses a SocketCAN virtual
 * interface (``vcan0``).
 */
import { createRequire } from "node:module";
import { Duplex } from "node:stream";
import * as asyncio from "../../../../../_compat/asyncio.js";
import { RuntimeError } from "../../../../../_compat/errors.js";
import { register } from "../../../../../_compat/lazy.js";
import { Field, PrivateAttr, define_fields, define_private } from "../../../../../_compat/pydantic.js";
import { _P2PStreamRPCProtocol } from "../_carriers/p2p.js";
import { register_comm_protocol } from "../base.js";

const _require = createRequire(import.meta.url);

const _INSTALL_HINT =
  "The CAN transport requires python-can and can-isotp. Install them with " +
  "`pip install laila-core[can]` (or `pip install python-can can-isotp`), " +
  "and bring up a CAN interface (e.g. `sudo modprobe vcan && " +
  "sudo ip link add dev vcan0 type vcan && sudo ip link set up vcan0`).";

/**
 * Minimal ISO-TP (ISO 15765-2, normal addressing) segmented stream over a
 * SocketCAN raw channel. Mirrors the kernel ``can-isotp`` socket the Python
 * transport binds: ``set_fc_opts(stmin=5, bs=10)``.
 */
class _IsoTpSocket extends Duplex {
  constructor(channel, rx_id, tx_id, { fd = false, stmin = 5, bs = 10 } = {}) {
    super();
    this._ch = channel;
    this._rx_id = rx_id;
    this._tx_id = tx_id;
    this._max = fd ? 64 : 8;
    this._stmin = stmin;
    this._bs = bs;
    // receive state
    this._rx = null;
    // transmit state
    this._tx_waiters = [];
    this._on_msg = (msg) => this._handle_frame(msg);
    channel.addListener("onMessage", this._on_msg);
  }

  _read() {
    /* push-driven */
  }

  // -- receive side ----------------------------------------------------
  _send_frame(data) {
    // classic CAN pads to a full 8-byte frame; CAN-FD sends the exact DLC
    const buf = Buffer.alloc(this._max > 8 ? data.length : 8);
    data.copy(buf);
    this._ch.send({ id: this._tx_id, ext: this._tx_id > 0x7ff, rtr: false, data: buf });
  }

  _send_fc(fs = 0) {
    this._send_frame(Buffer.from([0x30 | fs, this._bs, this._stmin]));
  }

  _handle_frame(msg) {
    if (msg.id !== this._rx_id) return;
    const d = Buffer.from(msg.data);
    if (!d.length) return;
    const pci = d[0] >> 4;
    if (pci === 0x0) {
      // Single Frame (with CAN-FD escape when low nibble is 0)
      let len = d[0] & 0x0f;
      let off = 1;
      if (len === 0) {
        len = d[1];
        off = 2;
      }
      this._rx = null;
      this.push(d.subarray(off, off + len));
    } else if (pci === 0x1) {
      // First Frame
      let len = ((d[0] & 0x0f) << 8) | d[1];
      let off = 2;
      if (len === 0) {
        len = d.readUInt32BE(2);
        off = 6;
      }
      this._rx = { len, parts: [d.subarray(off)], got: d.length - off, seq: 1, in_block: 0 };
      this._send_fc(0);
    } else if (pci === 0x2) {
      // Consecutive Frame
      const st = this._rx;
      if (!st) return;
      if ((d[0] & 0x0f) !== (st.seq & 0x0f)) {
        this._rx = null; // sequence error -> drop message
        return;
      }
      st.seq = (st.seq + 1) & 0x0f;
      st.parts.push(d.subarray(1));
      st.got += d.length - 1;
      if (st.got >= st.len) {
        this._rx = null;
        this.push(Buffer.concat(st.parts).subarray(0, st.len));
        return;
      }
      if (this._bs && ++st.in_block >= this._bs) {
        st.in_block = 0;
        this._send_fc(0);
      }
    } else if (pci === 0x3) {
      // Flow Control
      const w = this._tx_waiters.shift();
      if (w) w({ fs: d[0] & 0x0f, bs: d[1], stmin: d[2] });
    }
  }

  // -- transmit side ---------------------------------------------------
  _wait_fc(timeout_ms = 1000) {
    return new Promise((res, rej) => {
      const t = setTimeout(() => {
        const i = this._tx_waiters.indexOf(cb);
        if (i >= 0) this._tx_waiters.splice(i, 1);
        rej(new Error("ISO-TP flow-control timeout"));
      }, timeout_ms);
      const cb = (fc) => {
        clearTimeout(t);
        res(fc);
      };
      this._tx_waiters.push(cb);
    });
  }

  static _stmin_ms(stmin) {
    if (stmin <= 0x7f) return stmin;
    if (stmin >= 0xf1 && stmin <= 0xf9) return (stmin - 0xf0) / 10;
    return 127;
  }

  async _send_message(data) {
    const max = this._max;
    if (data.length <= max - 1 && (max === 8 || data.length <= 7)) {
      this._send_frame(Buffer.concat([Buffer.from([data.length]), data]));
      return;
    }
    if (max > 8 && data.length <= max - 2) {
      this._send_frame(Buffer.concat([Buffer.from([0x00, data.length]), data]));
      return;
    }
    // First Frame
    let header;
    if (data.length <= 0xfff) header = Buffer.from([0x10 | (data.length >> 8), data.length & 0xff]);
    else {
      header = Buffer.alloc(6);
      header[0] = 0x10;
      header[1] = 0x00;
      header.writeUInt32BE(data.length, 2);
    }
    let off = max - header.length;
    this._send_frame(Buffer.concat([header, data.subarray(0, off)]));
    let seq = 1;
    for (;;) {
      let fc = await this._wait_fc();
      while (fc.fs === 1) fc = await this._wait_fc(); // WAIT
      if (fc.fs === 2) throw new Error("ISO-TP receiver overflow");
      const bs = fc.bs;
      const gap = _IsoTpSocket._stmin_ms(fc.stmin);
      let sent = 0;
      while (off < data.length) {
        const chunk = data.subarray(off, off + max - 1);
        this._send_frame(Buffer.concat([Buffer.from([0x20 | (seq & 0x0f)]), chunk]));
        seq = (seq + 1) & 0x0f;
        off += chunk.length;
        sent += 1;
        if (off >= data.length) return;
        if (gap > 0) await new Promise((r) => setTimeout(r, gap));
        if (bs && sent >= bs) break;
      }
    }
  }

  _write(chunk, _enc, cb) {
    this._send_message(Buffer.from(chunk)).then(() => cb(), cb);
  }

  _final(cb) {
    cb();
  }

  _destroy(err, cb) {
    try {
      this._ch.removeListener("onMessage", this._on_msg);
    } catch {
      /* ignore */
    }
    cb(err);
  }

  fileno() {
    return -1;
  }

  close() {
    this.destroy();
  }
}

/**
 * CAN / CAN-FD transport (ISO-TP segmented stream over SocketCAN).
 *
 * Parameters
 * ----------
 * channel : str, default ``"vcan0"``
 *     SocketCAN interface name.
 * bustype : str, default ``"socketcan"``
 *     Bus backend.
 * tx_id, rx_id : int
 *     ISO-TP arbitration ids for this endpoint's transmit / receive.
 * fd : bool, default ``False``
 *     Use CAN-FD (64-byte frames).
 */
export class _LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL extends _P2PStreamRPCProtocol {
  static protocol_name = "can";
  static _TOKEN_ALIASES = Object.freeze(new Set(["can", "canfd", "can-fd"]));

  static {
    define_fields(this, {
      channel: ["str", Field({ default: "vcan0" })],
      bustype: ["str", Field({ default: "socketcan" })],
      tx_id: ["int", Field({ default: 0x123 })],
      rx_id: ["int", Field({ default: 0x456 })],
      fd: ["bool", Field({ default: false })],
    });
    define_private(this, {
      _bus: PrivateAttr({ default: null }),
      _isotp_sock: PrivateAttr({ default: null }),
      _read_transport: PrivateAttr({ default: null }),
    });
  }

  /** Accept ``"can"`` plus CAN-FD aliases. */
  static matches_token(token) {
    return this._TOKEN_ALIASES.has(token.toLowerCase());
  }

  /** Claim ``can://`` URIs. */
  static can_handle_uri(uri) {
    return uri.startsWith("can://");
  }

  async _open_stream() {
    let socketcan;
    try {
      socketcan = _require("socketcan");
    } catch (exc) {
      if (exc && (exc.code === "MODULE_NOT_FOUND" || exc.code === "ERR_MODULE_NOT_FOUND")) {
        const err = new RuntimeError(_INSTALL_HINT);
        err.__cause__ = exc;
        throw err;
      }
      throw exc;
    }
    const bus = socketcan.createRawChannel(this.channel, true);
    this._bus = bus;
    const sock = new _IsoTpSocket(bus, this.rx_id, this.tx_id, { fd: this.fd, stmin: 5, bs: 10 });
    bus.start();
    this._isotp_sock = sock;
    const [reader, writer] = asyncio._wrap_stream_socket(sock);
    this._read_transport = writer.transport;
    return [reader, writer];
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
    if (this._isotp_sock !== null && this._isotp_sock !== undefined) {
      try {
        this._isotp_sock.close();
      } catch {
        /* ignore */
      }
      this._isotp_sock = null;
    }
    if (this._bus !== null && this._bus !== undefined) {
      try {
        this._bus.stop();
      } catch {
        /* ignore */
      }
      this._bus = null;
    }
  }
}
register_comm_protocol(_LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL);

register("laila.policy.central.communication.protocols.wired.can", { _LAILA_IDENTIFIABLE_CAN_COMM_PROTOCOL, _IsoTpSocket });
