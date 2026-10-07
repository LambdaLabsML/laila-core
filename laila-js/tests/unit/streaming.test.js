/**
 * Stream-lane liveness / fake-peer / HEVC / UART-teardown suites: ports of
 *   tests/functional/policy/communication/streaming/unit_tests/test_channels_liveness.py
 *   tests/functional/policy/communication/streaming/unit_tests/test_fake_peers_pty.py
 *   tests/functional/policy/communication/streaming/unit_tests/test_hevc_stream.py
 *   tests/functional/policy/communication/streaming/unit_tests/test_uart_teardown.py
 *
 * ``_helpers.TwoPolicies`` is reproduced below (two ``DefaultPolicy``
 * instances peered over loopback / raw TCP on 127.0.0.1); ``_hevc`` lives in
 * ``fixtures/hevc.js``.
 *
 * PTY-based fixtures (``UartPtyPair`` / ``UartNullModem`` / ``FakeRpcOnlyPeer``
 * / ``FakeLanePeer``) rest on ``os.openpty()``: Node has no PTY API and the JS
 * port has no PTY/serial emulator (``wired/uart.js`` needs the optional
 * ``serialport`` package, which is not installed), so every test that drives
 * the UART carrier through a pty is skipped with that reason.
 *
 * Python's producer/consumer threads block in ``time.sleep`` / ``relay``
 * loops. laila-js threads are cooperative (a blocking wait pumps the loop
 * underneath the caller's frame), so a thread that would spin on a stop flag
 * is expressed as a ``Thread`` whose target is an ``async`` body (``await
 * asyncio.sleep`` / ``for await`` over the relay) -- the same concurrency, the
 * same assertions.
 */
import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";

const S = new URL("../../src/", import.meta.url).href;
const laila = (await import("./fixtures/laila_root.js")).default;
const H = await import("./fixtures/hevc.js");
const defaults = await import(S + "macros/defaults.js");
const E = await import(S + "_compat/errors.js");
const TH = await import(S + "_compat/threading.js");
const time = await import(S + "_compat/time.js");
const asyncio = await import(S + "_compat/asyncio.js");

const { DefaultPolicy } = defaults;

const PTY_SKIP =
  "needs os.openpty(): Node has no PTY API and the JS port has no PTY/serial emulator " +
  "(wired/uart.js requires the optional `serialport` package, which is not installed)";

const STRESS_SECONDS = Number(process.env.LAILA_STREAM_STRESS_SECONDS ?? "10");
const N_FRAMES = Number.parseInt(process.env.LAILA_HEVC_FRAMES ?? "300", 10);
const FPS = Number(process.env.LAILA_HEVC_FPS ?? "30");

// ---------------------------------------------------------------------------
// helpers (_helpers.py)
// ---------------------------------------------------------------------------

/** Run a body on a fresh macrotask (blocking waits cannot run from a microtask). */
function macrotask(fn) {
  return new Promise((resolve, reject) =>
    setImmediate(() => {
      try {
        resolve(fn());
      } catch (e) {
        reject(e);
      }
    }),
  );
}

function wait_until(pred, timeout = 5.0, step = 0.01) {
  const deadline = time.monotonic() + timeout;
  while (time.monotonic() < deadline) {
    if (pred()) return true;
    time.sleep(step);
  }
  return Boolean(pred());
}

const _conn = (policy) => Object.values(policy.central.communication.connections)[0];

/**
 * Two in-process policies peered over one transport kind (``_helpers.TwoPolicies``).
 * ``opts.liveness_interval`` goes on both hubs; every other key is a protocol field.
 */
function TwoPolicies(kind = "loopback", opts = {}) {
  const { liveness_interval = null, ...proto_kwargs } = opts;
  const cls = { loopback: defaults.DefaultLoopbackProtocol, tcp: defaults.DefaultTCPProtocol, udp: defaults.DefaultUDPProtocol }[kind];
  if (cls === undefined) throw new E.ValueError(kind);
  const kw = { ...proto_kwargs };
  if (kind === "tcp" || kind === "udp") kw.host ??= "127.0.0.1";
  const pa = new cls(kw);
  const pb = new cls(kw);
  const a = new DefaultPolicy();
  const b = new DefaultPolicy();
  if (liveness_interval !== null) {
    a.central.communication.liveness_interval = liveness_interval;
    b.central.communication.liveness_interval = liveness_interval;
  }
  a.central.communication.add_connection(pa);
  b.central.communication.add_connection(pb);
  const aid = a.global_id;
  const uri = kind === "loopback" ? `loopback://${b.global_id}` : `${kind}://127.0.0.1:${pb.bound_port}`;
  const bid = a.central.communication.add_peer(uri, pb.peer_secret_key);
  assert.equal(bid, b.global_id);
  if (!wait_until(() => aid in b.central.communication.peers)) throw new E.RuntimeError("peer B never registered A");
  const close = () => {
    for (const pol of [a, b]) {
      try {
        pol.central.communication.stop();
      } catch {
        /* best effort */
      }
    }
  };
  return { kind, a, b, pa, pb, aid, bid, ca: a.central.communication, cb: b.central.communication, close };
}

/** ``with make_pair() as tp:`` */
function with_pair(make_pair, fn) {
  const tp = make_pair();
  try {
    return fn(tp);
  } finally {
    tp.close();
  }
}

const _blob = (n, tag) => {
  const seed = crypto.createHash("sha256").update(`${n}:${tag}`).digest();
  if (!n) return Buffer.alloc(0);
  const reps = Math.ceil(n / seed.length);
  return Buffer.concat(Array(reps).fill(seed)).subarray(0, n);
};

const sha256 = (b) => crypto.createHash("sha256").update(b).digest("hex");

function median(xs) {
  const s = [...xs].sort((x, y) => x - y);
  const n = s.length;
  if (n === 0) throw new E.ValueError("median of empty list");
  return n % 2 ? s[(n - 1) / 2] : (s[n / 2 - 1] + s[n / 2]) / 2;
}

/** Python ``threading.Thread(target=..., daemon=True).start()`` with an async body. */
function spawn(target) {
  const th = new TH.Thread({ target, daemon: true });
  th.start();
  return th;
}

// ---------------------------------------------------------------------------
// test_channels_liveness.py
// ---------------------------------------------------------------------------
describe("TestLivenessDuringStream", () => {
  test("test_stream_then_peer_death_is_detected", () =>
    macrotask(() =>
      with_pair(
        () => TwoPolicies("tcp", { liveness_interval: 0.3, ping_timeout: 2.0 }),
        (tp) => {
          const ch_a = tp.ca.peers[tp.bid].channel("video");
          const ch_b = tp.cb.peers[tp.aid].channel("video");
          const stop = new TH.Event();
          const frame = _blob(200_000, 8);

          spawn(async () => {
            while (!stop.is_set()) {
              try {
                ch_a.send(frame);
              } catch (e) {
                if (e instanceof E.ConnectionError) return;
                throw e;
              }
              await asyncio.sleep(1 / 60);
            }
          });
          const received = [0];
          const ended = new TH.Event();

          spawn(async () => {
            for await (const _e of laila.relay(ch_b)) received[0] += 1;
            ended.set();
          });
          time.sleep(2.0); // several liveness rounds under load
          assert.ok(tp.bid in tp.ca.peers);
          assert.ok(tp.aid in tp.cb.peers);
          assert.ok(received[0] > 30, `received=${received[0]}`);
          assert.ok(!ended.is_set());

          // kill A's transport abruptly: B's socket sees EOF -> relay ends
          const t0 = time.monotonic();
          tp.pa.stop();
          stop.set();
          assert.ok(ended.wait(10), "relay did not end after peer death");
          const detect = time.monotonic() - t0;
          console.log(`\n[tcp liveness] peer death -> relay ended in ${(detect * 1e3).toFixed(0)}ms`);
          assert.ok(ch_b.closed);
          assert.ok(ch_a.closed);
          assert.throws(() => ch_b.send(Buffer.from("x")), E.ConnectionError);
          assert.throws(() => ch_a.send(Buffer.from("x")), E.ConnectionError);
          assert.ok(wait_until(() => !(tp.aid in tp.cb.peers), 10));
        },
      ),
    ));

  test("test_silent_peer_is_dropped_by_liveness_while_streaming_in", () =>
    macrotask(() =>
      with_pair(
        () => TwoPolicies("tcp", { liveness_interval: 0.3, ping_timeout: 1.0 }),
        (tp) => {
          // B keeps *receiving* frames but A stops answering pings -> B drops A.
          const ch_a = tp.ca.peers[tp.bid].channel("video");
          const ch_b = tp.cb.peers[tp.aid].channel("video");
          // Make A deaf to every inbound request (so B's pings go unanswered)
          // while A keeps streaming. Instance-level patch wins over the class method.
          tp.pa._handle_request_frame = () => null;
          const stop = new TH.Event();
          const tick = Buffer.from("tick".repeat(1000));

          spawn(async () => {
            while (!stop.is_set()) {
              try {
                ch_a.send(tick);
              } catch (e) {
                if (e instanceof E.ConnectionError) return;
                throw e;
              }
              await asyncio.sleep(0.01);
            }
          });
          try {
            const t0 = time.monotonic();
            assert.ok(wait_until(() => !(tp.aid in tp.cb.peers), 10), "B kept A alive on stream traffic alone");
            console.log(`\n[tcp liveness] deaf-but-streaming peer dropped in ${(time.monotonic() - t0).toFixed(2)}s`);
            assert.ok(wait_until(() => ch_b.closed, 5));
            // buffered frames drain, then the relay ends
            let drained = 0;
            for (const _e of laila.relay(ch_b, { timeout: 5 })) drained += 1;
            assert.ok(drained <= tp.pb.channel_queue_size, `drained=${drained}`);
            assert.throws(() => laila.relay(ch_b, { timeout: 5 }).__next__(), E.StopIteration);
          } finally {
            stop.set();
          }
        },
      ),
    ));
});

describe("TestSustainedStress", () => {
  const _stress = (make_pair, label, frame_bytes, fps) =>
    with_pair(make_pair, (tp) => {
      const ch_a = tp.ca.peers[tp.bid].channel("video");
      const ch_b = tp.cb.peers[tp.aid].channel("video");
      const stop = new TH.Event();
      const frame = _blob(frame_bytes, 14);
      const sent = [0];

      spawn(async () => {
        const period = 1.0 / fps;
        let nxt = time.monotonic();
        while (!stop.is_set()) {
          try {
            ch_a.send(frame);
            sent[0] += 1;
          } catch (e) {
            if (e instanceof E.ConnectionError) return;
            throw e;
          }
          nxt += period;
          const delay = nxt - time.monotonic();
          if (delay > 0) await asyncio.sleep(delay);
          else {
            nxt = time.monotonic();
            await asyncio.sleep(0);
          }
        }
      });

      const received = [0];
      const bad = [0];
      spawn(async () => {
        for await (const e of laila.relay(ch_b)) {
          received[0] += 1;
          if (e.data.length !== frame_bytes) bad[0] += 1;
        }
      });

      const rtts = [];
      const t_end = time.monotonic() + STRESS_SECONDS;
      while (time.monotonic() < t_end) {
        const t0 = time.monotonic();
        const ok = tp.pa.ping(tp.bid);
        rtts.push(time.monotonic() - t0);
        assert.ok(ok, "ping failed under sustained load");
        time.sleep(0.25);
      }
      stop.set();
      time.sleep(0.2);
      const med = median(rtts);
      const worst = Math.max(...rtts);
      const rate_mb = (received[0] * frame_bytes) / STRESS_SECONDS / 1e6;
      console.log(
        `\n[${label} stress ${STRESS_SECONDS.toFixed(0)}s] sent=${sent[0]} rcvd=${received[0]} ` +
          `(${rate_mb.toFixed(1)} MB/s delivered) tx_dropped=${ch_a.tx_dropped} ` +
          `rx_dropped=${ch_b.dropped} discarded=${ch_b.discarded} ` +
          `ping median=${(med * 1e3).toFixed(1)}ms max=${(worst * 1e3).toFixed(1)}ms`,
      );
      assert.ok(tp.bid in tp.ca.peers, "false liveness drop (A side)");
      assert.ok(tp.aid in tp.cb.peers, "false liveness drop (B side)");
      assert.equal(bad[0], 0);
      assert.equal(ch_b.discarded, 0);
      assert.ok(worst < tp.pa.ping_timeout, `ping max ${worst.toFixed(3)}s`);
      assert.ok(received[0] > 0);
      assert.ok(ch_b.qsize() <= tp.pb.channel_queue_size);
    });

  test("test_tcp_sustained", () =>
    macrotask(() => _stress(() => TwoPolicies("tcp", { liveness_interval: 0.5, ping_timeout: 3.0 }), "tcp", 500_000, 120)));

  test("test_loopback_sustained", () =>
    macrotask(() => _stress(() => TwoPolicies("loopback", { liveness_interval: 0.5 }), "loopback", 500_000, 240)));

  test("test_uart_null_modem_sustained", { skip: PTY_SKIP }, () => {});
});

// ---------------------------------------------------------------------------
// test_fake_peers_pty.py -- every test drives DefaultUARTProtocol on an
// os.openpty() slave with a fake peer on the master (UartPtyPair).
// ---------------------------------------------------------------------------
describe("TestRpcOnlyPeer", () => {
  test("test_older_peer_without_caps", { skip: PTY_SKIP }, () => {});
});

describe("TestSaturatedUart", () => {
  test("test_pings_and_rpc_under_saturated_stream", { skip: PTY_SKIP }, () => {});
});

describe("TestFirmwareLanePeer", () => {
  test("test_static_inbound_lane", { skip: PTY_SKIP }, () => {});
  test("test_malformed_and_unknown_reserved_frames_are_dropped", { skip: PTY_SKIP }, () => {});
});

describe("TestLossyLane", () => {
  test("test_only_complete_messages_are_delivered", { skip: PTY_SKIP }, () => {});
});

// ---------------------------------------------------------------------------
// test_hevc_stream.py
// ---------------------------------------------------------------------------
describe("TestGeneratorAndParser", () => {
  // The reference parser reproduces the generator's ground truth offline.
  test("test_roundtrip_offline", () => {
    const aus = H.generate({ seed: 1234, n_frames: N_FRAMES });
    const gt = H.ground_truth(aus);
    assert.equal(gt.au_count, N_FRAMES);
    assert.equal(gt.idr_count, Math.ceil(N_FRAMES / 30));
    const parsed = H.parse_annexb(Buffer.concat(H.packetize_per_au(aus)));
    assert.equal(parsed.length, gt.au_count);
    assert.equal(parsed.filter((p) => p.is_idr).length, gt.idr_count);
    assert.deepEqual(parsed.map((p) => p.size), gt.au_sizes);
    assert.deepEqual(parsed.map((p) => p.types), aus.map((au) => au.types));
    assert.deepEqual(H.histogram(parsed), gt.nal_histogram);
    for (const au of aus) {
      for (const [, nal] of au.nals) {
        const body = nal.subarray(4);
        assert.equal(body.indexOf(Buffer.from([0x00, 0x00, 0x01])), -1);
        assert.equal(body.indexOf(Buffer.from([0x00, 0x00, 0x00])), -1);
        assert.notEqual(body[body.length - 1], 0);
      }
    }
    assert.deepEqual(
      [...H.ground_truth(H.generate({ seed: 1234, n_frames: 5 })).au_sizes].sort((x, y) => x - y),
      [...H.ground_truth(H.generate({ seed: 1234, n_frames: 5 })).au_sizes].sort((x, y) => x - y),
    );
  });
});

describe("TestHevcOverLanes", () => {
  let aus, gt;
  before(() => {
    aus = H.generate({ seed: 1234, n_frames: N_FRAMES });
    gt = H.ground_truth(aus);
  });

  const _run = (make_pair, label, per_nal) => {
    const messages = per_nal ? H.packetize_per_nal(aus) : H.packetize_per_au(aus);
    const expected_hashes = messages.map(sha256);
    const total = messages.reduce((s, m) => s + m.length, 0);
    with_pair(make_pair, (tp) => {
      const ch_a = tp.ca.peers[tp.bid].channel("video");
      const ch_b = tp.cb.peers[tp.aid].channel("video");
      const send_times = [];
      const got = [];
      const done = new TH.Event();

      const tc = spawn(async () => {
        for await (const e of laila.relay(ch_b, { timeout: 30 })) {
          got.push(e);
          if (got.length === messages.length) break;
        }
        done.set();
      });
      // pace at FPS access units per second (per-NAL: all NALs of an AU go
      // out together, like an encoder callback would)
      const period = 1.0 / FPS;
      const t_start = time.monotonic();
      let nxt = t_start;
      const groups = per_nal ? aus.map((au) => au.nals.map(([, n]) => n)) : aus.map((au) => [au.data]);
      for (const group of groups) {
        for (const m of group) {
          send_times.push(time.monotonic());
          ch_a.send(m);
        }
        nxt += period;
        const delay = nxt - time.monotonic();
        if (delay > 0) time.sleep(delay);
      }
      const t_sent = time.monotonic();
      assert.ok(done.wait(60), `${label}: only ${got.length}/${messages.length} received${tc.exception ? ` (${tc.exception})` : ""}`);
      const t_done = time.monotonic();

      // byte-exact, in order, no loss
      assert.equal(got.length, messages.length);
      assert.deepEqual(got.map((e) => sha256(e.data)), expected_hashes);
      assert.deepEqual(got.map((e) => e.stream.seq), messages.map((_, i) => i));
      assert.equal(ch_b.dropped, 0);
      assert.equal(ch_b.discarded, 0);
      assert.equal(ch_a.tx_dropped, 0);

      // parse what arrived with the independent parser
      const stream = Buffer.concat(got.map((e) => Buffer.from(e.data)));
      const parsed = H.parse_annexb(stream);
      assert.equal(parsed.length, gt.au_count);
      assert.equal(parsed.filter((p) => p.is_idr).length, gt.idr_count);
      assert.deepEqual(parsed.map((p) => p.size), gt.au_sizes);
      assert.deepEqual(H.histogram(parsed), gt.nal_histogram);
      if (per_nal) {
        for (const e of got) {
          const nals = H.split_nals(e.data);
          assert.equal(nals.length, 1, "per-NAL entry must hold exactly one NAL");
        }
      } else {
        got.forEach((e, i) => {
          const au = aus[i];
          const p = H.parse_annexb(e.data);
          assert.equal(p.length, 1, "per-AU entry must hold exactly one AU");
          assert.deepEqual(p[0].types, au.types);
          assert.equal(p[0].size, au.size);
        });
      }

      const lat = got.map((e, i) => e.stream.arrived_at - send_times[i]);
      const lat_sorted = [...lat].sort((x, y) => x - y);
      const p50 = median(lat);
      const p99 = lat_sorted[Math.trunc(0.99 * (lat.length - 1))];
      const wall = t_done - t_start;
      console.log(
        `\n[hevc ${label} ${per_nal ? "per-NAL" : "per-AU"}] ${messages.length} msgs ` +
          `${(total / 1e6).toFixed(2)} MB in ${wall.toFixed(1)}s (${(total / wall / 1e6).toFixed(2)} MB/s, paced ${FPS.toFixed(0)} fps) ` +
          `latency p50=${(p50 * 1e3).toFixed(2)}ms p99=${(p99 * 1e3).toFixed(2)}ms max=${(lat_sorted[lat_sorted.length - 1] * 1e3).toFixed(2)}ms ` +
          `tail=${((t_done - t_sent) * 1e3).toFixed(0)}ms; RPC ping=${tp.pa.ping(tp.bid)}`,
      );
      assert.ok(tp.bid in tp.ca.peers);
      assert.ok(tp.aid in tp.cb.peers);
    });
  };

  test("test_loopback_per_au", () => macrotask(() => _run(() => TwoPolicies("loopback"), "loopback", false)));
  test("test_loopback_per_nal", () => macrotask(() => _run(() => TwoPolicies("loopback"), "loopback", true)));
  test("test_tcp_per_au", () => macrotask(() => _run(() => TwoPolicies("tcp"), "tcp", false)));
  test("test_tcp_per_nal", () => macrotask(() => _run(() => TwoPolicies("tcp"), "tcp", true)));
  test("test_uart_null_modem_per_au", { skip: PTY_SKIP }, () => {});
  test("test_uart_null_modem_per_nal", { skip: PTY_SKIP }, () => {});
  // msgpack is built into the JS port (src/_codecs/msgpack.js): no ImportError skip.
  test("test_tcp_msgpack_per_au", () => macrotask(() => _run(() => TwoPolicies("tcp", { codec: "msgpack" }), "tcp-msgpack", false)));
});

// ---------------------------------------------------------------------------
// test_uart_teardown.py -- every test runs DefaultUARTProtocol on an
// os.openpty() pair (UartPtyPair) and inspects the asyncio selector / fds.
// ---------------------------------------------------------------------------
describe("TestUartTeardownIsSilent", () => {
  test("test_communication_stop", { skip: PTY_SKIP }, () => {});
  test("test_laila_terminate", { skip: PTY_SKIP }, () => {});
  test("test_remove_connection", { skip: PTY_SKIP }, () => {});
});

describe("TestUartRestart", () => {
  test("test_stop_start_repeer", { skip: PTY_SKIP }, () => {});
});

describe("TestUartTeardownMidFrame", () => {
  test("test_stop_with_partial_frame_pending", { skip: PTY_SKIP }, () => {});
});

describe("TestUartTeardownUnderStream", () => {
  test("test_stop_while_lane_frames_arrive", { skip: PTY_SKIP }, () => {});
});
