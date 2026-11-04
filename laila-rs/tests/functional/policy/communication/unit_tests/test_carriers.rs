//! Hand-written twin of `tests/functional/policy/communication/unit_tests/test_carriers.py`
//! (override `handwritten:`): the in-memory lossy "ether" the Python test
//! builds by subclassing `_DatagramRPCProtocol` is a [`DatagramLink`]
//! installed on a plain datagram transport here.
#![allow(non_snake_case, non_upper_case_globals, dead_code, unused_variables, unused_imports, unused_mut, unused_assignments, clippy::all)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use laila::prelude::*;
use laila::macros::defaults::DefaultPolicy;
use laila::policy::central::communication::protocol as rpc;
use laila::policy::central::communication::protocols::_carriers::codec;
use laila::policy::central::communication::protocols::_carriers::datagram::{
    self, DatagramLink, HeaderStruct, _HEADER, _TYPE_DATA, _DatagramRPCProtocol,
};
use laila::policy::central::communication::protocols::_carriers::uri::uri_authority;
use laila::policy::central::communication::protocols::base::{
    CarrierKind, FieldDefault, Protocol, ProtocolSpec, Wire, SPEC_DEFAULTS,
};
use laila::python::sync::{wait_until, Condvar, Mutex};
use laila::python::{Args, LailaError};

// ------------------------------------------------------------------ #
// Datagram carrier over an in-memory lossy fake link
// ------------------------------------------------------------------ #

/// `_ETHER: dict = {}` -- address -> the endpoint listening there.
static _ETHER: std::sync::LazyLock<Mutex<HashMap<String, Arc<EtherEndpoint>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

fn ether() -> laila::python::sync::MutexGuard<'static, HashMap<String, Arc<EtherEndpoint>>> {
    _ETHER.lock()
}

/// `_ETHER.clear()`
pub fn ether_clear() {
    ether().clear();
}

/// One endpoint on the ether: its inbound packet queue.
pub struct EtherEndpoint {
    addr: String,
    inbox: Mutex<VecDeque<(String, Vec<u8>)>>,
    cv: Condvar,
}

/// The lossy link: `_create_datagram_endpoint` registers the endpoint on
/// the ether, `_sendto` drops the first `drop_first_data` DATA packets.
pub struct LossyLink {
    endpoint: Arc<EtherEndpoint>,
    drop_first_data: i64,
    dropped: AtomicI64,
}

impl DatagramLink for LossyLink {
    fn open(&self) -> Result<()> {
        ether().insert(self.endpoint.addr.clone(), self.endpoint.clone());
        Ok(())
    }
    fn local_addr(&self) -> String {
        self.endpoint.addr.clone()
    }
    fn resolve_peer_addr(&self, uri: &str) -> Result<String> {
        Ok(uri_authority(uri))
    }
    fn send_to(&self, addr: &str, packet: &[u8]) -> Result<()> {
        // Deterministically drop the first few DATA packets to test retry.
        if let Some((_magic, ptype, _i, _c)) = HeaderStruct::unpack_bytes(packet) {
            if ptype as i64 == _TYPE_DATA && self.dropped.load(Ordering::Relaxed) < self.drop_first_data {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        }
        let target = ether().get(addr).cloned();
        if let Some(t) = target {
            t.inbox.lock().push_back((self.endpoint.addr.clone(), packet.to_vec()));
            t.cv.notify_all();
        }
        Ok(())
    }
    fn recv_from(&self) -> Result<Option<(String, Vec<u8>)>> {
        let mut g = self.endpoint.inbox.lock();
        wait_until(&self.endpoint.cv, &mut g, Some(0.1), |q| !q.is_empty());
        Ok(g.pop_front())
    }
    fn close(&self) {
        ether().remove(&self.endpoint.addr);
    }
}

/// `_LossyDatagramProtocol`'s class description (`protocol_name = "fakedg"`).
pub static LOSSY_SPEC: ProtocolSpec = ProtocolSpec {
    class_name: "_LossyDatagramProtocol",
    module: "test_carriers",
    protocol_name: "fakedg",
    tokens: &["fakedg"],
    schemes: &["fakedg://"],
    carrier: CarrierKind::Datagram,
    wire: Wire::None,
    fields: &[("addr", FieldDefault::Str("")), ("drop_first_data", FieldDefault::Int(0))],
    bases: &["_DatagramRPCProtocol", "_CarrierRPCProtocol", "_LAILA_IDENTIFIABLE_COMM_PROTOCOL"],
    ..SPEC_DEFAULTS
};

/// Datagram carrier whose 'wire' is an in-process registry.
///
/// Drops the first ``drop_first_data`` DATA packets to force the
/// ack/retransmit path; small ``mtu`` forces fragmentation.
pub struct _LossyDatagramProtocol;

impl _LossyDatagramProtocol {
    /// The class object (`factory=_LossyDatagramProtocol` in the teardown pairs).
    pub fn class_object() -> laila::python::PyClass {
        laila::python::PyClass::with_ctor(
            LOSSY_SPEC.class_name,
            LOSSY_SPEC.module,
            Arc::new(|a: Args| Self::new(a).map(laila::python::IntoArg::into_arg)),
        )
    }
    pub fn new(args: impl Into<Args>) -> Result<Protocol> {
        let proto = Protocol::new(&LOSSY_SPEC, args)?;
        let endpoint = Arc::new(EtherEndpoint {
            addr: proto.str_field("addr"),
            inbox: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
        });
        datagram::set_link(
            &proto,
            Arc::new(LossyLink {
                endpoint,
                drop_first_data: proto.i64_field("drop_first_data", 0),
                dropped: AtomicI64::new(0),
            }),
        );
        Ok(proto)
    }
}

impl laila::python::IntoArg for _LossyDatagramProtocol {
    fn into_arg(self) -> Arg {
        laila::python::IntoArg::into_arg(_LossyDatagramProtocol::class_object())
    }
}
impl laila::python::builtins::ClassValue for _LossyDatagramProtocol {
    fn class_value() -> Arg {
        laila::python::IntoArg::into_arg(_LossyDatagramProtocol::class_object())
    }
}
impl laila::python::PyArg for _LossyDatagramProtocol {
    fn to_arg(&self) -> Arg {
        laila::python::IntoArg::into_arg(_LossyDatagramProtocol::class_object())
    }
}

laila::python! {
    #![unittest]
    #![modules(codec)]
    #![functions(ether_clear)]

    // ------------------------------------------------------------------ #
    // Codec
    // ------------------------------------------------------------------ #

    py_class!(TestCodec);
    impl TestCodec {
        fn test_json_roundtrip(&self) {
            let msg = {"jsonrpc": "2.0", "method": "rpc.call", "params": {"path": ["a", "b"]}, "id": "1"};
            self.assertEqual(codec.decode(codec.encode(msg, "json"), "json"), msg);
        }

        fn test_msgpack_roundtrip(&self) {
            let msg = {"jsonrpc": "2.0", "result": [1, 2, {"x": True}], "id": "2"};
            self.assertEqual(codec.decode(codec.encode(msg, "msgpack"), "msgpack"), msg);
        }

        fn test_unknown_codec_raises(&self) {
            with self.assertRaises(ValueError) {
                codec.encode({}, "yaml");
            }
            with self.assertRaises(ValueError) {
                codec.decode(b"", "yaml");
            }
        }

        fn test_encodes_laila_entry_under_both_codecs(&self) {
            // A real laila Entry must survive both codecs as the canonical
            // ``Entry.serialize`` envelope (F-W21-03) so a peer can rebuild it
            // with build_by_scope instead of receiving a lossy ``{}``.
            let entry = laila.constant(data={"k": [1, 2, 3]});
            let msg = rpc.make_result("rid-1", entry);
            for c in ("json", "msgpack") {
                let decoded = codec.decode(codec.encode(msg, c), c);
                self.assertEqual(decoded["id"], "rid-1");
                self.assertIsInstance(decoded["result"], dict);
                self.assertTrue(rpc.is_entry_envelope(decoded["result"]));
                self.assertEqual(decoded["result"]["_uuid"], str(entry.uuid));
                let (rebuilt, _) = rpc.rehydrate_entry_args([decoded["result"]], {});
                self.assertEqual(rebuilt[0].data, {"k": [1, 2, 3]});
            }
        }

        fn test_frame_prefix_is_length(&self) {
            let payload = b"hello world";
            let framed = codec.frame(payload);
            let (length,) = r#struct.unpack(">I", framed[:4]);
            self.assertEqual(length, len(payload));
            self.assertEqual(framed[4:], payload);
        }

        fn test_read_frame_roundtrip(&self) {
            // `asyncio.StreamReader` + `feed_data` / `feed_eof`: an in-memory
            // `io.BytesIO` reader plays the fed-then-closed stream.
            let reader = io.BytesIO(codec.frame(b"abc") + codec.frame(b"defg"));
            self.assertEqual(codec.read_frame(reader), b"abc");
            self.assertEqual(codec.read_frame(reader), b"defg");
            self.assertIsNone(codec.read_frame(reader));
        }
    }

    fn _mk(proto: Arg) -> Arg {
        let p = DefaultPolicy();
        p.central.communication.add_connection(proto);
        return p;
    }

    py_class!(TestDatagramReliability);
    impl TestDatagramReliability {
        fn setUp(&self) {
            ether_clear();
        }

        fn _run_echo(&self, drop_first_data: i64, payload: String) -> Arg {
            let a = _mk(_LossyDatagramProtocol(addr="A", mtu=64, ack_timeout=0.05));
            let b = _mk(
                _LossyDatagramProtocol(
                    addr="B",
                    mtu=64,
                    ack_timeout=0.05,
                    drop_first_data=drop_first_data,
                ),
            );
            try {
                let pb = next(iter(b.central.communication.connections.values()));
                let rid = a.central.communication.add_peer("fakedg://B", pb.peer_secret_key);
                self.assertEqual(rid, b.global_id);
                object.__setattr__(b, "echo", lambda m: f "len={len(m)}");
                let remote_b = a.central.communication.peers[b.global_id];
                return remote_b.echo(payload);
            } finally {
                a.central.communication.stop();
                b.central.communication.stop();
            }
        }

        fn test_fragmented_echo_no_loss(&self) {
            // ~4KB payload over a 64-byte MTU => ~64 fragments.
            self.assertEqual(self._run_echo(0, "z" * 4000), "len=4000");
        }

        fn test_fragmented_echo_with_loss_recovers(&self) {
            // Drop the first 3 data packets; ack/retransmit must recover.
            self.assertEqual(self._run_echo(3, "q" * 2000), "len=2000");
        }
    }
}
