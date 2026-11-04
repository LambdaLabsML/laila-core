//! Stream lanes: `Channel`, `Relay`, `StreamEntry`, `StreamMeta`
//! (`laila.policy.central.communication.channel`).
//!
//! A [`Channel`] is one named, bidirectional byte-stream *lane* to a peer,
//! multiplexed over the carrier next to RPC (`laila.peers[gid][name]`).
//! Outbound messages are sliced into chunks by the carrier; inbound
//! chunks are reassembled here (sequence gaps discard the in-progress
//! message whole) and queued, bounded by message count and bytes, with
//! the *oldest* message evicted when full (`dropped`). A [`Relay`]
//! iterates the queue as [`StreamEntry`] objects -- plain READY entries
//! whose `.stream` carries the [`StreamMeta`].

use alloc::collections::VecDeque;
use alloc::string::String;
use alloc::sync::{Arc, Weak};
use alloc::vec::Vec;

use crate::entry::Entry;
use crate::python::builtins::IsInstance;
use crate::python::object::{HasAttrs, PyObject};
use crate::python::sync::{wait_until, Condvar, Mutex};
use crate::python::{pysig, Arg, Args, IntoArg, LailaError, PyArg, PyDictBuilder, Result};

use super::wire as _codec;

/// The carrier side a channel talks back to (`Channel.protocol`).
pub trait ChannelOwner: Send + Sync {
    /// `proto._stream_enqueue(channel, payload)`: hand one outbound
    /// message to the wire. Must not block on I/O.
    fn _stream_enqueue(&self, channel: &Channel, payload: &[u8]) -> Result<()>;
    /// `proto._on_channel_closed_locally(channel)`: unbind and notify the peer.
    fn _on_channel_closed_locally(&self, channel: &Channel);
    /// `proto.rpc_timeout`
    fn rpc_timeout(&self) -> f64;
    /// `proto.stream_chunk_bytes`
    fn stream_chunk_bytes(&self) -> usize;
    /// The carrier as a Python value (`channel.protocol`).
    fn as_arg(&self) -> Arg;
}

/// Delivery metadata attached to every [`StreamEntry`] (`entry.stream`).
#[derive(Clone, Debug, PartialEq)]
pub struct StreamMeta {
    pub peer_id: String,
    pub channel: String,
    pub lane: u8,
    pub seq: u64,
    pub arrived_at: f64,
    pub arrived_wall: f64,
    pub dropped_before: u64,
}

impl StreamMeta {
    fn attr(&self, name: &str) -> Option<Arg> {
        Some(match name {
            "peer_id" => Arg::Str(self.peer_id.as_str().into()),
            "channel" => Arg::Str(self.channel.as_str().into()),
            "lane" => Arg::Int(self.lane as i64),
            "seq" => Arg::Int(self.seq as i64),
            "arrived_at" => Arg::Float(self.arrived_at),
            "arrived_wall" => Arg::Float(self.arrived_wall),
            "dropped_before" => Arg::Int(self.dropped_before as i64),
            _ => return None,
        })
    }
}

impl PyObject for StreamMeta {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "StreamMeta"
    }
    fn __repr__(&self) -> String {
        alloc::format!(
            "StreamMeta(peer_id={:?}, channel={:?}, lane={}, seq={}, arrived_at={}, arrived_wall={}, dropped_before={})",
            self.peer_id, self.channel, self.lane, self.seq, self.arrived_at, self.arrived_wall, self.dropped_before
        )
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<StreamMeta>()
            .is_some_and(|o| o == self)
    }
    fn __hash__(&self) -> Result<i64> {
        Arg::Str(self.__repr__().into()).__hash__()
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        self.attr(name).ok_or_else(|| {
            LailaError::attribute_error(alloc::format!(
                "'StreamMeta' object has no attribute '{name}'"
            ))
        })
    }
}
impl IntoArg for StreamMeta {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for StreamMeta {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for StreamMeta {
    fn isinstance(x: &Arg) -> bool {
        x.downcast_ref::<StreamMeta>().is_some()
    }
}

/// `StreamEntry(Entry)`: a READY entry built from one stream message,
/// with the delivery metadata under `_stream` / `stream`.
///
/// The port has one `Entry` type; a stream entry is an entry whose
/// `__dict__` carries the metadata, and `isinstance(e, StreamEntry)` tests
/// for that.
pub struct StreamEntry;

impl StreamEntry {
    /// `StreamEntry._from_payload(payload)`: a READY entry wrapping *payload*.
    pub fn _from_payload(payload: &[u8]) -> Result<Entry> {
        let mut a = Args::new();
        a.push_keyword("data", Arg::Bytes(Arc::from(payload)));
        Entry::constant(a)
    }
    /// The metadata of a stream entry, if it is one.
    pub fn stream_meta(e: &Entry) -> Option<StreamMeta> {
        e.__attrs__()
            .get("_stream")
            .and_then(|m| m.downcast_ref::<StreamMeta>().cloned())
    }
}
impl IsInstance for StreamEntry {
    fn isinstance(x: &Arg) -> bool {
        match x {
            Arg::Entry(e) => e.__attrs__().__hasattr__("_stream"),
            _ => false,
        }
    }
}

/// One buffered inbound message: `(payload, seq, arrived_at, wall, dropped_before)`.
enum Item {
    Msg {
        payload: Arc<[u8]>,
        seq: u64,
        arrived_at: f64,
        wall: f64,
        dropped_before: u64,
    },
    Sentinel,
}

impl Item {
    fn len(&self) -> usize {
        match self {
            Item::Msg { payload, .. } => payload.len(),
            Item::Sentinel => 0,
        }
    }
}

struct ChannelState {
    tx_lane_id: Option<u8>,
    closed: bool,
    closed_reason: Option<String>,
    rx_seq: u64,
    tx_seq: u64,
    dropped: u64,
    discarded: u64,
    tx_dropped: u64,
    queue: VecDeque<Item>,
    queue_bytes: usize,
    rx_buf: Vec<Arc<[u8]>>,
    rx_len: usize,
    rx_expected: Option<u64>,
    rx_active: bool,
    rx_poisoned: bool,
    tx_chunk_seq: u64,
    relay: Option<Relay>,
}

/// A carrier stand-in that is a plain Python object (a test's
/// `_DummyProto`): the owner calls go through the dynamic protocol.
struct DynChannelOwner(Arg);

impl ChannelOwner for DynChannelOwner {
    fn _stream_enqueue(&self, channel: &Channel, payload: &[u8]) -> Result<()> {
        self.0
            .__call_method__(
                "_stream_enqueue",
                Args {
                    positional: alloc::vec![
                        channel.to_arg(),
                        crate::python::object::bytes(payload)
                    ],
                    keywords: alloc::vec![],
                },
            )
            .map(|_| ())
    }
    fn _on_channel_closed_locally(&self, channel: &Channel) {
        let _ = self
            .0
            .__call_method__("_on_channel_closed_locally", Args::from(channel.to_arg()));
    }
    fn rpc_timeout(&self) -> f64 {
        self.0
            .__getattr__("rpc_timeout")
            .ok()
            .and_then(|v| v.as_f64())
            .unwrap_or(60.0)
    }
    fn stream_chunk_bytes(&self) -> usize {
        self.0
            .__getattr__("stream_chunk_bytes")
            .ok()
            .and_then(|v| v.as_i64())
            .map(|v| v.max(1) as usize)
            .unwrap_or(65536)
    }
    fn as_arg(&self) -> Arg {
        self.0.clone()
    }
}

pub struct ChannelInner {
    proto: Weak<dyn ChannelOwner>,
    /// Keeps a [`DynChannelOwner`] alive for the channel's lifetime (a
    /// real carrier owns its channels and is only referenced weakly).
    _dyn_owner: Option<Arc<dyn ChannelOwner>>,
    pub peer_id: String,
    pub name: String,
    pub lane_id: u8,
    pub max_message_bytes: usize,
    queue_size: usize,
    queue_bytes_cap: usize,
    state: Mutex<ChannelState>,
    /// Wakes relays (new message / sentinel) and openers (`_opened`).
    cv: Condvar,
}

/// A stream lane to one peer (handle; clones alias one channel).
#[derive(Clone)]
pub struct Channel(pub Arc<ChannelInner>);

impl Channel {
    /// `Channel(protocol, peer_id, name, lane_id, *, queue_size, queue_bytes,
    /// max_message_bytes, tx_lane_id=None)` -- the Python-shaped
    /// constructor. *protocol* is a carrier ([`super::protocols::Protocol`])
    /// or any object exposing `_stream_enqueue` / `_on_channel_closed_locally`.
    #[pysig(
        "Channel(protocol, peer_id, name, lane_id, *, queue_size, queue_bytes, max_message_bytes, tx_lane_id=None)"
    )]
    pub fn new(args: impl Into<Args>) -> Result<Channel> {
        let lane = lane_id
            .as_i64()
            .ok_or_else(|| LailaError::type_error("Channel(): lane_id must be an int"))?;
        let usize_of = |v: &Arg, what: &str| -> Result<usize> {
            v.as_i64().map(|n| n.max(0) as usize).ok_or_else(|| {
                LailaError::type_error(alloc::format!("Channel(): {what} must be an int"))
            })
        };
        let (owner, dyn_owner): (Weak<dyn ChannelOwner>, Option<Arc<dyn ChannelOwner>>) =
            match protocol.downcast_ref::<super::protocols::Protocol>() {
                Some(p) => (p.owner_weak(), None),
                None => {
                    let o: Arc<dyn ChannelOwner> = Arc::new(DynChannelOwner(protocol.clone()));
                    (Arc::downgrade(&o), Some(o))
                }
            };
        let mut ch = Channel::build(
            owner,
            &peer_id.__str__(),
            &name.__str__(),
            lane as u8,
            usize_of(&queue_size, "queue_size")?,
            usize_of(&queue_bytes, "queue_bytes")?,
            usize_of(&max_message_bytes, "max_message_bytes")?,
            tx_lane_id.as_i64().map(|v| v as u8),
        );
        if let Some(o) = dyn_owner {
            Arc::get_mut(&mut ch.0).expect("fresh channel")._dyn_owner = Some(o);
        }
        Ok(ch)
    }

    /// The typed constructor used by the carriers.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        protocol: Weak<dyn ChannelOwner>,
        peer_id: &str,
        name: &str,
        lane_id: u8,
        queue_size: usize,
        queue_bytes: usize,
        max_message_bytes: usize,
        tx_lane_id: Option<u8>,
    ) -> Channel {
        Channel(Arc::new(ChannelInner {
            proto: protocol,
            _dyn_owner: None,
            peer_id: String::from(peer_id),
            name: String::from(name),
            lane_id,
            max_message_bytes,
            queue_size: queue_size.max(1),
            queue_bytes_cap: queue_bytes.max(1),
            state: Mutex::new(ChannelState {
                tx_lane_id,
                closed: false,
                closed_reason: None,
                rx_seq: 0,
                tx_seq: 0,
                dropped: 0,
                discarded: 0,
                tx_dropped: 0,
                queue: VecDeque::new(),
                queue_bytes: 0,
                rx_buf: Vec::new(),
                rx_len: 0,
                rx_expected: None,
                rx_active: false,
                rx_poisoned: false,
                tx_chunk_seq: 0,
                relay: None,
            }),
            cv: Condvar::new(),
        }))
    }

    /// Python `is`.
    pub fn is(&self, other: &Channel) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    /// `channel.protocol` -- the owning carrier (`None` once it is gone).
    pub fn protocol(&self) -> Option<Arc<dyn ChannelOwner>> {
        self.0.proto.upgrade()
    }
    pub fn peer_id(&self) -> &str {
        &self.0.peer_id
    }
    pub fn name(&self) -> &str {
        &self.0.name
    }
    pub fn lane_id(&self) -> u8 {
        self.0.lane_id
    }
    pub fn tx_lane_id(&self) -> Option<u8> {
        self.0.state.lock().tx_lane_id
    }
    /// `channel.tx_lane_id = lane` (the peer re-opened after a lost close).
    pub fn set_tx_lane_id(&self, lane: Option<u8>) {
        self.0.state.lock().tx_lane_id = lane;
    }
    pub fn max_message_bytes(&self) -> usize {
        self.0.max_message_bytes
    }
    pub fn closed(&self) -> bool {
        self.0.state.lock().closed
    }
    pub fn closed_reason(&self) -> Option<String> {
        self.0.state.lock().closed_reason.clone()
    }
    /// `True` once the peer confirmed the lane and it is not closed.
    pub fn opened(&self) -> bool {
        let s = self.0.state.lock();
        s.tx_lane_id.is_some() && !s.closed
    }
    pub fn rx_seq(&self) -> u64 {
        self.0.state.lock().rx_seq
    }
    pub fn tx_seq(&self) -> u64 {
        self.0.state.lock().tx_seq
    }
    pub fn dropped(&self) -> u64 {
        self.0.state.lock().dropped
    }
    pub fn discarded(&self) -> u64 {
        self.0.state.lock().discarded
    }
    pub fn tx_dropped(&self) -> u64 {
        self.0.state.lock().tx_dropped
    }
    /// The carrier's writer evicted one of our queued messages.
    pub fn note_tx_dropped(&self) {
        self.0.state.lock().tx_dropped += 1;
    }
    /// `channel.qsize()`
    pub fn qsize(&self) -> usize {
        self.0
            .state
            .lock()
            .queue
            .iter()
            .filter(|i| matches!(i, Item::Msg { .. }))
            .count()
    }

    /// `channel.send(payload)`: queue one message for the peer.
    ///
    /// `ConnectionError` if closed / not yet open after `rpc_timeout`;
    /// `ValueError` if larger than `max_message_bytes`.
    pub fn send(&self, payload: &[u8]) -> Result<()> {
        if self.closed() {
            return Err(LailaError::connection_error(alloc::format!(
                "Channel {:?} to peer {} is closed ({}). Re-index laila.peers[gid][name] after the peer reconnects.",
                self.0.name,
                self.0.peer_id,
                self.closed_reason().unwrap_or_else(|| String::from("no reason"))
            )));
        }
        if payload.len() > self.0.max_message_bytes {
            return Err(LailaError::value_error(alloc::format!(
                "Stream message of {} bytes exceeds the {}-byte limit of channel {:?} (max_stream_frame_bytes, or mtu - header on datagram carriers).",
                payload.len(),
                self.0.max_message_bytes,
                self.0.name
            )));
        }
        let proto = self.protocol().ok_or_else(|| {
            LailaError::connection_error("The carrier owning this channel no longer exists.")
        })?;
        if self.tx_lane_id().is_none() {
            let wait = proto.rpc_timeout();
            let opened = self.wait_opened(Some(wait));
            if !opened || self.tx_lane_id().is_none() {
                return Err(LailaError::connection_error(alloc::format!(
                    "Channel {:?} to peer {} is still opening.",
                    self.0.name,
                    self.0.peer_id
                )));
            }
            if self.closed() {
                return Err(LailaError::connection_error(alloc::format!(
                    "Channel {:?} to peer {} closed while opening ({}).",
                    self.0.name,
                    self.0.peer_id,
                    self.closed_reason().unwrap_or_default()
                )));
            }
        }
        proto._stream_enqueue(self, payload)?;
        self.0.state.lock().tx_seq += 1;
        Ok(())
    }

    /// Block until `_opened` (the peer confirmed the lane, or the channel
    /// closed); `timeout` in seconds. Returns whether it was set.
    pub fn wait_opened(&self, timeout: Option<f64>) -> bool {
        let mut g = self.0.state.lock();
        wait_until(&self.0.cv, &mut g, timeout, |s| {
            s.tx_lane_id.is_some() || s.closed
        })
    }

    /// Reserve *count* consecutive outbound chunk sequence numbers; the first.
    pub fn _next_chunk_seqs(&self, count: u64) -> u64 {
        let mut s = self.0.state.lock();
        let first = s.tx_chunk_seq;
        s.tx_chunk_seq = (first + count) % _codec::SEQ_MODULUS;
        first
    }

    /// One inbound chunk from the wire (carrier inbound thread).
    pub fn _on_chunk(&self, seq: u64, flags: u8, data: &[u8], arrived_at: f64) {
        let start = flags & _codec::FLAG_START != 0;
        let end = flags & _codec::FLAG_END != 0;
        let payload: Option<Arc<[u8]>> = {
            let mut s = self.0.state.lock();
            if start {
                if s.rx_active {
                    s.discarded += 1;
                }
                s.rx_buf.clear();
                s.rx_len = 0;
                s.rx_active = true;
                s.rx_poisoned = false;
            } else {
                if !s.rx_active {
                    s.rx_expected = Some((seq + 1) % _codec::SEQ_MODULUS);
                    if end {
                        s.discarded += 1;
                    }
                    return;
                }
                if s.rx_expected.is_some_and(|e| seq != e) {
                    s.rx_poisoned = true;
                }
            }
            s.rx_expected = Some((seq + 1) % _codec::SEQ_MODULUS);
            if !s.rx_poisoned {
                s.rx_len += data.len();
                if s.rx_len > self.0.max_message_bytes {
                    s.rx_poisoned = true;
                    s.rx_buf.clear();
                } else {
                    s.rx_buf.push(Arc::from(data));
                }
            }
            if !end {
                return;
            }
            s.rx_active = false;
            let buf = core::mem::take(&mut s.rx_buf);
            s.rx_len = 0;
            if s.rx_poisoned {
                s.rx_poisoned = false;
                s.discarded += 1;
                return;
            }
            Some(match buf.len() {
                1 => buf.into_iter().next().expect("one"),
                0 => Arc::from(&[][..]),
                _ => {
                    let mut all = Vec::new();
                    for b in &buf {
                        all.extend_from_slice(b);
                    }
                    Arc::from(all)
                }
            })
        };
        if let Some(p) = payload {
            self._enqueue_inbound_at(&p, Some(arrived_at));
        }
    }

    /// `channel._enqueue_inbound(payload)`: queue one complete message
    /// (evicting the oldest when full).
    pub fn _enqueue_inbound(&self, payload: &[u8]) {
        self._enqueue_inbound_at(payload, None);
    }

    fn _enqueue_inbound_at(&self, payload: &[u8], arrived_at: Option<f64>) {
        let arrived_at = arrived_at.unwrap_or_else(|| crate::python::stdlib::time.monotonic());
        let wall = crate::python::stdlib::time.time();
        let size = payload.len();
        let mut s = self.0.state.lock();
        if s.closed {
            return;
        }
        let seq = s.rx_seq;
        s.rx_seq += 1;
        let item = Item::Msg {
            payload: Arc::from(payload),
            seq,
            arrived_at,
            wall,
            dropped_before: s.dropped,
        };
        loop {
            let over_bytes = s.queue_bytes > 0 && s.queue_bytes + size > self.0.queue_bytes_cap;
            if !over_bytes && s.queue.len() < self.0.queue_size {
                s.queue.push_back(item);
                s.queue_bytes += size;
                self.0.cv.notify_all();
                return;
            }
            match s.queue.pop_front() {
                None => {
                    s.queue_bytes = 0;
                    continue;
                }
                Some(Item::Sentinel) => {
                    s.queue.push_front(Item::Sentinel);
                    return;
                }
                Some(old) => {
                    s.queue_bytes = s.queue_bytes.saturating_sub(old.len());
                    s.dropped += 1;
                }
            }
        }
    }

    /// `channel._mark_opened(tx_lane_id)`
    pub fn _mark_opened(&self, tx_lane_id: u8) {
        let mut s = self.0.state.lock();
        s.tx_lane_id = Some(tx_lane_id);
        self.0.cv.notify_all();
    }

    /// Close without telling the peer; `True` if this call closed it.
    pub fn _close_local(&self, reason: &str) -> bool {
        let mut s = self.0.state.lock();
        if s.closed {
            return false;
        }
        s.closed = true;
        s.closed_reason = Some(String::from(reason));
        while s.queue.len() >= self.0.queue_size {
            match s.queue.pop_front() {
                None => break,
                Some(Item::Sentinel) => {}
                Some(old) => {
                    s.queue_bytes = s.queue_bytes.saturating_sub(old.len());
                    s.dropped += 1;
                }
            }
        }
        s.queue.push_back(Item::Sentinel);
        s.rx_buf.clear();
        s.rx_len = 0;
        s.rx_active = false;
        self.0.cv.notify_all();
        true
    }

    /// `channel.close(reason="closed locally")`: close and notify the peer.
    pub fn close(&self, reason: &str) {
        let proto = self.protocol();
        if !self._close_local(reason) {
            return;
        }
        if let Some(p) = proto {
            p._on_channel_closed_locally(self);
        }
    }

    /// `channel.relay(timeout=None)` -- the (single, cached) iterator.
    pub fn relay(&self, timeout: Option<f64>) -> Relay {
        let mut s = self.0.state.lock();
        match &s.relay {
            Some(r) => {
                r.set_timeout(timeout);
                r.clone()
            }
            None => {
                let r = Relay::new(self.clone(), timeout);
                s.relay = Some(r.clone());
                r
            }
        }
    }

    /// Pop the next message, blocking up to *timeout* seconds; `Ok(None)`
    /// when closed and drained; `TimeoutError` when nothing arrived.
    fn next_item(&self, timeout: Option<f64>) -> Result<Option<Entry>> {
        let mut g = self.0.state.lock();
        let arrived = wait_until(&self.0.cv, &mut g, timeout, |s| {
            !s.queue.is_empty() || s.closed
        });
        if !arrived {
            return Err(LailaError::new(
                crate::python::ExcKind::TimeoutError,
                alloc::format!(
                    "No message on channel {:?} from {} within {}s.",
                    self.0.name,
                    self.0.peer_id,
                    timeout.unwrap_or(0.0)
                ),
            ));
        }
        match g.queue.pop_front() {
            None | Some(Item::Sentinel) => Ok(None),
            Some(Item::Msg {
                payload,
                seq,
                arrived_at,
                wall,
                dropped_before,
            }) => {
                g.queue_bytes = g.queue_bytes.saturating_sub(payload.len());
                drop(g);
                let entry = StreamEntry::_from_payload(&payload)?;
                let meta = StreamMeta {
                    peer_id: self.0.peer_id.clone(),
                    channel: self.0.name.clone(),
                    lane: self.0.lane_id,
                    seq,
                    arrived_at,
                    arrived_wall: wall,
                    dropped_before,
                };
                entry.__attrs__().__setattr__("_stream", meta.clone())?;
                entry.__attrs__().__setattr__("stream", meta)?;
                Ok(Some(entry))
            }
        }
    }

    fn attr(&self, name: &str) -> Option<Arg> {
        Some(match name {
            "peer_id" => Arg::Str(self.0.peer_id.as_str().into()),
            "name" => Arg::Str(self.0.name.as_str().into()),
            "lane_id" => Arg::Int(self.0.lane_id as i64),
            "tx_lane_id" => match self.tx_lane_id() {
                Some(l) => Arg::Int(l as i64),
                None => Arg::None,
            },
            "max_message_bytes" => Arg::Int(self.0.max_message_bytes as i64),
            "closed" => Arg::Bool(self.closed()),
            "closed_reason" => match self.closed_reason() {
                Some(r) => Arg::Str(r.into()),
                None => Arg::None,
            },
            "opened" => Arg::Bool(self.opened()),
            "rx_seq" => Arg::Int(self.rx_seq() as i64),
            "tx_seq" => Arg::Int(self.tx_seq() as i64),
            "dropped" => Arg::Int(self.dropped() as i64),
            "discarded" => Arg::Int(self.discarded() as i64),
            "tx_dropped" => Arg::Int(self.tx_dropped() as i64),
            "_tx_chunk_seq" => Arg::Int(self.0.state.lock().tx_chunk_seq as i64),
            "_queue_bytes" => Arg::Int(self.0.state.lock().queue_bytes as i64),
            "protocol" => match self.protocol() {
                Some(p) => p.as_arg(),
                None => Arg::None,
            },
            _ => return None,
        })
    }
}

impl core::fmt::Debug for Channel {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let state = if self.closed() {
            "closed"
        } else if self.opened() {
            "open"
        } else {
            "opening"
        };
        write!(
            f,
            "Channel({:?}, {:?}, rx_lane={}, tx_lane={}, {state})",
            self.0.peer_id,
            self.0.name,
            self.0.lane_id,
            match self.tx_lane_id() {
                Some(l) => alloc::format!("{l}"),
                None => String::from("None"),
            }
        )
    }
}

impl PyObject for Channel {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Channel"
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Channel>()
            .is_some_and(|o| o.is(self))
    }
    /// Every handle to the one channel object is the same object (`is`).
    fn __is__(&self, other: &dyn PyObject) -> bool {
        PyObject::__eq__(self, other)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        self.attr(name).ok_or_else(|| {
            LailaError::attribute_error(alloc::format!(
                "'Channel' object has no attribute '{name}'"
            ))
        })
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        match name {
            "tx_lane_id" => {
                self.set_tx_lane_id(value.as_i64().map(|v| v as u8));
                Ok(())
            }
            "_tx_chunk_seq" => {
                let v = value.as_i64().ok_or_else(|| {
                    LailaError::type_error("Channel._tx_chunk_seq must be an int")
                })?;
                self.0.state.lock().tx_chunk_seq = (v.max(0) as u64) % _codec::SEQ_MODULUS;
                Ok(())
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Channel' object attribute '{other}' is read-only"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, args: Args) -> Result<Arg> {
        let first = args.positional.first().cloned().unwrap_or(Arg::None);
        Ok(match name {
            "send" => {
                let payload: Vec<u8> = match &first {
                    Arg::Bytes(b) => b.to_vec(),
                    Arg::ByteArray(b) => b.read().to_vec(),
                    Arg::Str(s) => s.as_bytes().to_vec(),
                    other => {
                        return Err(LailaError::type_error(alloc::format!(
                            "a bytes-like object is required, not '{}'",
                            other.type_name()
                        )))
                    }
                };
                self.send(&payload)?;
                Arg::None
            }
            "close" => {
                let reason = match &first {
                    Arg::None => match args.get("reason") {
                        Arg::None => String::from("closed locally"),
                        r => r.__str__(),
                    },
                    r => r.__str__(),
                };
                self.close(&reason);
                Arg::None
            }
            "relay" => {
                let timeout = match &first {
                    Arg::None => args.get("timeout"),
                    t => t.clone(),
                };
                self.relay(timeout.as_f64()).into_arg()
            }
            "qsize" => Arg::Int(self.qsize() as i64),
            "_close_local" => Arg::Bool(self._close_local(&first.__str__())),
            "_mark_opened" => {
                self._mark_opened(first.as_i64().unwrap_or(0) as u8);
                Arg::None
            }
            "_enqueue_inbound" => {
                let payload: Vec<u8> = match &first {
                    Arg::Bytes(b) => b.to_vec(),
                    Arg::ByteArray(b) => b.read().to_vec(),
                    other => other.__str__().into_bytes(),
                };
                self._enqueue_inbound(&payload);
                Arg::None
            }
            "_next_chunk_seqs" => {
                Arg::Int(self._next_chunk_seqs(first.as_i64().unwrap_or(1) as u64) as i64)
            }
            "_on_chunk" => {
                let seq = first.as_i64().unwrap_or(0) as u64;
                let flags = args.positional.get(1).and_then(|f| f.as_i64()).unwrap_or(0) as u8;
                let data: Vec<u8> = match args.positional.get(2) {
                    Some(Arg::Bytes(b)) => b.to_vec(),
                    Some(Arg::ByteArray(b)) => b.read().to_vec(),
                    _ => Vec::new(),
                };
                let at = args
                    .positional
                    .get(3)
                    .and_then(|a| a.as_f64())
                    .unwrap_or_else(|| crate::python::stdlib::time.monotonic());
                self._on_chunk(seq, flags, &data, at);
                Arg::None
            }
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'Channel' object has no attribute '{other}'"
                )))
            }
        })
    }
    /// `for entry in channel:` is not defined (relay() is); kept
    /// explicit so a stray iteration does not silently drain the queue.
    fn __iter__(&self) -> Result<Vec<Arg>> {
        Err(LailaError::type_error("'Channel' object is not iterable"))
    }
}
impl IntoArg for Channel {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Channel {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for Channel {
    fn isinstance(x: &Arg) -> bool {
        x.downcast_ref::<Channel>().is_some()
    }
}

/// `Relay`: the blocking iterator over a channel's inbound messages
/// (`for entry in channel.relay(timeout=..)`).
#[derive(Clone)]
pub struct Relay(Arc<RelayInner>);

pub struct RelayInner {
    pub channel: Channel,
    timeout: Mutex<Option<f64>>,
}

impl Relay {
    fn new(channel: Channel, timeout: Option<f64>) -> Relay {
        Relay(Arc::new(RelayInner {
            channel,
            timeout: Mutex::new(timeout),
        }))
    }
    pub fn channel(&self) -> &Channel {
        &self.0.channel
    }
    pub fn timeout(&self) -> Option<f64> {
        *self.0.timeout.lock()
    }
    pub fn set_timeout(&self, timeout: Option<f64>) {
        *self.0.timeout.lock() = timeout;
    }
    pub fn closed_reason(&self) -> Option<String> {
        self.0.channel.closed_reason()
    }
    /// `next(relay)`: the next message as a [`StreamEntry`]; `Ok(None)` at
    /// end of stream (`StopIteration`); `TimeoutError` when the per-message
    /// timeout elapses.
    pub fn next(&self) -> Result<Option<Entry>> {
        self.0.channel.next_item(self.timeout())
    }
    /// `next(relay)` raising `StopIteration` at the end.
    pub fn __next__(&self) -> Result<Arg> {
        match self.next()? {
            Some(e) => Ok(Arg::Entry(e)),
            None => Err(LailaError::new(crate::python::ExcKind::StopIteration, "")),
        }
    }
}

impl core::fmt::Debug for Relay {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "Relay({:?}, timeout={})",
            self.0.channel,
            match self.timeout() {
                Some(t) => Arg::Float(t).__repr__(),
                None => String::from("None"),
            }
        )
    }
}

impl PyObject for Relay {
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }
    fn type_name(&self) -> &'static str {
        "Relay"
    }
    fn __repr__(&self) -> String {
        alloc::format!("{self:?}")
    }
    fn __eq__(&self, other: &dyn PyObject) -> bool {
        other
            .as_any()
            .downcast_ref::<Relay>()
            .is_some_and(|o| Arc::ptr_eq(&o.0, &self.0))
    }
    /// The relay is a per-channel singleton: every handle is the same object.
    fn __is__(&self, other: &dyn PyObject) -> bool {
        PyObject::__eq__(self, other)
    }
    fn __hash__(&self) -> Result<i64> {
        Ok(Arc::as_ptr(&self.0) as usize as i64)
    }
    fn __getattr__(&self, name: &str) -> Result<Arg> {
        Ok(match name {
            "channel" => self.0.channel.to_arg(),
            "timeout" => match self.timeout() {
                Some(t) => Arg::Float(t),
                None => Arg::None,
            },
            "closed_reason" => match self.closed_reason() {
                Some(r) => Arg::Str(r.into()),
                None => Arg::None,
            },
            other => {
                return Err(LailaError::attribute_error(alloc::format!(
                    "'Relay' object has no attribute '{other}'"
                )))
            }
        })
    }
    fn __setattr__(&self, name: &str, value: Arg) -> Result<()> {
        match name {
            "timeout" => {
                self.set_timeout(value.as_f64());
                Ok(())
            }
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Relay' object has no attribute '{other}'"
            ))),
        }
    }
    fn __call_method__(&self, name: &str, _args: Args) -> Result<Arg> {
        match name {
            "__next__" => self.__next__(),
            "__iter__" => Ok(self.to_arg()),
            other => Err(LailaError::attribute_error(alloc::format!(
                "'Relay' object has no attribute '{other}'"
            ))),
        }
    }
    /// `list(relay)`: drain until end of stream (or the per-message
    /// timeout, which is raised as in Python). `for e in relay:` pulls
    /// lazily through `__next__` instead.
    fn __iter__(&self) -> Result<Vec<Arg>> {
        let mut out = Vec::new();
        while let Some(e) = self.next()? {
            out.push(Arg::Entry(e));
        }
        Ok(out)
    }
    fn __is_iterator__(&self) -> bool {
        true
    }
    fn __next__(&self) -> Result<Arg> {
        Relay::__next__(self)
    }
}
/// `for e in relay:` on a typed `Relay`: lazy, one `__next__` per item.
impl crate::python::builtins::PyIter for Relay {
    type Item = Arg;
    fn py_iter(self) -> Result<crate::python::builtins::PyIterator<Arg>> {
        Ok(crate::python::builtins::PyIterator::lazy(move || {
            Ok(self.next()?.map(Arg::Entry))
        }))
    }
}
impl crate::python::builtins::PyIter for &Relay {
    type Item = Arg;
    fn py_iter(self) -> Result<crate::python::builtins::PyIterator<Arg>> {
        self.clone().py_iter()
    }
}
impl IntoArg for Relay {
    fn into_arg(self) -> Arg {
        Arg::Object(Arc::new(self))
    }
}
impl PyArg for Relay {
    fn to_arg(&self) -> Arg {
        Arg::Object(Arc::new(self.clone()))
    }
}
impl IsInstance for Relay {
    fn isinstance(x: &Arg) -> bool {
        x.downcast_ref::<Relay>().is_some()
    }
}

/// `StreamMeta(**fields)` as a dict (for `as_dict`-style consumers).
pub fn stream_meta_dict(m: &StreamMeta) -> Arg {
    let mut d = PyDictBuilder::new();
    d.insert("peer_id", m.peer_id.as_str());
    d.insert("channel", m.channel.as_str());
    d.insert("lane", m.lane as i64);
    d.insert("seq", m.seq as i64);
    d.insert("arrived_at", m.arrived_at);
    d.insert("arrived_wall", m.arrived_wall);
    d.insert("dropped_before", m.dropped_before as i64);
    d.finish()
}
