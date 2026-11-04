//! Reusable RPC *carrier* bases for the communication transports.
//!
//! A transport only has to move framed JSON-RPC bytes between two peers;
//! the handshake, peer registry, future virtualisation and inbound
//! dispatch are identical across every wire, so the carriers implement
//! that machinery once:
//!
//! - [`stream`] -- reliable duplex byte streams (TCP, TLS, unix socket).
//! - [`datagram`] -- lossy packet links (UDP, radios): fragmentation,
//!   ack / retry, de-duplication. [`lazy`] is the driver-loading variant.
//! - [`p2p`] -- one already-connected duplex link (serial lines, USB-CDC,
//!   CAN ISO-TP, ...): no listen / accept step.
//! - [`broker`] -- pub/sub fabrics (MQTT, AMQP, XMPP, DDS, ZeroMQ): inbox
//!   topics and `reply_to` addressing.
//! - [`register`] -- polled master/slave register buses (Modbus, I2C,
//!   SPI, EtherNet/IP): mailbox regions.
//!
//! All carriers share the codec in [`codec`] and the dispatch / peer
//! helpers in [`base`].

pub mod base;
pub mod broker;
pub mod codec;
pub mod datagram;
pub mod lazy;
pub mod loopthread;
pub mod p2p;
pub mod register;
pub mod stream;
#[cfg(feature = "tls")]
pub mod tls;
pub mod transport;
pub mod uri;

// The module *objects* (`from .._carriers import codec`).
pub use codec::codec;
pub use loopthread::loopthread;

pub use base::_CarrierRPCProtocol;
pub use broker::_BrokerRPCProtocol;
pub use datagram::_DatagramRPCProtocol;
pub use lazy::_LazyDatagramRPCProtocol;
pub use p2p::_P2PStreamRPCProtocol;
pub use register::_RegisterRPCProtocol;
pub use stream::_StreamRPCProtocol;
