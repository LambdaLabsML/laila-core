"""Inter-policy peer-to-peer communication sub-system.

This sub-package wires together everything needed for one laila
:class:`Policy` to talk to another in a different process or on a
different host. It is structured around four collaborating pieces:

- :class:`_LAILA_IDENTIFIABLE_COMMUNICATION`
    Per-policy "central communication" hub. Owns the local listener
    that exposes the policy to remote peers, the registry of known
    peers, and the proxy bookkeeping that lets remote calls feel like
    local method invocations. Always reachable from a policy through
    ``policy.central.communication``.

- :class:`_LAILA_IDENTIFIABLE_COMM_PROTOCOL`
    Pluggable transport. Concrete subclasses (currently
    :class:`_LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL`) implement the
    actual wire encoding, listener loop, and request/response routing.
    A protocol is chosen per :class:`Communication` instance and is
    decoupled from the API surface.

- :class:`RemotePolicyProxy`
    Client-side handle to a remote peer's policy. Method calls on the
    proxy are translated into protocol messages, dispatched, and
    awaited; the result is returned to the caller as if it had been
    produced locally. Pickling-friendly so a proxy can be shipped
    across process boundaries.

- The ``connection`` module's helpers
    Thin abstractions over a single live transport, used by the
    protocols when they need point-to-point streams (for example,
    long-lived TCP/IP sockets between two known peers).
"""

from .protocols.base import _LAILA_IDENTIFIABLE_COMM_PROTOCOL
from .protocols.bluetooth import _LAILA_IDENTIFIABLE_BLUETOOTH_COMM_PROTOCOL
from .protocols.lora import _LAILA_IDENTIFIABLE_LORA_COMM_PROTOCOL
from .protocols.tcpip import _LAILA_IDENTIFIABLE_TCPIP_COMM_PROTOCOL
from .proxy import RemotePolicyProxy
from .schema.base import _LAILA_IDENTIFIABLE_COMMUNICATION
