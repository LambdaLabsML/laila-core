// Go twin of examples/communication/localhost/peer_active_policy.ipynb:
// two policies in one process, each with its own WebSocket (``tcpip``)
// transport on 127.0.0.1, peered with a shared secret. The "remote" node
// stores an entry in a named pool; the "local" node peers with it, calls
// into its pool router over RPC and remembers the entry across the wire.
//
// Python                                                 Go
// ------                                                 --
// DefaultPolicy()                                        laila.New_DefaultPolicy()
// DefaultTCPIPProtocol(host=, port=0, peer_secret_key=)  laila.New_DefaultTCPIPProtocol(py.Kw{...})
// laila.active_policy = node                             laila.Activate_policy(node)
// laila.communication.add_connection(proto)              comm.Add_connection(proto)
// laila.communication.add_tcpip_peer(host, port, secret) comm.Add_tcpip_peer(host, port, secret)
// laila.peers[gid]                                       laila.Peers() -> registry.Getitem(gid)
// proxy.central.memory.pool_router.route(entries=[...])  proxy.Attr("central").Attr("memory").Attr("pool_router").Attr("route").Invoke(py.Kw{...})
// laila.remember(gid, dst_policy=peer, dst_pool=nick)    laila.Remember(gid, py.Kw{"dst_policy": peer, "dst_pool": nick})
// laila.communication.remove_connection(proto)           comm.Remove_connection(proto)
//
// Run: go run ./examples/peers_tcp
package main

import (
	"fmt"
	"os"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
	"github.com/amirzadehlambda/laila/laila-go/runtime"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// comm is ``policy.central.communication`` for a policy object.
func comm(policy any) *laila.LAILA_IDENTIFIABLE_COMMUNICATION_ {
	return laila.As_communication_(policy.(*laila.LAILA_IDENTIFIABLE_POLICY_).Central.Communication)
}

func run() error {
	defer laila.Terminate()

	local_node, err := laila.New_DefaultPolicy()
	if err != nil {
		return err
	}
	remote_node, err := laila.New_DefaultPolicy()
	if err != nil {
		return err
	}
	shared_secret := py.Uuid4().Hex()

	local_tcp, err := laila.New_DefaultTCPIPProtocol(py.Kw{"host": "127.0.0.1", "port": 0, "peer_secret_key": shared_secret})
	if err != nil {
		return err
	}
	remote_tcp, err := laila.New_DefaultTCPIPProtocol(py.Kw{"host": "127.0.0.1", "port": 0, "peer_secret_key": shared_secret})
	if err != nil {
		return err
	}

	laila.Activate_policy(local_node)
	if err := comm(local_node).Add_connection(local_tcp); err != nil {
		return err
	}
	laila.Activate_policy(remote_node)
	if err := comm(remote_node).Add_connection(remote_tcp); err != nil {
		return err
	}
	local_gid := local_node.(*laila.LAILA_IDENTIFIABLE_POLICY_).Global_id()
	remote_gid := remote_node.(*laila.LAILA_IDENTIFIABLE_POLICY_).Global_id()
	fmt.Println("Local node: ", local_gid)
	fmt.Println("Remote node:", remote_gid)

	// --- the remote node stores an entry in a named pool -------------------
	laila.Activate_policy(remote_node)
	remote_pool, err := laila.New_DefaultPool()
	if err != nil {
		return err
	}
	mem, err := laila.Memory()
	if err != nil {
		return err
	}
	if err := laila.As_central_memory_(mem).Extend(remote_pool, py.Kw{"pool_nickname": "remote-store"}); err != nil {
		return err
	}
	entry, err := laila.Constant(py.DictOf("message", "stored on the remote node"), py.Kw{"nickname": "remote-entry"})
	if err != nil {
		return err
	}
	if err := laila.Guarantee.With(func() error {
		_, err := laila.Memorize(py.Kw{"entries": entry, "pool_nickname": "remote-store"})
		return err
	}); err != nil {
		return err
	}
	stored, err := laila.As_pool_(remote_pool).Contains(entry.Global_id())
	if err != nil {
		return err
	}
	fmt.Println("Entry:", entry.Global_id())
	fmt.Println("Remote pool has entry:", stored)

	// --- the local node peers with it --------------------------------------
	laila.Activate_policy(local_node)
	peer_id, err := comm(local_node).Add_tcpip_peer("127.0.0.1", remote_tcp.Bound_port(), remote_tcp.Peer_secret_key)
	if err != nil {
		return err
	}
	time.Sleep(300 * time.Millisecond)
	fmt.Println("Peered successfully.")
	fmt.Printf("Local peers:  %s\n", py.Repr(comm(local_node).Peers.Keys()))
	fmt.Printf("Remote peers: %s\n", py.Repr(comm(remote_node).Peers.Keys()))

	// ``laila.peers[remote_node.global_id]`` -- a RemotePolicyProxy whose
	// attribute chains become RPCs.
	peers, err := laila.Peers()
	if err != nil {
		return err
	}
	proxy, err := peers.(*laila.PeerRegistry).Getitem(peer_id)
	if err != nil {
		return err
	}
	remote_proxy := proxy.(*laila.PeerProxy)

	routed, err := remote_proxy.Attr("central").Attr("memory").Attr("pool_router").Attr("route").Invoke(
		py.Kw{"entries": py.NewList(entry.Global_id()), "pool_nickname": "remote-store"})
	if err != nil {
		return err
	}
	fmt.Println("Remote route returned:", py.Pytypename(routed))
	fmt.Println("Pool data (serialized via RPC):")
	if d, ok := routed.(*py.Dict); ok {
		for _, item := range d.Items() {
			if py.Eq(item.Key, "resource") {
				n, _ := py.Len(item.Value)
				fmt.Printf("  resource: %d entries stored\n", n)
			} else {
				fmt.Printf("  %s: %s\n", py.Str(item.Key), py.Repr(item.Value))
			}
		}
	}

	// --- and remembers the entry across the wire -----------------------------
	fut, err := laila.Remember(entry.Global_id(), py.Kw{"dst_policy": peer_id, "dst_pool": "remote-store", "persist": false})
	if err != nil {
		return err
	}
	recalled, err := runtime.Wait(fut)
	if err != nil {
		return err
	}
	data, err := recalled.(*laila.Entry).Data()
	if err != nil {
		return err
	}
	fmt.Println("Remembered over TCP/IP:", py.Repr(data))

	// --- teardown --------------------------------------------------------------
	laila.Activate_policy(local_node)
	if err := comm(local_node).Remove_connection(local_tcp); err != nil {
		return err
	}
	laila.Activate_policy(remote_node)
	if err := comm(remote_node).Remove_connection(remote_tcp); err != nil {
		return err
	}
	fmt.Println("Connections closed.")
	return nil
}
