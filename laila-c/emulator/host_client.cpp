// Host side of the QEMU networking test: connect to the emulated ESP32's
// laila-C inbound server (via QEMU hostfwd) and pull an entry from its
// "remote-store" pool -- the computer-pulls-from-device flow over real TCP.
//
// Usage: host_client <entry_global_id> <host_port>
#include <cstdio>
#include <string>

#include "laila/laila.hpp"

using namespace laila_c;

int main(int argc, char** argv) {
  if (argc < 3) {
    std::printf("usage: %s <gid> <port>\n", argv[0]);
    return 2;
  }
  std::string gid = argv[1];
  uint16_t port = (uint16_t)std::stoi(argv[2]);

  auto p = std::make_shared<Policy>();
  activate_policy(p);

  std::string peer = laila->add_tcpip_peer("127.0.0.1", port, "s3cr3t");
  RememberOpts opts;
  opts.policy_id = peer;
  opts.pool_nickname = "remote-store";
  opts.persist = false;
  auto value = laila->remember(gid, opts)->data();
  std::printf("HOST_GOT=%s\n", value.as_string().c_str());
  return 0;
}
