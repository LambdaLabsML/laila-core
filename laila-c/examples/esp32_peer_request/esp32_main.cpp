// ESP32 (laila-C) peering with a Python `laila` policy and issuing a request.
//
// The ESP32 runs its own laila policy. It adds the Python machine's policy as a
// peer over TCP, then calls laila->request(...) to ask the Python policy to
// remember an entry -- exactly like having the ESP32 be a peer of the Python
// node. Mirrors the Python form:
//
//     # on the ESP32 side, in Python that would read:
//     peer = laila.add_peer("tcp://192.168.1.50:8770", secret="s3cr3t")
//     value = laila.request(peer, "central.memory.remember", [gid]).data
//
// In laila-C (only `.`->`->` differs):
//
//     auto peer = laila->add_peer("tcp://192.168.1.50:8770", "s3cr3t");
//     auto value = laila->request(peer, "central.memory.remember", {gid})->data();
//
// This file compiles on a host too (TCP works on POSIX). On ESP-IDF, bring up
// Wi-Fi first (sketched in the #ifdef block) and build with LAILA_PLATFORM=esp32.
#include <cstdio>
#include <string>

#include "laila/laila.hpp"

using namespace laila_c;

// Address of the Python machine running a laila policy + the JSON-RPC adapter
// (see python_policy.py). Override at build time as needed.
#ifndef LAILA_PYTHON_PEER_URI
#define LAILA_PYTHON_PEER_URI "tcp://127.0.0.1:8770"
#endif

#if defined(ESP_PLATFORM)
extern "C" void laila_esp32_wifi_connect(const char* ssid, const char* pass);
#endif

int main() {
#if defined(ESP_PLATFORM)
  // On a real ESP32, join the network before peering.
  laila_esp32_wifi_connect("my-ssid", "my-pass");
#endif

  const std::string peer_uri = LAILA_PYTHON_PEER_URI;
  const std::string secret = "s3cr3t";
  // The id of an entry the Python policy holds (here a nickname-derived id).
  const std::string entry_id =
      to_global_id(generate_uuid_from_nickname("sensor_calibration"), {scope::ENTRY}, std::nullopt);

  std::printf("[esp32] peering with Python policy at %s\n", peer_uri.c_str());
  std::string peer = laila->add_peer(peer_uri, secret);

  // laila.request: ask the Python policy to remember the entry for us.
  Json args = Json::array();
  args.push_back(Json(entry_id));
  auto future = laila->request(peer, "central.memory.remember", args);

  LailaValue value = future->data();  // blocks until the peer responds
  std::printf("[esp32] remembered from Python peer: %s\n",
              value.kind() == LailaValue::Kind::String ? value.as_string().c_str()
                                                        : value.as_json().dump().c_str());

  // The convenience verb reads identically to a local remember:
  auto value2 = laila->peer(peer)->remember(entry_id)->data();
  (void)value2;

  laila->terminate();
  return 0;
}
