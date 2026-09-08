// On-target test for laila-C on an emulated ESP32 (QEMU).
//
//   Phase 1 (boot proof): run laila-C core verbs (memorize/remember, JSON,
//   identity) on xtensa -- no network needed.
//   Phase 2 (network proof): bring up emulated Ethernet (openeth, the QEMU NIC),
//   put an entry in a named pool, and run laila-C's INBOUND RPC server so a host
//   process can pull it over real TCP (the same lwIP socket path Wi-Fi uses).
#include <cstdio>

#include "esp_eth.h"
#include "esp_eth_mac_openeth.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "freertos/FreeRTOS.h"
#include "freertos/event_groups.h"
#include "freertos/task.h"

#include "laila/laila.hpp"
#include "laila/pools.hpp"

using namespace laila_c;

static EventGroupHandle_t s_eg;
static const int GOT_IP = BIT0;
static esp_ip4_addr_t s_ip;

static void on_ip(void*, esp_event_base_t, int32_t, void* data) {
  auto* e = static_cast<ip_event_got_ip_t*>(data);
  s_ip = e->ip_info.ip;
  xEventGroupSetBits(s_eg, GOT_IP);
}

// Bring up the emulated Ethernet NIC (OpenCores ETH) + DHCP, mirroring IDF's
// protocol_examples_common openeth path.
static void ethernet_up() {
  ESP_ERROR_CHECK(esp_netif_init());
  ESP_ERROR_CHECK(esp_event_loop_create_default());
  s_eg = xEventGroupCreate();

  esp_netif_config_t netif_cfg = ESP_NETIF_DEFAULT_ETH();
  esp_netif_t* netif = esp_netif_new(&netif_cfg);

  eth_mac_config_t mac_config = ETH_MAC_DEFAULT_CONFIG();
  eth_phy_config_t phy_config = ETH_PHY_DEFAULT_CONFIG();
  phy_config.autonego_timeout_ms = 100;
  esp_eth_mac_t* mac = esp_eth_mac_new_openeth(&mac_config);
  esp_eth_phy_t* phy = esp_eth_phy_new_dp83848(&phy_config);

  esp_eth_config_t config = ETH_DEFAULT_CONFIG(mac, phy);
  esp_eth_handle_t eth = nullptr;
  ESP_ERROR_CHECK(esp_eth_driver_install(&config, &eth));
  ESP_ERROR_CHECK(esp_netif_attach(netif, esp_eth_new_netif_glue(eth)));
  ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_ETH_GOT_IP, on_ip, nullptr));
  ESP_ERROR_CHECK(esp_eth_start(eth));
  xEventGroupWaitBits(s_eg, GOT_IP, pdFALSE, pdTRUE, portMAX_DELAY);
}

extern "C" void app_main() {
  std::printf("\nLAILA_BOOT_START platform=%s\n", hal::get().platform_name());
  std::fflush(stdout);

  // ---- Phase 1: core verbs on-target ----
  auto policy = std::make_shared<Policy>();
  activate_policy(policy);
  auto e = laila->constant(LailaValue::from_string("hello-from-esp32"));
  laila->memorize(e)->wait();
  std::printf("LAILA_STRING value=%s\n",
              laila->remember(e->global_id())->data().as_string().c_str());
  Json obj = Json::object();
  obj["sensor"] = std::string("temp");
  obj["reading"] = (int64_t)42;
  auto j = laila->constant(LailaValue::from_json(obj));
  laila->memorize(j)->wait();
  std::printf("LAILA_JSON value=%s\n", laila->remember(j->global_id())->data().as_json().dump().c_str());
  std::printf("LAILA_BOOT_OK\n");
  std::fflush(stdout);

  // ---- Phase 2: networked inbound server ----
  ethernet_up();
  std::printf("LAILA_IP " IPSTR "\n", IP2STR(&s_ip));
  std::fflush(stdout);

  // An entry the host will pull from a named pool on this device.
  auto store = std::make_shared<DefaultPool>();
  store->set_nickname("remote-store");
  policy->memory().extend(store, {.pool_nickname = "remote-store"});
  auto r = laila->constant(LailaValue::from_string("sensor=42"));
  policy->memory().memorize(r, "remote-store");
  std::printf("LAILA_ENTRY_GID %s\n", r->global_id().c_str());

  // Serve peer RPCs (golden rule: only this policy's own memory). add_connection
  // brings the inbound listener up and serves it in the background -- the secret
  // travels with the protocol (peer_secret_key). The listener auto-detects the
  // peer: a raw laila-C TCP frame OR an RFC6455 WebSocket upgrade (so an
  // unmodified Python `laila` can add_peer("ws://<device>:5556")).
  laila->add_connection(std::make_shared<DefaultTCPIPProtocol>(std::string("0.0.0.0"),
                                                               (uint16_t)5556, std::string("s3cr3t")));
  std::printf("LAILA_SERVE_READY port=5556\n");
  std::fflush(stdout);

  for (;;) {
    vTaskDelay(pdMS_TO_TICKS(100));  // serving runs in the background
  }
}
