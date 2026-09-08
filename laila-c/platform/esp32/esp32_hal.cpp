// ESP32 (ESP-IDF) HAL backend.
//
// Cooperative executor (futures resolve inline), FreeRTOS recursive mutex,
// esp_timer clock, esp_random entropy, storage over a mounted VFS
// (SPIFFS/LittleFS/FAT) at LAILA_ESP32_FS_ROOT, and a lwIP BSD-socket TCP/UDP
// transport with an inbound listener (so this device can both dial out to a
// peer and serve peer RPCs). Wi-Fi bring-up is the app's responsibility (see
// examples/esp32_peer_request) -- this layer assumes the station has an IP.
//
// Compiles within an ESP-IDF build. The includes below resolve against IDF.
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "esp_random.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"

// lwIP BSD sockets (same API surface as POSIX; provided by ESP-IDF).
#include "lwip/netdb.h"
#include "lwip/sockets.h"

#include "laila/hal/hal.hpp"

#ifndef LAILA_ESP32_FS_ROOT
#define LAILA_ESP32_FS_ROOT "/spiffs"
#endif

namespace laila_c {
namespace hal {
namespace {

class EspClock : public Clock {
public:
  uint64_t now_ms() override { return (uint64_t)(esp_timer_get_time() / 1000); }
  void sleep_ms(uint32_t ms) override { vTaskDelay(pdMS_TO_TICKS(ms ? ms : 1)); }
};

class EspMutex : public Mutex {
public:
  EspMutex() : h_(xSemaphoreCreateRecursiveMutex()) {}
  void lock() override { xSemaphoreTakeRecursive(h_, portMAX_DELAY); }
  void unlock() override { xSemaphoreGiveRecursive(h_); }
private:
  SemaphoreHandle_t h_;
};

class InlineExecutor : public Executor {
public:
  void submit(std::function<void()> task) override { task(); }
  void drain() override {}
  bool is_cooperative() const override { return true; }
};

class EspRandom : public Random {
public:
  void fill(uint8_t* buf, size_t n) override {
    size_t i = 0;
    while (i + 4 <= n) { uint32_t r = esp_random(); for (int b = 0; b < 4; ++b) buf[i++] = (r >> (b * 8)) & 0xFF; }
    if (i < n) { uint32_t r = esp_random(); while (i < n) { buf[i++] = r & 0xFF; r >>= 8; } }
  }
};

// lwIP BSD-socket connection (TCP or UDP), matching the POSIX SocketConnection.
class EspSocketConnection : public Connection {
public:
  explicit EspSocketConnection(int fd) : fd_(fd) {}
  ~EspSocketConnection() override { close(); }
  bool send(const std::vector<uint8_t>& d) override {
    size_t sent = 0;
    while (sent < d.size()) {
      int n = ::lwip_send(fd_, d.data() + sent, d.size() - sent, 0);
      if (n <= 0) return false;
      sent += (size_t)n;
    }
    return true;
  }
  bool recv(std::vector<uint8_t>& out, int timeout_ms) override {
    if (fd_ < 0) return false;
    fd_set rf;
    FD_ZERO(&rf);
    FD_SET(fd_, &rf);
    timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    int r = ::lwip_select(fd_ + 1, &rf, nullptr, nullptr, timeout_ms < 0 ? nullptr : &tv);
    if (r <= 0) return false;
    uint8_t buf[1024];
    int n = ::lwip_recv(fd_, buf, sizeof(buf), 0);
    if (n == 0) { close(); return false; }  // peer closed (EOF) -> mark not-open
    if (n < 0) return false;                 // transient
    out.assign(buf, buf + n);
    return true;
  }
  void close() override {
    if (fd_ >= 0) { ::lwip_close(fd_); fd_ = -1; }
  }
  bool is_open() const override { return fd_ >= 0; }

private:
  int fd_;
};

// Listening TCP socket: accept() returns one EspSocketConnection per inbound peer.
class EspListener : public Listener {
public:
  explicit EspListener(int fd) : fd_(fd) {}
  ~EspListener() override { close(); }
  Connection* accept(int timeout_ms) override {
    if (fd_ < 0) return nullptr;
    fd_set rf;
    FD_ZERO(&rf);
    FD_SET(fd_, &rf);
    timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    int r = ::lwip_select(fd_ + 1, &rf, nullptr, nullptr, timeout_ms < 0 ? nullptr : &tv);
    if (r <= 0) return nullptr;
    int c = ::lwip_accept(fd_, nullptr, nullptr);
    if (c < 0) return nullptr;
    return new EspSocketConnection(c);
  }
  void close() override {
    if (fd_ >= 0) { ::lwip_close(fd_); fd_ = -1; }
  }
  bool is_listening() const override { return fd_ >= 0; }
  uint16_t local_port() const override {
    if (fd_ < 0) return 0;
    struct sockaddr_in addr {};
    socklen_t alen = sizeof(addr);
    if (::lwip_getsockname(fd_, (struct sockaddr*)&addr, &alen) != 0) return 0;
    return lwip_ntohs(addr.sin_port);
  }

private:
  int fd_;
};

// ESP32 transport: live TCP/UDP via lwIP sockets, plus an inbound TCP listener.
class EspTransport : public Transport {
public:
  bool supported() const override { return false; }  // legacy peer-RPC flag
  bool supports(ConnectionType t) const override {
    return t == ConnectionType::TCP || t == ConnectionType::UDP;
  }
  Connection* open(ConnectionType t, const ConnectionConfig& cfg) override {
    if (t != ConnectionType::TCP && t != ConnectionType::UDP) return nullptr;
    struct addrinfo hints {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = (t == ConnectionType::TCP) ? SOCK_STREAM : SOCK_DGRAM;
    struct addrinfo* res = nullptr;
    std::string port = std::to_string(cfg.port);
    if (::lwip_getaddrinfo(cfg.host.c_str(), port.c_str(), &hints, &res) != 0 || !res)
      return nullptr;
    int fd = -1;
    for (struct addrinfo* p = res; p; p = p->ai_next) {
      fd = ::lwip_socket(p->ai_family, p->ai_socktype, p->ai_protocol);
      if (fd < 0) continue;
      if (::lwip_connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
      ::lwip_close(fd);
      fd = -1;
    }
    ::lwip_freeaddrinfo(res);
    if (fd < 0) return nullptr;
    return new EspSocketConnection(fd);
  }
  bool supports_listen(ConnectionType t) const override { return t == ConnectionType::TCP; }
  Listener* listen(ConnectionType t, const ConnectionConfig& cfg) override {
    if (t != ConnectionType::TCP) return nullptr;
    int fd = ::lwip_socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return nullptr;
    int yes = 1;
    ::lwip_setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = (cfg.host.empty() || cfg.host == "0.0.0.0")
                               ? lwip_htonl(INADDR_ANY)
                               : ::ipaddr_addr(cfg.host.c_str());
    addr.sin_port = lwip_htons(cfg.port);
    if (::lwip_bind(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) { ::lwip_close(fd); return nullptr; }
    if (::lwip_listen(fd, 4) != 0) { ::lwip_close(fd); return nullptr; }
    return new EspListener(fd);
  }
};

// VFS-backed storage (one subdirectory per storage name).
class VfsStorage : public Storage {
public:
  explicit VfsStorage(const std::string& dir) : dir_(dir) {}
  bool read(const std::string& key, std::vector<uint8_t>& out) override {
    FILE* f = fopen(path(key).c_str(), "rb");
    if (!f) return false;
    fseek(f, 0, SEEK_END); long n = ftell(f); fseek(f, 0, SEEK_SET);
    out.resize(n > 0 ? (size_t)n : 0);
    if (n > 0) { size_t rd = fread(out.data(), 1, (size_t)n, f); out.resize(rd); }
    fclose(f);
    return true;
  }
  bool write(const std::string& key, const std::vector<uint8_t>& data) override {
    FILE* f = fopen(path(key).c_str(), "wb");
    if (!f) return false;
    if (!data.empty()) fwrite(data.data(), 1, data.size(), f);
    fclose(f);
    return true;
  }
  bool remove(const std::string& key) override { return ::remove(path(key).c_str()) == 0; }
  bool exists(const std::string& key) override {
    FILE* f = fopen(path(key).c_str(), "rb"); if (!f) return false; fclose(f); return true;
  }
  std::vector<std::string> keys() override { return {}; }  // VFS dir scan: TODO per FS
  void clear() override {}
private:
  static std::string encode(const std::string& key) {
    std::string out; for (char c : key) out += (c == '/' || c == ':') ? '_' : c; return out;
  }
  std::string path(const std::string& key) const { return dir_ + "/" + encode(key); }
  std::string dir_;
};

class EspHal : public Hal {
public:
  Clock& clock() override { return clock_; }
  Mutex& mutex() override { return mutex_; }
  Executor& executor() override { return executor_; }
  Random& random() override { return random_; }
  Transport& transport() override { return transport_; }
  Storage* open_storage(const std::string& name) override {
    auto it = stores_.find(name);
    if (it != stores_.end()) return it->second.get();
    auto s = std::make_unique<VfsStorage>(std::string(LAILA_ESP32_FS_ROOT) + "/" + name);
    Storage* p = s.get();
    stores_[name] = std::move(s);
    return p;
  }
  const char* platform_name() const override { return "esp32"; }
private:
  EspClock clock_;
  EspMutex mutex_;
  InlineExecutor executor_;
  EspRandom random_;
  EspTransport transport_;
  std::map<std::string, std::unique_ptr<VfsStorage>> stores_;
};

}  // namespace

Hal& get() {
  static EspHal instance;
  return instance;
}

}  // namespace hal
}  // namespace laila_c
