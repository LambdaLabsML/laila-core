// RP2040 (Raspberry Pi Pico SDK) HAL backend.
//
// Cooperative executor, pico recursive mutex, the SDK absolute-time clock, and
// pico_rand entropy. Storage is in-RAM here (volatile across resets); a
// hardware_flash-backed Storage is the persistence follow-up. Networking is
// unsupported (Pico W Wi-Fi transport plugs into hal::Transport later).
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "pico/rand.h"
#include "pico/stdlib.h"
#include "pico/sync.h"

#include "laila/hal/hal.hpp"

namespace laila_c {
namespace hal {
namespace {

class PicoClock : public Clock {
public:
  uint64_t now_ms() override { return to_ms_since_boot(get_absolute_time()); }
  void sleep_ms(uint32_t ms) override { ::sleep_ms(ms ? ms : 1); }
};

class PicoMutex : public Mutex {
public:
  PicoMutex() { recursive_mutex_init(&m_); }
  void lock() override { recursive_mutex_enter_blocking(&m_); }
  void unlock() override { recursive_mutex_exit(&m_); }
private:
  recursive_mutex_t m_;
};

class InlineExecutor : public Executor {
public:
  void submit(std::function<void()> task) override { task(); }
  void drain() override {}
  bool is_cooperative() const override { return true; }
};

class PicoRandom : public Random {
public:
  void fill(uint8_t* buf, size_t n) override {
    size_t i = 0;
    while (i + 8 <= n) { uint64_t r = get_rand_64(); for (int b = 0; b < 8; ++b) buf[i++] = (r >> (b * 8)) & 0xFF; }
    if (i < n) { uint64_t r = get_rand_64(); while (i < n) { buf[i++] = r & 0xFF; r >>= 8; } }
  }
};

class NoTransport : public Transport {
public:
  bool supported() const override { return false; }
};

class RamStorage : public Storage {
public:
  bool read(const std::string& k, std::vector<uint8_t>& out) override {
    auto it = map_.find(k); if (it == map_.end()) return false; out = it->second; return true;
  }
  bool write(const std::string& k, const std::vector<uint8_t>& d) override { map_[k] = d; return true; }
  bool remove(const std::string& k) override { return map_.erase(k) > 0; }
  bool exists(const std::string& k) override { return map_.count(k) > 0; }
  std::vector<std::string> keys() override {
    std::vector<std::string> o; for (auto& kv : map_) o.push_back(kv.first); return o;
  }
  void clear() override { map_.clear(); }
private:
  std::map<std::string, std::vector<uint8_t>> map_;
};

class PicoHal : public Hal {
public:
  Clock& clock() override { return clock_; }
  Mutex& mutex() override { return mutex_; }
  Executor& executor() override { return executor_; }
  Random& random() override { return random_; }
  Transport& transport() override { return transport_; }
  Storage* open_storage(const std::string& name) override {
    auto it = stores_.find(name);
    if (it != stores_.end()) return it->second.get();
    auto s = std::make_unique<RamStorage>();
    Storage* p = s.get(); stores_[name] = std::move(s); return p;
  }
  const char* platform_name() const override { return "rp2040"; }
private:
  PicoClock clock_;
  PicoMutex mutex_;
  InlineExecutor executor_;
  PicoRandom random_;
  NoTransport transport_;
  std::map<std::string, std::unique_ptr<RamStorage>> stores_;
};

}  // namespace

Hal& get() {
  static PicoHal instance;
  return instance;
}

}  // namespace hal
}  // namespace laila_c
