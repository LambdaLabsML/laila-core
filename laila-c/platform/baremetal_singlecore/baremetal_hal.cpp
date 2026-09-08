// Generic single-core / bare-metal HAL backend.
//
// Targets MCUs with no OS, no threads, and no filesystem (and also builds on a
// host to exercise the cooperative path). Everything is single-threaded:
// the executor runs work inline, the mutex is a no-op, storage is in-RAM, and
// the clock is a simple monotonic counter. Networking is unsupported.
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "laila/hal/hal.hpp"

namespace laila_c {
namespace hal {
namespace {

class CounterClock : public Clock {
public:
  uint64_t now_ms() override { return t_; }
  void sleep_ms(uint32_t ms) override { t_ += ms ? ms : 1; }  // advance virtual time
private:
  uint64_t t_ = 0;
};

// Single core: no contention, so locking is a no-op.
class NoopMutex : public Mutex {
public:
  void lock() override {}
  void unlock() override {}
};

class InlineExecutor : public Executor {
public:
  void submit(std::function<void()> task) override { task(); }
  void drain() override {}
  bool is_cooperative() const override { return true; }
};

// xorshift64 PRNG. Deterministic by default; on real hardware a platform port
// would reseed from an entropy peripheral (RNG/ADC noise) in the constructor.
class XorShiftRandom : public Random {
public:
  void fill(uint8_t* buf, size_t n) override {
    for (size_t i = 0; i < n; ++i) buf[i] = static_cast<uint8_t>(next() & 0xFF);
  }
private:
  uint64_t next() {
    s_ ^= s_ << 13; s_ ^= s_ >> 7; s_ ^= s_ << 17;
    return s_;
  }
  uint64_t s_ = 0x9E3779B97F4A7C15ull;
};

class NoTransport : public Transport {
public:
  bool supported() const override { return false; }
};

// In-RAM key/value storage (stands in for flash/NVS on a real port).
class RamStorage : public Storage {
public:
  bool read(const std::string& key, std::vector<uint8_t>& out) override {
    auto it = map_.find(key);
    if (it == map_.end()) return false;
    out = it->second;
    return true;
  }
  bool write(const std::string& key, const std::vector<uint8_t>& data) override {
    map_[key] = data;
    return true;
  }
  bool remove(const std::string& key) override { return map_.erase(key) > 0; }
  bool exists(const std::string& key) override { return map_.count(key) > 0; }
  std::vector<std::string> keys() override {
    std::vector<std::string> out;
    for (auto& kv : map_) out.push_back(kv.first);
    return out;
  }
  void clear() override { map_.clear(); }
private:
  std::map<std::string, std::vector<uint8_t>> map_;
};

class BaremetalHal : public Hal {
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
    Storage* ptr = s.get();
    stores_[name] = std::move(s);
    return ptr;
  }
  const char* platform_name() const override { return "baremetal_singlecore"; }

private:
  CounterClock clock_;
  NoopMutex mutex_;
  InlineExecutor executor_;
  XorShiftRandom random_;
  NoTransport transport_;
  std::map<std::string, std::unique_ptr<RamStorage>> stores_;
};

}  // namespace

Hal& get() {
  static BaremetalHal instance;
  return instance;
}

}  // namespace hal
}  // namespace laila_c
