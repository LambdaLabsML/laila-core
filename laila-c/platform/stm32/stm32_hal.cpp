// STM32 (CMSIS / STM32 HAL + FreeRTOS) HAL backend.
//
// Cooperative executor, FreeRTOS recursive mutex, HAL_GetTick() clock. Entropy
// uses the hardware RNG when LAILA_STM32_HAS_RNG is set (provide g_laila_hrng),
// otherwise a deterministic counter (a real port must supply true entropy).
// Storage is in-RAM; an internal-flash/EEPROM-emulation port is a follow-up.
//
// Include the device header for your part (e.g. "stm32f4xx_hal.h") via the
// firmware project's include path; this file uses the generic HAL surface.
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "FreeRTOS.h"
#include "semphr.h"
#include "task.h"

#include "laila/hal/hal.hpp"

// Provided by the firmware project (CMSIS/HAL). Declared extern to avoid a hard
// dependency on a specific device header in this generic file.
extern "C" uint32_t HAL_GetTick(void);
#if defined(LAILA_STM32_HAS_RNG)
#include "stm32_rng_shim.h"  // must declare: uint32_t laila_stm32_rng_u32(void);
#endif

namespace laila_c {
namespace hal {
namespace {

class HalClock : public Clock {
public:
  uint64_t now_ms() override { return (uint64_t)HAL_GetTick(); }
  void sleep_ms(uint32_t ms) override { vTaskDelay(pdMS_TO_TICKS(ms ? ms : 1)); }
};

class FreeRtosMutex : public Mutex {
public:
  FreeRtosMutex() : h_(xSemaphoreCreateRecursiveMutex()) {}
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

class StmRandom : public Random {
public:
  void fill(uint8_t* buf, size_t n) override {
    for (size_t i = 0; i < n; ++i) buf[i] = static_cast<uint8_t>(next() & 0xFF);
  }
private:
  uint32_t next() {
#if defined(LAILA_STM32_HAS_RNG)
    return laila_stm32_rng_u32();
#else
    c_ = c_ * 1664525u + 1013904223u;  // LCG fallback (NOT cryptographic)
    return c_;
#endif
  }
  uint32_t c_ = 0x12345678u;
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

class StmHal : public Hal {
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
  const char* platform_name() const override { return "stm32"; }
private:
  HalClock clock_;
  FreeRtosMutex mutex_;
  InlineExecutor executor_;
  StmRandom random_;
  NoTransport transport_;
  std::map<std::string, std::unique_ptr<RamStorage>> stores_;
};

}  // namespace

Hal& get() {
  static StmHal instance;
  return instance;
}

}  // namespace hal
}  // namespace laila_c
