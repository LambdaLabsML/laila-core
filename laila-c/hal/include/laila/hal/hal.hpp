// laila-C Hardware Abstraction Layer (HAL) interfaces.
//
// laila-core depends ONLY on these abstract interfaces; it contains no
// platform includes. Each platform backend (posix, esp32, rp2040, stm32,
// baremetal_singlecore) provides a concrete Hal via laila_c::hal::get().
//
// A backend that cannot honor a capability must return/raise
// laila_status::Unsupported (LAILA_UNSUPPORTED) rather than crash.
#ifndef LAILA_HAL_HAL_HPP
#define LAILA_HAL_HAL_HPP

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

namespace laila_c {
namespace hal {

// Connection/transport kinds for central.communication. laila ships TCP/IP;
// laila-C generalizes to the broad set of links an embedded or server target
// might use. A backend advertises which it supports via Transport::supports().
enum class ConnectionType {
  Loopback,         // in-process (always available; handled in core)
  TCP, UDP, TLS,    // IP transports
  WebSocket, HTTP, MQTT, CoAP, AMQP, GRPC,  // application protocols
  LoRa, LoRaWAN,    // long-range radio
  BLE, BluetoothClassic, Zigbee, Thread, NFC, WiFiDirect, Cellular,  // wireless
  Serial, I2C, SPI, CAN, RS485, Ethernet,   // wired / buses
};
const char* connection_type_name(ConnectionType t);

// Union of fields used across connection kinds (only the relevant ones are set
// per type). Mirrors laila's tcpip host/port/peer_secret_key and adds radio/bus
// parameters.
struct ConnectionConfig {
  std::string host;          // TCP/UDP/TLS/WebSocket/HTTP/MQTT host or peer id
  uint16_t port = 0;
  std::string uri;           // full uri (e.g. mqtt://broker:1883/topic) or endpoint name
  std::string secret;        // peer_secret_key / PSK / token
  std::string topic;         // MQTT/AMQP topic or channel
  std::string device;        // serial/i2c/spi/can device path or bus id
  uint32_t baud = 0;         // serial baud rate
  uint32_t frequency_hz = 0; // LoRa/radio frequency
  uint8_t spreading_factor = 0;  // LoRa SF
  uint8_t address = 0;       // I2C/CAN/RS485 node address
  std::map<std::string, std::string> extra;  // backend-specific knobs
};

// A bidirectional byte channel opened by a Transport. send/recv operate on
// framed byte buffers; recv() returns false on timeout/closed.
class Connection {
public:
  virtual ~Connection() = default;
  virtual bool send(const std::vector<uint8_t>& data) = 0;
  virtual bool recv(std::vector<uint8_t>& out, int timeout_ms) = 0;
  virtual void close() = 0;
  virtual bool is_open() const = 0;
};

// A passive endpoint that accepts inbound Connections (server side). Opened by
// Transport::listen(). accept() returns a freshly accepted Connection, or
// nullptr on timeout (no caller blocked indefinitely so a cooperative,
// single-core target can poll it from its run loop without a dedicated thread).
class Listener {
public:
  virtual ~Listener() = default;
  virtual Connection* accept(int timeout_ms) = 0;  // nullptr == no inbound yet
  virtual void close() = 0;
  virtual bool is_listening() const = 0;
  // The actually-bound local port (resolves an OS-assigned port when the caller
  // requested port 0, mirroring laila's bound_port). 0 if not applicable.
  virtual uint16_t local_port() const { return 0; }
};

// Monotonic clock, milliseconds. Timeouts in laila-C are expressed in ms
// (the translation contract maps Python float seconds -> ms).
class Clock {
public:
  virtual ~Clock() = default;
  virtual uint64_t now_ms() = 0;
  virtual void sleep_ms(uint32_t ms) = 0;
};

// Process-wide coarse lock. Single-core backends may return a no-op.
class Mutex {
public:
  virtual ~Mutex() = default;
  virtual void lock() = 0;
  virtual void unlock() = 0;
};

// Executor strategy behind every taskforce. submit() must eventually run the
// task; cooperative backends run it inline (run-to-completion), threaded
// backends may run it on a worker. drain() pumps pending work (cooperative).
class Executor {
public:
  virtual ~Executor() = default;
  virtual void submit(std::function<void()> task) = 0;
  virtual void drain() = 0;       // run any queued cooperative work
  virtual bool is_cooperative() const = 0;
};

// Byte-blob key/value storage abstraction (flash, FS, NVS, RAM-backed).
class Storage {
public:
  virtual ~Storage() = default;
  virtual bool read(const std::string& key, std::vector<uint8_t>& out) = 0;
  virtual bool write(const std::string& key, const std::vector<uint8_t>& data) = 0;
  virtual bool remove(const std::string& key) = 0;
  virtual bool exists(const std::string& key) = 0;
  virtual std::vector<std::string> keys() = 0;
  virtual void clear() = 0;
};

// Connection factory for central.communication. `supported()` is the legacy
// "is there any peer-RPC transport" flag; `supports(type)` advertises a specific
// connection kind, and `open()` establishes it (returns nullptr if unsupported).
// Defaults make every kind unsupported, so a backend opts in only to what it has.
class Transport {
public:
  virtual ~Transport() = default;
  virtual bool supported() const = 0;
  virtual bool supports(ConnectionType /*type*/) const { return false; }
  virtual Connection* open(ConnectionType /*type*/, const ConnectionConfig& /*cfg*/) {
    return nullptr;
  }
  // Inbound serving: a backend opts in to acting as a server for a link type.
  // listen() binds/listens and returns a Listener (nullptr if unsupported).
  virtual bool supports_listen(ConnectionType /*type*/) const { return false; }
  virtual Listener* listen(ConnectionType /*type*/, const ConnectionConfig& /*cfg*/) {
    return nullptr;
  }
};

// Entropy source for UUID generation.
class Random {
public:
  virtual ~Random() = default;
  virtual void fill(uint8_t* buf, size_t n) = 0;
};

// Aggregate HAL handed to the core by the active platform backend.
class Hal {
public:
  virtual ~Hal() = default;
  virtual Clock& clock() = 0;
  virtual Mutex& mutex() = 0;          // process-wide coarse lock
  virtual Executor& executor() = 0;    // default/alpha executor
  virtual Random& random() = 0;
  virtual Transport& transport() = 0;
  // Storage opened by name (e.g. a directory or NVS namespace). May return
  // nullptr if the backend cannot provide storage for that name.
  virtual Storage* open_storage(const std::string& name) = 0;
  virtual const char* platform_name() const = 0;
};

// Provided by exactly one platform backend translation unit.
Hal& get();

// RAII guard over the process-wide mutex.
class Guard {
public:
  Guard() : m_(get().mutex()) { m_.lock(); }
  ~Guard() { m_.unlock(); }
  Guard(const Guard&) = delete;
  Guard& operator=(const Guard&) = delete;
private:
  Mutex& m_;
};

}  // namespace hal
}  // namespace laila_c

#endif  // LAILA_HAL_HAL_HPP
