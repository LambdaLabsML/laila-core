// POSIX HAL backend (desktop / AWS / any Linux/macOS host). The reference
// backend used for host tests. Uses a cooperative (run-to-completion) executor
// so behavior matches the single-core path; storage is directory-backed.
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "laila/hal/hal.hpp"

namespace laila_c {
namespace hal {
namespace {

namespace fs = std::filesystem;

class PosixClock : public Clock {
public:
  uint64_t now_ms() override {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
  }
  void sleep_ms(uint32_t ms) override { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }
};

class PosixMutex : public Mutex {
public:
  void lock() override { m_.lock(); }
  void unlock() override { m_.unlock(); }
private:
  std::recursive_mutex m_;
};

// Cooperative, run-to-completion executor: submitted work executes inline.
class CooperativeExecutor : public Executor {
public:
  void submit(std::function<void()> task) override { task(); }
  void drain() override {}
  bool is_cooperative() const override { return true; }
};

// Worker-thread pool executor (the threaded taskforce strategy). Futures block
// in their poll-based wait() until a worker completes the task.
class ThreadPoolExecutor : public Executor {
public:
  explicit ThreadPoolExecutor(unsigned n) {
    if (n == 0) n = 1;
    for (unsigned i = 0; i < n; ++i) workers_.emplace_back([this] { run(); });
  }
  ~ThreadPoolExecutor() override {
    { std::unique_lock<std::mutex> lk(m_); stop_ = true; }
    cv_.notify_all();
    for (auto& t : workers_) if (t.joinable()) t.join();
  }
  void submit(std::function<void()> task) override {
    { std::unique_lock<std::mutex> lk(m_); queue_.push_back(std::move(task)); }
    cv_.notify_one();
  }
  void drain() override {}
  bool is_cooperative() const override { return false; }

private:
  void run() {
    for (;;) {
      std::function<void()> job;
      {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
        if (stop_ && queue_.empty()) return;
        job = std::move(queue_.front());
        queue_.pop_front();
      }
      job();
    }
  }
  std::vector<std::thread> workers_;
  std::deque<std::function<void()>> queue_;
  std::mutex m_;
  std::condition_variable cv_;
  bool stop_ = false;
};

class PosixRandom : public Random {
public:
  void fill(uint8_t* buf, size_t n) override {
    std::random_device rd;
    for (size_t i = 0; i < n; ++i) buf[i] = static_cast<uint8_t>(rd() & 0xFF);
  }
};

// Real BSD-socket connection (TCP or UDP) for the host backend.
class SocketConnection : public Connection {
public:
  explicit SocketConnection(int fd) : fd_(fd) {}
  ~SocketConnection() override { close(); }
  bool send(const std::vector<uint8_t>& d) override {
    size_t sent = 0;
    while (sent < d.size()) {
      ssize_t n = ::send(fd_, d.data() + sent, d.size() - sent, 0);
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
    int r = ::select(fd_ + 1, &rf, nullptr, nullptr, timeout_ms < 0 ? nullptr : &tv);
    if (r <= 0) return false;
    uint8_t buf[4096];
    ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
    if (n == 0) { close(); return false; }  // peer closed (EOF) -> mark not-open
    if (n < 0) return false;                 // transient (EINTR/EAGAIN)
    out.assign(buf, buf + n);
    return true;
  }
  void close() override {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
  }
  bool is_open() const override { return fd_ >= 0; }

private:
  int fd_;
};

// Listening TCP socket: accept() returns a SocketConnection per inbound peer.
class PosixListener : public Listener {
public:
  explicit PosixListener(int fd) : fd_(fd) {}
  ~PosixListener() override { close(); }
  Connection* accept(int timeout_ms) override {
    if (fd_ < 0) return nullptr;
    fd_set rf;
    FD_ZERO(&rf);
    FD_SET(fd_, &rf);
    timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    int r = ::select(fd_ + 1, &rf, nullptr, nullptr, timeout_ms < 0 ? nullptr : &tv);
    if (r <= 0) return nullptr;
    int c = ::accept(fd_, nullptr, nullptr);
    if (c < 0) return nullptr;
    return new SocketConnection(c);
  }
  void close() override {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
  }
  bool is_listening() const override { return fd_ >= 0; }
  uint16_t local_port() const override {
    if (fd_ < 0) return 0;
    sockaddr_in addr{};
    socklen_t alen = sizeof(addr);
    if (::getsockname(fd_, (sockaddr*)&addr, &alen) != 0) return 0;
    return ntohs(addr.sin_port);
  }

private:
  int fd_;
};

// POSIX transport: live TCP/UDP via sockets; other link types unsupported.
class PosixTransport : public Transport {
public:
  bool supported() const override { return false; }  // legacy peer-RPC flag
  bool supports(ConnectionType t) const override {
    return t == ConnectionType::TCP || t == ConnectionType::UDP;
  }
  Connection* open(ConnectionType t, const ConnectionConfig& cfg) override {
    if (t != ConnectionType::TCP && t != ConnectionType::UDP) return nullptr;
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = (t == ConnectionType::TCP) ? SOCK_STREAM : SOCK_DGRAM;
    addrinfo* res = nullptr;
    std::string port = std::to_string(cfg.port);
    if (::getaddrinfo(cfg.host.c_str(), port.c_str(), &hints, &res) != 0 || !res) return nullptr;
    int fd = -1;
    for (addrinfo* p = res; p; p = p->ai_next) {
      fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
      if (fd < 0) continue;
      if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
      ::close(fd);
      fd = -1;
    }
    ::freeaddrinfo(res);
    if (fd < 0) return nullptr;
    return new SocketConnection(fd);
  }
  bool supports_listen(ConnectionType t) const override { return t == ConnectionType::TCP; }
  Listener* listen(ConnectionType t, const ConnectionConfig& cfg) override {
    if (t != ConnectionType::TCP) return nullptr;
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return nullptr;
    int yes = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = cfg.host.empty() || cfg.host == "0.0.0.0"
                               ? htonl(INADDR_ANY)
                               : ::inet_addr(cfg.host.c_str());
    addr.sin_port = htons(cfg.port);
    if (::bind(fd, (sockaddr*)&addr, sizeof(addr)) != 0) { ::close(fd); return nullptr; }
    if (::listen(fd, 8) != 0) { ::close(fd); return nullptr; }
    return new PosixListener(fd);
  }
};

class DirStorage : public Storage {
public:
  explicit DirStorage(const fs::path& dir) : dir_(dir) {
    std::error_code ec;
    fs::create_directories(dir_, ec);
  }
  bool read(const std::string& key, std::vector<uint8_t>& out) override {
    std::ifstream f(path(key), std::ios::binary);
    if (!f) return false;
    out.assign((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    return true;
  }
  bool write(const std::string& key, const std::vector<uint8_t>& data) override {
    std::ofstream f(path(key), std::ios::binary | std::ios::trunc);
    if (!f) return false;
    f.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
    return true;
  }
  bool remove(const std::string& key) override {
    std::error_code ec;
    return fs::remove(path(key), ec);
  }
  bool exists(const std::string& key) override { return fs::exists(path(key)); }
  std::vector<std::string> keys() override {
    std::vector<std::string> out;
    std::error_code ec;
    for (auto& e : fs::directory_iterator(dir_, ec)) out.push_back(decode(e.path().filename().string()));
    return out;
  }
  void clear() override {
    std::error_code ec;
    for (auto& e : fs::directory_iterator(dir_, ec)) fs::remove(e.path(), ec);
  }
private:
  // Keys are laila global_ids; encode ':' and '/' for portable filenames.
  static std::string encode(const std::string& key) {
    std::string out;
    for (char c : key) {
      if (c == ':' ) out += "%3A";
      else if (c == '/') out += "%2F";
      else out.push_back(c);
    }
    return out;
  }
  static std::string decode(const std::string& name) {
    std::string out;
    for (size_t i = 0; i < name.size(); ++i) {
      if (name[i] == '%' && i + 2 < name.size()) {
        std::string h = name.substr(i + 1, 2);
        out.push_back(static_cast<char>(std::stoi(h, nullptr, 16)));
        i += 2;
      } else out.push_back(name[i]);
    }
    return out;
  }
  fs::path path(const std::string& key) const { return dir_ / encode(key); }
  fs::path dir_;
};

class PosixHal : public Hal {
public:
  Clock& clock() override { return clock_; }
  Mutex& mutex() override { return mutex_; }
  Executor& executor() override { return *executor_; }
  Random& random() override { return random_; }
  Transport& transport() override { return transport_; }
  Storage* open_storage(const std::string& name) override {
    auto it = stores_.find(name);
    if (it != stores_.end()) return it->second.get();
    fs::path root = fs::temp_directory_path() / "laila" / name;
    auto s = std::make_unique<DirStorage>(root);
    Storage* ptr = s.get();
    stores_[name] = std::move(s);
    return ptr;
  }
  const char* platform_name() const override { return "posix"; }

  PosixHal() {
#if defined(LAILA_POSIX_THREADED)
    executor_ = std::make_unique<ThreadPoolExecutor>(std::thread::hardware_concurrency());
#else
    executor_ = std::make_unique<CooperativeExecutor>();
#endif
  }

private:
  PosixClock clock_;
  PosixMutex mutex_;
  std::unique_ptr<Executor> executor_;
  PosixRandom random_;
  PosixTransport transport_;
  std::map<std::string, std::unique_ptr<DirStorage>> stores_;
};

}  // namespace

Hal& get() {
  static PosixHal instance;
  return instance;
}

}  // namespace hal
}  // namespace laila_c
