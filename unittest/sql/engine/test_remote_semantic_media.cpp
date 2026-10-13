/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "sql/engine/basic/ob_semantic_media.h"
#include "sql/engine/basic/ob_semantic_runtime.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "common/json_type/ob_json_tree.h"
#include "common/object/file_ref.h"
#include "lib/allocator/page_arena.h"
#include "lib/encode/ob_base64_encode.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_encryption_util.h"
#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <curl/curl.h>
#include <fcntl.h>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <openssl/rsa.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <poll.h>
#include <signal.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <zlib.h>

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{
using Bytes = std::vector<uint8_t>;
using Task = std::shared_ptr<SemanticMediaTask>;
using Runtime = std::shared_ptr<SemanticMediaRuntime>;
using Budget = std::shared_ptr<SemanticQueryBudget>;
using Stage = SemanticMediaRuntime::TestStage;
std::atomic<int64_t> cleanup_errors{0};

void require(bool condition, const std::string &message)
{
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void equal(int64_t actual, int64_t expected, const std::string &message)
{
  if (actual != expected) {
    std::ostringstream out;
    out << message << ": expected " << expected << ", got " << actual;
    throw std::runtime_error(out.str());
  }
}

ObString view(const std::string &value)
{
  return ObString(static_cast<int64_t>(value.size()), value.data());
}

std::string copy(const ObString &value)
{
  return value.empty() ? std::string() : std::string(value.ptr(), value.length());
}

template <typename Predicate>
void wait_until(Predicate predicate, const std::string &message, int seconds = 10)
{
  const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
  while (!predicate() && std::chrono::steady_clock::now() < end) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  require(predicate(), message);
}

Budget make_budget(int64_t limit = 16 * 1024 * 1024)
{
  static std::atomic<uint64_t> next{0x52454d4f5445};
  return std::make_shared<SemanticQueryBudget>(next.fetch_add(1), limit, 64);
}

Runtime acquire(const Budget &budget, const std::string &local, const std::string &remote,
                int64_t max_bytes = 1024 * 1024, int64_t parallel = 4,
                int64_t deadline = INT64_MAX)
{
  Runtime runtime;
  equal(SemanticMediaRuntime::acquire(budget, view(local), view(remote), max_bytes,
                                     parallel, deadline, runtime),
        OB_SUCCESS, "acquire remote-enabled media runtime");
  require(static_cast<bool>(runtime), "successful acquisition returns a handle");
  return runtime;
}

Task demand(const Runtime &runtime, const std::string &encoded)
{
  Task task;
  equal(runtime->demand(view(encoded), task), OB_SUCCESS, "admit media demand");
  require(static_cast<bool>(task), "successful admission returns a handle");
  return task;
}

void denied(const Runtime &runtime, const std::string &encoded, int expected = OB_SUCCESS)
{
  Task task;
  const int result = runtime->demand(view(encoded), task);
  if (expected != OB_SUCCESS) {
    equal(result, expected, "deny demand before I/O");
  } else {
    require(result != OB_SUCCESS && result != OB_EAGAIN
            && result != OB_TIMEOUT && result != OB_CANCELED,
            "policy denial reports a terminal error before I/O");
  }
  require(!task, "denied demand does not publish a task");
}

void ready(const Task &task)
{
  wait_until([&] { return task->ready(); }, "bounded asynchronous task completion");
}

void failed(const Task &task, int expected = OB_SUCCESS)
{
  ready(task);
  if (expected != OB_SUCCESS) {
    equal(task->result(), expected, "preserve explicit media failure");
  } else {
    require(task->result() != OB_SUCCESS && task->result() != OB_EAGAIN
            && task->result() != OB_CANCELED && task->result() != OB_TIMEOUT,
            "invalid remote media reports a terminal validation/transport error");
  }
  require(task->base64().empty() && task->mime_type().empty() && task->audio_format().empty(),
          "failed tasks do not publish partial bytes or success-shaped metadata");
  require(!task->is_image() && !task->is_audio(), "failed tasks have no media classification");
}

void expect_bytes(const Task &task, const Bytes &bytes, const char *mime, const char *audio = "")
{
  ready(task);
  equal(task->result(), OB_SUCCESS, "verified remote media succeeds");
  equal(task->base64().length(), ObBase64Encoder::needed_encoded_length(bytes.size()),
        "base64 has the exact unwrapped output length");
  require(copy(task->mime_type()) == mime && copy(task->audio_format()) == audio,
          "container validation derives canonical MIME and audio format");
  require(task->is_audio() == (*audio != '\0') && task->is_image() == (*audio == '\0'),
          "media classification follows the validated container");
  Bytes decoded(ObBase64Encoder::needed_decoded_length(task->base64().length()));
  int64_t length = 0;
  equal(ObBase64Encoder::decode(task->base64().ptr(), task->base64().length(), decoded.data(),
                               decoded.size(), length), OB_SUCCESS, "decode immutable media");
  equal(length, bytes.size(), "download preserves exact byte length");
  require(std::equal(bytes.begin(), bytes.end(), decoded.begin()), "download preserves every byte");
}

class Gate
{
public:
  explicit Gate(const Budget &budget = Budget()) : budget_(budget) {}
  ~Gate() { release(); }
  static void hook(void *context, Stage stage)
  {
    auto &gate = *static_cast<Gate *>(context);
    if (gate.budget_ && stage == Stage::AFTER_ENCODE_CHUNK) {
      const int64_t used = gate.budget_->memory_used();
      int64_t peak = gate.peak_.load();
      while (used > peak && !gate.peak_.compare_exchange_weak(peak, used)) {}
    }
    if (stage == Stage::BEFORE_ENCODE) {
      std::unique_lock<std::mutex> lock(gate.mutex_);
      ++gate.arrived_;
      gate.changed_.notify_all();
      gate.changed_.wait(lock, [&] { return gate.released_; });
    }
  }
  bool released()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return released_;
  }
  int64_t arrived()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return arrived_;
  }
  int64_t peak() const { return peak_.load(); }
  void release()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    released_ = true;
    changed_.notify_all();
  }
private:
  std::mutex mutex_;
  std::condition_variable changed_;
  int64_t arrived_ = 0;
  bool released_ = false;
  Budget budget_;
  std::atomic<int64_t> peak_{0};
};

class ReleaseGate
{
public:
  explicit ReleaseGate(Gate &gate) : gate_(gate) {}
  ~ReleaseGate() { gate_.release(); }
private:
  Gate &gate_;
};

class Fd
{
public:
  explicit Fd(int value = -1) : value_(value) {}
  ~Fd()
  {
    if (value_ >= 0 && ::close(value_) != 0 && errno != EINTR) {
      cleanup_errors.fetch_add(1);
      std::cerr << "fixture descriptor close failed: " << std::strerror(errno) << std::endl;
    }
  }
  Fd(const Fd &) = delete;
  Fd &operator=(const Fd &) = delete;
  int get() const { return value_; }
private:
  int value_;
};

enum class Framing { LENGTH, CHUNKED, CLOSE };

struct Response
{
  Bytes bytes_;
  int status_ = 200;
  Framing framing_ = Framing::LENGTH;
  int64_t content_length_ = -1;
  std::string headers_;
  std::shared_ptr<Gate> gate_;
  size_t before_gate_ = 0;
};

struct Request
{
  std::string method_;
  std::string target_;
  bool disconnected_ = false;
};

using SslContext = std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)>;
using Ssl = std::unique_ptr<SSL, decltype(&SSL_free)>;

SslContext self_signed_tls()
{
  std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> generator(
      EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr), &EVP_PKEY_CTX_free);
  require(static_cast<bool>(generator), "allocate ephemeral fixture key generator");
  equal(EVP_PKEY_keygen_init(generator.get()), 1, "initialize fixture key generation");
  equal(EVP_PKEY_CTX_set_rsa_keygen_bits(generator.get(), 2048), 1, "set fixture key width");
  EVP_PKEY *raw_key = nullptr;
  const int generated = EVP_PKEY_keygen(generator.get(), &raw_key);
  std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(raw_key, &EVP_PKEY_free);
  equal(generated, 1, "generate a private ephemeral, non-persistent fixture key");
  std::unique_ptr<X509, decltype(&X509_free)> certificate(X509_new(), &X509_free);
  require(static_cast<bool>(certificate), "allocate untrusted fixture certificate");
  equal(X509_set_version(certificate.get(), 2), 1, "set fixture certificate version");
  equal(ASN1_INTEGER_set(X509_get_serialNumber(certificate.get()), 1), 1,
        "set fixture certificate serial");
  require(X509_gmtime_adj(X509_get_notBefore(certificate.get()), -60) != nullptr
          && X509_gmtime_adj(X509_get_notAfter(certificate.get()), 600) != nullptr,
          "fixture certificate is currently valid");
  equal(X509_set_pubkey(certificate.get(), key.get()), 1, "set fixture certificate public key");
  X509_NAME *subject = X509_get_subject_name(certificate.get());
  const unsigned char name[] = "127.0.0.1";
  equal(X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC, name, -1, -1, 0), 1,
        "set fixture certificate subject");
  equal(X509_set_issuer_name(certificate.get(), subject), 1, "self-sign the fixture issuer");
  char alternative_name[] = "IP:127.0.0.1";
  std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> extension(
      X509V3_EXT_conf_nid(nullptr, nullptr, NID_subject_alt_name, alternative_name),
      &X509_EXTENSION_free);
  require(static_cast<bool>(extension), "create a matching numeric-loopback certificate SAN");
  equal(X509_add_ext(certificate.get(), extension.get(), -1), 1, "set matching fixture SAN");
  require(X509_sign(certificate.get(), key.get(), EVP_sha256()) > 0, "sign fixture certificate");
  SslContext context(SSL_CTX_new(TLS_server_method()), &SSL_CTX_free);
  require(static_cast<bool>(context), "allocate private TLS fixture context");
  equal(SSL_CTX_use_certificate(context.get(), certificate.get()), 1, "install fixture certificate");
  equal(SSL_CTX_use_PrivateKey(context.get(), key.get()), 1, "install ephemeral fixture key");
  equal(SSL_CTX_check_private_key(context.get()), 1, "fixture key matches its certificate");
  return context;
}

class Storage
{
public:
  explicit Storage(bool tls = false)
      : listener_(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0)),
        tls_(tls ? self_signed_tls() : SslContext(nullptr, &SSL_CTX_free))
  {
    require(listener_.get() >= 0, "create numeric-loopback storage listener");
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    equal(::bind(listener_.get(), reinterpret_cast<sockaddr *>(&address), sizeof(address)), 0,
          "bind private ephemeral storage port");
    equal(::listen(listener_.get(), 128), 0, "listen on private storage socket");
    socklen_t size = sizeof(address);
    equal(::getsockname(listener_.get(), reinterpret_cast<sockaddr *>(&address), &size), 0,
          "discover private storage port");
    origin_ = std::string(tls ? "https" : "http") + "://127.0.0.1:"
        + std::to_string(ntohs(address.sin_port));
    acceptor_ = std::thread([this] { accept_loop(); });
  }

  ~Storage()
  {
    stopping_.store(true);
    if (acceptor_.joinable()) {
      acceptor_.join();
    }
    for (auto &worker : workers_) {
      worker.join();
    }
    for (const auto &error : errors_) {
      cleanup_errors.fetch_add(1);
      std::cerr << "storage fixture failed: " << error << std::endl;
    }
  }

  std::string root() const { return origin_ + "/media/"; }
  std::string url(const std::string &target) const { return origin_ + target; }

  void add(const std::string &target, const Response &response)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    require(responses_.emplace(target, response).second, "storage targets are unique");
  }

  int64_t count(const std::string &target = "")
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::count_if(requests_.begin(), requests_.end(),
                        [&](const Request &request) { return target.empty() || request.target_ == target; });
  }

  int64_t disconnected()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::count_if(requests_.begin(), requests_.end(),
                        [](const Request &request) { return request.disconnected_; });
  }

  void healthy()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    require(errors_.empty(), errors_.empty() ? "" : errors_.front());
    for (const auto &request : requests_) {
      require(request.method_ == "GET", "remote backend performs GET only, never HEAD");
      require(responses_.count(request.target_) == 1, "GET preserves the exact escaped path/query bytes");
    }
  }

private:
  bool wait_socket(int fd, short events)
  {
    pollfd descriptor{fd, events, 0};
    const int result = ::poll(&descriptor, 1, 10);
    if (result < 0 && errno != EINTR) {
      throw std::runtime_error("fixture socket poll failed");
    }
    return result > 0;
  }

  ssize_t receive(int fd, SSL *tls, void *buffer, size_t size, bool peek = false)
  {
    if (tls == nullptr) {
      return ::recv(fd, buffer, size, peek ? MSG_PEEK | MSG_DONTWAIT : 0);
    }
    const int count = peek ? SSL_peek(tls, buffer, static_cast<int>(size))
                           : SSL_read(tls, buffer, static_cast<int>(size));
    if (count > 0) {
      return count;
    }
    const int error = SSL_get_error(tls, count);
    if (error == SSL_ERROR_ZERO_RETURN) {
      return 0;
    }
    errno = (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) ? EAGAIN : ECONNRESET;
    return -1;
  }

  bool send_bytes(int fd, SSL *tls, const void *data, size_t size)
  {
    const char *bytes = static_cast<const char *>(data);
    size_t sent = 0;
    while (sent < size && !stopping_.load()) {
      ssize_t count = tls == nullptr ? ::send(fd, bytes + sent, size - sent, MSG_NOSIGNAL)
          : SSL_write(tls, bytes + sent, static_cast<int>(size - sent));
      if (tls != nullptr && count <= 0) {
        const int error = SSL_get_error(tls, static_cast<int>(count));
        errno = (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) ? EAGAIN : ECONNRESET;
        count = -1;
      }
      if (count > 0) {
        sent += count;
      } else if (count < 0 && errno == EINTR) {
        continue;
      } else if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        wait_socket(fd, tls != nullptr && SSL_want_read(tls) ? POLLIN : POLLOUT);
      } else {
        return false;
      }
    }
    return sent == size;
  }

  bool send_body(int fd, SSL *tls, const Response &response, size_t begin, size_t end)
  {
    while (begin < end) {
      const size_t size = std::min<size_t>(16384, end - begin);
      if (response.framing_ == Framing::CHUNKED) {
        std::ostringstream chunk;
        chunk << std::hex << size << "\r\n";
        const std::string header = chunk.str();
        if (!send_bytes(fd, tls, header.data(), header.size())) {
          return false;
        }
      }
      if (!send_bytes(fd, tls, response.bytes_.data() + begin, size)) {
        return false;
      }
      if (response.framing_ == Framing::CHUNKED && !send_bytes(fd, tls, "\r\n", 2)) {
        return false;
      }
      begin += size;
    }
    return true;
  }

  bool peer_closed(int fd, SSL *tls)
  {
    char byte;
    const ssize_t count = receive(fd, tls, &byte, 1, true);
    return count == 0 || (count < 0 && (errno == ECONNRESET || errno == ENOTCONN));
  }

  void serve(int socket)
  {
    Fd connection(socket);
    try {
      Ssl tls(nullptr, &SSL_free);
      if (tls_) {
        tls.reset(SSL_new(tls_.get()));
        require(static_cast<bool>(tls), "allocate private TLS fixture connection");
        equal(SSL_set_fd(tls.get(), socket), 1, "attach owned TLS fixture socket");
        const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!stopping_.load()) {
          const int accepted = SSL_accept(tls.get());
          if (accepted == 1) {
            break;
          }
          const int error = SSL_get_error(tls.get(), accepted);
          if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE) {
            return;
          }
          require(std::chrono::steady_clock::now() < end, "bounded fixture TLS handshake");
          wait_socket(socket, error == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT);
        }
      }
      std::string headers;
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
      while (headers.find("\r\n\r\n") == std::string::npos && !stopping_.load()) {
        char buffer[1024];
        const ssize_t count = receive(socket, tls.get(), buffer, sizeof(buffer));
        if (count > 0) {
          headers.append(buffer, count);
          require(headers.size() <= 16384, "bounded fixture request headers");
        } else if (count == 0) {
          return;
        } else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
          return;
        }
        require(std::chrono::steady_clock::now() < deadline, "fixture request headers arrive");
        if (count < 0) {
          wait_socket(socket, tls && SSL_want_write(tls.get()) ? POLLOUT : POLLIN);
        }
      }
      if (stopping_.load()) {
        return;
      }
      const size_t first_space = headers.find(' ');
      const size_t second_space = headers.find(' ', first_space + 1);
      require(first_space != std::string::npos && second_space != std::string::npos,
              "fixture receives a complete HTTP request line");
      Request request{headers.substr(0, first_space),
                      headers.substr(first_space + 1, second_space - first_space - 1), false};
      Response response;
      size_t index = 0;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        index = requests_.size();
        requests_.push_back(request);
        const auto found = responses_.find(request.target_);
        if (found == responses_.end()) {
          response.status_ = 404;
        } else {
          response = found->second;
        }
      }
      std::ostringstream reply;
      reply << "HTTP/1.1 " << response.status_ << " Fixture\r\nConnection: close\r\n";
      if (response.framing_ == Framing::CHUNKED) {
        reply << "Transfer-Encoding: chunked\r\n";
      } else if (response.framing_ == Framing::LENGTH) {
        reply << "Content-Length: " << (response.content_length_ < 0
            ? static_cast<int64_t>(response.bytes_.size()) : response.content_length_) << "\r\n";
      }
      reply << response.headers_ << "\r\n";
      const std::string raw = reply.str();
      if (!send_bytes(socket, tls.get(), raw.data(), raw.size()) || request.method_ == "HEAD") {
        return;
      }
      const size_t before_gate = std::min(response.before_gate_, response.bytes_.size());
      if (!send_body(socket, tls.get(), response, 0, before_gate)) {
        return;
      }
      while (response.gate_ && !response.gate_->released() && !stopping_.load()) {
        if (peer_closed(socket, tls.get())) {
          std::lock_guard<std::mutex> lock(mutex_);
          requests_[index].disconnected_ = true;
          return;
        }
        wait_socket(socket, POLLIN);
      }
      if (send_body(socket, tls.get(), response, before_gate, response.bytes_.size())
          && response.framing_ == Framing::CHUNKED) {
        send_bytes(socket, tls.get(), "0\r\n\r\n", 5);
      }
    } catch (const std::exception &error) {
      std::lock_guard<std::mutex> lock(mutex_);
      errors_.push_back(error.what());
    }
  }

  void accept_loop()
  {
    try {
      while (!stopping_.load()) {
        if (!wait_socket(listener_.get(), POLLIN)) {
          continue;
        }
        const int socket = ::accept4(listener_.get(), nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
        if (socket >= 0) {
          try {
            workers_.emplace_back([this, socket] { serve(socket); });
          } catch (...) {
            Fd failed_connection(socket);
            throw;
          }
        } else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
          throw std::runtime_error("fixture accept failed");
        }
      }
    } catch (const std::exception &error) {
      std::lock_guard<std::mutex> lock(mutex_);
      errors_.push_back(error.what());
    }
  }

  Fd listener_;
  SslContext tls_;
  std::string origin_;
  std::atomic<bool> stopping_{false};
  std::thread acceptor_;
  std::vector<std::thread> workers_;
  std::mutex mutex_;
  std::map<std::string, Response> responses_;
  std::vector<Request> requests_;
  std::vector<std::string> errors_;
};

class LocalFile
{
public:
  explicit LocalFile(const Bytes &bytes)
  {
    char pattern[] = "/tmp/seekdb-remote-media-XXXXXX";
    const char *directory = ::mkdtemp(pattern);
    require(directory != nullptr, "create owned local fixture directory");
    root_ = directory;
    path_ = root_ + "/local.wav";
    try {
      Fd file(::open(path_.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600));
      require(file.get() >= 0, "create local fixture media");
      size_t written = 0;
      while (written < bytes.size()) {
        const ssize_t count = ::write(file.get(), bytes.data() + written, bytes.size() - written);
        if (count < 0 && errno == EINTR) {
          continue;
        }
        require(count > 0, "write owned local fixture");
        written += count;
      }
    } catch (...) {
      cleanup();
      throw;
    }
  }
  ~LocalFile() { cleanup(); }
  const std::string &root() const { return root_; }
  const std::string &path() const { return path_; }
private:
  void cleanup()
  {
    if (::unlink(path_.c_str()) != 0 && errno != ENOENT) {
      cleanup_errors.fetch_add(1);
      std::cerr << "fixture unlink failed: " << std::strerror(errno) << std::endl;
    }
    if (::rmdir(root_.c_str()) != 0) {
      cleanup_errors.fetch_add(1);
      std::cerr << "fixture rmdir failed: " << std::strerror(errno) << std::endl;
    }
  }
  std::string root_;
  std::string path_;
};

void big32(Bytes &bytes, uint32_t value)
{
  for (int shift : {24, 16, 8, 0}) {
    bytes.push_back(static_cast<uint8_t>(value >> shift));
  }
}

void little(Bytes &bytes, uint32_t value, int width)
{
  for (int i = 0; i < width; ++i) {
    bytes.push_back(static_cast<uint8_t>(value >> (8 * i)));
  }
}

void text(Bytes &bytes, const char *value, size_t length)
{
  bytes.insert(bytes.end(), value, value + length);
}

Bytes png()
{
  Bytes bytes{137, 80, 78, 71, 13, 10, 26, 10};
  const auto chunk = [&](const char *kind, const Bytes &payload) {
    big32(bytes, payload.size());
    const size_t begin = bytes.size();
    text(bytes, kind, 4);
    bytes.insert(bytes.end(), payload.begin(), payload.end());
    big32(bytes, ::crc32(0, bytes.data() + begin, bytes.size() - begin));
  };
  Bytes header;
  big32(header, 1);
  big32(header, 1);
  header.insert(header.end(), {8, 6, 0, 0, 0});
  chunk("IHDR", header);
  const uint8_t pixel[]{0, 0, 0, 0, 255};
  Bytes compressed(::compressBound(sizeof(pixel)));
  uLongf length = compressed.size();
  equal(::compress2(compressed.data(), &length, pixel, sizeof(pixel), Z_BEST_SPEED), Z_OK,
        "generate a valid synthetic PNG");
  compressed.resize(length);
  chunk("IDAT", compressed);
  chunk("IEND", {});
  return bytes;
}

Bytes jpeg()
{
  Bytes bytes{0xff, 0xd8, 0xff, 0xdb, 0, 67, 0};
  bytes.insert(bytes.end(), 64, 1);
  bytes.insert(bytes.end(), {0xff, 0xc0, 0, 11, 8, 0, 1, 0, 1, 1, 1, 0x11, 0});
  bytes.insert(bytes.end(), {0xff, 0xc4, 0, 38});
  for (const uint8_t table : {uint8_t(0), uint8_t(0x10)}) {
    bytes.insert(bytes.end(), {table, 1});
    bytes.insert(bytes.end(), 15, 0);
    bytes.push_back(0);
  }
  bytes.insert(bytes.end(), {0xff, 0xda, 0, 8, 1, 1, 0, 0, 63, 0, 0x3f, 0xff, 0xd9});
  return bytes;
}

Bytes wav(int64_t samples = 32)
{
  Bytes bytes;
  text(bytes, "RIFF", 4);
  little(bytes, 36 + samples * 2, 4);
  text(bytes, "WAVEfmt ", 8);
  little(bytes, 16, 4);
  little(bytes, 1, 2);
  little(bytes, 1, 2);
  little(bytes, 8000, 4);
  little(bytes, 16000, 4);
  little(bytes, 2, 2);
  little(bytes, 16, 2);
  text(bytes, "data", 4);
  little(bytes, samples * 2, 4);
  bytes.insert(bytes.end(), samples * 2, 0);
  return bytes;
}

Bytes mp3()
{
  Bytes bytes(417, 0);
  bytes[0] = 0xff;
  bytes[1] = 0xfb;
  bytes[2] = 0x90;
  bytes[3] = 0xc0;
  const Bytes frame = bytes;
  bytes.insert(bytes.end(), frame.begin(), frame.end());
  return bytes;
}

std::string descriptor(const std::string &uri, const std::string &mime = "",
                       int64_t size = -1, const std::string &version = "")
{
  ObArenaAllocator allocator("RemoteMediaTest");
  ObString encoded;
  ObJsonObject object(&allocator);
  ObJsonInt format(FileRefCodec::FORMAT_VERSION);
  ObJsonString location(view(uri));
  ObJsonString media_type(view(mime));
  ObJsonInt length(size);
  ObJsonString revision(view(version));
  equal(object.object_add(ObString::make_string("format_version"), &format), OB_SUCCESS,
        "add FILE format version");
  equal(object.object_add(ObString::make_string("uri"), &location), OB_SUCCESS, "add FILE URI");
  if (!mime.empty()) {
    equal(object.object_add(ObString::make_string("mime_type"), &media_type), OB_SUCCESS,
          "add supplied MIME metadata");
  }
  if (size >= 0) {
    equal(object.object_add(ObString::make_string("size_bytes"), &length), OB_SUCCESS,
          "add supplied size metadata");
  }
  if (!version.empty()) {
    equal(object.object_add(ObString::make_string("object_version"), &revision), OB_SUCCESS,
          "add content version metadata");
  }
  equal(FileRefCodec::from_json(allocator, object, encoded), OB_SUCCESS, "encode canonical FILE");
  return copy(encoded);
}

std::string sha256(const Bytes &bytes)
{
  char digest[32];
  int64_t length = 0;
  equal(oceanbase::share::ObHashUtil::hash(oceanbase::share::OB_HASH_SH256,
      reinterpret_cast<const char *>(bytes.data()), bytes.size(), digest, sizeof(digest), length),
      OB_SUCCESS, "hash synthetic content");
  equal(length, sizeof(digest), "SHA256 digest length");
  const char *hex = "0123456789abcdef";
  std::string version = "sha256:";
  for (const char byte : digest) {
    version += hex[static_cast<uint8_t>(byte) >> 4];
    version += hex[static_cast<uint8_t>(byte) & 15];
  }
  return version;
}

void drained(Runtime &runtime, std::vector<Task> &tasks, const Budget &budget)
{
  wait_until([&] { return runtime->get_active_count() == 0 && !runtime->has_pending(); },
             "query media activity drains");
  tasks.clear();
  runtime.reset();
  wait_until([&] { return budget->memory_used() == 0; }, "release every query media reservation");
  equal(budget->active_tasks(), 0, "media does not consume provider task credits");
}

void test_exact_get_query_and_formats()
{
  Storage storage;
  const Bytes formats[]{png(), jpeg(), wav(), mp3()};
  const char *mimes[]{"image/png", "image/jpeg", "audio/wav", "audio/mpeg"};
  const char *audio[]{"", "", "wav", "mp3"};
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, "", storage.root());
  std::vector<Task> tasks;
  for (int i = 0; i < 4; ++i) {
    const std::string target = "/media/a%20b-" + std::to_string(i)
        + ".bin?signature=fixture%2B%2f&repeat=one+two&repeat=%252F&blank=";
    Response response{formats[i]};
    response.headers_ = "Content-Type: application/octet-stream\r\nETag: \"not-a-content-hash\"\r\n";
    storage.add(target, response);
    const std::string uri = storage.url(target);
    const std::string encoded = descriptor(uri, mimes[i], formats[i].size(), sha256(formats[i]));
    FileRef ref;
    equal(FileRefCodec::decode(view(encoded), ref), OB_SUCCESS, "decode signed FILE descriptor");
    require(copy(ref.uri_) == uri, "descriptor preserves signed URI byte for byte");
    tasks.push_back(demand(runtime, encoded));
    expect_bytes(tasks.back(), formats[i], mimes[i], audio[i]);
    equal(storage.count(target), 1, "one complete GET, no HEAD or retry");
  }
  equal(runtime->get_loaded_count(), 4, "all four remote formats are validated and encoded");
  storage.healthy();
  drained(runtime, tasks, budget);
  const Bytes large_audio = wav(200000);
  storage.add("/media/raw-release", Response{large_audio});
  const Budget buffer_budget = make_budget();
  Gate observation(buffer_budget);
  observation.release();
  runtime = acquire(buffer_budget, "", storage.root());
  equal(runtime->set_test_hook_for_testing(&Gate::hook, &observation), OB_SUCCESS,
        "observe remote raw and base64 reservations during encoding");
  tasks.push_back(demand(runtime, descriptor(storage.url("/media/raw-release"))));
  expect_bytes(tasks.back(), large_audio, "audio/wav", "wav");
  wait_until([&] { return runtime->get_active_count() == 0; }, "remote preparation completes");
  require(observation.peak() - buffer_budget->memory_used() >= static_cast<int64_t>(large_audio.size()),
          "remote completion releases raw buffers while the cached base64 task stays alive");
  drained(runtime, tasks, buffer_budget);
  storage.healthy();
}

void test_coalescing_full_url_and_metadata()
{
  Storage storage;
  const Bytes bytes = png();
  auto gate = std::make_shared<Gate>();
  Response response{bytes};
  response.gate_ = gate;
  const std::string first = "/media/shared?sig=one%2Btwo";
  const std::string other = "/media/shared?sig=one+two";
  storage.add(first, response);
  storage.add(other, response);
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, "", storage.root());
  ReleaseGate release(*gate);
  Runtime same = acquire(budget, "", storage.root());
  require(same == runtime, "the same query shares remote policy, cache and parallel limit");
  Runtime rejected;
  equal(SemanticMediaRuntime::acquire(budget, ObString(), view(storage.url("/different/")),
      1024 * 1024, 4, INT64_MAX, rejected), OB_ERR_UNEXPECTED,
      "reacquisition cannot split a query's remote root policy");
  const std::string version = sha256(bytes);
  const std::string encoded = descriptor(storage.url(first), "image/png", bytes.size(), version);
  std::vector<Task> tasks{demand(runtime, encoded)};
  tasks.push_back(demand(runtime, descriptor(storage.url(first), "IMAGE/PNG", bytes.size(), version)));
  require(tasks[0] == tasks[1], "duplicate pending demands coalesce normalized metadata");
  denied(runtime, descriptor(storage.url(first), "image/png", bytes.size() + 1, version),
         OB_INVALID_ARGUMENT);
  denied(runtime, descriptor(storage.url(first), "", -1, version), OB_INVALID_ARGUMENT);
  tasks.push_back(demand(runtime, descriptor(storage.url(other), "image/png", bytes.size(), version)));
  require(tasks[0] != tasks[2], "distinct signed query bytes are distinct cache keys");
  wait_until([&] { return storage.count() == 2; }, "two distinct signed URLs reach storage");
  equal(runtime->get_started_count(), 2, "duplicate demands do not consume fetch/prepare credits");
  gate->release();
  for (const auto &task : tasks) {
    expect_bytes(task, bytes, "image/png");
  }
  require(demand(runtime, encoded) == tasks[0], "completed results remain coalesced");
  equal(storage.count(), 2, "cache hits do not revalidate with HEAD or a second GET");
  std::string wrong_version = version;
  wrong_version.back() = wrong_version.back() == '0' ? '1' : '0';
  tasks.push_back(demand(runtime, descriptor(
      storage.url(first), "image/png", bytes.size(), wrong_version)));
  require(tasks.back() != tasks[0], "a different content version is a different full-URL cache key");
  failed(tasks.back(), OB_CHECKSUM_ERROR);
  equal(storage.count(first), 2, "a new version is verified with its own complete GET");
  require(demand(runtime, encoded) == tasks[0], "a version mismatch does not replace verified cached bytes");
  equal(runtime->get_started_count(), 3, "full URL and content version both participate in coalescing");
  same.reset();
  storage.healthy();
  drained(runtime, tasks, budget);
}

void test_default_prefix_and_local_policy_denial()
{
  Storage storage;
  const Bytes bytes = png();
  LocalFile local(bytes);
  const Budget budget = make_budget();
  Runtime runtime;
  equal(SemanticMediaRuntime::acquire(budget, ObString(), ObString(), 1024, 4,
      INT64_MAX, runtime), OB_OP_NOT_ALLOW, "both empty policies deny by default");
  equal(budget->memory_used(), 0, "default denial allocates no query buffers");
  equal(SemanticMediaRuntime::acquire(budget, view(local.root()), 1024, 4, INT64_MAX, runtime),
        OB_SUCCESS, "the old local-only overload is preserved");
  denied(runtime, descriptor(storage.url("/media/image?sig=private")), OB_OP_NOT_ALLOW);
  runtime.reset();
  runtime = acquire(budget, "", storage.root());
  for (const std::string &target : {"/media-sibling/image", "/outside/image", "/Media/image"}) {
    denied(runtime, descriptor(storage.url(target)));
  }
  Storage other_origin;
  denied(runtime, descriptor(other_origin.url("/media/image")));
  denied(runtime, descriptor("file://" + local.path()), OB_OP_NOT_ALLOW);
  equal(runtime->get_started_count(), 0, "policy denial does not enqueue physical fetch/prepare work");
  equal(runtime->get_active_count(), 0, "policy denial does not consume shared media admission");
  equal(storage.count(), 0, "prefix/default/local denial happens before storage I/O");
  equal(other_origin.count(), 0, "origin/port mismatch happens before storage I/O");
  runtime.reset();
  equal(budget->memory_used(), 0, "denied policies leave zero accounting");
  storage.healthy();
  other_origin.healthy();
}

class Environment
{
public:
  Environment(const char *name, const std::string &value) : name_(name)
  {
    const char *old = std::getenv(name);
    present_ = old != nullptr;
    if (present_) {
      old_ = old;
    }
    equal(::setenv(name, value.c_str(), 1), 0, "configure an owned process proxy sentinel");
  }
  ~Environment()
  {
    const int result = present_ ? ::setenv(name_.c_str(), old_.c_str(), 1) : ::unsetenv(name_.c_str());
    if (result != 0) {
      cleanup_errors.fetch_add(1);
      std::cerr << "fixture environment restore failed" << std::endl;
    }
  }
private:
  std::string name_;
  std::string old_;
  bool present_ = false;
};

void verify_tls_fixture(Storage &storage, const Bytes &expected)
{
  std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> client(curl_easy_init(), &curl_easy_cleanup);
  require(static_cast<bool>(client), "allocate owned TLS fixture probe");
  const std::string uri = storage.url("/media/fixture-probe");
  Bytes received;
  received.reserve(1024);
  const auto callback = +[](char *data, size_t size, size_t count, void *context) noexcept -> size_t {
    auto &bytes = *static_cast<Bytes *>(context);
    if (size != 1 || count > 1024 - bytes.size()) {
      return 0;
    }
    bytes.insert(bytes.end(), data, data + count);
    return count;
  };
  equal(curl_easy_setopt(client.get(), CURLOPT_URL, uri.c_str()), CURLE_OK, "set fixture probe URI");
  equal(curl_easy_setopt(client.get(), CURLOPT_PROXY, ""), CURLE_OK, "fixture probe bypasses proxies");
  equal(curl_easy_setopt(client.get(), CURLOPT_SSL_VERIFYPEER, 0L), CURLE_OK,
        "only the fixture self-check trusts its ephemeral self-signed certificate");
  equal(curl_easy_setopt(client.get(), CURLOPT_SSL_VERIFYHOST, 2L), CURLE_OK,
        "the ephemeral fixture certificate actually matches the loopback host");
  equal(curl_easy_setopt(client.get(), CURLOPT_TIMEOUT_MS, 3000L), CURLE_OK, "bound fixture TLS probe");
  equal(curl_easy_setopt(client.get(), CURLOPT_NOSIGNAL, 1L), CURLE_OK, "fixture probe uses no signals");
  equal(curl_easy_setopt(client.get(), CURLOPT_WRITEFUNCTION, callback), CURLE_OK,
        "collect bounded TLS fixture bytes");
  equal(curl_easy_setopt(client.get(), CURLOPT_WRITEDATA, &received), CURLE_OK, "set fixture probe output");
  equal(curl_easy_perform(client.get()), CURLE_OK, "TLS fixture can serve a complete valid GET");
  require(received == expected, "TLS fixture self-check receives the exact generated media");
}

void test_no_redirects_proxy_or_untrusted_tls()
{
  Storage storage;
  Storage proxy;
  Environment http_proxy("http_proxy", proxy.url(""));
  Environment upper_proxy("HTTP_PROXY", proxy.url(""));
  Environment https_proxy("https_proxy", proxy.url(""));
  Environment upper_https_proxy("HTTPS_PROXY", proxy.url(""));
  Environment all_proxy("ALL_PROXY", proxy.url(""));
  Environment lower_all_proxy("all_proxy", proxy.url(""));
  Environment no_proxy("no_proxy", "");
  Environment upper_no_proxy("NO_PROXY", "");
  const Bytes bytes = png();
  storage.add("/media/direct", Response{bytes});
  storage.add("/media/redirect-target", Response{bytes});
  Response redirect;
  redirect.status_ = 302;
  redirect.headers_ = "Location: " + storage.url("/media/redirect-target") + "\r\n";
  storage.add("/media/redirect", redirect);
  redirect.headers_ = "Location: " + proxy.url("/media/redirected?sig=must-not-follow") + "\r\n";
  storage.add("/media/redirect-origin", redirect);
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, "", storage.root());
  std::vector<Task> tasks{demand(runtime, descriptor(storage.url("/media/direct")))};
  expect_bytes(tasks[0], bytes, "image/png");
  tasks.push_back(demand(runtime, descriptor(storage.url("/media/redirect"))));
  failed(tasks.back());
  tasks.push_back(demand(runtime, descriptor(storage.url("/media/redirect-origin"))));
  failed(tasks.back());
  equal(storage.count(), 3, "direct GET and both rejected redirects each execute once");
  equal(storage.count("/media/redirect-target"), 0, "even a permitted same-prefix redirect is not followed");
  equal(proxy.count(), 0, "neither environment proxies nor redirect targets are contacted");
  storage.healthy();
  proxy.healthy();
  drained(runtime, tasks, budget);
  Storage untrusted_tls(true);
  untrusted_tls.add("/media/fixture-probe", Response{bytes});
  untrusted_tls.add("/media/untrusted", Response{bytes});
  verify_tls_fixture(untrusted_tls, bytes);
  const Budget tls_budget = make_budget();
  runtime = acquire(tls_budget, "", untrusted_tls.root());
  tasks.push_back(demand(runtime, descriptor(untrusted_tls.url("/media/untrusted"))));
  failed(tasks.back());
  equal(untrusted_tls.count("/media/untrusted"), 0,
        "mandatory TLS verification rejects an untrusted matching certificate before GET");
  equal(proxy.count(), 0, "HTTPS certificate rejection cannot use an environment proxy");
  untrusted_tls.healthy();
  drained(runtime, tasks, tls_budget);
}

void test_framing_declared_size_and_byte_caps()
{
  Storage storage;
  const Bytes bytes = wav(20000);
  int index = 0;
  for (const Framing framing : {Framing::LENGTH, Framing::CHUNKED, Framing::CLOSE}) {
    Response response{bytes};
    response.framing_ = framing;
    const std::string target = "/media/framing-" + std::to_string(index++);
    storage.add(target, response);
    const Budget budget = make_budget();
    Runtime runtime = acquire(budget, "", storage.root(), bytes.size());
    std::vector<Task> tasks{demand(runtime, descriptor(storage.url(target), "audio/wav", bytes.size()))};
    expect_bytes(tasks[0], bytes, "audio/wav", "wav");
    drained(runtime, tasks, budget);

    const Budget small_budget = make_budget();
    runtime = acquire(small_budget, "", storage.root(), bytes.size() - 1);
    denied(runtime, descriptor(storage.url(target), "audio/wav", bytes.size()), OB_SIZE_OVERFLOW);
    const int64_t before = storage.count(target);
    tasks.push_back(demand(runtime, descriptor(storage.url(target))));
    failed(tasks.back(), OB_SIZE_OVERFLOW);
    equal(storage.count(target), before + 1, "unknown/declared HTTP size cannot bypass the byte cap");
    drained(runtime, tasks, small_budget);
  }
  Response oversized_header{bytes};
  oversized_header.content_length_ = bytes.size() + 1;
  storage.add("/media/large-header", oversized_header);
  Response truncated{bytes};
  truncated.content_length_ = bytes.size() + 4;
  storage.add("/media/truncated", truncated);
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, "", storage.root(), bytes.size());
  std::vector<Task> tasks{demand(runtime, descriptor(storage.url("/media/large-header")))};
  failed(tasks.back(), OB_SIZE_OVERFLOW);
  drained(runtime, tasks, budget);
  const Budget second_budget = make_budget();
  runtime = acquire(second_budget, "", storage.root(), bytes.size() + 16);
  tasks.push_back(demand(runtime, descriptor(storage.url("/media/truncated"))));
  failed(tasks.back());
  tasks.push_back(demand(runtime, descriptor(storage.url("/media/framing-0"),
                                          "audio/wav", bytes.size() + 1)));
  failed(tasks.back(), OB_INVALID_DATA);
  drained(runtime, tasks, second_budget);
  storage.healthy();
}

void test_error_statuses_without_retries()
{
  Storage storage;
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, "", storage.root());
  std::vector<Task> tasks;
  for (const int status : {400, 403, 404, 429, 500, 503}) {
    const std::string target = "/media/status-" + std::to_string(status) + "?sig=private-test";
    Response response{png()};
    response.status_ = status;
    response.headers_ = "Retry-After: 1\r\n";
    storage.add(target, response);
    tasks.push_back(demand(runtime, descriptor(storage.url(target))));
    failed(tasks.back());
    equal(storage.count(target), 1, "error statuses never retry a pre-signed GET");
  }
  equal(runtime->get_loaded_count(), 0, "HTTP failures never publish valid-looking media");
  storage.healthy();
  drained(runtime, tasks, budget);
}

void test_mime_hash_etag_and_container_validation()
{
  Storage storage;
  const Bytes bytes = png();
  std::string wrong = sha256(bytes);
  wrong.back() = wrong.back() == '0' ? '1' : '0';
  Response response{bytes};
  response.headers_ = "ETag: \"" + wrong + "\"\r\nContent-Type: text/plain\r\n";
  storage.add("/media/hash-valid", response);
  storage.add("/media/hash-wrong", response);
  storage.add("/media/mime-wrong", response);
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, "", storage.root());
  std::vector<Task> tasks{demand(runtime, descriptor(storage.url("/media/hash-valid"),
      "image/png", bytes.size(), sha256(bytes)))};
  expect_bytes(tasks[0], bytes, "image/png");
  tasks.push_back(demand(runtime, descriptor(storage.url("/media/hash-wrong"),
      "image/png", bytes.size(), wrong)));
  failed(tasks.back(), OB_CHECKSUM_ERROR);
  tasks.push_back(demand(runtime, descriptor(storage.url("/media/mime-wrong"), "image/jpeg")));
  failed(tasks.back(), OB_INVALID_DATA);
  denied(runtime, descriptor(storage.url("/media/not-fetched"), "", -1, "etag:opaque"),
         OB_NOT_SUPPORTED);
  denied(runtime, descriptor(storage.url("/media/not-fetched"), "application/octet-stream"),
         OB_NOT_SUPPORTED);
  const Bytes formats[]{png(), jpeg(), wav(), mp3()};
  for (int i = 0; i < 4; ++i) {
    const std::string target = "/media/corrupt-" + std::to_string(i);
    Response corrupt{formats[i]};
    corrupt.bytes_.resize(8);
    storage.add(target, corrupt);
    tasks.push_back(demand(runtime, descriptor(storage.url(target))));
    failed(tasks.back(), OB_INVALID_DATA);
  }
  equal(storage.count("/media/not-fetched"), 0, "unsupported metadata is denied before HTTP");
  equal(runtime->get_loaded_count(), 1, "only content-verified media is published");
  storage.healthy();
  drained(runtime, tasks, budget);
}

void test_four_held_downloads_do_not_block_local_workers()
{
  Storage storage;
  auto gate = std::make_shared<Gate>();
  const Bytes image = png();
  const Bytes sound = wav();
  LocalFile local(sound);
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, local.root(), storage.root(), 1024 * 1024, 5);
  ReleaseGate release(*gate);
  std::vector<Task> tasks;
  for (int i = 0; i < 4; ++i) {
    const std::string target = "/media/held-" + std::to_string(i);
    Response response{image};
    response.gate_ = gate;
    storage.add(target, response);
    tasks.push_back(demand(runtime, descriptor(storage.url(target))));
  }
  wait_until([&] { return storage.count() == 4; }, "all four GETs are held before body delivery");
  tasks.push_back(demand(runtime, descriptor("file://" + local.path())));
  expect_bytes(tasks.back(), sound, "audio/wav", "wav");
  for (int i = 0; i < 4; ++i) {
    require(!tasks[i]->ready(), "remote downloads remain held while local work finishes");
  }
  equal(runtime->get_loaded_count(), 1, "local FILE completes without borrowing a blocked HTTP worker");
  gate->release();
  for (int i = 0; i < 4; ++i) {
    expect_bytes(tasks[i], image, "image/png");
  }
  storage.healthy();
  drained(runtime, tasks, budget);
}

void test_shared_parallel_limit_and_encoding_overlap()
{
  Storage storage;
  auto download_gate = std::make_shared<Gate>();
  const Budget budget = make_budget();
  Gate encode_gate(budget);
  const Bytes sound = wav(100000);
  LocalFile local(sound);
  Response response{png()};
  response.gate_ = download_gate;
  storage.add("/media/held", response);
  storage.add("/media/later", Response{png()});
  Runtime runtime = acquire(budget, local.root(), storage.root(), 1024 * 1024, 2);
  ReleaseGate release_download(*download_gate);
  equal(runtime->set_test_hook_for_testing(&Gate::hook, &encode_gate), OB_SUCCESS,
        "hold CPU preparation separately from HTTP fetching");
  ReleaseGate release_encode(encode_gate);
  std::vector<Task> tasks{demand(runtime, descriptor(storage.url("/media/held")))};
  wait_until([&] { return storage.count() == 1; }, "remote fetch occupies one query media credit");
  tasks.push_back(demand(runtime, descriptor("file://" + local.path())));
  wait_until([&] { return encode_gate.arrived() == 1; }, "local preparation overlaps held remote fetch");
  equal(runtime->get_active_count(), 2, "fetch and preparation share the exact parallel limit");
  require(!runtime->can_demand(), "the third mixed media demand must wait for a shared credit");
  denied(runtime, descriptor(storage.url("/media/later")), OB_EAGAIN);
  equal(storage.count("/media/later"), 0, "parallel denial does not start another GET");
  download_gate->release();
  wait_until([&] { return encode_gate.arrived() == 2; },
             "remote preparation reaches a second CPU worker while local preparation is held");
  equal(runtime->get_active_count(), 2, "fetch completion does not release its preparation credit early");
  encode_gate.release();
  expect_bytes(tasks[0], response.bytes_, "image/png");
  expect_bytes(tasks[1], sound, "audio/wav", "wav");
  wait_until([&] { return runtime->get_active_count() == 0; }, "both preparations release their credits");
  require(encode_gate.peak() - budget->memory_used() >= static_cast<int64_t>(sound.size()),
          "completion releases raw media buffers while immutable cached base64 remains live");
  wait_until([&] { return runtime->can_demand(); }, "a completed preparation releases admission");
  tasks.push_back(demand(runtime, descriptor(storage.url("/media/later"))));
  expect_bytes(tasks.back(), png(), "image/png");
  equal(runtime->get_started_count(), 3, "the retried admission starts exactly once");
  storage.healthy();
  drained(runtime, tasks, budget);
}

void test_cancel_partial_download_socket_and_accounting()
{
  Storage storage;
  auto gate = std::make_shared<Gate>();
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, "", storage.root());
  ReleaseGate release(*gate);
  const int64_t baseline = budget->memory_used();
  std::vector<Task> tasks;
  for (int i = 0; i < 4; ++i) {
    const std::string target = "/media/cancel-" + std::to_string(i);
    Response response{wav(200000)};
    response.framing_ = Framing::CHUNKED;
    response.gate_ = gate;
    response.before_gate_ = 65536;
    storage.add(target, response);
    tasks.push_back(demand(runtime, descriptor(storage.url(target))));
  }
  wait_until([&] { return storage.count() == 4 && budget->memory_used() > baseline + 131072; },
             "partial remote raw buffers are charged to the shared query budget");
  const int64_t peak = budget->memory_used();
  equal(runtime->cancel(), OB_SUCCESS, "cancel a query with four partial GETs");
  for (const Task &task : tasks) {
    failed(task, OB_CANCELED);
  }
  wait_until([&] { return storage.disconnected() == 4; },
             "cancellation closes every storage socket before the fixture releases bodies");
  require(budget->memory_used() < peak - 65536, "cancel releases raw download buffers immediately");
  equal(runtime->get_loaded_count(), 0, "cancelled partial containers are never published");
  storage.healthy();
  drained(runtime, tasks, budget);
}

void test_deadline_and_query_failure_cleanup()
{
  Storage storage;
  {
    const Budget budget = make_budget();
    Runtime rejected;
    equal(SemanticMediaRuntime::acquire(budget, ObString(), view(storage.root()), 1024, 4,
        ObTimeUtility::current_time() - 1, rejected), OB_TIMEOUT,
        "expired acquisition does not initiate a GET");
    equal(budget->memory_used(), 0, "expired acquisition leaves zero accounting");
    equal(storage.count(), 0, "expired acquisition performs no storage I/O");
  }
  for (int i = 0; i < 2; ++i) {
    auto gate = std::make_shared<Gate>();
    const std::string target = "/media/deadline-" + std::to_string(i);
    Response response{png()};
    response.gate_ = gate;
    storage.add(target, response);
    const Budget budget = make_budget();
    const int64_t deadline = i == 0 ? ObTimeUtility::current_time() + 1500000 : INT64_MAX;
    Runtime runtime = acquire(budget, "", storage.root(), 1024, 4, deadline);
    ReleaseGate release(*gate);
    std::vector<Task> tasks{demand(runtime, descriptor(storage.url(target)))};
    wait_until([&] { return storage.count(target) == 1; }, "held GET starts before cancellation/deadline");
    if (i == 1) {
      budget->fail(OB_ERR_UNEXPECTED);
    }
    const int error = i == 0 ? OB_TIMEOUT : OB_ERR_UNEXPECTED;
    failed(tasks[0], error);
    wait_until([&] { return storage.disconnected() == i + 1; },
               "deadline/query failure closes the held storage socket without server assistance");
    denied(runtime, descriptor(storage.url(target)), error);
    drained(runtime, tasks, budget);
  }
  storage.healthy();
}

void test_scheduler_shutdown_closes_remote_sockets()
{
  Storage storage;
  auto gate = std::make_shared<Gate>();
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, "", storage.root());
  ReleaseGate release(*gate);
  std::vector<Task> tasks;
  for (int i = 0; i < 2; ++i) {
    const std::string target = "/media/shutdown-" + std::to_string(i);
    Response response{png()};
    response.gate_ = gate;
    storage.add(target, response);
    tasks.push_back(demand(runtime, descriptor(storage.url(target))));
  }
  wait_until([&] { return storage.count() == 2; }, "shutdown sees held remote transfers");
  std::atomic<bool> stopped{false};
  std::thread shutdown([&] {
    SemanticMediaRuntime::stop_scheduler();
    stopped.store(true);
  });
  try {
    wait_until([&] { return stopped.load(); }, "scheduler shutdown is bounded without fixture assistance");
  } catch (...) {
    gate->release();
    shutdown.join();
    throw;
  }
  shutdown.join();
  for (const Task &task : tasks) {
    failed(task, OB_CANCELED);
  }
  wait_until([&] { return storage.disconnected() == 2; },
             "scheduler shutdown closes curl sockets before fixture gate release");
  Runtime rejected;
  equal(SemanticMediaRuntime::acquire(make_budget(), ObString(), view(storage.root()), 1024, 1,
      INT64_MAX, rejected), OB_CANCELED, "stopped remote scheduler does not silently restart");
  storage.healthy();
  drained(runtime, tasks, budget);
}

struct TestCase
{
  const char *name_;
  void (*run_)();
};

const TestCase TESTS[]{
  {"exact_get_query_and_formats", test_exact_get_query_and_formats},
  {"coalescing_full_url_and_metadata", test_coalescing_full_url_and_metadata},
  {"default_prefix_and_local_policy_denial", test_default_prefix_and_local_policy_denial},
  {"no_redirects_proxy_or_untrusted_tls", test_no_redirects_proxy_or_untrusted_tls},
  {"framing_declared_size_and_byte_caps", test_framing_declared_size_and_byte_caps},
  {"error_statuses_without_retries", test_error_statuses_without_retries},
  {"mime_hash_etag_and_container_validation", test_mime_hash_etag_and_container_validation},
  {"four_held_downloads_do_not_block_local_workers", test_four_held_downloads_do_not_block_local_workers},
  {"shared_parallel_limit_and_encoding_overlap", test_shared_parallel_limit_and_encoding_overlap},
  {"cancel_partial_download_socket_and_accounting", test_cancel_partial_download_socket_and_accounting},
  {"deadline_and_query_failure_cleanup", test_deadline_and_query_failure_cleanup},
  {"scheduler_shutdown_closes_remote_sockets", test_scheduler_shutdown_closes_remote_sockets},
};

void run_case(const TestCase &test, int64_t repeat, int64_t &completed)
{
  const int64_t accounting = ObAIFuncClient::pipeline_buffer_usage();
  const int64_t cleanup = cleanup_errors.load();
  test.run_();
  equal(ObAIFuncClient::pipeline_buffer_usage(), accounting, "case leaves shared accounting unchanged");
  equal(cleanup_errors.load(), cleanup, "case closes sockets and removes owned fixture paths");
  std::cout << "PASS " << test.name_ << " repeat=" << repeat << std::endl;
  ++completed;
}
}

int main(int argc, char **argv)
{
  try {
    require(::signal(SIGPIPE, SIG_IGN) != SIG_ERR, "disable SIGPIPE for owned TLS fixture sockets");
    int64_t repeats = 1;
    std::string selected;
    bool list = false;
    for (int i = 1; i < argc; ++i) {
      const std::string option = argv[i];
      if (option == "--list-cases") {
        list = true;
      } else if (option == "--case" || option == "--repeat") {
        require(++i < argc, "native test options require a value");
        if (option == "--case") {
          selected = argv[i];
          require(!selected.empty(), "--case must not be empty");
        } else {
          char *end = nullptr;
          repeats = std::strtoll(argv[i], &end, 10);
          require(end != argv[i] && *end == '\0' && repeats >= 1 && repeats <= 100,
                  "--repeat must be in [1, 100]");
        }
      } else {
        throw std::runtime_error("usage: remote_media_tests [--case NAME] [--repeat COUNT] [--list-cases]");
      }
    }
    require(selected.empty() || std::any_of(std::begin(TESTS), std::end(TESTS),
        [&](const TestCase &test) { return selected == test.name_; }), "unknown remote media native case");
    if (list) {
      for (const auto &test : TESTS) {
        std::cout << test.name_ << std::endl;
      }
      return 0;
    }
    int64_t completed = 0;
    const auto &shutdown = TESTS[sizeof(TESTS) / sizeof(TESTS[0]) - 1];
    require(selected != shutdown.name_ || repeats == 1, "scheduler shutdown is irreversible");
    for (int64_t repeat = 1; repeat <= repeats; ++repeat) {
      for (const auto &test : TESTS) {
        if (&test != &shutdown && (selected.empty() || selected == test.name_)) {
          run_case(test, repeat, completed);
        }
      }
    }
    if (selected.empty() || selected == shutdown.name_) {
      run_case(shutdown, 1, completed);
    }
    SemanticMediaRuntime::stop_scheduler();
    std::cout << "remote media native tests passed: " << completed << " cases" << std::endl;
    return 0;
  } catch (const std::exception &error) {
    SemanticMediaRuntime::stop_scheduler();
    std::cerr << "FAIL remote media native tests: " << error.what() << std::endl;
    return 1;
  }
}
