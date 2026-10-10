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
#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <signal.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <utility>
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

void require_equal(int64_t actual, int64_t expected, const std::string &message)
{
  if (actual != expected) {
    std::ostringstream out;
    out << message << ": expected " << expected << ", got " << actual;
    throw std::runtime_error(out.str());
  }
}

ObString string_view(const std::string &value)
{
  return ObString(static_cast<int64_t>(value.size()), value.data());
}

std::string string_copy(const ObString &value)
{
  return value.empty() ? std::string() : std::string(value.ptr(), value.length());
}

template <typename Predicate>
void wait_until(Predicate predicate, const std::string &message)
{
  const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(20);
  while (!predicate() && std::chrono::steady_clock::now() < end) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  require(predicate(), message);
}

void wait_ready(const Task &task)
{
  require(static_cast<bool>(task), "an admitted task has a handle");
  wait_until([&] { return task->ready(); }, "asynchronous media task completes");
}

void wait_cache_only_worker_released(const Task &task)
{
  wait_ready(task);
  wait_until([&] { return task.use_count() <= 2; }, "executor releases its finished handle");
}

uint64_t next_execution_id()
{
  static std::atomic<uint64_t> next{0x46494c450000};
  return next.fetch_add(1);
}

Budget make_budget(int64_t bytes = 16 * 1024 * 1024)
{
  return std::make_shared<SemanticQueryBudget>(next_execution_id(), bytes, 64);
}

Runtime acquire(const Budget &budget, const std::string &root, int64_t max_bytes = 1024 * 1024,
                int64_t parallel = 4, int64_t deadline = std::numeric_limits<int64_t>::max())
{
  Runtime runtime;
  require_equal(SemanticMediaRuntime::acquire(budget, string_view(root), max_bytes, parallel,
                                            deadline, runtime), OB_SUCCESS, "acquire media runtime");
  require(static_cast<bool>(runtime), "acquire returns a runtime");
  return runtime;
}

Task demand(const Runtime &runtime, const std::string &encoded)
{
  Task task;
  require_equal(runtime->demand(string_view(encoded), task), OB_SUCCESS, "admit media demand");
  return task;
}

class Fixture
{
public:
  Fixture()
  {
    char pattern[] = "/tmp/seekdb-semantic-media-XXXXXX";
    char *directory = ::mkdtemp(pattern);
    require(directory != nullptr, "create private media test directory");
    root_ = directory;
    directories_.push_back(root_);
  }

  ~Fixture()
  {
    for (const auto &directory : directories_) {
      ::chmod(directory.c_str(), 0700);
    }
    for (auto file = files_.rbegin(); file != files_.rend(); ++file) {
      if (::unlink(file->c_str()) != 0 && errno != ENOENT) {
        cleanup_errors.fetch_add(1);
        std::cerr << "test cleanup unlink failed: " << *file << ": " << std::strerror(errno) << std::endl;
      }
    }
    for (auto directory = directories_.rbegin(); directory != directories_.rend(); ++directory) {
      if (::rmdir(directory->c_str()) != 0) {
        cleanup_errors.fetch_add(1);
        std::cerr << "test cleanup rmdir failed: " << *directory << ": " << std::strerror(errno) << std::endl;
      }
    }
  }

  const std::string &root() const { return root_; }
  std::string path(const std::string &name) const { return root_ + "/" + name; }

  std::string directory(const std::string &path)
  {
    require_equal(::mkdir(path.c_str(), 0700), 0, "create owned fixture subdirectory");
    directories_.push_back(path);
    return path;
  }

  std::string file(const std::string &name, const Bytes &bytes)
  {
    return file_at(path(name), bytes);
  }

  std::string file_at(const std::string &path, const Bytes &bytes)
  {
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    require(fd >= 0, "create regular fixture file: " + path);
    files_.push_back(path);
    int64_t pos = 0;
    while (pos < static_cast<int64_t>(bytes.size())) {
      const ssize_t count = ::write(fd, bytes.data() + pos, bytes.size() - pos);
      if (count < 0 && errno == EINTR) {
        continue;
      }
      if (count <= 0) {
        const int error = errno;
        ::close(fd);
        throw std::runtime_error("write fixture file: " + std::string(std::strerror(error)));
      }
      pos += count;
    }
    require_equal(::close(fd), 0, "close fixture file");
    return path;
  }

  std::string symlink(const std::string &name, const std::string &target)
  {
    const std::string link = path(name);
    require_equal(::symlink(target.c_str(), link.c_str()), 0, "create owned test symlink");
    files_.push_back(link);
    return link;
  }

  std::string fifo(const std::string &name)
  {
    const std::string pipe = path(name);
    require_equal(::mkfifo(pipe.c_str(), 0600), 0, "create owned test FIFO");
    files_.push_back(pipe);
    return pipe;
  }

private:
  Fixture(const Fixture &) = delete;
  Fixture &operator=(const Fixture &) = delete;
  std::string root_;
  std::vector<std::string> files_;
  std::vector<std::string> directories_;
};

void append_big32(Bytes &bytes, uint32_t value)
{
  bytes.push_back(static_cast<uint8_t>(value >> 24));
  bytes.push_back(static_cast<uint8_t>(value >> 16));
  bytes.push_back(static_cast<uint8_t>(value >> 8));
  bytes.push_back(static_cast<uint8_t>(value));
}

void set_big32(Bytes &bytes, size_t pos, uint32_t value)
{
  for (int i = 0; i < 4; ++i) {
    bytes[pos + i] = static_cast<uint8_t>(value >> (24 - i * 8));
  }
}

void append_little16(Bytes &bytes, uint16_t value)
{
  bytes.push_back(static_cast<uint8_t>(value));
  bytes.push_back(static_cast<uint8_t>(value >> 8));
}

void append_little32(Bytes &bytes, uint32_t value)
{
  bytes.push_back(static_cast<uint8_t>(value));
  bytes.push_back(static_cast<uint8_t>(value >> 8));
  bytes.push_back(static_cast<uint8_t>(value >> 16));
  bytes.push_back(static_cast<uint8_t>(value >> 24));
}

void append_text(Bytes &bytes, const char *text, size_t size)
{
  bytes.insert(bytes.end(), text, text + size);
}

void png_chunk(Bytes &bytes, const char *type, const Bytes &payload)
{
  append_big32(bytes, static_cast<uint32_t>(payload.size()));
  const size_t begin = bytes.size();
  append_text(bytes, type, 4);
  bytes.insert(bytes.end(), payload.begin(), payload.end());
  append_big32(bytes, static_cast<uint32_t>(::crc32(0, bytes.data() + begin,
                                                 static_cast<uInt>(bytes.size() - begin))));
}

Bytes png()
{
  Bytes bytes = {137, 80, 78, 71, 13, 10, 26, 10};
  Bytes header;
  append_big32(header, 1);
  append_big32(header, 1);
  header.insert(header.end(), {8, 6, 0, 0, 0});
  png_chunk(bytes, "IHDR", header);
  const uint8_t pixel[] = {0, 0, 0, 0, 255};
  Bytes compressed(::compressBound(sizeof(pixel)));
  uLongf length = compressed.size();
  require_equal(::compress2(compressed.data(), &length, pixel, sizeof(pixel), Z_BEST_SPEED),
                Z_OK, "compress real one-pixel PNG scanline");
  compressed.resize(length);
  png_chunk(bytes, "IDAT", compressed);
  png_chunk(bytes, "IEND", {});
  return bytes;
}

Bytes jpeg()
{
  Bytes bytes = {0xff, 0xd8, 0xff, 0xdb, 0, 67, 0};
  bytes.insert(bytes.end(), 64, 1);
  bytes.insert(bytes.end(), {0xff, 0xc0, 0, 11, 8, 0, 1, 0, 1, 1, 1, 0x11, 0});
  bytes.insert(bytes.end(), {0xff, 0xc4, 0, 38});
  for (const uint8_t table : {uint8_t(0), uint8_t(0x10)}) {
    bytes.push_back(table);
    bytes.push_back(1);
    bytes.insert(bytes.end(), 15, 0);
    bytes.push_back(0);
  }
  bytes.insert(bytes.end(), {0xff, 0xda, 0, 8, 1, 1, 0, 0, 63, 0, 0x3f, 0xff, 0xd9});
  return bytes;
}

Bytes wav(int64_t sample_bytes = 4)
{
  require(sample_bytes > 0 && sample_bytes % 2 == 0, "PCM fixture has complete samples");
  Bytes bytes;
  append_text(bytes, "RIFF", 4);
  append_little32(bytes, static_cast<uint32_t>(36 + sample_bytes));
  append_text(bytes, "WAVEfmt ", 8);
  append_little32(bytes, 16);
  append_little16(bytes, 1);
  append_little16(bytes, 1);
  append_little32(bytes, 8000);
  append_little32(bytes, 16000);
  append_little16(bytes, 2);
  append_little16(bytes, 16);
  append_text(bytes, "data", 4);
  append_little32(bytes, static_cast<uint32_t>(sample_bytes));
  bytes.insert(bytes.end(), sample_bytes, 0);
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

std::string descriptor(const std::string &path, const std::string &mime = "",
                       int64_t size = -1, const std::string &version = "")
{
  ObArenaAllocator allocator("MediaRefTest");
  ObString encoded;
  require_equal(FileRefCodec::from_path(allocator, string_view(path), encoded),
                OB_SUCCESS, "encode fixture FILE path");
  if (!mime.empty() || size >= 0 || !version.empty()) {
    FileRef ref;
    require_equal(FileRefCodec::decode(encoded, ref), OB_SUCCESS, "decode canonical fixture URI");
    ObJsonObject object(&allocator);
    ObJsonInt format_version(FileRefCodec::FORMAT_VERSION);
    ObJsonString uri(ref.uri_);
    ObJsonString mime_type(string_view(mime));
    ObJsonInt size_bytes(size);
    ObJsonString object_version(string_view(version));
    require_equal(object.object_add(ObString::make_string("format_version"), &format_version),
                  OB_SUCCESS, "add FILE format version");
    require_equal(object.object_add(ObString::make_string("uri"), &uri), OB_SUCCESS, "add FILE URI");
    if (!mime.empty()) {
      require_equal(object.object_add(ObString::make_string("mime_type"), &mime_type),
                    OB_SUCCESS, "add FILE MIME metadata");
    }
    if (size >= 0) {
      require_equal(object.object_add(ObString::make_string("size_bytes"), &size_bytes),
                    OB_SUCCESS, "add FILE size metadata");
    }
    if (!version.empty()) {
      require_equal(object.object_add(ObString::make_string("object_version"), &object_version),
                    OB_SUCCESS, "add FILE version metadata");
    }
    require_equal(FileRefCodec::from_json(allocator, object, encoded),
                  OB_SUCCESS, "encode fixture FILE metadata");
  }
  return string_copy(encoded);
}

void expect_bytes(const Task &task, const Bytes &expected, const std::string &mime,
                  const std::string &audio = "")
{
  wait_ready(task);
  require_equal(task->result(), OB_SUCCESS, "verified media task succeeds");
  require_equal(task->base64().length(), ObBase64Encoder::needed_encoded_length(expected.size()),
                "exact unwrapped base64 output length");
  require(string_copy(task->mime_type()) == mime, "verified MIME is canonical");
  require(string_copy(task->audio_format()) == audio, "audio format is derived from actual bytes");
  require(task->is_audio() == !audio.empty(), "audio classification is correct");
  require(task->is_image() == audio.empty(), "image classification is correct");
  Bytes decoded(ObBase64Encoder::needed_decoded_length(task->base64().length()));
  int64_t pos = 0;
  require_equal(ObBase64Encoder::decode(task->base64().ptr(), task->base64().length(),
                                      decoded.data(), decoded.size(), pos),
                OB_SUCCESS, "decode immutable base64 result");
  require_equal(pos, expected.size(), "base64 preserves exact source length");
  require(std::equal(expected.begin(), expected.end(), decoded.begin()), "base64 preserves source bytes");
}

void expect_failure(const Runtime &runtime, const std::string &encoded, int expected)
{
  const Task task = demand(runtime, encoded);
  wait_ready(task);
  require_equal(task->result(), expected, "media task reports explicit failure");
  require(task->base64().empty() && task->mime_type().empty() && task->audio_format().empty(),
          "failed tasks do not publish media or success-shaped metadata");
  require(!task->is_audio() && !task->is_image(), "failed tasks have no media classification");
}

void expect_demand_failure(const Runtime &runtime, const std::string &encoded, int expected)
{
  Task task;
  require_equal(runtime->demand(string_view(encoded), task), expected, "demand fails without admission");
  require(!task, "failed admission has no task handle");
}

class Gate
{
public:
  explicit Gate(Stage stage = Stage::BEFORE_READ) : stage_(stage) {}
  ~Gate() { release(); }
  static void hook(void *context, Stage stage)
  {
    auto &gate = *static_cast<Gate *>(context);
    if (stage == gate.stage_) {
      std::unique_lock<std::mutex> lock(gate.mutex_);
      ++gate.arrived_;
      gate.changed_.notify_all();
      gate.changed_.wait(lock, [&] { return gate.released_; });
    }
  }
  void wait(int64_t count)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    require(changed_.wait_for(lock, std::chrono::seconds(20), [&] { return arrived_ >= count; }),
            "controlled media workers reach the test gate");
  }
  int64_t arrived()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return arrived_;
  }
  void release()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    released_ = true;
    changed_.notify_all();
  }
private:
  Stage stage_;
  std::mutex mutex_;
  std::condition_variable changed_;
  int64_t arrived_ = 0;
  bool released_ = false;
};

class ReleaseGate
{
public:
  explicit ReleaseGate(Gate &gate) : gate_(gate) {}
  ~ReleaseGate() { gate_.release(); }
private:
  Gate &gate_;
};

class JoiningThreads
{
public:
  explicit JoiningThreads(Gate &gate) : gate_(gate) {}
  ~JoiningThreads()
  {
    gate_.release();
    join();
  }
  template <typename Function>
  void start(Function function) { threads_.emplace_back(std::move(function)); }
  void join()
  {
    for (auto &thread : threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }
private:
  Gate &gate_;
  std::vector<std::thread> threads_;
};

class TaskCredit
{
public:
  explicit TaskCredit(Budget budget) : budget_(std::move(budget))
  {
    require_equal(budget_->acquire_task(), OB_SUCCESS, "reserve an independent HTTP task credit");
  }
  ~TaskCredit() { budget_->release_task(); }
private:
  Budget budget_;
};

void test_valid_formats()
{
  Fixture fixture;
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, fixture.root());
  const Bytes formats[] = {png(), jpeg(), wav(), mp3()};
  const char *mimes[] = {"image/png", "image/jpeg", "audio/wav", "audio/mpeg"};
  const char *audio[] = {"", "", "wav", "mp3"};
  for (int i = 0; i < 4; ++i) {
    const std::string file = fixture.file("misleading-" + std::to_string(i) + ".txt", formats[i]);
    expect_bytes(demand(runtime, descriptor(file)), formats[i], mimes[i], audio[i]);
  }
  require_equal(runtime->get_loaded_count(), 4, "all supported formats load");
  runtime.reset();
  require_equal(budget->memory_used(), 0, "successful cache teardown releases all bytes");
}

void test_canonical_escaped_paths()
{
  Fixture fixture;
  const Bytes bytes = png();
  const std::string directory = fixture.directory(fixture.path("space # % \xE4\xB8\xAD"));
  const std::string file = fixture.file_at(directory + "/image ? %.png", bytes);
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, directory);
  expect_bytes(demand(runtime, descriptor(file)), bytes, "image/png");
  runtime.reset();
  require_equal(budget->memory_used(), 0, "escaped native path cleanup");
}

void test_structural_corruption()
{
  Fixture fixture;
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, fixture.root(), 1024 * 1024, 1);
  struct Invalid
  {
    std::string name_;
    Bytes bytes_;
    int error_;
  };
  std::vector<Invalid> invalid;
  invalid.push_back({"empty", {}, OB_INVALID_DATA});
  invalid.push_back({"text", {'n', 'o', 't', 'm', 'e', 'd', 'i', 'a'}, OB_INVALID_DATA});
  invalid.push_back({"png-magic", {137, 80, 78, 71, 13, 10, 26, 10}, OB_INVALID_DATA});
  Bytes image = png();
  image.pop_back();
  invalid.push_back({"png-truncated", image, OB_INVALID_DATA});
  image = png();
  image.back() ^= 1;
  invalid.push_back({"png-crc", image, OB_CHECKSUM_ERROR});
  image = png();
  set_big32(image, 16, 0);
  set_big32(image, 29, static_cast<uint32_t>(::crc32(0, image.data() + 12, 17)));
  invalid.push_back({"png-zero-width", image, OB_INVALID_DATA});
  image = png();
  set_big32(image, 16, 32769);
  set_big32(image, 29, static_cast<uint32_t>(::crc32(0, image.data() + 12, 17)));
  invalid.push_back({"png-dimension-limit", image, OB_SIZE_OVERFLOW});
  image = png();
  image[24] = 3;
  set_big32(image, 29, static_cast<uint32_t>(::crc32(0, image.data() + 12, 17)));
  invalid.push_back({"png-depth", image, OB_INVALID_DATA});
  invalid.push_back({"jpeg-magic", {0xff, 0xd8, 0xff, 0xd9}, OB_INVALID_DATA});
  image = jpeg();
  image.pop_back();
  invalid.push_back({"jpeg-truncated", image, OB_INVALID_DATA});
  image = jpeg();
  image[76] = 0;
  image[77] = 0;
  invalid.push_back({"jpeg-zero-height", image, OB_INVALID_DATA});
  Bytes sound = wav();
  sound.pop_back();
  invalid.push_back({"wav-truncated", sound, OB_INVALID_DATA});
  sound = wav();
  sound[32] = 1;
  invalid.push_back({"wav-block-alignment", sound, OB_INVALID_DATA});
  sound = wav();
  sound[40] = 10;
  invalid.push_back({"wav-chunk-truncation", sound, OB_INVALID_DATA});
  invalid.push_back({"wav-magic", {'R', 'I', 'F', 'F', 4, 0, 0, 0, 'W', 'A', 'V', 'E'}, OB_INVALID_DATA});
  sound = mp3();
  sound.pop_back();
  invalid.push_back({"mp3-truncated", sound, OB_INVALID_DATA});
  sound = mp3();
  sound[2] = 0;
  invalid.push_back({"mp3-free-bitrate", sound, OB_INVALID_DATA});
  invalid.push_back({"mp3-magic", {0xff, 0xfb, 0x90, 0xc0}, OB_INVALID_DATA});
  invalid.push_back({"mp3-tag-only", {'I', 'D', '3', 4, 0, 0, 0, 0, 0, 0}, OB_INVALID_DATA});
  for (const auto &test : invalid) {
    expect_failure(runtime, descriptor(fixture.file(test.name_, test.bytes_)), test.error_);
  }
  require_equal(runtime->get_loaded_count(), 0, "corrupt formats never count as loaded");
  runtime.reset();
  require_equal(budget->memory_used(), 0, "corrupt result cache releases charges");
}

std::string sha256_version(const Bytes &bytes)
{
  char digest[32];
  int64_t size = 0;
  require_equal(oceanbase::share::ObHashUtil::hash(oceanbase::share::OB_HASH_SH256,
      reinterpret_cast<const char *>(bytes.data()), bytes.size(), digest, sizeof(digest), size),
      OB_SUCCESS, "hash known fixture content");
  require_equal(size, 32, "SHA256 digest width");
  static const char HEX[] = "0123456789abcdef";
  std::string version = "sha256:";
  for (const char byte : digest) {
    version += HEX[static_cast<uint8_t>(byte) >> 4];
    version += HEX[static_cast<uint8_t>(byte) & 0xf];
  }
  return version;
}

void test_metadata_and_versions()
{
  Fixture fixture;
  const Bytes bytes = png();
  const std::string file = fixture.file("image", bytes);
  const std::string version = sha256_version(bytes);
  {
    const Budget budget = make_budget();
    const Runtime runtime = acquire(budget, fixture.root());
    expect_bytes(demand(runtime, descriptor(file, "IMAGE/PNG", bytes.size(), version)),
                 bytes, "image/png");
    const Task same = demand(runtime, descriptor(file, "image/png", bytes.size(), version));
    expect_bytes(same, bytes, "image/png");
    require_equal(runtime->get_loaded_count(), 1, "normalized metadata coalesces the same content");
  }
  {
    const Budget budget = make_budget();
    const Runtime runtime = acquire(budget, fixture.root());
    expect_failure(runtime, descriptor(file, "image/jpeg", bytes.size()), OB_INVALID_DATA);
  }
  {
    const Budget budget = make_budget();
    const Runtime runtime = acquire(budget, fixture.root());
    expect_failure(runtime, descriptor(file, "image/png", bytes.size() + 1), OB_INVALID_DATA);
  }
  {
    const Budget budget = make_budget();
    const Runtime runtime = acquire(budget, fixture.root());
    std::string wrong = version;
    wrong.back() = wrong.back() == '0' ? '1' : '0';
    expect_failure(runtime, descriptor(file, "image/png", bytes.size(), wrong), OB_CHECKSUM_ERROR);
    expect_demand_failure(runtime, descriptor(file, "", -1, "etag:unverifiable"), OB_NOT_SUPPORTED);
    expect_demand_failure(runtime, descriptor(file, "", -1, "sha256:bad"), OB_INVALID_ARGUMENT);
    std::string invalid_hex = version;
    invalid_hex.back() = 'g';
    expect_demand_failure(runtime, descriptor(file, "", -1, invalid_hex), OB_INVALID_ARGUMENT);
    expect_demand_failure(runtime, descriptor(file, "application/octet-stream"), OB_NOT_SUPPORTED);
  }
  {
    const Bytes audio = wav();
    const std::string sound = fixture.file("audio", audio);
    const Budget budget = make_budget();
    const Runtime runtime = acquire(budget, fixture.root());
    expect_bytes(demand(runtime, descriptor(sound, "audio/x-wav")), audio, "audio/wav", "wav");
  }
}

void test_missing_oversize_and_descriptors()
{
  Fixture fixture;
  const Bytes bytes = png();
  const std::string file = fixture.file("image", bytes);
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, fixture.root(), bytes.size() - 1);
  expect_failure(runtime, descriptor(file), OB_SIZE_OVERFLOW);
  expect_demand_failure(runtime, descriptor(file, "", bytes.size()), OB_SIZE_OVERFLOW);
  expect_failure(runtime, descriptor(fixture.path("missing")), OB_FILE_NOT_EXIST);
  expect_demand_failure(runtime, "file://" + file, OB_INVALID_ARGUMENT);
  expect_demand_failure(runtime, std::string(16 * 1024 + 1, 'x'), OB_SIZE_OVERFLOW);
  std::string truncated = descriptor(file);
  truncated.pop_back();
  expect_demand_failure(runtime, truncated, OB_INVALID_ARGUMENT);
  std::string bad_magic = descriptor(file);
  bad_magic[0] ^= 1;
  expect_demand_failure(runtime, bad_magic, OB_INVALID_ARGUMENT);
  runtime.reset();
  require_equal(budget->memory_used(), 0, "failed admission and oversized reads release all charges");
  const Budget large_policy = make_budget();
  runtime = acquire(large_policy, fixture.root(), INT64_MAX);
  expect_bytes(demand(runtime, descriptor(file)), bytes, "image/png");
  expect_demand_failure(runtime, descriptor(file, "", (INT32_MAX / 4) * 3LL + 1), OB_SIZE_OVERFLOW);
  runtime.reset();
  require_equal(large_policy->memory_used(), 0, "native base64 length limit is explicit, not a silent clamp");
}

void test_root_policy_and_traversal()
{
  Fixture fixture;
  const Bytes bytes = png();
  const std::string root_file = fixture.file("image", bytes);
  const std::string sibling = fixture.directory(fixture.root() + "-sibling");
  const std::string outside = fixture.file_at(sibling + "/outside", bytes);
  const Budget budget = make_budget();
  Runtime runtime;
  require_equal(SemanticMediaRuntime::acquire(budget, ObString(), 1024, 1, INT64_MAX, runtime),
                OB_OP_NOT_ALLOW, "default empty root denies rather than reading arbitrary files");
  require(!runtime, "denied policy has no runtime");
  require_equal(SemanticMediaRuntime::acquire(budget, ObString::make_string("relative"),
      1024, 1, INT64_MAX, runtime), OB_INVALID_ARGUMENT, "relative root is invalid");
  require_equal(SemanticMediaRuntime::acquire(budget, string_view(fixture.path("missing-root")),
      1024, 1, INT64_MAX, runtime), OB_FILE_NOT_EXIST, "missing root is reported");
  runtime = acquire(budget, fixture.root());
  expect_demand_failure(runtime, descriptor(outside), OB_OP_NOT_ALLOW);
  expect_demand_failure(runtime, descriptor(fixture.root() + "/../"
      + sibling.substr(sibling.find_last_of('/') + 1) + "/outside"), OB_OP_NOT_ALLOW);
  expect_bytes(demand(runtime, descriptor(root_file)), bytes, "image/png");
  const std::string directory = fixture.directory(fixture.path("aa"));
  const std::string nested = fixture.file_at(directory + "/nested", bytes);
  std::string altered = descriptor(nested);
  FileRef ref;
  require_equal(FileRefCodec::decode(string_view(altered), ref), OB_SUCCESS, "decode traversal fixture");
  const size_t offset = static_cast<size_t>(ref.uri_.ptr() - altered.data());
  const std::string uri = string_copy(ref.uri_);
  const size_t segment = uri.find("/aa/");
  require(segment != std::string::npos, "locate owned test path component");
  altered[offset + segment + 1] = '.';
  altered[offset + segment + 2] = '.';
  expect_demand_failure(runtime, altered, OB_INVALID_ARGUMENT);
  runtime.reset();
  require_equal(budget->memory_used(), 0, "root policy cleanup");
}

void test_symlinks_and_special_files()
{
  Fixture fixture;
  const Bytes bytes = png();
  const std::string target = fixture.file("target", bytes);
  const std::string directory = fixture.directory(fixture.path("directory"));
  fixture.file_at(directory + "/nested", bytes);
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, fixture.root());
  expect_failure(runtime, descriptor(fixture.symlink("same-root-link", target)), OB_OP_NOT_ALLOW);
  expect_failure(runtime, descriptor(fixture.symlink("device-link", "/dev/null")), OB_OP_NOT_ALLOW);
  const std::string link = fixture.symlink("directory-link", directory);
  expect_failure(runtime, descriptor(link + "/nested"), OB_OP_NOT_ALLOW);
  expect_failure(runtime, descriptor(fixture.fifo("pipe")), OB_OP_NOT_ALLOW);
  expect_failure(runtime, descriptor(directory), OB_OP_NOT_ALLOW);
  Runtime denied;
  require_equal(SemanticMediaRuntime::acquire(make_budget(), string_view(link), 1024, 1,
      INT64_MAX, denied), OB_OP_NOT_ALLOW, "symlink root is never followed");
  require(!denied, "symlink root does not produce a runtime");
  runtime.reset();
  require_equal(budget->memory_used(), 0, "symlink and special-file cleanup");
}

int count_open_fds()
{
  DIR *directory = ::opendir("/proc/self/fd");
  require(directory != nullptr, "inspect process-owned descriptors");
  int count = 0;
  errno = 0;
  while (const struct dirent *entry = ::readdir(directory)) {
    if (entry->d_name[0] != '.') {
      ++count;
    }
    errno = 0;
  }
  const int scan_error = errno;
  require_equal(::closedir(directory), 0, "close descriptor inspection directory");
  require_equal(scan_error, 0, "enumerate every process-owned descriptor");
  return count;
}

struct ReadFdFlags
{
  dev_t device_;
  ino_t inode_;
  std::atomic<int> found_{0};
  std::atomic<int> error_{0};

  static void hook(void *context, Stage stage)
  {
    if (stage == Stage::BEFORE_READ) {
      auto &check = *static_cast<ReadFdFlags *>(context);
      DIR *directory = ::opendir("/proc/self/fd");
      if (directory == nullptr) {
        check.error_.store(errno);
      } else {
        errno = 0;
        while (const struct dirent *entry = ::readdir(directory)) {
          char *end = nullptr;
          const long candidate = std::strtol(entry->d_name, &end, 10);
          struct stat file = {};
          if (end != entry->d_name && *end == '\0' && candidate >= 0 && candidate <= INT_MAX
              && ::fstat(static_cast<int>(candidate), &file) == 0
              && file.st_dev == check.device_ && file.st_ino == check.inode_) {
            const int flags = ::fcntl(static_cast<int>(candidate), F_GETFL);
            const int descriptor_flags = ::fcntl(static_cast<int>(candidate), F_GETFD);
            if (flags < 0 || descriptor_flags < 0) {
              check.error_.store(errno);
            } else if ((flags & O_ACCMODE) != O_RDONLY || (flags & O_NONBLOCK) == 0
                || (flags & O_PATH) != 0 || (descriptor_flags & FD_CLOEXEC) == 0) {
              check.error_.store(EINVAL);
            }
            check.found_.fetch_add(1);
          }
          errno = 0;
        }
        if (errno != 0) {
          check.error_.store(errno);
        }
        if (::closedir(directory) != 0) {
          check.error_.store(errno);
        }
      }
    }
  }
};

void run_nonregular_probe(const std::string &root)
{
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, root, 4096, 1);
  struct stat source = {};
  require_equal(::stat((root + "/regular").c_str(), &source), 0, "inspect owned regular fixture");
  ReadFdFlags flags{source.st_dev, source.st_ino};
  require_equal(runtime->set_test_hook_for_testing(&ReadFdFlags::hook, &flags),
                OB_SUCCESS, "inspect verified read-open descriptor flags");
  expect_bytes(demand(runtime, descriptor(root + "/regular")), png(), "image/png");
  require_equal(flags.error_.load(), 0, "read descriptor is read-only, nonblocking and close-on-exec");
  require_equal(flags.found_.load(), 1, "only the verified read descriptor remains before I/O");
  require_equal(runtime->set_test_hook_for_testing(nullptr, nullptr), OB_SUCCESS, "clear descriptor inspection hook");
  const int before = count_open_fds();
  for (int index = 0; index < 8; ++index) {
    expect_failure(runtime, descriptor(root + "/pipe-" + std::to_string(index)), OB_OP_NOT_ALLOW);
  }
  expect_failure(runtime, descriptor(root + "/directory"), OB_OP_NOT_ALLOW);
  expect_failure(runtime, descriptor(root + "/device-link"), OB_OP_NOT_ALLOW);
  {
    const Budget device_budget = make_budget();
    const Runtime devices = acquire(device_budget, "/dev", 4096, 1);
    expect_failure(devices, descriptor("/dev/null"), OB_OP_NOT_ALLOW);
  }
  require_equal(count_open_fds(), before, "nonregular rejection closes pinned and traversal descriptors");
  runtime.reset();
  require_equal(budget->memory_used(), 0, "nonregular rejection releases its reservations");
  require_equal(count_open_fds(), before - 1, "teardown closes the configured root descriptor");
  SemanticMediaRuntime::stop_scheduler();
}

class ProbeChild
{
public:
  explicit ProbeChild(pid_t pid) : pid_(pid) {}
  ~ProbeChild()
  {
    if (pid_ > 0) {
      if (::kill(pid_, SIGKILL) != 0 && errno != ESRCH) {
        cleanup_errors.fetch_add(1);
        std::cerr << "terminate owned probe PID failed: " << pid_ << std::endl;
      }
      while (::waitpid(pid_, nullptr, 0) < 0 && errno == EINTR) {
      }
    }
  }
  bool wait(int &status)
  {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    do {
      const pid_t completed = ::waitpid(pid_, &status, WNOHANG);
      if (completed == pid_) {
        pid_ = -1;
        return true;
      } else if (completed < 0 && errno != EINTR) {
        if (errno == ECHILD) {
          pid_ = -1;
        }
        require(false, "wait for owned nonregular-file probe failed");
      }
      ::usleep(1000);
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
  }
private:
  ProbeChild(const ProbeChild &) = delete;
  ProbeChild &operator=(const ProbeChild &) = delete;
  pid_t pid_;
};

void test_nonregular_bounded_completion_and_shutdown()
{
  Fixture fixture;
  fixture.file("regular", png());
  for (int index = 0; index < 8; ++index) {
    fixture.fifo("pipe-" + std::to_string(index));
  }
  fixture.directory(fixture.path("directory"));
  fixture.symlink("device-link", "/dev/null");
  const char *root = fixture.root().c_str();
  const pid_t pid = ::fork();
  require(pid >= 0, "fork isolated nonregular-file regression probe");
  if (pid == 0) {
    ::execl("/proc/self/exe", "semantic_media_tests", "--nonregular-probe", root,
            static_cast<char *>(nullptr));
    ::_exit(127);
  }
  ProbeChild child(pid);
  int status = 0;
  require(child.wait(status), "FIFO/nonregular completion and scheduler shutdown must finish within five seconds");
  require(WIFEXITED(status), "nonregular-file probe exits without a signal");
  require_equal(WEXITSTATUS(status), 0, "nonregular-file probe and shutdown succeed");
}

void test_permissions()
{
  if (::geteuid() == 0) {
    std::cout << "SKIP permission-denial assertions under uid 0" << std::endl;
    return;
  }
  Fixture fixture;
  const std::string file = fixture.file("image", png());
  const Budget budget = make_budget();
  require_equal(::chmod(fixture.root().c_str(), 0000), 0, "deny root directory permissions");
  Runtime denied;
  require_equal(SemanticMediaRuntime::acquire(budget, string_view(fixture.root()), 1024, 1,
      INT64_MAX, denied), OB_OP_NOT_ALLOW, "root permission error is surfaced");
  require_equal(budget->memory_used(), 0, "root permission error rolls back its reservation");
  require_equal(::chmod(fixture.root().c_str(), 0700), 0, "restore owned root permissions");
  Runtime runtime = acquire(budget, fixture.root());
  require_equal(::chmod(file.c_str(), 0000), 0, "deny media file permissions");
  expect_failure(runtime, descriptor(file), OB_OP_NOT_ALLOW);
  runtime.reset();
  require_equal(budget->memory_used(), 0, "file permission error cleanup");
}

struct Mutation
{
  std::string file_;
  Stage stage_;
  bool mtime_only_ = false;
  std::atomic<bool> done_{false};
  std::atomic<int> error_{0};

  static void hook(void *context, Stage stage)
  {
    auto &mutation = *static_cast<Mutation *>(context);
    if (stage == mutation.stage_ && !mutation.done_.exchange(true)) {
      const int fd = ::open(mutation.file_.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC | O_NOFOLLOW);
      if (fd < 0) {
        mutation.error_.store(errno);
      } else if (mutation.mtime_only_) {
        struct stat before = {};
        if (::fstat(fd, &before) != 0) {
          mutation.error_.store(errno);
        } else {
          const struct timespec times[] = {{0, UTIME_OMIT},
              {before.st_mtim.tv_sec - 1, before.st_mtim.tv_nsec}};
          if (::futimens(fd, times) != 0) {
            mutation.error_.store(errno);
          }
        }
        if (::close(fd) != 0) {
          mutation.error_.store(errno);
        }
      } else {
        const char byte = 0;
        if (::write(fd, &byte, 1) != 1) {
          mutation.error_.store(errno == 0 ? EIO : errno);
        }
        if (::close(fd) != 0) {
          mutation.error_.store(errno);
        }
      }
    }
  }
};

void test_file_identity_changes()
{
  Fixture fixture;
  int index = 0;
  for (const Stage stage : {Stage::AFTER_READ_CHUNK, Stage::BEFORE_ENCODE, Stage::AFTER_ENCODE_CHUNK}) {
    for (const bool mtime_only : {false, true}) {
      Mutation mutation{fixture.file("changing-" + std::to_string(index++), png()), stage, mtime_only};
      const Budget budget = make_budget();
      const Runtime runtime = acquire(budget, fixture.root());
      require_equal(runtime->set_test_hook_for_testing(&Mutation::hook, &mutation),
                    OB_SUCCESS, "install bounded file mutation hook");
      expect_failure(runtime, descriptor(mutation.file_), OB_STATE_NOT_MATCH);
      require(mutation.done_.load(), "controlled file mutation happened");
      require_equal(mutation.error_.load(), 0, "controlled file mutation succeeded");
      require_equal(runtime->get_loaded_count(), 0, "changing file is not published");
    }
  }
}

void test_query_registry_and_policy()
{
  Fixture fixture;
  const Budget budget = make_budget();
  const int64_t deadline = INT64_MAX;
  Runtime first = acquire(budget, fixture.root(), 4096, 2, deadline);
  std::weak_ptr<SemanticMediaRuntime> original = first;
  require_equal(SemanticMediaRuntime::acquire(budget, string_view(fixture.root()), 4096, 2,
      deadline, first), OB_SUCCESS, "reacquisition preserves an already-owned runtime");
  require(original.lock() == first, "reacquisition does not cancel or replace its only output owner");
  Runtime second = acquire(budget, fixture.root() + "/", 4096, 2, deadline);
  require(first == second, "workers share a runtime for the same execution identity");
  Runtime rejected;
  require_equal(SemanticMediaRuntime::acquire(budget, string_view(fixture.root()), 4097, 2,
      deadline, rejected), OB_ERR_UNEXPECTED, "workers must agree on byte policy");
  require(!rejected, "inconsistent byte policy has no output");
  require_equal(SemanticMediaRuntime::acquire(budget, string_view(fixture.root()), 4096, 3,
      deadline, rejected), OB_ERR_UNEXPECTED, "workers must agree on parallel limit");
  require_equal(SemanticMediaRuntime::acquire(budget, string_view(fixture.root()), 4096, 2,
      deadline - 1, rejected), OB_ERR_UNEXPECTED, "workers must agree on deadline");
  const std::string other_root = fixture.directory(fixture.path("other"));
  require_equal(SemanticMediaRuntime::acquire(budget, string_view(other_root), 4096, 2,
      deadline, rejected), OB_ERR_UNEXPECTED, "workers must agree on root policy");
  const Budget duplicate = std::make_shared<SemanticQueryBudget>(budget->execution_id(),
      budget->memory_limit(), budget->task_limit());
  require_equal(SemanticMediaRuntime::acquire(duplicate, string_view(fixture.root()), 4096, 2,
      deadline, rejected), OB_ERR_UNEXPECTED, "a query cannot split into distinct memory counters");
  require_equal(first->cancel(OB_SUCCESS), OB_INVALID_ARGUMENT, "success is not a cancellation reason");
  require_equal(first->cancel(OB_EAGAIN), OB_INVALID_ARGUMENT, "retry is not a cancellation reason");
  std::weak_ptr<SemanticMediaRuntime> weak = first;
  first.reset();
  second.reset();
  require(weak.expired(), "weak registry does not retain query runtimes");
  require_equal(budget->memory_used(), 0, "last runtime releases its base reservation");
  first = acquire(budget, fixture.root(), 4096, 2, deadline);
  first.reset();
  require_equal(budget->memory_used(), 0, "expired registry entries are safely reusable");
}

void test_concurrent_acquire_at_exact_budget()
{
  Fixture fixture;
  int64_t base_charge = 0;
  {
    const Budget probe = make_budget();
    const Runtime runtime = acquire(probe, fixture.root(), 4096, 2);
    base_charge = probe->memory_used();
  }
  Gate gate;
  const Budget budget = make_budget(base_charge);
  std::vector<Runtime> runtimes(8);
  std::vector<int> results(runtimes.size(), OB_EAGAIN);
  JoiningThreads workers(gate);
  for (size_t i = 0; i < runtimes.size(); ++i) {
    workers.start([&, i] {
      Gate::hook(&gate, Stage::BEFORE_READ);
      results[i] = SemanticMediaRuntime::acquire(budget, string_view(fixture.root()),
          4096, 2, INT64_MAX, runtimes[i]);
    });
  }
  gate.wait(runtimes.size());
  gate.release();
  workers.join();
  for (size_t i = 0; i < runtimes.size(); ++i) {
    require_equal(results[i], OB_SUCCESS, "simultaneous workers do not duplicate the base reservation");
    require(runtimes[i] == runtimes[0], "simultaneous acquisition shares one runtime");
  }
  require_equal(budget->memory_used(), base_charge, "parallel acquisition charges runtime metadata once");
  runtimes.clear();
  require_equal(budget->memory_used(), 0, "parallel registry teardown releases the one reservation");
}

void test_concurrent_registry_teardown()
{
  Fixture fixture;
  Gate gate;
  const Budget budget = make_budget();
  std::atomic<int> failure{OB_SUCCESS};
  JoiningThreads workers(gate);
  for (int worker = 0; worker < 8; ++worker) {
    workers.start([&] {
      Gate::hook(&gate, Stage::BEFORE_READ);
      for (int iteration = 0; iteration < 128 && failure.load() == OB_SUCCESS; ++iteration) {
        Runtime first;
        Runtime second;
        int ret = SemanticMediaRuntime::acquire(budget, string_view(fixture.root()),
                                               4096, 2, INT64_MAX, first);
        if (ret == OB_SUCCESS) {
          ret = SemanticMediaRuntime::acquire(budget, string_view(fixture.root()),
                                              4096, 2, INT64_MAX, second);
        }
        if (ret == OB_SUCCESS && first != second) {
          ret = OB_ERR_UNEXPECTED;
        }
        if (ret != OB_SUCCESS) {
          int expected = OB_SUCCESS;
          failure.compare_exchange_strong(expected, ret);
        }
      }
    });
  }
  gate.wait(8);
  gate.release();
  workers.join();
  require_equal(failure.load(), OB_SUCCESS, "weak expiry and last-owner teardown never shadow a query entry");
  require_equal(budget->memory_used(), 0, "concurrent query runtime teardown releases every reservation");
}

void test_shared_load_and_consistent_metadata()
{
  Fixture fixture;
  const Bytes bytes = png();
  const std::string file = fixture.file("shared", bytes);
  const std::string encoded = descriptor(file, "image/png", bytes.size());
  Gate gate;
  const Budget budget = make_budget();
  const Runtime runtime = acquire(budget, fixture.root(), 4096, 1);
  require_equal(runtime->set_test_hook_for_testing(&Gate::hook, &gate),
                OB_SUCCESS, "install shared-load gate");
  ReleaseGate release(gate);
  std::vector<Task> tasks(8);
  std::vector<int> results(tasks.size(), OB_EAGAIN);
  JoiningThreads callers(gate);
  for (size_t i = 0; i < tasks.size(); ++i) {
    callers.start([&, i] {
      const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(20);
      do {
        results[i] = runtime->demand(string_view(encoded), tasks[i]);
        if (results[i] == OB_EAGAIN) {
          std::this_thread::yield();
        }
      } while (results[i] == OB_EAGAIN && std::chrono::steady_clock::now() < end);
    });
  }
  callers.join();
  gate.wait(1);
  for (size_t i = 0; i < tasks.size(); ++i) {
    require_equal(results[i], OB_SUCCESS, "concurrent callers are admitted/coalesced");
    require(tasks[i] == tasks[0], "concurrent workers receive exactly the same task");
  }
  require_equal(runtime->get_started_count(), 1, "shared URI performs one physical load");
  expect_demand_failure(runtime, descriptor(file, "image/png", bytes.size() + 1), OB_INVALID_ARGUMENT);
  expect_demand_failure(runtime, descriptor(file), OB_INVALID_ARGUMENT);
  gate.release();
  expect_bytes(tasks[0], bytes, "image/png");
  require_equal(runtime->get_loaded_count(), 1, "coalesced load publishes once");
}

void test_async_overlap_and_parallel_limit()
{
  Fixture fixture;
  const Bytes bytes = png();
  std::vector<std::string> files;
  for (int i = 0; i < 5; ++i) {
    files.push_back(fixture.file("overlap-" + std::to_string(i), bytes));
  }
  Gate gate;
  const Budget budget_a = make_budget();
  const Budget budget_b = make_budget();
  const Runtime a = acquire(budget_a, fixture.root(), 4096, 2);
  const Runtime b = acquire(budget_b, fixture.root(), 4096, 2);
  require_equal(a->set_test_hook_for_testing(&Gate::hook, &gate), OB_SUCCESS, "gate query A");
  require_equal(b->set_test_hook_for_testing(&Gate::hook, &gate), OB_SUCCESS, "gate query B");
  ReleaseGate release(gate);
  std::vector<Task> tasks = {demand(a, descriptor(files[0])), demand(a, descriptor(files[1])),
                             demand(b, descriptor(files[2])), demand(b, descriptor(files[3]))};
  gate.wait(4);
  require_equal(gate.arrived(), 4, "four independent file reads overlap before any is released");
  require_equal(a->get_active_count(), 2, "query A parallel limit is exact");
  require_equal(b->get_active_count(), 2, "query B parallel limit is exact");
  require(!a->can_demand(), "parallel exhaustion is not spuriously ready");
  require(a->admission_ready() == a->can_demand(), "legacy readiness accessor remains compatible");
  expect_demand_failure(a, descriptor(files[4]), OB_EAGAIN);
  require_equal(a->set_test_hook_for_testing(nullptr, nullptr), OB_STATE_NOT_MATCH,
                "test hooks cannot change under executing tasks");
  for (const Task &task : tasks) {
    require(!task->ready(), "gated task is not ready");
    require_equal(task->result(), OB_EAGAIN, "pending result is exactly OB_EAGAIN");
    require(task->base64().empty() && task->mime_type().empty() && task->audio_format().empty(),
            "pending outputs are safely empty");
  }
  gate.release();
  for (const Task &task : tasks) {
    expect_bytes(task, bytes, "image/png");
  }
  require(a->can_demand(), "released query slot wakes admission retries");
  expect_bytes(demand(a, descriptor(files[4])), bytes, "image/png");
  require_equal(a->get_loaded_count(), 3, "retried demand loads exactly once");
}

void test_global_queue_admission()
{
  Fixture fixture;
  const Bytes bytes = png();
  const int64_t count = SemanticMediaRuntime::QUEUE_CAPACITY + SemanticMediaRuntime::WORKER_COUNT;
  std::vector<std::string> files;
  for (int64_t i = 0; i <= count; ++i) {
    files.push_back(fixture.file("queued-" + std::to_string(i), bytes));
  }
  Gate gate;
  std::vector<Budget> budgets = {make_budget(), make_budget(), make_budget()};
  std::vector<Runtime> runtimes;
  for (const Budget &budget : budgets) {
    runtimes.push_back(acquire(budget, fixture.root(), 4096, 64));
    require_equal(runtimes.back()->set_test_hook_for_testing(&Gate::hook, &gate),
                  OB_SUCCESS, "gate bounded global queue");
  }
  ReleaseGate release(gate);
  std::vector<Task> tasks;
  for (int64_t i = 0; i < SemanticMediaRuntime::WORKER_COUNT; ++i) {
    tasks.push_back(demand(runtimes[0], descriptor(files[i])));
  }
  gate.wait(SemanticMediaRuntime::WORKER_COUNT);
  for (int64_t i = SemanticMediaRuntime::WORKER_COUNT; i < count; ++i) {
    tasks.push_back(demand(runtimes[i / 64], descriptor(files[i])));
  }
  require_equal(gate.arrived(), SemanticMediaRuntime::WORKER_COUNT,
                "queued files do not start extra per-file threads");
  require_equal(runtimes.back()->get_started_count(), 0, "last query has queued loads only");
  require(runtimes.back()->has_pending(), "queued loads count as query-shared pending work");
  require(!runtimes.back()->can_demand(), "full global queue blocks readiness until capacity changes");
  expect_demand_failure(runtimes.back(), descriptor(files.back()), OB_EAGAIN);
  for (const Task &task : tasks) {
    require(!task->ready(), "running and queued work is pending before gate release");
  }
  gate.release();
  for (const Task &task : tasks) {
    expect_bytes(task, bytes, "image/png");
  }
  wait_until([&] { return runtimes.back()->can_demand(); }, "queue capacity becomes retryable");
  expect_bytes(demand(runtimes.back(), descriptor(files.back())), bytes, "image/png");
  require(!runtimes.back()->has_pending(), "completed queue and retained results are no longer pending");
}

void test_pending_loads_exclude_completed_cache()
{
  Fixture fixture;
  const Bytes bytes = png();
  const std::string encoded = descriptor(fixture.file("pending-state", bytes));
  Gate gate;
  const Budget budget = make_budget();
  const Runtime runtime = acquire(budget, fixture.root(), 4096, 1);
  const Runtime shared = acquire(budget, fixture.root(), 4096, 1);
  require(runtime == shared, "pending state belongs to the shared query runtime");
  require(!runtime->has_pending(), "idle query has no pending media");
  require_equal(runtime->set_test_hook_for_testing(&Gate::hook, &gate),
                OB_SUCCESS, "gate pending-state transition");
  ReleaseGate release(gate);
  const Task task = demand(runtime, encoded);
  gate.wait(1);
  require(runtime->has_pending() && shared->has_pending(), "all workers observe the running load");
  gate.release();
  expect_bytes(task, bytes, "image/png");
  require(!runtime->has_pending() && !shared->has_pending(), "completed load clears shared pending state");
  require_equal(runtime->get_cached_count(), 1, "completed bytes remain retained for query reuse");
  const Task reused = demand(shared, encoded);
  require(reused == task, "ready retained task is reused while external handles remain alive");
  require(!runtime->has_pending(), "completed cached and externally held handles never count as pending");
  require_equal(runtime->get_loaded_count(), 1, "ready reuse does not start new media work");
}

void test_media_admission_with_http_inflight()
{
  Fixture fixture;
  const Bytes bytes = png();
  const std::string first = descriptor(fixture.file("http-and-media-first", bytes));
  const std::string second = descriptor(fixture.file("http-and-media-second", bytes));
  Gate gate;
  const Budget budget = std::make_shared<SemanticQueryBudget>(next_execution_id(), 16 * 1024 * 1024, 1);
  TaskCredit http(budget);
  require(!budget->task_available(), "fixture exhausts all HTTP task credits");
  const Runtime runtime = acquire(budget, fixture.root(), 4096, 1);
  require(!runtime->has_pending(), "HTTP credits alone do not count as pending media");
  require_equal(runtime->set_test_hook_for_testing(&Gate::hook, &gate),
                OB_SUCCESS, "gate media independently of HTTP admission");
  ReleaseGate release(gate);
  require(runtime->can_demand(), "occupied HTTP credits do not block the first media admission");
  const Task task = demand(runtime, first);
  gate.wait(1);
  require(!runtime->can_demand(), "media parallel exhaustion suppresses continuation spinning");
  expect_demand_failure(runtime, second, OB_EAGAIN);
  require(!budget->task_available(), "media loading leaves the HTTP credit owned by its request");
  gate.release();
  expect_bytes(task, bytes, "image/png");
  require(runtime->can_demand(), "media completion wakes admission while HTTP is still in flight");
  expect_bytes(demand(runtime, second), bytes, "image/png");
  require(!budget->task_available(), "media completion never releases another request's HTTP credit");
  require(!runtime->has_pending(), "ready media excludes the still-in-flight HTTP request");
}

void test_live_handles_beyond_cache()
{
  Fixture fixture;
  const Bytes bytes = png();
  const Budget budget = make_budget();
  const Runtime runtime = acquire(budget, fixture.root(), 4096, 1);
  std::vector<std::string> encoded;
  std::vector<Task> tasks;
  for (int64_t i = 0; i < SemanticMediaRuntime::CACHE_CAPACITY + 8; ++i) {
    encoded.push_back(descriptor(fixture.file("held-" + std::to_string(i), bytes)));
    tasks.push_back(demand(runtime, encoded.back()));
    expect_bytes(tasks.back(), bytes, "image/png");
  }
  require_equal(runtime->get_cached_count(), SemanticMediaRuntime::CACHE_CAPACITY,
                "strong result cache stays bounded despite large held batches");
  const Task same = demand(runtime, encoded.back());
  require(same == tasks.back(), "weak index coalesces externally held uncached results");
  require_equal(runtime->get_loaded_count(), tasks.size(), "held batch has no duplicate physical loads");
}

void test_bounded_cache_eviction()
{
  Fixture fixture;
  const Bytes bytes = png();
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, fixture.root(), 4096, 1);
  std::string first;
  const int64_t count = SemanticMediaRuntime::CACHE_CAPACITY + 16;
  for (int64_t i = 0; i < count; ++i) {
    const std::string encoded = descriptor(fixture.file("cached-" + std::to_string(i), bytes));
    if (i == 0) {
      first = encoded;
    }
    const Task task = demand(runtime, encoded);
    expect_bytes(task, bytes, "image/png");
    require(runtime->get_cached_count() <= SemanticMediaRuntime::CACHE_CAPACITY, "cache never grows without bound");
  }
  require_equal(runtime->get_loaded_count(), count, "each independent file loads once");
  require(runtime->get_evicted_count() >= 16, "unused READY entries are evicted at the bound");
  expect_bytes(demand(runtime, first), bytes, "image/png");
  require_equal(runtime->get_loaded_count(), count + 1, "an evicted reference is explicitly reloaded");
  runtime.reset();
  require_equal(budget->memory_used(), 0, "evicted and retained cache reservations are all released");
}

struct BudgetObservation
{
  Budget budget_;
  std::atomic<int64_t> peak_{0};
  static void hook(void *context, Stage)
  {
    auto &observation = *static_cast<BudgetObservation *>(context);
    const int64_t current = observation.budget_->memory_used();
    int64_t old = observation.peak_.load();
    while (old < current && !observation.peak_.compare_exchange_weak(old, current)) {
    }
  }
};

void test_exact_budget_threshold()
{
  Fixture fixture;
  const Bytes bytes = png();
  const std::string encoded = descriptor(fixture.file("threshold", bytes));
  int64_t peak = 0;
  {
    const Budget budget = make_budget();
    BudgetObservation observation{budget};
    const Runtime runtime = acquire(budget, fixture.root(), 4096, 1);
    require_equal(runtime->set_test_hook_for_testing(&BudgetObservation::hook, &observation),
                  OB_SUCCESS, "observe actual descriptor/raw/base64 charges");
    expect_bytes(demand(runtime, encoded), bytes, "image/png");
    peak = observation.peak_.load();
    require(peak > budget->memory_used(), "peak includes raw and immutable output simultaneously");
  }
  {
    const Budget budget = make_budget(peak);
    Runtime runtime = acquire(budget, fixture.root(), 4096, 1);
    expect_bytes(demand(runtime, encoded), bytes, "image/png");
    require(budget->memory_used() <= peak, "exact native byte limit is honored");
    runtime.reset();
    require_equal(budget->memory_used(), 0, "exact-boundary reservation releases once");
  }
  {
    const Budget budget = make_budget(peak - 1);
    Runtime runtime = acquire(budget, fixture.root(), 4096, 1);
    expect_failure(runtime, encoded, OB_SIZE_OVERFLOW);
    runtime.reset();
    require_equal(budget->memory_used(), 0, "one-byte denial has no leaked reservations");
  }
  {
    const Budget budget = make_budget(1);
    Runtime runtime;
    require_equal(SemanticMediaRuntime::acquire(budget, string_view(fixture.root()), 4096, 1,
        INT64_MAX, runtime), OB_SIZE_OVERFLOW, "runtime metadata allocation is also budgeted");
    require(!runtime, "metadata memory denial has no success-shaped output");
    require_equal(budget->memory_used(), 0, "base reservation denial rolls back");
  }
}

void test_memory_pressure_eviction()
{
  Fixture fixture;
  const Bytes bytes = png();
  const std::string first = descriptor(fixture.file("first", bytes));
  const std::string second = descriptor(fixture.file("other", bytes));
  int64_t limit = 0;
  {
    const Budget budget = make_budget();
    const Runtime runtime = acquire(budget, fixture.root(), 4096, 1);
    const Task task = demand(runtime, first);
    expect_bytes(task, bytes, "image/png");
    wait_cache_only_worker_released(task);
    limit = budget->memory_used() + bytes.size() + 128;
  }
  const Budget budget = make_budget(limit);
  Runtime runtime = acquire(budget, fixture.root(), 4096, 1);
  {
    const Task task = demand(runtime, first);
    expect_bytes(task, bytes, "image/png");
    wait_cache_only_worker_released(task);
  }
  expect_bytes(demand(runtime, second), bytes, "image/png");
  require(runtime->get_evicted_count() > 0, "memory pressure evicts unused READY bytes before failing");
  require(budget->memory_used() <= limit, "pressure reclamation preserves the byte bound");
  runtime.reset();
  require_equal(budget->memory_used(), 0, "pressure eviction releases each reservation exactly once");
}

void test_descriptor_and_result_lifetime()
{
  Fixture fixture;
  const Bytes bytes = png();
  std::string encoded = descriptor(fixture.file("owned", bytes));
  Gate gate;
  Budget budget = make_budget();
  Runtime runtime = acquire(budget, fixture.root(), 4096, 1);
  require_equal(runtime->set_test_hook_for_testing(&Gate::hook, &gate), OB_SUCCESS, "gate owned descriptor");
  ReleaseGate release(gate);
  Task task = demand(runtime, encoded);
  gate.wait(1);
  encoded.assign(encoded.size(), 'x');
  gate.release();
  expect_bytes(task, bytes, "image/png");
  runtime.reset();
  require(budget->memory_used() > 0, "result handle owns its immutable bytes after runtime destruction");
  std::weak_ptr<SemanticQueryBudget> weak = budget;
  budget.reset();
  require(!weak.expired(), "task keeps its budget alive to safely release reservations");
  expect_bytes(task, bytes, "image/png");
  task.reset();
  require(weak.expired(), "there is no budget/runtime ownership cycle");
}

void test_first_load_is_pinned_while_cached()
{
  Fixture fixture;
  const Bytes bytes = png();
  const std::string file = fixture.file("pinned", bytes);
  const std::string encoded = descriptor(file);
  const Budget budget = make_budget();
  const Runtime runtime = acquire(budget, fixture.root());
  const Task first = demand(runtime, encoded);
  expect_bytes(first, bytes, "image/png");
  const int fd = ::open(file.c_str(), O_WRONLY | O_TRUNC | O_CLOEXEC | O_NOFOLLOW);
  require(fd >= 0, "change source after first verified load");
  require_equal(::close(fd), 0, "close changed source");
  const Task second = demand(runtime, encoded);
  require(second == first, "reuse pins the immutable first-load bytes rather than rereading the path");
  expect_bytes(second, bytes, "image/png");
  require_equal(runtime->get_loaded_count(), 1, "cached reuse makes no external MVCC claim or reread");
}

void test_cancel_running_and_queued()
{
  Fixture fixture;
  Gate gate;
  const Budget budget = make_budget();
  const Runtime runtime = acquire(budget, fixture.root(), 4096, 8);
  require_equal(runtime->set_test_hook_for_testing(&Gate::hook, &gate), OB_SUCCESS, "gate cancelable queue");
  ReleaseGate release(gate);
  std::vector<Task> tasks;
  for (int i = 0; i < 8; ++i) {
    tasks.push_back(demand(runtime, descriptor(fixture.file("cancel-" + std::to_string(i), png()))));
  }
  gate.wait(SemanticMediaRuntime::WORKER_COUNT);
  require_equal(runtime->cancel(), OB_SUCCESS, "cancel the media runtime");
  require(runtime->can_demand(), "cancellation wakes an admission continuation to collect failure");
  gate.release();
  for (const Task &task : tasks) {
    wait_ready(task);
    require_equal(task->result(), OB_CANCELED, "running and queued work both complete as canceled");
    require(task->base64().empty(), "canceled work never publishes payload");
  }
  require_equal(runtime->get_loaded_count(), 0, "canceled tasks do not publish success");
  expect_demand_failure(runtime, descriptor(fixture.path("later")), OB_CANCELED);
}

void test_cancel_during_encoding()
{
  Fixture fixture;
  const std::string encoded = descriptor(fixture.file("large-wav", wav(200000)));
  Gate gate(Stage::AFTER_ENCODE_CHUNK);
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, fixture.root());
  require_equal(runtime->set_test_hook_for_testing(&Gate::hook, &gate), OB_SUCCESS, "gate chunked encoder");
  ReleaseGate release(gate);
  Task task = demand(runtime, encoded);
  gate.wait(1);
  const int64_t peak = budget->memory_used();
  require_equal(runtime->cancel(), OB_SUCCESS, "cancel after one encoding chunk");
  gate.release();
  wait_ready(task);
  require_equal(task->result(), OB_CANCELED, "encoding checks cancellation between chunks");
  require(task->base64().empty(), "partially encoded media is never exposed");
  require(budget->memory_used() < peak - 200000, "cancellation releases raw and base64 scratch buffers");
  runtime.reset();
  task.reset();
  require_equal(budget->memory_used(), 0, "encoding cancellation fully releases accounting");
}

void test_deadline_and_query_failure()
{
  Fixture fixture;
  const std::string encoded = descriptor(fixture.file("deadline", png()));
  {
    const Budget budget = make_budget();
    Runtime runtime;
    require_equal(SemanticMediaRuntime::acquire(budget, string_view(fixture.root()), 4096, 1,
        ObTimeUtility::current_time() - 1, runtime), OB_TIMEOUT, "expired query deadline denies acquisition");
    require_equal(budget->memory_used(), 0, "expired acquisition allocates nothing");
  }
  {
    Gate gate(Stage::AFTER_READ_CHUNK);
    const Budget budget = make_budget();
    const int64_t deadline = ObTimeUtility::current_time() + 2000000;
    const Runtime runtime = acquire(budget, fixture.root(), 4096, 1, deadline);
    require_equal(runtime->set_test_hook_for_testing(&Gate::hook, &gate), OB_SUCCESS, "gate async deadline");
    ReleaseGate release(gate);
    const Task task = demand(runtime, encoded);
    gate.wait(1);
    wait_until([&] { return ObTimeUtility::current_time() >= deadline; }, "absolute query deadline expires");
    gate.release();
    wait_ready(task);
    require_equal(task->result(), OB_TIMEOUT, "chunked file read observes deadline after I/O");
    expect_demand_failure(runtime, encoded, OB_TIMEOUT);
  }
  {
    Gate gate(Stage::AFTER_READ_CHUNK);
    const Budget budget = make_budget();
    const Runtime runtime = acquire(budget, fixture.root(), 4096, 1);
    require_equal(runtime->set_test_hook_for_testing(&Gate::hook, &gate), OB_SUCCESS, "gate query failure");
    ReleaseGate release(gate);
    const Task task = demand(runtime, encoded);
    gate.wait(1);
    budget->fail(OB_ERR_UNEXPECTED);
    gate.release();
    wait_ready(task);
    require_equal(task->result(), OB_ERR_UNEXPECTED, "media workers preserve the first query failure");
    require(runtime->can_demand(), "query failure wakes admission retry to report the real error");
    expect_demand_failure(runtime, encoded, OB_ERR_UNEXPECTED);
  }
}

void test_destructor_cancels_and_drains()
{
  Fixture fixture;
  Gate gate;
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, fixture.root(), 4096, 1);
  require_equal(runtime->set_test_hook_for_testing(&Gate::hook, &gate), OB_SUCCESS, "gate runtime destruction");
  ReleaseGate release(gate);
  Task task = demand(runtime, descriptor(fixture.file("drain", png())));
  gate.wait(1);
  std::weak_ptr<SemanticMediaRuntime> weak = runtime;
  std::atomic<bool> entered{false};
  std::atomic<bool> returned{false};
  JoiningThreads owner(gate);
  owner.start([owned = std::move(runtime), &entered, &returned]() mutable {
    entered.store(true);
    owned.reset();
    returned.store(true);
  });
  wait_until([&] { return entered.load() && weak.expired(); }, "destructor starts without a strong cycle");
  require(!returned.load(), "destructor waits for its own gated task instead of freeing live state");
  gate.release();
  owner.join();
  require(returned.load(), "destructor safely drains when I/O worker can finish");
  wait_ready(task);
  require_equal(task->result(), OB_CANCELED, "destruction cancels owned pending work");
  task.reset();
  require_equal(budget->memory_used(), 0, "drained destruction releases all owned bytes");
}

void test_scheduler_shutdown()
{
  Fixture fixture;
  Gate gate;
  const Budget budget = make_budget();
  Runtime runtime = acquire(budget, fixture.root(), 4096, 8);
  require_equal(runtime->set_test_hook_for_testing(&Gate::hook, &gate), OB_SUCCESS, "gate scheduler shutdown");
  ReleaseGate release(gate);
  std::vector<Task> tasks;
  for (int i = 0; i < 8; ++i) {
    tasks.push_back(demand(runtime, descriptor(fixture.file("shutdown-" + std::to_string(i), png()))));
  }
  gate.wait(SemanticMediaRuntime::WORKER_COUNT);
  std::atomic<bool> returned{false};
  JoiningThreads shutdown(gate);
  shutdown.start([&] {
    SemanticMediaRuntime::stop_scheduler();
    returned.store(true);
  });
  wait_until([&] { return runtime->status() == OB_CANCELED; }, "shutdown cancels file executor work");
  require(!returned.load(), "global shutdown joins workers rather than abandoning I/O");
  gate.release();
  shutdown.join();
  for (const Task &task : tasks) {
    wait_ready(task);
    require_equal(task->result(), OB_CANCELED, "shutdown cancels both running and queued work");
  }
  require_equal(runtime->get_active_count(), 0, "shutdown drains each query task counter");
  expect_demand_failure(runtime, descriptor(fixture.path("later")), OB_CANCELED);
  Runtime rejected;
  require_equal(SemanticMediaRuntime::acquire(make_budget(), string_view(fixture.root()), 4096, 1,
      INT64_MAX, rejected), OB_CANCELED, "stopped scheduler cannot silently restart");
  runtime.reset();
  tasks.clear();
  require_equal(budget->memory_used(), 0, "scheduler shutdown and query teardown release all bytes");
}

struct TestCase
{
  const char *name_;
  void (*run_)();
};

const TestCase TESTS[] = {
  {"valid_formats", test_valid_formats},
  {"canonical_escaped_paths", test_canonical_escaped_paths},
  {"structural_corruption", test_structural_corruption},
  {"metadata_and_versions", test_metadata_and_versions},
  {"missing_oversize_and_descriptors", test_missing_oversize_and_descriptors},
  {"root_policy_and_traversal", test_root_policy_and_traversal},
  {"symlinks_and_special_files", test_symlinks_and_special_files},
  {"nonregular_bounded_completion_and_shutdown", test_nonregular_bounded_completion_and_shutdown},
  {"permissions", test_permissions},
  {"file_identity_changes", test_file_identity_changes},
  {"query_registry_and_policy", test_query_registry_and_policy},
  {"concurrent_acquire_at_exact_budget", test_concurrent_acquire_at_exact_budget},
  {"concurrent_registry_teardown", test_concurrent_registry_teardown},
  {"shared_load_and_consistent_metadata", test_shared_load_and_consistent_metadata},
  {"async_overlap_and_parallel_limit", test_async_overlap_and_parallel_limit},
  {"global_queue_admission", test_global_queue_admission},
  {"pending_loads_exclude_completed_cache", test_pending_loads_exclude_completed_cache},
  {"media_admission_with_http_inflight", test_media_admission_with_http_inflight},
  {"live_handles_beyond_cache", test_live_handles_beyond_cache},
  {"bounded_cache_eviction", test_bounded_cache_eviction},
  {"exact_budget_threshold", test_exact_budget_threshold},
  {"memory_pressure_eviction", test_memory_pressure_eviction},
  {"descriptor_and_result_lifetime", test_descriptor_and_result_lifetime},
  {"first_load_is_pinned_while_cached", test_first_load_is_pinned_while_cached},
  {"cancel_running_and_queued", test_cancel_running_and_queued},
  {"cancel_during_encoding", test_cancel_during_encoding},
  {"deadline_and_query_failure", test_deadline_and_query_failure},
  {"destructor_cancels_and_drains", test_destructor_cancels_and_drains},
  {"scheduler_shutdown", test_scheduler_shutdown},
};

void run_case(const TestCase &test, int64_t repeat, int64_t &completed)
{
  const int64_t baseline = ObAIFuncClient::pipeline_buffer_usage();
  const int64_t cleanup_baseline = cleanup_errors.load();
  test.run_();
  require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline,
                std::string(test.name_) + " leaves shared accounting unchanged");
  require_equal(cleanup_errors.load(), cleanup_baseline,
                std::string(test.name_) + " cleans up every owned fixture path");
  std::cout << "PASS " << test.name_ << " repeat=" << repeat << std::endl;
  ++completed;
}
}

int main(int argc, char **argv)
{
  try {
    if (argc == 3 && std::strcmp(argv[1], "--nonregular-probe") == 0) {
      run_nonregular_probe(argv[2]);
      return 0;
    }
    int64_t repeats = 1;
    std::string selected;
    for (int i = 1; i < argc; i += 2) {
      require(i + 1 < argc, "native test options require a value");
      if (std::string(argv[i]) == "--repeat") {
        char *end = nullptr;
        repeats = std::strtoll(argv[i + 1], &end, 10);
        require(end != argv[i + 1] && *end == '\0' && repeats > 0 && repeats <= 100,
                "--repeat must be in [1, 100]");
      } else if (std::string(argv[i]) == "--case") {
        selected = argv[i + 1];
        require(!selected.empty(), "--case must not be empty");
      } else {
        require(false, "usage: semantic_media_tests [--repeat COUNT] [--case NAME]");
      }
    }
    require(selected.empty() || std::any_of(std::begin(TESTS), std::end(TESTS),
        [&](const TestCase &test) { return selected == test.name_; }), "unknown native media test case");
    require(selected != "scheduler_shutdown" || repeats == 1, "scheduler shutdown is irreversible");
    int64_t completed = 0;
    for (int64_t repeat = 1; repeat <= repeats; ++repeat) {
      for (const auto &test : TESTS) {
        if (std::strcmp(test.name_, "scheduler_shutdown") == 0
            || (!selected.empty() && selected != test.name_)) {
          continue;
        }
        run_case(test, repeat, completed);
      }
    }
    if (selected.empty() || selected == "scheduler_shutdown") {
      run_case(TESTS[sizeof(TESTS) / sizeof(TESTS[0]) - 1], 1, completed);
    }
    SemanticMediaRuntime::stop_scheduler();
    std::cout << "semantic media native tests passed: " << completed << " cases" << std::endl;
    return 0;
  } catch (const std::exception &error) {
    SemanticMediaRuntime::stop_scheduler();
    std::cerr << "FAIL semantic media native tests: " << error.what() << std::endl;
    return 1;
  }
}
