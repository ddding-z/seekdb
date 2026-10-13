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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_semantic_media.h"
#include "sql/engine/basic/ob_semantic_runtime.h"
#include "common/object/file_ref.h"
#include "lib/encode/ob_base64_encode.h"
#include "lib/oblog/ob_log.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/threads.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_encryption_util.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdio>
#include <cstring>
#include <curl/curl.h>
#include <fcntl.h>
#include <new>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <zlib.h>

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{
constexpr int64_t MAX_DESCRIPTOR_BYTES = FileRefCodec::MAX_REF_BYTES;
constexpr int64_t MAX_URI_BYTES = FileRefCodec::MAX_URI_BYTES;
constexpr int64_t MAX_SOURCE_BYTES = (INT32_MAX / 4) * 3LL;
constexpr int64_t READ_CHUNK_BYTES = 64 * 1024;
constexpr int64_t ENCODE_CHUNK_BYTES = 48 * 1024;
constexpr int64_t MAX_IMAGE_DIMENSION = 32768;
constexpr int64_t MAX_IMAGE_PIXELS = 100000000;
constexpr int64_t MAX_FORMAT_CHUNKS = 65536;
constexpr int64_t MAX_QUERY_RUNTIMES = 1024;
// Conservatively charge allocator/control-block and map-node bookkeeping as well.
constexpr int64_t ALLOCATION_OVERHEAD = 128;
constexpr int64_t REGISTRY_OVERHEAD = 256;
constexpr int64_t HASH_WORK_BYTES = 4096;
constexpr int64_t REMOTE_REQUEST_BYTES = 8192;
constexpr int64_t MAX_REMOTE_HEADER_BYTES = 32 * 1024;

struct MediaRegistry
{
  std::mutex mutex_;
  std::map<uint64_t, std::weak_ptr<SemanticMediaRuntime>> queries_;
};

MediaRegistry &media_registry()
{
  static MediaRegistry registry;
  return registry;
}

class FileDescriptor
{
public:
  explicit FileDescriptor(int fd = -1) : fd_(fd) {}
  ~FileDescriptor() { reset(); }
  int get() const { return fd_; }
  void reset(int fd = -1)
  {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    fd_ = fd;
  }
  int detach()
  {
    const int fd = fd_;
    fd_ = -1;
    return fd;
  }
private:
  FileDescriptor(const FileDescriptor &) = delete;
  FileDescriptor &operator=(const FileDescriptor &) = delete;
  int fd_;
};

class Reservation
{
public:
  Reservation(std::shared_ptr<SemanticQueryBudget> budget, int64_t bytes)
      : budget_(std::move(budget)), bytes_(bytes) {}
  ~Reservation()
  {
    if (bytes_ > 0) {
      budget_->release(bytes_);
    }
  }
  void detach() { bytes_ = 0; }
private:
  Reservation(const Reservation &) = delete;
  Reservation &operator=(const Reservation &) = delete;
  std::shared_ptr<SemanticQueryBudget> budget_;
  int64_t bytes_;
};

const ObString &empty_string()
{
  static const ObString empty;
  return empty;
}

bool same_string(const ObString &left, const ObString &right)
{
  return left.length() == right.length()
      && (left.empty() || 0 == std::memcmp(left.ptr(), right.ptr(), left.length()));
}

bool equals_mime(const ObString &value, const char *literal)
{
  bool same = value.length() == static_cast<int64_t>(std::strlen(literal));
  for (int32_t i = 0; same && i < value.length(); ++i) {
    const unsigned char ch = static_cast<unsigned char>(value.ptr()[i]);
    same = (ch >= 'A' && ch <= 'Z' ? ch + ('a' - 'A') : ch) == literal[i];
  }
  return same;
}

int normalize_mime(const ObString &supplied, ObString &canonical)
{
  int ret = OB_SUCCESS;
  canonical.reset();
  if (supplied.empty()) {
  } else if (equals_mime(supplied, "image/png")) {
    canonical = ObString::make_string("image/png");
  } else if (equals_mime(supplied, "image/jpeg")) {
    canonical = ObString::make_string("image/jpeg");
  } else if (equals_mime(supplied, "audio/wav") || equals_mime(supplied, "audio/x-wav")) {
    canonical = ObString::make_string("audio/wav");
  } else if (equals_mime(supplied, "audio/mpeg")) {
    canonical = ObString::make_string("audio/mpeg");
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int file_error(int error)
{
  int ret = OB_IO_ERROR;
  if (error == ENOENT) {
    ret = OB_FILE_NOT_EXIST;
  } else if (error == EACCES || error == EPERM || error == ELOOP || error == ENOTDIR) {
    ret = OB_OP_NOT_ALLOW;
  } else if (error == ENAMETOOLONG || error == ENOMEM) {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

int canonical_path(const ObString &path)
{
  int ret = OB_SUCCESS;
  if (path.length() <= 0 || path.empty() || path.ptr()[0] != '/' || path.length() > MAX_URI_BYTES
      || (path.length() > 1 && path.ptr()[path.length() - 1] == '/')) {
    ret = OB_INVALID_ARGUMENT;
  }
  int64_t component = 1;
  for (int64_t pos = 1; OB_SUCC(ret) && pos <= path.length(); ++pos) {
    if (pos < path.length()) {
      const unsigned char ch = static_cast<unsigned char>(path.ptr()[pos]);
      if (ch < 0x20 || ch == 0x7f) {
        ret = OB_INVALID_ARGUMENT;
      }
    }
    if (OB_SUCC(ret) && (pos == path.length() || path.ptr()[pos] == '/')) {
      const int64_t length = pos - component;
      if (path.length() == 1) {
      } else if (length <= 0 || length > NAME_MAX
          || (length == 1 && path.ptr()[component] == '.')
          || (length == 2 && path.ptr()[component] == '.' && path.ptr()[component + 1] == '.')) {
        ret = OB_INVALID_ARGUMENT;
      }
      component = pos + 1;
    }
  }
  return ret;
}

int hex_digit(char ch);

int local_path(const ObString &uri, char *buffer, ObString &path)
{
  int ret = OB_SUCCESS;
  if (uri.length() < 8 || uri.length() > MAX_URI_BYTES
      || 0 != std::memcmp(uri.ptr(), "file:///", 8)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int32_t length = 0;
    for (int32_t pos = 7; OB_SUCC(ret) && pos < uri.length(); ++pos) {
      if (uri.ptr()[pos] != '%') {
        buffer[length++] = uri.ptr()[pos];
      } else if (uri.length() - pos < 3 || hex_digit(uri.ptr()[pos + 1]) < 0
                 || hex_digit(uri.ptr()[pos + 2]) < 0) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        buffer[length++] = static_cast<char>((hex_digit(uri.ptr()[pos + 1]) << 4)
                                             | hex_digit(uri.ptr()[pos + 2]));
        pos += 2;
      }
    }
    if (OB_SUCC(ret)) {
      buffer[length] = '\0';
      path.assign_ptr(buffer, length);
      ret = canonical_path(path);
    }
  }
  return ret;
}

int relative_path(const ObString &root, const ObString &path, ObString &relative)
{
  int ret = OB_SUCCESS;
  if (root.length() == 1) {
    if (path.length() <= 1 || path.ptr()[0] != '/') {
      ret = OB_OP_NOT_ALLOW;
    } else {
      relative.assign_ptr(path.ptr() + 1, path.length() - 1);
    }
  } else if (path.length() <= root.length() + 1
      || 0 != std::memcmp(root.ptr(), path.ptr(), root.length())
      || path.ptr()[root.length()] != '/') {
    ret = OB_OP_NOT_ALLOW;
  } else {
    relative.assign_ptr(path.ptr() + root.length() + 1, path.length() - root.length() - 1);
  }
  return ret;
}

int open_root(const ObString &root, FileDescriptor &directory)
{
  int ret = OB_SUCCESS;
  const int first = ::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (first < 0) {
    ret = file_error(errno);
  } else {
    directory.reset(first);
  }
  int64_t component = 1;
  for (int64_t pos = 1; OB_SUCC(ret) && root.length() > 1 && pos <= root.length(); ++pos) {
    if (pos == root.length() || root.ptr()[pos] == '/') {
      char name[NAME_MAX + 1];
      const int64_t length = pos - component;
      std::memcpy(name, root.ptr() + component, length);
      name[length] = '\0';
      const int fd = ::openat(directory.get(), name,
                              O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
      if (fd < 0) {
        ret = file_error(errno);
      } else {
        directory.reset(fd);
      }
      component = pos + 1;
    }
  }
  return ret;
}

int reopen_regular(int pinned_fd, const struct stat &pinned, FileDescriptor &file)
{
  int ret = OB_SUCCESS;
  char path[64];
  const int length = std::snprintf(path, sizeof(path), "/proc/self/fd/%d", pinned_fd);
  if (length < 0 || length >= sizeof(path)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    FileDescriptor readable(::open(path, O_RDONLY | O_CLOEXEC | O_NONBLOCK));
    struct stat actual = {};
    if (readable.get() < 0) {
      ret = errno == ENOENT || errno == ENOTDIR ? OB_NOT_SUPPORTED : file_error(errno);
    } else if (::fstat(readable.get(), &actual) != 0) {
      ret = file_error(errno);
    } else if (!S_ISREG(actual.st_mode) || actual.st_dev != pinned.st_dev || actual.st_ino != pinned.st_ino) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      file.reset(readable.detach());
    }
  }
  return ret;
}

int open_media(int root_fd, const ObString &relative, FileDescriptor &file)
{
  int ret = OB_SUCCESS;
  FileDescriptor directory(::fcntl(root_fd, F_DUPFD_CLOEXEC, 0));
  if (directory.get() < 0) {
    ret = file_error(errno);
  }
  int64_t component = 0;
  for (int64_t pos = 0; OB_SUCC(ret) && pos <= relative.length(); ++pos) {
    if (pos == relative.length() || relative.ptr()[pos] == '/') {
      char name[NAME_MAX + 1];
      const int64_t length = pos - component;
      std::memcpy(name, relative.ptr() + component, length);
      name[length] = '\0';
      const bool last = pos == relative.length();
      // O_PATH pins metadata without entering FIFO/device open operations before fstat.
      const int flags = O_CLOEXEC | O_NOFOLLOW | (last ? O_PATH : O_RDONLY | O_DIRECTORY);
      FileDescriptor opened(::openat(directory.get(), name, flags));
      struct stat actual = {};
      if (opened.get() < 0) {
        ret = file_error(errno);
      } else if (last && ::fstat(opened.get(), &actual) != 0) {
        ret = file_error(errno);
      } else if (last && !S_ISREG(actual.st_mode)) {
        ret = OB_OP_NOT_ALLOW;
      } else if (last) {
        // Only a kernel-owned procfd link is followed, never the mutable FILE pathname.
        ret = reopen_regular(opened.get(), actual, file);
      } else {
        directory.reset(opened.detach());
      }
      component = pos + 1;
    }
  }
  return ret;
}

bool same_file(const struct stat &before, const struct stat &after)
{
  return S_ISREG(after.st_mode) && before.st_dev == after.st_dev && before.st_ino == after.st_ino
      && before.st_size == after.st_size
      && before.st_mtim.tv_sec == after.st_mtim.tv_sec
      && before.st_mtim.tv_nsec == after.st_mtim.tv_nsec
      && before.st_ctim.tv_sec == after.st_ctim.tv_sec
      && before.st_ctim.tv_nsec == after.st_ctim.tv_nsec;
}

int unchanged_file(int fd, const struct stat &before)
{
  struct stat after = {};
  return ::fstat(fd, &after) != 0 ? file_error(errno)
      : (same_file(before, after) ? OB_SUCCESS : OB_STATE_NOT_MATCH);
}

uint16_t big16(const uint8_t *data)
{
  return static_cast<uint16_t>((static_cast<uint16_t>(data[0]) << 8) | data[1]);
}

uint32_t big32(const uint8_t *data)
{
  return (static_cast<uint32_t>(data[0]) << 24) | (static_cast<uint32_t>(data[1]) << 16)
      | (static_cast<uint32_t>(data[2]) << 8) | data[3];
}

uint16_t little16(const uint8_t *data)
{
  return static_cast<uint16_t>((static_cast<uint16_t>(data[1]) << 8) | data[0]);
}

uint32_t little32(const uint8_t *data)
{
  return (static_cast<uint32_t>(data[3]) << 24) | (static_cast<uint32_t>(data[2]) << 16)
      | (static_cast<uint32_t>(data[1]) << 8) | data[0];
}

int image_dimensions(int64_t width, int64_t height)
{
  return width <= 0 || height <= 0 ? OB_INVALID_DATA
      : (width > MAX_IMAGE_DIMENSION || height > MAX_IMAGE_DIMENSION
         || width * height > MAX_IMAGE_PIXELS ? OB_SIZE_OVERFLOW : OB_SUCCESS);
}

int validate_png(const uint8_t *data, int64_t size, const SemanticMediaRuntime &runtime)
{
  static const uint8_t signature[] = {137, 80, 78, 71, 13, 10, 26, 10};
  int ret = size < 33 || 0 != std::memcmp(data, signature, sizeof(signature))
      ? OB_INVALID_DATA : OB_SUCCESS;
  int64_t pos = 8;
  int64_t chunks = 0;
  int64_t idat_bytes = 0;
  uint8_t zlib_header[2] = {};
  uint8_t depth = 0;
  uint8_t color = 0;
  bool header = false;
  bool palette = false;
  bool idat = false;
  bool idat_ended = false;
  bool ended = false;
  while (OB_SUCC(ret) && pos < size && !ended) {
    if (OB_FAIL(runtime.status())) {
    } else if (++chunks > MAX_FORMAT_CHUNKS || size - pos < 12) {
      ret = OB_INVALID_DATA;
    } else {
      const int64_t length = big32(data + pos);
      const uint8_t *type = data + pos + 4;
      const uint8_t *payload = data + pos + 8;
      if (length > size - pos - 12) {
        ret = OB_INVALID_DATA;
      }
      for (int i = 0; OB_SUCC(ret) && i < 4; ++i) {
        if (!((type[i] >= 'A' && type[i] <= 'Z') || (type[i] >= 'a' && type[i] <= 'z'))
            || (i == 2 && (type[i] & 0x20) != 0)) {
          ret = OB_INVALID_DATA;
        }
      }
      uLong checksum = ::crc32(0, Z_NULL, 0);
      for (int64_t offset = 0; OB_SUCC(ret) && offset < length + 4;) {
        if (OB_FAIL(runtime.status())) {
        } else {
          const int64_t count = std::min(READ_CHUNK_BYTES, length + 4 - offset);
          checksum = ::crc32(checksum, type + offset, static_cast<uInt>(count));
          offset += count;
        }
      }
      if (OB_SUCC(ret) && static_cast<uint32_t>(checksum) != big32(payload + length)) {
        ret = OB_CHECKSUM_ERROR;
      }
      if (OB_FAIL(ret)) {
      } else if (0 == std::memcmp(type, "IHDR", 4)) {
        if (header || pos != 8 || length != 13) {
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(image_dimensions(big32(payload), big32(payload + 4)))) {
        } else {
          depth = payload[8];
          color = payload[9];
          const bool valid_depth = color == 0 ? depth == 1 || depth == 2 || depth == 4
                  || depth == 8 || depth == 16
              : color == 3 ? depth == 1 || depth == 2 || depth == 4 || depth == 8
              : (color == 2 || color == 4 || color == 6) && (depth == 8 || depth == 16);
          if (!valid_depth || payload[10] != 0 || payload[11] != 0 || payload[12] > 1) {
            ret = OB_INVALID_DATA;
          } else {
            header = true;
          }
        }
      } else if (!header) {
        ret = OB_INVALID_DATA;
      } else if (0 == std::memcmp(type, "PLTE", 4)) {
        if (palette || idat || length == 0 || length > 768 || length % 3 != 0
            || color == 0 || color == 4 || (color == 3 && length / 3 > (1 << depth))) {
          ret = OB_INVALID_DATA;
        } else {
          palette = true;
        }
      } else if (0 == std::memcmp(type, "IDAT", 4)) {
        if (idat_ended || (color == 3 && !palette)) {
          ret = OB_INVALID_DATA;
        } else {
          for (int64_t i = 0; i < length && idat_bytes + i < 2; ++i) {
            zlib_header[idat_bytes + i] = payload[i];
          }
          idat_bytes += length;
          idat = true;
        }
      } else if (0 == std::memcmp(type, "IEND", 4)) {
        if (length != 0 || !idat || idat_bytes < 6 || pos + 12 != size
            || (zlib_header[0] & 0x0f) != 8 || (zlib_header[0] >> 4) > 7
            || ((zlib_header[0] << 8) | zlib_header[1]) % 31 != 0
            || (zlib_header[1] & 0x20) != 0) {
          ret = OB_INVALID_DATA;
        } else {
          ended = true;
        }
      } else {
        idat_ended = idat;
        if ((type[0] & 0x20) == 0) {
          ret = OB_NOT_SUPPORTED;
        }
      }
      pos += length + 12;
    }
  }
  return OB_SUCC(ret) && !ended ? OB_INVALID_DATA : ret;
}

int validate_jpeg(const uint8_t *data, int64_t size, const SemanticMediaRuntime &runtime)
{
  int ret = size < 4 || data[0] != 0xff || data[1] != 0xd8 ? OB_INVALID_DATA : OB_SUCCESS;
  bool quantization[4] = {};
  bool huffman[2][4] = {};
  uint8_t component_ids[4] = {};
  uint8_t component_quantization[4] = {};
  int component_count = 0;
  uint32_t initial_components = 0;
  int64_t scans = 0;
  int64_t markers = 0;
  int64_t pos = 2;
  bool progressive = false;
  bool ended = false;
  uint16_t restart_interval = 0;
  while (OB_SUCC(ret) && pos < size && !ended) {
    if (OB_FAIL(runtime.status())) {
    } else if (++markers > MAX_FORMAT_CHUNKS || data[pos++] != 0xff) {
      ret = OB_INVALID_DATA;
    } else {
      while (pos < size && data[pos] == 0xff) {
        ++pos;
      }
      if (pos >= size) {
        ret = OB_INVALID_DATA;
        break;
      }
      const uint8_t marker = data[pos++];
      if (marker == 0xd9) {
        ended = scans > 0 && component_count > 0
            && initial_components == ((1U << component_count) - 1) && pos == size;
        if (!ended) {
          ret = OB_INVALID_DATA;
        }
      } else if (marker == 0 || marker == 0xd8 || marker == 1
                 || (marker >= 0xd0 && marker <= 0xd7) || size - pos < 2) {
        ret = OB_INVALID_DATA;
      } else {
        const int64_t length = big16(data + pos);
        if (length < 2 || length > size - pos) {
          ret = OB_INVALID_DATA;
          break;
        }
        const uint8_t *payload = data + pos + 2;
        const int64_t payload_size = length - 2;
        pos += length;
        if (marker == 0xdb) {
          int64_t offset = 0;
          while (OB_SUCC(ret) && offset < payload_size) {
            const uint8_t descriptor = payload[offset++];
            const int precision = descriptor >> 4;
            const int id = descriptor & 0xf;
            const int64_t table_size = 64 * (precision + 1);
            if (precision > 1 || id > 3 || table_size > payload_size - offset) {
              ret = OB_INVALID_DATA;
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < 64; ++i) {
                if ((precision == 0 ? payload[offset + i] : big16(payload + offset + i * 2)) == 0) {
                  ret = OB_INVALID_DATA;
                }
              }
              quantization[id] = true;
              offset += table_size;
            }
          }
          if (payload_size == 0) {
            ret = OB_INVALID_DATA;
          }
        } else if (marker == 0xc4) {
          int64_t offset = 0;
          while (OB_SUCC(ret) && offset < payload_size) {
            if (payload_size - offset < 17) {
              ret = OB_INVALID_DATA;
            } else {
              const uint8_t descriptor = payload[offset++];
              const int kind = descriptor >> 4;
              const int id = descriptor & 0xf;
              int symbols = 0;
              int available = 1;
              for (int i = 0; i < 16; ++i) {
                symbols += payload[offset + i];
                available = available * 2 - payload[offset + i];
                if (available < 0) {
                  ret = OB_INVALID_DATA;
                }
              }
              offset += 16;
              if (kind > 1 || id > 3 || symbols <= 0 || symbols > 256
                  || symbols > payload_size - offset) {
                ret = OB_INVALID_DATA;
              }
              for (int i = 0; OB_SUCC(ret) && i < symbols; ++i) {
                const uint8_t symbol = payload[offset + i];
                if ((kind == 0 && symbol > 11) || (kind == 1 && (symbol & 0xf) > 10)) {
                  ret = OB_INVALID_DATA;
                }
              }
              if (OB_SUCC(ret)) {
                huffman[kind][id] = true;
                offset += symbols;
              }
            }
          }
          if (payload_size == 0) {
            ret = OB_INVALID_DATA;
          }
        } else if (marker == 0xc0 || marker == 0xc2) {
          if (component_count != 0 || scans != 0 || payload_size < 6 || payload[0] != 8
              || payload[5] < 1 || payload[5] > 4 || payload_size != 6 + 3 * payload[5]) {
            ret = OB_INVALID_DATA;
          } else if (OB_FAIL(image_dimensions(big16(payload + 3), big16(payload + 1)))) {
          } else {
            component_count = payload[5];
            progressive = marker == 0xc2;
            int blocks = 0;
            for (int i = 0; OB_SUCC(ret) && i < component_count; ++i) {
              component_ids[i] = payload[6 + i * 3];
              component_quantization[i] = payload[8 + i * 3];
              const int horizontal = payload[7 + i * 3] >> 4;
              const int vertical = payload[7 + i * 3] & 0xf;
              blocks += horizontal * vertical;
              if (horizontal < 1 || horizontal > 4 || vertical < 1 || vertical > 4
                  || blocks > 10 || component_quantization[i] > 3) {
                ret = OB_INVALID_DATA;
              }
              for (int j = 0; OB_SUCC(ret) && j < i; ++j) {
                if (component_ids[j] == component_ids[i]) {
                  ret = OB_INVALID_DATA;
                }
              }
            }
          }
        } else if (marker == 0xdd) {
          if (payload_size != 2) {
            ret = OB_INVALID_DATA;
          } else {
            restart_interval = big16(payload);
          }
        } else if (marker == 0xda) {
          if (component_count == 0 || payload_size < 6 || payload[0] == 0
              || payload[0] > component_count || payload_size != 4 + 2 * payload[0]) {
            ret = OB_INVALID_DATA;
          } else {
            const int count = payload[0];
            const uint8_t first = payload[1 + count * 2];
            const uint8_t last = payload[2 + count * 2];
            const uint8_t approximation = payload[3 + count * 2];
            if ((!progressive && (first != 0 || last != 63 || approximation != 0))
                || (progressive && (first > last || last > 63 || (first == 0 && last != 0)
                    || (first != 0 && count != 1) || (approximation >> 4) > 13
                    || (approximation & 0xf) > 13
                    || ((approximation >> 4) != 0
                        && (approximation >> 4) != (approximation & 0xf) + 1)))) {
              ret = OB_INVALID_DATA;
            }
            uint32_t selected = 0;
            for (int i = 0; OB_SUCC(ret) && i < count; ++i) {
              int component = 0;
              while (component < component_count && component_ids[component] != payload[1 + i * 2]) {
                ++component;
              }
              const int dc = payload[2 + i * 2] >> 4;
              const int ac = payload[2 + i * 2] & 0xf;
              if (component == component_count || dc > 3 || ac > 3
                  || (selected & (1U << component)) != 0
                  || !quantization[component_quantization[component]]
                  || (first == 0 && (approximation >> 4) == 0 && !huffman[0][dc])
                  || (last > 0 && !huffman[1][ac])) {
                ret = OB_INVALID_DATA;
              } else {
                selected |= 1U << component;
              }
            }
            if (OB_SUCC(ret) && first == 0 && (approximation >> 4) == 0) {
              if ((initial_components & selected) != 0) {
                ret = OB_INVALID_DATA;
              } else {
                initial_components |= selected;
              }
            }
            bool entropy = false;
            int next_restart = 0;
            int64_t next_status_pos = pos;
            while (OB_SUCC(ret) && pos < size) {
              if (pos >= next_status_pos) {
                ret = runtime.status();
                next_status_pos = pos + READ_CHUNK_BYTES;
              }
              if (OB_FAIL(ret)) {
              } else if (data[pos] != 0xff) {
                entropy = true;
                ++pos;
              } else if (pos + 1 >= size) {
                ret = OB_INVALID_DATA;
              } else if (data[pos + 1] == 0) {
                entropy = true;
                pos += 2;
              } else if (data[pos + 1] >= 0xd0 && data[pos + 1] <= 0xd7) {
                if (restart_interval == 0 || data[pos + 1] != 0xd0 + next_restart) {
                  ret = OB_INVALID_DATA;
                } else {
                  next_restart = (next_restart + 1) % 8;
                  pos += 2;
                }
              } else {
                break;
              }
            }
            if (OB_SUCC(ret) && !entropy) {
              ret = OB_INVALID_DATA;
            }
            ++scans;
          }
        } else if ((marker >= 0xe0 && marker <= 0xef) || marker == 0xfe) {
        } else {
          ret = OB_NOT_SUPPORTED;
        }
      }
    }
  }
  return OB_SUCC(ret) && !ended ? OB_INVALID_DATA : ret;
}

int validate_wav(const uint8_t *data, int64_t size, const SemanticMediaRuntime &runtime)
{
  int ret = size < 12 || std::memcmp(data, "RIFF", 4) != 0
      || std::memcmp(data + 8, "WAVE", 4) != 0 || little32(data + 4) != size - 8
      ? OB_INVALID_DATA : OB_SUCCESS;
  bool format = false;
  bool samples = false;
  int64_t block_align = 0;
  int64_t pos = 12;
  int64_t chunks = 0;
  while (OB_SUCC(ret) && pos < size) {
    if (OB_FAIL(runtime.status())) {
    } else if (++chunks > MAX_FORMAT_CHUNKS || size - pos < 8) {
      ret = OB_INVALID_DATA;
    } else {
      const int64_t length = little32(data + pos + 4);
      const uint8_t *payload = data + pos + 8;
      const int64_t padded = length + (length & 1);
      if (padded > size - pos - 8) {
        ret = OB_INVALID_DATA;
      } else if (std::memcmp(data + pos, "fmt ", 4) == 0) {
        if (format || length < 16 || (length > 16 && (length < 18 || little16(payload + 16) > length - 18))) {
          ret = OB_INVALID_DATA;
        } else {
          uint16_t kind = little16(payload);
          const int64_t channels = little16(payload + 2);
          const int64_t rate = little32(payload + 4);
          const int64_t bytes_per_second = little32(payload + 8);
          block_align = little16(payload + 12);
          const int64_t bits = little16(payload + 14);
          if (kind == 0xfffe) {
            static const uint8_t guid_suffix[] = {0, 0, 0, 0, 16, 0, 128, 0, 0, 170, 0, 56, 155, 113};
            if (length < 40 || little16(payload + 16) < 22
                || little16(payload + 18) == 0 || little16(payload + 18) > bits
                || std::memcmp(payload + 26, guid_suffix, sizeof(guid_suffix)) != 0) {
              ret = OB_INVALID_DATA;
            } else {
              kind = little16(payload + 24);
            }
          }
          if (OB_FAIL(ret)) {
          } else if (kind != 1 && kind != 3) {
            ret = OB_NOT_SUPPORTED;
          } else if (channels <= 0 || channels > 32 || rate <= 0 || rate > 384000
              || (kind == 1 && bits != 8 && bits != 16 && bits != 24 && bits != 32)
              || (kind == 3 && bits != 32 && bits != 64)
              || block_align != channels * (bits / 8) || bytes_per_second != rate * block_align) {
            ret = OB_INVALID_DATA;
          } else {
            format = true;
          }
        }
      } else if (std::memcmp(data + pos, "data", 4) == 0) {
        if (!format || samples || length == 0 || length % block_align != 0) {
          ret = OB_INVALID_DATA;
        } else {
          samples = true;
        }
      }
      pos += 8 + padded;
    }
  }
  return OB_SUCC(ret) && (!format || !samples || pos != size) ? OB_INVALID_DATA : ret;
}

int validate_mp3(const uint8_t *data, int64_t size, const SemanticMediaRuntime &runtime)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (size >= 3 && std::memcmp(data, "ID3", 3) == 0) {
    if (size < 10 || data[3] < 2 || data[3] > 4 || data[4] == 0xff
        || (data[5] & (data[3] == 4 ? 0x0f : data[3] == 3 ? 0x1f : 0x3f)) != 0
        || ((data[6] | data[7] | data[8] | data[9]) & 0x80) != 0) {
      ret = OB_INVALID_DATA;
    } else {
      const int64_t length = (static_cast<int64_t>(data[6]) << 21)
          | (static_cast<int64_t>(data[7]) << 14) | (static_cast<int64_t>(data[8]) << 7) | data[9];
      pos = 10 + length + (data[3] == 4 && (data[5] & 0x10) != 0 ? 10 : 0);
      if (pos > size) {
        ret = OB_INVALID_DATA;
      } else if (data[3] == 4 && (data[5] & 0x10) != 0
                 && (std::memcmp(data + pos - 10, "3DI", 3) != 0
                     || std::memcmp(data + pos - 7, data + 3, 7) != 0)) {
        ret = OB_INVALID_DATA;
      }
    }
  }
  static const int bitrates_v1[] = {0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320};
  static const int bitrates_v2[] = {0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160};
  static const int sample_rates[] = {44100, 48000, 32000};
  int version = -1;
  int rate = 0;
  int64_t frames = 0;
  while (OB_SUCC(ret) && pos < size) {
    if (OB_FAIL(runtime.status())) {
    } else if (size - pos == 128 && std::memcmp(data + pos, "TAG", 3) == 0 && frames > 0) {
      pos = size;
    } else if (size - pos < 4 || data[pos] != 0xff || (data[pos + 1] & 0xe0) != 0xe0) {
      ret = OB_INVALID_DATA;
    } else {
      const int current_version = (data[pos + 1] >> 3) & 3;
      const int layer = (data[pos + 1] >> 1) & 3;
      const int bitrate_index = data[pos + 2] >> 4;
      const int rate_index = (data[pos + 2] >> 2) & 3;
      const int padding = (data[pos + 2] >> 1) & 1;
      const bool mono = (data[pos + 3] >> 6) == 3;
      const bool has_crc = (data[pos + 1] & 1) == 0;
      if (current_version == 1 || layer != 1 || bitrate_index == 0 || bitrate_index == 15
          || rate_index == 3 || (data[pos + 3] & 3) == 2) {
        ret = OB_INVALID_DATA;
      } else {
        const int current_rate = sample_rates[rate_index]
            / (current_version == 3 ? 1 : current_version == 2 ? 2 : 4);
        const int bitrate = current_version == 3 ? bitrates_v1[bitrate_index] : bitrates_v2[bitrate_index];
        const int64_t length = (current_version == 3 ? 144000 : 72000) * bitrate / current_rate + padding;
        const int64_t side_info = current_version == 3 ? (mono ? 17 : 32) : (mono ? 9 : 17);
        if (length < 4 + (has_crc ? 2 : 0) + side_info || length > size - pos
            || (version >= 0 && (version != current_version || rate != current_rate))) {
          ret = OB_INVALID_DATA;
        } else {
          version = current_version;
          rate = current_rate;
          pos += length;
          ++frames;
        }
      }
    }
  }
  return OB_SUCC(ret) && frames == 0 ? OB_INVALID_DATA : ret;
}

enum class MediaKind { PNG, JPEG, WAV, MP3, UNKNOWN };

int validate_media(const uint8_t *data, int64_t size, const SemanticMediaRuntime &runtime,
                   MediaKind &kind, ObString &mime, ObString &audio)
{
  int ret = OB_SUCCESS;
  kind = MediaKind::UNKNOWN;
  if (size >= 8 && data[0] == 137 && std::memcmp(data + 1, "PNG\r\n\x1a\n", 7) == 0) {
    kind = MediaKind::PNG;
    mime = ObString::make_string("image/png");
    ret = validate_png(data, size, runtime);
  } else if (size >= 2 && data[0] == 0xff && data[1] == 0xd8) {
    kind = MediaKind::JPEG;
    mime = ObString::make_string("image/jpeg");
    ret = validate_jpeg(data, size, runtime);
  } else if (size >= 4 && std::memcmp(data, "RIFF", 4) == 0) {
    kind = MediaKind::WAV;
    mime = ObString::make_string("audio/wav");
    audio = ObString::make_string("wav");
    ret = validate_wav(data, size, runtime);
  } else if ((size >= 3 && std::memcmp(data, "ID3", 3) == 0)
             || (size >= 2 && data[0] == 0xff && (data[1] & 0xe0) == 0xe0)) {
    kind = MediaKind::MP3;
    mime = ObString::make_string("audio/mpeg");
    audio = ObString::make_string("mp3");
    ret = validate_mp3(data, size, runtime);
  } else {
    ret = OB_INVALID_DATA;
  }
  return ret;
}

int hex_digit(char ch)
{
  return ch >= '0' && ch <= '9' ? ch - '0'
      : ch >= 'a' && ch <= 'f' ? ch - 'a' + 10
      : ch >= 'A' && ch <= 'F' ? ch - 'A' + 10 : -1;
}

int validate_version(const ObString &version)
{
  int ret = OB_SUCCESS;
  if (version.empty()) {
  } else if (version.length() < 7 || std::memcmp(version.ptr(), "sha256:", 7) != 0) {
    ret = OB_NOT_SUPPORTED;
  } else if (version.length() != 71) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int i = 7; OB_SUCC(ret) && i < version.length(); ++i) {
      if (hex_digit(version.ptr()[i]) < 0) {
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  return ret;
}

int verify_version(const ObString &version, const uint8_t *data, int64_t size)
{
  int ret = OB_SUCCESS;
  if (!version.empty()) {
    char digest[32];
    int64_t output_length = 0;
    if (OB_FAIL(share::ObHashUtil::hash(share::OB_HASH_SH256,
            reinterpret_cast<const char *>(data), size, digest, sizeof(digest), output_length))) {
    } else if (output_length != sizeof(digest)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      for (int i = 0; OB_SUCC(ret) && i < output_length; ++i) {
        const int expected = (hex_digit(version.ptr()[7 + i * 2]) << 4)
            | hex_digit(version.ptr()[8 + i * 2]);
        if (static_cast<uint8_t>(digest[i]) != expected) {
          ret = OB_CHECKSUM_ERROR;
        }
      }
    }
  }
  return ret;
}
}

class SemanticMediaScheduler final : public lib::Threads
{
public:
  struct Job
  {
    SemanticMediaRuntime *runtime_ = nullptr;
    std::shared_ptr<SemanticMediaTask> task_;
  };

  SemanticMediaScheduler()
      : lib::Threads(SemanticMediaRuntime::WORKER_COUNT), jobs_{}, head_(0), count_(0),
        started_(false), stopping_(false) {}
  ~SemanticMediaScheduler() override { shutdown(); }

  static SemanticMediaScheduler &instance()
  {
    static SemanticMediaScheduler scheduler;
    return scheduler;
  }

  int ensure_started()
  {
    int ret = OB_SUCCESS;
    if (stopping_.load()) {
      ret = OB_CANCELED;
    } else if (!started_.load()) {
      std::unique_lock<std::mutex> lifecycle(lifecycle_mutex_, std::try_to_lock);
      if (!lifecycle.owns_lock()) {
        ret = stopping_.load() ? OB_CANCELED : OB_EAGAIN;
      } else if (stopping_.load()) {
        ret = OB_CANCELED;
      } else if (!started_.load()) {
        // No queue/query lock is held: Threads::start may join partially started workers.
        ret = start();
        if (ret == OB_ALLOCATE_MEMORY_FAILED) {
          ret = OB_SIZE_OVERFLOW;
        }
        started_.store(OB_SUCC(ret));
        if (OB_FAIL(ret)) {
          LOG_WARN("start semantic media executor failed", K(ret));
        }
      }
    }
    return ret;
  }

  int submit(SemanticMediaRuntime &runtime, const std::shared_ptr<SemanticMediaTask> &task)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    int ret = OB_SUCCESS;
    if (stopping_.load()) {
      ret = OB_CANCELED;
    } else if (count_ == jobs_.size()) {
      ret = OB_EAGAIN;
    } else {
      Job &job = jobs_[(head_ + count_) % jobs_.size()];
      job.runtime_ = &runtime;
      job.task_ = task;
      ++count_;
      changed_.notify_one();
    }
    return ret;
  }

  bool can_submit() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    bool ready = stopping_.load() || count_ < jobs_.size();
    if (ready && !stopping_.load() && !started_.load()) {
      std::unique_lock<std::mutex> lifecycle(lifecycle_mutex_, std::try_to_lock);
      ready = lifecycle.owns_lock() || started_.load() || stopping_.load();
    }
    return ready;
  }

  bool stopping() const { return stopping_.load(); }

  void shutdown()
  {
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopping_.store(true);
      stop();
    }
    changed_.notify_all();
    wait();
  }

private:
  void run1() override
  {
    lib::set_thread_name("SemanticMedia");
    for (;;) {
      Job job;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        changed_.wait_for(lock, std::chrono::milliseconds(20),
            [this] { return count_ > 0 || stopping_.load() || has_set_stop(); });
        if (count_ > 0) {
          job = std::move(jobs_[head_]);
          jobs_[head_] = Job{};
          head_ = (head_ + 1) % jobs_.size();
          --count_;
        } else if (stopping_.load() || has_set_stop()) {
          break;
        }
      }
      if (job.task_) {
        job.runtime_->run_task(std::move(job.task_));
      }
    }
  }

  mutable std::mutex lifecycle_mutex_;
  mutable std::mutex mutex_;
  std::condition_variable changed_;
  std::array<Job, SemanticMediaRuntime::QUEUE_CAPACITY> jobs_;
  size_t head_;
  size_t count_;
  std::atomic<bool> started_;
  std::atomic<bool> stopping_;
};

class SemanticRemoteMediaScheduler final : public lib::Threads
{
public:
  static constexpr int64_t ACTIVE_CAPACITY = 64;
  using Job = SemanticMediaScheduler::Job;

  SemanticRemoteMediaScheduler()
      : lib::Threads(1), jobs_{}, requests_{}, head_(0), count_(0),
        started_(false), stopping_(false) {}
  ~SemanticRemoteMediaScheduler() override { shutdown(); }

  static SemanticRemoteMediaScheduler &instance()
  {
    static SemanticRemoteMediaScheduler scheduler;
    return scheduler;
  }

  int ensure_started()
  {
    int ret = OB_SUCCESS;
    if (stopping_.load()) {
      ret = OB_CANCELED;
    } else if (!started_.load()) {
      std::unique_lock<std::mutex> lifecycle(lifecycle_mutex_, std::try_to_lock);
      if (!lifecycle.owns_lock()) {
        ret = stopping_.load() ? OB_CANCELED : OB_EAGAIN;
      } else if (stopping_.load()) {
        ret = OB_CANCELED;
      } else if (!started_.load()) {
        const curl_version_info_data *version = curl_version_info(CURLVERSION_NOW);
        if (nullptr == version || (version->features & CURL_VERSION_ASYNCHDNS) == 0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("remote media requires curl asynchronous DNS", K(ret));
        } else {
          ret = start();
          if (ret == OB_ALLOCATE_MEMORY_FAILED) {
            ret = OB_SIZE_OVERFLOW;
          }
          started_.store(OB_SUCC(ret));
          if (OB_FAIL(ret)) {
            LOG_WARN("start remote media executor failed", K(ret));
          }
        }
      }
    }
    return ret;
  }

  int submit(SemanticMediaRuntime &runtime, const std::shared_ptr<SemanticMediaTask> &task)
  {
    int ret = OB_SUCCESS;
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_.load()) {
      ret = OB_CANCELED;
    } else if (count_ == jobs_.size()) {
      ret = OB_EAGAIN;
    } else {
      jobs_[(head_ + count_) % jobs_.size()] = Job{&runtime, task};
      ++count_;
      changed_.notify_one();
    }
    return ret;
  }

  bool can_submit() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return stopping_.load() || count_ < jobs_.size();
  }

  bool stopping() const { return stopping_.load(); }
  void wake() { changed_.notify_one(); }

  void shutdown()
  {
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
    stopping_.store(true);
    stop();
    changed_.notify_all();
    wait();
  }

private:
  struct Request
  {
    Job job_;
    CURL *handle_ = nullptr;
    char url_[MAX_URI_BYTES + 1] = {};
    int ret_ = OB_SUCCESS;
    long http_code_ = 0;
    int64_t expected_size_ = -1;
    int64_t header_bytes_ = 0;
    int64_t charged_bytes_ = 0;
    bool attached_ = false;
    bool fetched_ = false;
  };

  static int http_error(long code)
  {
    return code == 404 ? OB_FILE_NOT_EXIST
        : code == 401 || code == 403 || (code >= 300 && code < 400) ? OB_OP_NOT_ALLOW
        : code == 200 ? OB_SUCCESS : OB_IO_ERROR;
  }

  static size_t write_callback(char *data, size_t size, size_t count, void *context)
  {
    auto &request = *static_cast<Request *>(context);
    int ret = request.job_.runtime_->status();
    if (OB_SUCC(ret) && request.http_code_ != 200) {
      ret = http_error(request.http_code_);
    }
    if (OB_SUCC(ret) && (size > INT64_MAX || count > INT64_MAX ||
        (size > 0 && count > static_cast<size_t>(INT64_MAX) / size))) {
      ret = OB_SIZE_OVERFLOW;
    }
    if (OB_SUCC(ret)) {
      ret = request.job_.runtime_->append_remote_data(*request.job_.task_, data, size * count);
    }
    if (OB_FAIL(ret)) {
      request.ret_ = ret;
    }
    return OB_SUCC(ret) ? size * count : 0;
  }

  static size_t header_callback(char *data, size_t size, size_t count, void *context)
  {
    auto &request = *static_cast<Request *>(context);
    int ret = request.job_.runtime_->status();
    if (OB_SUCC(ret) && (size > MAX_REMOTE_HEADER_BYTES || count > MAX_REMOTE_HEADER_BYTES ||
        (size > 0 && count > static_cast<size_t>(MAX_REMOTE_HEADER_BYTES) / size))) {
      ret = OB_SIZE_OVERFLOW;
    }
    const int64_t length = OB_SUCC(ret) ? size * count : 0;
    if (OB_SUCC(ret) && request.header_bytes_ > MAX_REMOTE_HEADER_BYTES - length) {
      ret = OB_SIZE_OVERFLOW;
    }
    if (OB_SUCC(ret)) {
      request.header_bytes_ += length;
      if (length >= 5 && std::memcmp(data, "HTTP/", 5) == 0) {
        int64_t offset = 5;
        while (offset < length && data[offset] != ' ') {
          ++offset;
        }
        if (offset + 4 >= length || data[offset + 1] < '0' || data[offset + 1] > '9'
            || data[offset + 2] < '0' || data[offset + 2] > '9'
            || data[offset + 3] < '0' || data[offset + 3] > '9') {
          ret = OB_INVALID_DATA;
        } else {
          request.http_code_ = (data[offset + 1] - '0') * 100 +
                               (data[offset + 2] - '0') * 10 + data[offset + 3] - '0';
          request.expected_size_ = -1;
        }
      } else {
        int64_t colon = 0;
        while (colon < length && data[colon] != ':') {
          ++colon;
        }
        if (colon < length && request.http_code_ == 200) {
          ObString name(colon, data);
          ObString value(length - colon - 1, data + colon + 1);
          value = value.trim();
          if (equals_mime(name, "content-length")) {
            int64_t bytes = 0;
            if (value.empty() || request.expected_size_ != -1) {
              ret = OB_INVALID_DATA;
            }
            for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
              const char digit = value.ptr()[i];
              if (digit < '0' || digit > '9' || bytes > (INT64_MAX - (digit - '0')) / 10) {
                ret = OB_INVALID_DATA;
              } else {
                bytes = bytes * 10 + digit - '0';
              }
            }
            if (OB_SUCC(ret)) {
              const auto &task = *request.job_.task_;
              if (bytes > request.job_.runtime_->max_bytes_ || bytes > MAX_SOURCE_BYTES) {
                ret = OB_SIZE_OVERFLOW;
              } else if (task.declared_size_ >= 0 && bytes != task.declared_size_) {
                ret = OB_INVALID_DATA;
              } else {
                request.expected_size_ = bytes;
              }
            }
          } else if (equals_mime(name, "content-encoding") &&
                     !equals_mime(value, "identity")) {
            ret = OB_NOT_SUPPORTED;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      request.ret_ = ret;
    }
    return OB_SUCC(ret) ? length : 0;
  }

  static int progress_callback(void *context, curl_off_t, curl_off_t, curl_off_t, curl_off_t)
  {
    auto &request = *static_cast<Request *>(context);
    const int ret = request.job_.runtime_->status();
    if (OB_SUCCESS != ret) {
      request.ret_ = ret;
    }
    return OB_SUCCESS == ret ? 0 : 1;
  }

  int start_request(CURLM *multi, Request &request)
  {
    auto &runtime = *request.job_.runtime_;
    auto &task = *request.job_.task_;
    runtime.started_count_.fetch_add(1);
    int ret = runtime.status();
    const int64_t charge = REMOTE_REQUEST_BYTES + task.uri_.length() + 1;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(runtime.reserve_with_eviction(charge))) {
    } else {
      request.charged_bytes_ = charge;
      task.charged_bytes_ += charge;
      std::memcpy(request.url_, task.uri_.ptr(), task.uri_.length());
      request.url_[task.uri_.length()] = '\0';
      if (nullptr == (request.handle_ = curl_easy_init())) {
        ret = OB_CURL_ERROR;
      } else {
        CURL *handle = request.handle_;
        const int64_t remaining_ms = std::max<int64_t>(1,
            std::min<int64_t>(LONG_MAX, (runtime.deadline_ - ObTimeUtility::current_time()) / 1000));
        if (CURLE_OK != curl_easy_setopt(handle, CURLOPT_URL, request.url_) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_HTTPGET, 1L) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1L) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_PATH_AS_IS, 1L) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 0L) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_PROTOCOLS,
                                        static_cast<long>(CURLPROTO_HTTP | CURLPROTO_HTTPS)) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_REDIR_PROTOCOLS,
                                        static_cast<long>(CURLPROTO_HTTPS)) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_PROXY, "") ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_NOPROXY, "*") ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_SSL_VERIFYPEER, 1L) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_SSL_VERIFYHOST, 2L) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_ACCEPT_ENCODING, "identity") ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_HTTP_CONTENT_DECODING, 0L) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_MAXFILESIZE_LARGE,
                                        static_cast<curl_off_t>(runtime.max_bytes_)) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_CONNECTTIMEOUT_MS,
                                        std::min<int64_t>(10000, remaining_ms)) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_TIMEOUT_MS, remaining_ms) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, write_callback) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_WRITEDATA, &request) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_HEADERFUNCTION, header_callback) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_HEADERDATA, &request) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_NOPROGRESS, 0L) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_XFERINFOFUNCTION, progress_callback) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_XFERINFODATA, &request) ||
            CURLE_OK != curl_easy_setopt(handle, CURLOPT_PRIVATE, &request) ||
            CURLM_OK != curl_multi_add_handle(multi, handle)) {
          ret = OB_CURL_ERROR;
        } else {
          request.attached_ = true;
        }
      }
    }
    return ret;
  }

  void close_request(CURLM *multi, Request &request)
  {
    if (request.handle_ != nullptr) {
      if (request.attached_ && CURLM_OK != curl_multi_remove_handle(multi, request.handle_)) {
        const int ret = OB_CURL_ERROR;
        LOG_WARN("remove remote media request failed", K(ret));
      }
      curl_easy_cleanup(request.handle_);
      request.handle_ = nullptr;
      request.attached_ = false;
    }
    if (request.charged_bytes_ > 0) {
      request.job_.task_->charged_bytes_ -= request.charged_bytes_;
      request.job_.runtime_->budget_->release(request.charged_bytes_);
      request.charged_bytes_ = 0;
    }
  }

  void fail_request(CURLM *multi, Request &request, int ret)
  {
    close_request(multi, request);
    request.job_.runtime_->complete_task(std::move(request.job_.task_), ret);
    request = Request{};
  }

  void finish_request(CURLM *multi, Request &request, CURLcode code)
  {
    int ret = request.job_.runtime_->status();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(request.ret_)) {
    } else if (request.http_code_ != 0 && request.http_code_ != 200) {
      ret = http_error(request.http_code_);
    } else if (code != CURLE_OK) {
      ret = code == CURLE_OPERATION_TIMEDOUT ? OB_TIMEOUT
          : code == CURLE_FILESIZE_EXCEEDED ? OB_SIZE_OVERFLOW
          : code == CURLE_PARTIAL_FILE ? OB_INVALID_DATA : OB_CURL_ERROR;
    } else if (request.http_code_ != 200) {
      ret = OB_INVALID_DATA;
    } else {
      const auto &task = *request.job_.task_;
      if (task.source_size_ <= 0 ||
          (request.expected_size_ >= 0 && task.source_size_ != request.expected_size_) ||
          (task.declared_size_ >= 0 && task.source_size_ != task.declared_size_)) {
        ret = OB_INVALID_DATA;
      }
    }
    close_request(multi, request);
    if (OB_FAIL(ret)) {
      LOG_WARN("remote media GET failed", K(ret), "http_code", request.http_code_,
               "curl_code", static_cast<int>(code), "curl_error", curl_easy_strerror(code));
      fail_request(multi, request, ret);
    } else {
      request.fetched_ = true;
    }
  }

  void run1() override
  {
    lib::set_thread_name("SemanticFetch");
    CURLM *multi = curl_multi_init();
    int ret = nullptr == multi ? OB_CURL_ERROR : OB_SUCCESS;
    if (OB_SUCCESS != ret) {
      LOG_WARN("initialize remote media curl multi failed", K(ret));
    }
    if (OB_SUCC(ret) &&
        (CURLM_OK != curl_multi_setopt(multi, CURLMOPT_MAX_TOTAL_CONNECTIONS, ACTIVE_CAPACITY) ||
         CURLM_OK != curl_multi_setopt(multi, CURLMOPT_MAXCONNECTS, ACTIVE_CAPACITY))) {
      ret = OB_CURL_ERROR;
      LOG_WARN("configure remote media curl multi failed", K(ret));
    }
    for (;;) {
      for (auto &request : requests_) {
        if (!request.job_.task_) {
          {
            std::lock_guard<std::mutex> lock(mutex_);
            if (count_ > 0) {
              request.job_ = std::move(jobs_[head_]);
              jobs_[head_] = Job{};
              head_ = (head_ + 1) % jobs_.size();
              --count_;
            }
          }
          if (request.job_.task_) {
            const int started = OB_SUCCESS == ret ? start_request(multi, request) : ret;
            if (OB_SUCCESS != started) {
              fail_request(multi, request, started);
            }
          }
        } else if (request.job_.runtime_->status() != OB_SUCCESS) {
          fail_request(multi, request, request.job_.runtime_->status());
        } else if (request.fetched_) {
          const int submitted = SemanticMediaScheduler::instance().submit(
              *request.job_.runtime_, request.job_.task_);
          if (OB_SUCCESS == submitted) {
            request = Request{};
          } else if (OB_EAGAIN != submitted) {
            fail_request(multi, request, submitted);
          }
        }
      }
      int running = 0;
      if (OB_SUCC(ret)) {
        const CURLMcode code = curl_multi_perform(multi, &running);
        if (CURLM_OK != code) {
          ret = OB_CURL_ERROR;
          LOG_WARN("drive remote media network failed", K(ret), "curl_code", static_cast<int>(code));
        }
      }
      if (OB_SUCCESS != ret) {
        for (auto &request : requests_) {
          if (request.job_.task_) {
            fail_request(multi, request, ret);
          }
        }
      } else {
        int messages = 0;
        CURLMsg *message = nullptr;
        while (nullptr != (message = curl_multi_info_read(multi, &messages))) {
          if (message->msg == CURLMSG_DONE) {
            Request *request = nullptr;
            if (CURLE_OK != curl_easy_getinfo(message->easy_handle, CURLINFO_PRIVATE, &request) ||
                nullptr == request || !request->job_.task_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("remote media completion has no owned request", K(ret));
            } else {
              finish_request(multi, *request, message->data.result);
            }
          }
        }
      }
      const bool active = std::any_of(requests_.begin(), requests_.end(),
          [](const Request &request) { return static_cast<bool>(request.job_.task_); });
      {
        std::unique_lock<std::mutex> lock(mutex_);
        if (stopping_.load() && count_ == 0 && !active) {
          break;
        } else if (running == 0) {
          changed_.wait_for(lock, std::chrono::milliseconds(10));
        }
      }
      if (running > 0 && OB_SUCCESS == ret) {
        int descriptors = 0;
        if (CURLM_OK != curl_multi_poll(multi, nullptr, 0, 10, &descriptors)) {
          ret = OB_CURL_ERROR;
          LOG_WARN("poll remote media network failed", K(ret));
        }
      }
    }
    if (nullptr != multi) {
      curl_multi_cleanup(multi);
    }
  }

  mutable std::mutex lifecycle_mutex_;
  mutable std::mutex mutex_;
  std::condition_variable changed_;
  std::array<Job, SemanticMediaRuntime::QUEUE_CAPACITY> jobs_;
  std::array<Request, ACTIVE_CAPACITY> requests_;
  size_t head_;
  size_t count_;
  std::atomic<bool> started_;
  std::atomic<bool> stopping_;
};

SemanticMediaTask::SemanticMediaTask(std::shared_ptr<SemanticQueryBudget> budget,
                                     int64_t charged_bytes)
    : budget_(std::move(budget)), descriptor_buffer_(), base64_buffer_(), source_buffer_(),
      source_size_(0), source_capacity_(0), source_charge_(0), remote_(false), uri_(),
      declared_mime_type_(), object_version_(), declared_size_(-1), charged_bytes_(charged_bytes),
      base64_(), mime_type_(), audio_format_(), is_audio_(false), result_(OB_EAGAIN)
{}

SemanticMediaTask::~SemanticMediaTask()
{
  base64_buffer_.reset();
  source_buffer_.reset();
  descriptor_buffer_.reset();
  budget_->release(charged_bytes_);
}

bool SemanticMediaTask::ready() const { return result() != OB_EAGAIN; }
int SemanticMediaTask::result() const { return result_.load(std::memory_order_acquire); }
const ObString &SemanticMediaTask::base64() const
{
  return result() == OB_SUCCESS ? base64_ : empty_string();
}
const ObString &SemanticMediaTask::mime_type() const
{
  return result() == OB_SUCCESS ? mime_type_ : empty_string();
}
const ObString &SemanticMediaTask::audio_format() const
{
  return result() == OB_SUCCESS ? audio_format_ : empty_string();
}
bool SemanticMediaTask::is_audio() const { return result() == OB_SUCCESS && is_audio_; }
bool SemanticMediaTask::is_image() const { return result() == OB_SUCCESS && !is_audio_; }

SemanticMediaRuntime::SemanticMediaRuntime(std::shared_ptr<SemanticQueryBudget> budget,
    int64_t max_bytes, int64_t parallel, int64_t deadline, int64_t charged_bytes)
    : budget_(std::move(budget)), root_buffer_(), root_(), remote_root_buffer_(), remote_root_(),
      root_fd_(-1), max_bytes_(max_bytes),
      parallel_(parallel), deadline_(deadline), charged_bytes_(charged_bytes), registered_(false),
      cancel_ret_(OB_SUCCESS), mutex_(), drained_(), cache_{}, live_(), active_count_(0), waiting_sources_(0),
      cache_clock_(0), started_count_(0), loaded_count_(0), evicted_count_(0),
      test_hook_(nullptr), test_hook_context_(nullptr)
{}

bool SemanticMediaRuntime::MediaKeyLess::operator()(const MediaKey &left, const MediaKey &right) const
{
  const int uri_compare = left.uri_.compare(right.uri_);
  return uri_compare < 0 || (uri_compare == 0 && left.version_.compare(right.version_) < 0);
}

int SemanticMediaRuntime::matches_policy(const std::shared_ptr<SemanticQueryBudget> &budget,
    const ObString &root, const ObString &remote_root, int64_t max_bytes,
    int64_t parallel, int64_t deadline) const
{
  return budget_.get() != budget.get() || !same_string(root_, root) ||
      !same_string(remote_root_, remote_root) || max_bytes_ != max_bytes
      || parallel_ != parallel || deadline_ != deadline ? OB_ERR_UNEXPECTED : OB_SUCCESS;
}

int SemanticMediaRuntime::acquire(std::shared_ptr<SemanticQueryBudget> budget, const ObString &root,
    int64_t max_bytes, int64_t parallel, int64_t deadline, std::shared_ptr<SemanticMediaRuntime> &out)
{
  return acquire(std::move(budget), root, ObString(), max_bytes, parallel, deadline, out);
}

int SemanticMediaRuntime::acquire(std::shared_ptr<SemanticQueryBudget> budget, const ObString &root,
    const ObString &remote_root, int64_t max_bytes, int64_t parallel, int64_t deadline,
    std::shared_ptr<SemanticMediaRuntime> &out)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<SemanticMediaRuntime> previous = std::move(out);
  ObString normalized = root;
  while (normalized.length() > 1 && normalized.ptr()[normalized.length() - 1] == '/') {
    normalized.assign_ptr(normalized.ptr(), normalized.length() - 1);
  }
  if (!budget || max_bytes <= 0 || parallel <= 0 || deadline <= 0
      || budget->execution_id() == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
  } else if (root.empty() && remote_root.empty()) {
    ret = OB_OP_NOT_ALLOW;
  } else if (!root.empty() && OB_FAIL(canonical_path(normalized))) {
  } else if (OB_FAIL(FileRefCodec::validate_remote_root(remote_root))) {
  } else if (OB_FAIL(budget->failure())) {
  } else if (ObTimeUtility::current_time() >= deadline) {
    ret = OB_TIMEOUT;
  } else if (SemanticMediaScheduler::instance().stopping()) {
    ret = OB_CANCELED;
  }
  auto &registry = media_registry();
  std::shared_ptr<SemanticMediaRuntime> selected;
  std::shared_ptr<SemanticMediaRuntime> candidate;
  if (OB_SUCC(ret)) {
    // Serialize creation so parallel workers cannot transiently reserve multiple runtimes.
    std::lock_guard<std::mutex> lock(registry.mutex_);
    for (auto entry = registry.queries_.begin(); entry != registry.queries_.end();) {
      if (entry->second.expired()) {
        entry = registry.queries_.erase(entry);
      } else {
        ++entry;
      }
    }
    auto entry = registry.queries_.find(budget->execution_id());
    selected = entry == registry.queries_.end() ? nullptr : entry->second.lock();
    if (entry != registry.queries_.end() && !selected) {
      registry.queries_.erase(entry);
    }
    if (selected) {
      ret = selected->matches_policy(budget, normalized, remote_root, max_bytes, parallel, deadline);
      if (OB_SUCC(ret)) {
        ret = selected->status();
      }
    } else if (registry.queries_.size() >= MAX_QUERY_RUNTIMES) {
      ret = OB_EAGAIN;
    } else {
      const int64_t charge = sizeof(SemanticMediaRuntime) + normalized.length() + 1
          + ALLOCATION_OVERHEAD * 2 + REGISTRY_OVERHEAD +
          (remote_root.empty() ? 0 : remote_root.length() + 1 + ALLOCATION_OVERHEAD);
      if (OB_FAIL(budget->reserve(charge))) {
      } else {
        Reservation reservation(budget, charge);
        FileDescriptor root_fd;
        if (!root.empty() && OB_FAIL(open_root(normalized, root_fd))) {
        } else {
          try {
            auto *created = new SemanticMediaRuntime(budget, max_bytes, parallel, deadline, charge);
            reservation.detach();
            candidate.reset(created);
            candidate->root_buffer_.reset(new char[normalized.length() + 1]);
            if (!normalized.empty()) {
              std::memcpy(candidate->root_buffer_.get(), normalized.ptr(), normalized.length());
            }
            candidate->root_buffer_[normalized.length()] = '\0';
            candidate->root_.assign_ptr(candidate->root_buffer_.get(), normalized.length());
            candidate->root_fd_ = root_fd.detach();
            if (!remote_root.empty()) {
              candidate->remote_root_buffer_.reset(new char[remote_root.length() + 1]);
              std::memcpy(candidate->remote_root_buffer_.get(), remote_root.ptr(), remote_root.length());
              candidate->remote_root_buffer_[remote_root.length()] = '\0';
              candidate->remote_root_.assign_ptr(candidate->remote_root_buffer_.get(), remote_root.length());
            }
            if (OB_FAIL(candidate->status())) {
            } else if (!registry.queries_.emplace(budget->execution_id(), candidate).second) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              candidate->registered_ = true;
              selected = candidate;
            }
          } catch (const std::bad_alloc &) {
            ret = OB_SIZE_OVERFLOW;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    out = std::move(selected);
  } else if (ret != OB_EAGAIN) {
    LOG_WARN("acquire semantic media runtime failed", K(ret), K(root), K(max_bytes), K(parallel), K(deadline));
  }
  return ret;
}

SemanticMediaRuntime::~SemanticMediaRuntime()
{
  int expected = OB_SUCCESS;
  cancel_ret_.compare_exchange_strong(expected, OB_CANCELED);
  {
    std::unique_lock<std::mutex> lock(mutex_);
    drained_.wait(lock, [this] { return active_count_ == 0; });
    for (auto &entry : cache_) {
      entry.task_.reset();
    }
    for (auto &entry : live_) {
      budget_->release(entry.second.charged_bytes_);
    }
    live_.clear();
  }
  if (root_fd_ >= 0) {
    ::close(root_fd_);
    root_fd_ = -1;
  }
  root_buffer_.reset();
  remote_root_buffer_.reset();
  budget_->release(charged_bytes_);
  if (registered_) {
    auto &registry = media_registry();
    std::lock_guard<std::mutex> lock(registry.mutex_);
    auto entry = registry.queries_.find(budget_->execution_id());
    if (entry != registry.queries_.end() && entry->second.expired()) {
      registry.queries_.erase(entry);
    }
  }
}

int SemanticMediaRuntime::status() const
{
  int ret = budget_->failure();
  if (OB_SUCC(ret)) {
    ret = cancel_ret_.load();
  }
  if (OB_SUCC(ret) && (SemanticMediaScheduler::instance().stopping() ||
                      SemanticRemoteMediaScheduler::instance().stopping())) {
    ret = OB_CANCELED;
  }
  if (OB_SUCC(ret) && ObTimeUtility::current_time() >= deadline_) {
    ret = OB_TIMEOUT;
  }
  return ret;
}

int SemanticMediaRuntime::cancel(int ret)
{
  if (ret >= OB_SUCCESS || ret == OB_EAGAIN) {
    LOG_WARN("invalid semantic media cancellation", K(ret));
    return OB_INVALID_ARGUMENT;
  }
  int expected = OB_SUCCESS;
  cancel_ret_.compare_exchange_strong(expected, ret);
  SemanticRemoteMediaScheduler::instance().wake();
  return OB_SUCCESS;
}

void SemanticMediaRuntime::stop_scheduler()
{
  SemanticRemoteMediaScheduler::instance().shutdown();
  SemanticMediaScheduler::instance().shutdown();
}

int64_t SemanticMediaRuntime::get_cached_count() const
{
  std::lock_guard<std::mutex> lock(mutex_);
  return std::count_if(cache_.begin(), cache_.end(),
                       [](const CacheEntry &entry) { return static_cast<bool>(entry.task_); });
}

int64_t SemanticMediaRuntime::get_active_count() const
{
  std::lock_guard<std::mutex> lock(mutex_);
  return active_count_;
}

bool SemanticMediaRuntime::has_pending() const
{
  return get_active_count() != 0;
}

int SemanticMediaRuntime::set_test_hook_for_testing(TestHook hook, void *context)
{
  int ret = OB_SUCCESS;
  std::lock_guard<std::mutex> lock(mutex_);
  if (active_count_ != 0) {
    ret = OB_STATE_NOT_MATCH;
  } else if (hook == nullptr && context != nullptr) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    test_hook_ = hook;
    test_hook_context_ = context;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("set semantic media test hook failed", K(ret));
  }
  return ret;
}

void SemanticMediaRuntime::invoke_test_hook(TestStage stage)
{
  TestHook hook = nullptr;
  void *context = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    hook = test_hook_;
    context = test_hook_context_;
  }
  if (hook != nullptr) {
    hook(context, stage);
  }
}

void SemanticMediaRuntime::purge_expired_locked()
{
  for (auto entry = live_.begin(); entry != live_.end();) {
    if (entry->second.task_.expired()) {
      const int64_t bytes = entry->second.charged_bytes_;
      entry = live_.erase(entry);
      budget_->release(bytes);
    } else {
      ++entry;
    }
  }
}

void SemanticMediaRuntime::erase_live_locked(const MediaKey &key)
{
  auto entry = live_.find(key);
  if (entry != live_.end()) {
    const int64_t bytes = entry->second.charged_bytes_;
    live_.erase(entry);
    budget_->release(bytes);
  }
}

bool SemanticMediaRuntime::evict_one_locked()
{
  CacheEntry *victim = nullptr;
  for (auto &entry : cache_) {
    if (entry.task_ && entry.task_->ready() && entry.task_.use_count() == 1
        && (victim == nullptr || entry.last_use_ < victim->last_use_)) {
      victim = &entry;
    }
  }
  if (victim != nullptr) {
    erase_live_locked({victim->task_->uri_, victim->task_->object_version_});
    victim->task_.reset();
    victim->last_use_ = 0;
    evicted_count_.fetch_add(1);
  }
  return victim != nullptr;
}

int SemanticMediaRuntime::reserve_locked(int64_t bytes)
{
  purge_expired_locked();
  int ret = budget_->reserve(bytes);
  while (ret == OB_SIZE_OVERFLOW && evict_one_locked()) {
    ret = budget_->reserve(bytes);
  }
  return ret;
}

int SemanticMediaRuntime::reserve_with_eviction(int64_t bytes)
{
  std::lock_guard<std::mutex> lock(mutex_);
  return reserve_locked(bytes);
}

bool SemanticMediaRuntime::can_demand() const
{
  if (status() != OB_SUCCESS) {
    return true;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  const uint8_t waiting = waiting_sources_ == 0 ? 3 : waiting_sources_;
  return active_count_ < parallel_ &&
      (((waiting & 1) != 0 && !root_.empty() && SemanticMediaScheduler::instance().can_submit()) ||
       ((waiting & 2) != 0 && !remote_root_.empty() &&
        SemanticRemoteMediaScheduler::instance().can_submit()));
}

bool SemanticMediaRuntime::admission_ready() const
{
  return can_demand();
}

int SemanticMediaRuntime::demand(const ObString &encoded_file, std::shared_ptr<SemanticMediaTask> &out)
{
  out.reset();
  int ret = status();
  FileRef ref;
  char path_buffer[MAX_URI_BYTES + 1];
  ObString path;
  ObString relative;
  ObString declared_mime;
  bool remote = false;
  if (OB_FAIL(ret)) {
  } else if (encoded_file.empty() || encoded_file.length() < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (encoded_file.length() > MAX_DESCRIPTOR_BYTES) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(FileRefCodec::decode(encoded_file, ref))) {
  } else if (FALSE_IT(remote = FileRefCodec::is_remote_uri(ref.uri_))) {
  } else if (remote && OB_FAIL(remote_allowed(ref.uri_))) {
  } else if (!remote && root_.empty()) {
    ret = OB_OP_NOT_ALLOW;
  } else if (!remote && OB_FAIL(local_path(ref.uri_, path_buffer, path))) {
  } else if (!remote && OB_FAIL(relative_path(root_, path, relative))) {
  } else if (OB_FAIL(validate_version(ref.object_version_))) {
  } else if (ref.size_bytes_ < -1) {
    ret = OB_INVALID_ARGUMENT;
  } else if (ref.size_bytes_ > max_bytes_ || ref.size_bytes_ > MAX_SOURCE_BYTES) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(normalize_mime(ref.mime_type_, declared_mime))) {
  } else if (OB_FAIL(SemanticMediaScheduler::instance().ensure_started())) {
  } else if (remote && OB_FAIL(SemanticRemoteMediaScheduler::instance().ensure_started())) {
  }
  if (OB_SUCC(ret)) {
    std::lock_guard<std::mutex> lock(mutex_);
    purge_expired_locked();
    const MediaKey key{ref.uri_, ref.object_version_};
    auto existing = live_.find(key);
    std::shared_ptr<SemanticMediaTask> task = existing == live_.end()
        ? nullptr : existing->second.task_.lock();
    if (existing != live_.end() && !task) {
      erase_live_locked(key);
    }
    if (OB_FAIL(status())) {
    } else if (task) {
      if (!same_string(task->declared_mime_type_, declared_mime) || task->declared_size_ != ref.size_bytes_) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        for (auto &entry : cache_) {
          if (entry.task_ == task) {
            entry.last_use_ = ++cache_clock_;
          }
        }
        out = std::move(task);
      }
    } else if (active_count_ >= parallel_ ||
        !(remote ? SemanticRemoteMediaScheduler::instance().can_submit()
                 : SemanticMediaScheduler::instance().can_submit())) {
      ret = OB_EAGAIN;
      waiting_sources_ |= remote ? 2 : 1;
    } else {
      const int64_t task_charge = sizeof(SemanticMediaTask) + encoded_file.length() + 1
          + ALLOCATION_OVERHEAD * 2;
      const int64_t key_size = ref.uri_.length() + ref.object_version_.length() + 2;
      const int64_t key_charge = sizeof(MediaKey) + sizeof(LiveEntry) + key_size
          + ALLOCATION_OVERHEAD * 2;
      if (OB_FAIL(reserve_locked(task_charge + key_charge))) {
      } else {
        Reservation task_reservation(budget_, task_charge);
        Reservation key_reservation(budget_, key_charge);
        bool inserted = false;
        CacheEntry *cached = nullptr;
        try {
          auto *created = new SemanticMediaTask(budget_, task_charge);
          task_reservation.detach();
          task.reset(created);
          task->descriptor_buffer_.reset(new char[encoded_file.length() + 1]);
          std::memcpy(task->descriptor_buffer_.get(), encoded_file.ptr(), encoded_file.length());
          task->descriptor_buffer_[encoded_file.length()] = '\0';
          FileRef owned;
          ret = FileRefCodec::decode(ObString(encoded_file.length(), task->descriptor_buffer_.get()), owned);
          if (OB_SUCC(ret)) {
            task->uri_ = owned.uri_;
            task->declared_mime_type_ = declared_mime;
            task->object_version_ = owned.object_version_;
            task->declared_size_ = owned.size_bytes_;
            task->remote_ = remote;
            LiveEntry live;
            live.key_buffer_.reset(new char[key_size]);
            std::memcpy(live.key_buffer_.get(), ref.uri_.ptr(), ref.uri_.length());
            live.key_buffer_[ref.uri_.length()] = '\0';
            if (!ref.object_version_.empty()) {
              std::memcpy(live.key_buffer_.get() + ref.uri_.length() + 1,
                          ref.object_version_.ptr(), ref.object_version_.length());
            }
            live.key_buffer_[key_size - 1] = '\0';
            MediaKey owned_key{ObString(ref.uri_.length(), live.key_buffer_.get()),
                ObString(ref.object_version_.length(), live.key_buffer_.get() + ref.uri_.length() + 1)};
            live.task_ = task;
            live.charged_bytes_ = key_charge;
            inserted = live_.emplace(owned_key, std::move(live)).second;
            if (!inserted) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              key_reservation.detach();
              auto available = std::find_if(cache_.begin(), cache_.end(),
                  [](const CacheEntry &entry) { return !entry.task_; });
              if (available == cache_.end() && evict_one_locked()) {
                available = std::find_if(cache_.begin(), cache_.end(),
                    [](const CacheEntry &entry) { return !entry.task_; });
              }
              if (available != cache_.end()) {
                cached = &*available;
                cached->task_ = task;
                cached->last_use_ = ++cache_clock_;
              }
              // Full caches with live handles must not deadlock a batch larger than the cache.
              // The budgeted weak index still coalesces those externally owned results.
              ++active_count_;
              ret = remote ? SemanticRemoteMediaScheduler::instance().submit(*this, task)
                           : SemanticMediaScheduler::instance().submit(*this, task);
              if (OB_FAIL(ret)) {
                --active_count_;
                if (ret == OB_EAGAIN) {
                  waiting_sources_ |= remote ? 2 : 1;
                }
              } else {
                waiting_sources_ &= ~(remote ? 2 : 1);
                out = task;
              }
            }
          }
        } catch (const std::bad_alloc &) {
          ret = OB_SIZE_OVERFLOW;
        }
        if (OB_FAIL(ret)) {
          if (cached != nullptr) {
            cached->task_.reset();
            cached->last_use_ = 0;
          }
          if (inserted) {
            erase_live_locked(key);
          }
        }
      }
    }
  }
  if (OB_FAIL(ret) && ret != OB_EAGAIN) {
    LOG_WARN("demand semantic media failed", K(ret), K(encoded_file.length()), K(remote));
  }
  return ret;
}

int SemanticMediaRuntime::remote_allowed(const ObString &uri) const
{
  return remote_root_.empty() || !uri.prefix_match(remote_root_) ? OB_OP_NOT_ALLOW : OB_SUCCESS;
}

int SemanticMediaRuntime::append_remote_data(SemanticMediaTask &task, const char *data, int64_t size)
{
  int ret = status();
  if (OB_FAIL(ret)) {
  } else if (size < 0 || task.source_size_ > std::min(max_bytes_, MAX_SOURCE_BYTES) - size) {
    ret = OB_SIZE_OVERFLOW;
  } else if (task.declared_size_ >= 0 && task.source_size_ > task.declared_size_ - size) {
    ret = OB_INVALID_DATA;
  } else if (task.source_size_ + size > task.source_capacity_) {
    const int64_t limit = std::min(max_bytes_, MAX_SOURCE_BYTES);
    const int64_t capacity = std::min(limit, std::max(task.source_size_ + size,
        task.source_capacity_ == 0 ? READ_CHUNK_BYTES : task.source_capacity_ * 2));
    const int64_t charge = capacity + ALLOCATION_OVERHEAD;
    if (OB_FAIL(reserve_with_eviction(charge))) {
    } else {
      Reservation reservation(budget_, charge);
      std::unique_ptr<uint8_t[]> buffer(new (std::nothrow) uint8_t[capacity]);
      if (!buffer) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        if (task.source_size_ > 0) {
          std::memcpy(buffer.get(), task.source_buffer_.get(), task.source_size_);
        }
        task.source_buffer_ = std::move(buffer);
        task.charged_bytes_ += charge - task.source_charge_;
        budget_->release(task.source_charge_);
        task.source_charge_ = charge;
        task.source_capacity_ = capacity;
        reservation.detach();
      }
    }
  }
  if (OB_SUCC(ret) && size > 0) {
    std::memcpy(task.source_buffer_.get() + task.source_size_, data, size);
    task.source_size_ += size;
  }
  return ret;
}

void SemanticMediaRuntime::release_source(SemanticMediaTask &task)
{
  task.source_buffer_.reset();
  budget_->release(task.source_charge_);
  task.charged_bytes_ -= task.source_charge_;
  task.source_size_ = 0;
  task.source_capacity_ = 0;
  task.source_charge_ = 0;
}

int SemanticMediaRuntime::load(SemanticMediaTask &task)
{
  if (task.remote_) {
    return encode(task, task.source_buffer_.get(), task.source_size_);
  }
  int ret = status();
  char path_buffer[MAX_URI_BYTES + 1];
  ObString path;
  ObString relative;
  FileDescriptor file;
  struct stat before = {};
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(local_path(task.uri_, path_buffer, path))) {
  } else if (OB_FAIL(relative_path(root_, path, relative))) {
  } else if (OB_FAIL(open_media(root_fd_, relative, file))) {
  } else if (::fstat(file.get(), &before) != 0) {
    ret = file_error(errno);
  } else if (!S_ISREG(before.st_mode)) {
    ret = OB_OP_NOT_ALLOW;
  } else if (before.st_size <= 0) {
    ret = OB_INVALID_DATA;
  } else if (before.st_size > max_bytes_ || before.st_size > MAX_SOURCE_BYTES) {
    ret = OB_SIZE_OVERFLOW;
  } else if (task.declared_size_ >= 0 && task.declared_size_ != before.st_size) {
    ret = OB_INVALID_DATA;
  }
  if (OB_SUCC(ret)) {
    invoke_test_hook(TestStage::BEFORE_READ);
    ret = status();
  }
  if (OB_SUCC(ret)) {
    const int64_t size = before.st_size;
    const int64_t raw_charge = size + ALLOCATION_OVERHEAD;
    if (OB_FAIL(reserve_with_eviction(raw_charge))) {
    } else {
      Reservation raw_reservation(budget_, raw_charge);
      std::unique_ptr<uint8_t[]> raw(new uint8_t[size]);
      int64_t pos = 0;
      while (OB_SUCC(ret) && pos < size) {
        if (OB_FAIL(status())) {
        } else {
          const int64_t requested = std::min(READ_CHUNK_BYTES, size - pos);
          const ssize_t count = ::read(file.get(), raw.get() + pos, requested);
          if (count < 0 && errno == EINTR) {
            ret = status();
          } else if (count < 0) {
            ret = file_error(errno);
          } else if (count == 0) {
            ret = OB_INVALID_DATA;
          } else {
            pos += count;
            invoke_test_hook(TestStage::AFTER_READ_CHUNK);
            ret = status();
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(status())) {
      } else if (OB_FAIL(unchanged_file(file.get(), before))) {
      } else if (OB_FAIL(encode(task, raw.get(), size))) {
      } else if (OB_FAIL(unchanged_file(file.get(), before))) {
      }
    }
  }
  return ret;
}

int SemanticMediaRuntime::encode(SemanticMediaTask &task, const uint8_t *raw, int64_t size)
{
  int ret = status();
  MediaKind kind = MediaKind::UNKNOWN;
  ObString mime;
  ObString audio;
  const int64_t hash_charge = task.object_version_.empty() ? 0 : HASH_WORK_BYTES;
  if (OB_SUCC(ret) && hash_charge > 0) {
    ret = reserve_with_eviction(hash_charge);
  }
  Reservation hash_reservation(budget_, OB_SUCC(ret) ? hash_charge : 0);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(validate_media(raw, size, *this, kind, mime, audio))) {
  } else if (!task.declared_mime_type_.empty() && !same_string(task.declared_mime_type_, mime)) {
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(status())) {
  } else if (OB_FAIL(verify_version(task.object_version_, raw, size))) {
  } else if (OB_FAIL(status())) {
  }
  if (OB_SUCC(ret)) {
    invoke_test_hook(TestStage::BEFORE_ENCODE);
    ret = status();
  }
  if (OB_SUCC(ret)) {
    const int64_t encoded_size = ObBase64Encoder::needed_encoded_length(size);
    const int64_t encoded_charge = encoded_size + 1 + ALLOCATION_OVERHEAD;
    if (OB_FAIL(reserve_with_eviction(encoded_charge))) {
    } else {
      Reservation encoded_reservation(budget_, encoded_charge);
      std::unique_ptr<char[]> encoded(new char[encoded_size + 1]);
      int64_t output_pos = 0;
      for (int64_t input_pos = 0; OB_SUCC(ret) && input_pos < size;) {
        if (OB_FAIL(status())) {
        } else {
          const int64_t count = std::min(ENCODE_CHUNK_BYTES, size - input_pos);
          if (OB_FAIL(ObBase64Encoder::encode(raw + input_pos, count,
                                             encoded.get(), encoded_size, output_pos))) {
          } else {
            input_pos += count;
            invoke_test_hook(TestStage::AFTER_ENCODE_CHUNK);
            ret = status();
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (output_pos != encoded_size) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(status())) {
      } else {
        encoded[encoded_size] = '\0';
        task.base64_buffer_ = std::move(encoded);
        task.charged_bytes_ += encoded_charge;
        encoded_reservation.detach();
        task.base64_.assign_ptr(task.base64_buffer_.get(), static_cast<int32_t>(encoded_size));
        task.mime_type_ = mime;
        task.audio_format_ = audio;
        task.is_audio_ = kind == MediaKind::WAV || kind == MediaKind::MP3;
      }
    }
  }
  return ret;
}

void SemanticMediaRuntime::run_task(std::shared_ptr<SemanticMediaTask> task)
{
  if (!task->remote_) {
    started_count_.fetch_add(1);
  }
  int ret = OB_SUCCESS;
  try {
    ret = load(*task);
  } catch (const std::bad_alloc &) {
    ret = OB_SIZE_OVERFLOW;
  }
  complete_task(std::move(task), ret);
}

void SemanticMediaRuntime::complete_task(std::shared_ptr<SemanticMediaTask> task, int ret)
{
  {
    std::lock_guard<std::mutex> lock(mutex_);
    release_source(*task);
    if (OB_SUCC(ret)) {
      ret = status();
    }
    if (OB_SUCC(ret)) {
      loaded_count_.fetch_add(1);
    } else if (task->base64_buffer_) {
      const int64_t bytes = task->base64_.length() + 1 + ALLOCATION_OVERHEAD;
      task->base64_buffer_.reset();
      task->base64_.reset();
      task->mime_type_.reset();
      task->audio_format_.reset();
      task->charged_bytes_ -= bytes;
      budget_->release(bytes);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("asynchronous semantic media load failed", K(ret), "remote", task->remote_);
    }
    task->result_.store(ret, std::memory_order_release);
    task.reset();
    --active_count_;
    drained_.notify_all();
  }
}

}
}
