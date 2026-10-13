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

#define USING_LOG_PREFIX COMMON

#include "common/object/file_ref.h"
#include "common/json_type/ob_json_tree.h"
#include "lib/charset/ob_charset.h"
#include "lib/utility/serialization.h"

namespace oceanbase
{
namespace common
{
namespace
{
constexpr int32_t FILE_REF_MAGIC = 0x46494c45;
// Big-endian magic, version, three byte lengths, size, then the three strings.
constexpr int64_t FILE_REF_HEADER_BYTES = 4 + 1 + 3 * 4 + 8;

int hex_digit(const unsigned char c)
{
  return c >= '0' && c <= '9' ? c - '0'
       : c >= 'a' && c <= 'f' ? c - 'a' + 10
       : c >= 'A' && c <= 'F' ? c - 'A' + 10 : -1;
}

bool is_unreserved(const unsigned char c)
{
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
      || (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~';
}

int validate_utf8(const ObString &str)
{
  int ret = OB_SUCCESS;
  int64_t well_formed_len = 0;
  if (str.length() < 0 || (str.length() > 0 && OB_ISNULL(str.ptr()))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (str.length() > 0) {
    if (OB_SUCCESS != ObCharset::well_formed_len(
          CS_TYPE_UTF8MB4_BIN, str.ptr(), str.length(), well_formed_len)
        || well_formed_len != str.length()) {
      ret = OB_INVALID_ARGUMENT;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < str.length(); ++i) {
      const unsigned char c = static_cast<unsigned char>(str.ptr()[i]);
      if (c < 0x20 || c == 0x7f) {
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  return ret;
}

int normalize_uri(const ObString input, char *uri_buf, ObString &uri)
{
  int ret = OB_SUCCESS;
  uri.reset();
  if (FileRefCodec::is_remote_uri(input)) {
    ObString origin;
    ObString remote_path;
    if (OB_FAIL(FileRefCodec::parse_remote_uri(input, origin, remote_path))) {
    } else {
      MEMCPY(uri_buf, input.ptr(), input.length());
      uri.assign_ptr(uri_buf, input.length());
    }
    return ret;
  }
  char path[FileRefCodec::MAX_URI_BYTES];
  int64_t path_len = 0;
  const bool is_uri = input.length() >= 8 && OB_NOT_NULL(input.ptr())
      && 0 == MEMCMP(input.ptr(), "file:///", 8);
  const int64_t begin = is_uri ? 7 : 0;
  if (input.empty() || OB_ISNULL(input.ptr())
      || input.length() > FileRefCodec::MAX_URI_BYTES || input.ptr()[begin] != '/') {
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t i = begin; OB_SUCC(ret) && i < input.length(); ++i) {
    unsigned char c = static_cast<unsigned char>(input.ptr()[i]);
    if (is_uri && (c == '?' || c == '#')) {
      ret = OB_INVALID_ARGUMENT;
    } else if (is_uri && c == '%') {
      if (i + 2 >= input.length()
          || hex_digit(static_cast<unsigned char>(input.ptr()[i + 1])) < 0
          || hex_digit(static_cast<unsigned char>(input.ptr()[i + 2])) < 0) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        c = static_cast<unsigned char>(
            hex_digit(static_cast<unsigned char>(input.ptr()[i + 1])) * 16
            + hex_digit(static_cast<unsigned char>(input.ptr()[i + 2])));
        i += 2;
      }
    }
    if (OB_SUCC(ret)) {
      path[path_len++] = static_cast<char>(c);
    }
  }
  if (OB_SUCC(ret)) {
    ret = validate_utf8(ObString(static_cast<int32_t>(path_len), path));
  }
  int64_t normalized_len = 1;
  for (int64_t i = 1; OB_SUCC(ret) && i < path_len;) {
    while (i < path_len && path[i] == '/') {
      ++i;
    }
    const int64_t segment_begin = i;
    while (i < path_len && path[i] != '/') {
      ++i;
    }
    const int64_t segment_len = i - segment_begin;
    if (segment_len == 0 || (segment_len == 1 && path[segment_begin] == '.')) {
    } else if (segment_len == 2 && path[segment_begin] == '.'
               && path[segment_begin + 1] == '.') {
      if (normalized_len == 1) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        while (normalized_len > 1 && path[normalized_len - 1] != '/') {
          --normalized_len;
        }
        if (normalized_len > 1) {
          --normalized_len;
        }
      }
    } else {
      if (normalized_len > 1) {
        path[normalized_len++] = '/';
      }
      MEMMOVE(path + normalized_len, path + segment_begin, segment_len);
      normalized_len += segment_len;
    }
  }
  if (OB_SUCC(ret)) {
    static const char HEX[] = "0123456789ABCDEF";
    MEMCPY(uri_buf, "file://", 7);
    int64_t uri_len = 7;
    for (int64_t i = 0; OB_SUCC(ret) && i < normalized_len; ++i) {
      const unsigned char c = static_cast<unsigned char>(path[i]);
      const int64_t bytes = c == '/' || is_unreserved(c) ? 1 : 3;
      if (uri_len + bytes > FileRefCodec::MAX_URI_BYTES) {
        ret = OB_SIZE_OVERFLOW;
      } else if (bytes == 1) {
        uri_buf[uri_len++] = static_cast<char>(c);
      } else {
        uri_buf[uri_len++] = '%';
        uri_buf[uri_len++] = HEX[c >> 4];
        uri_buf[uri_len++] = HEX[c & 0xf];
      }
    }
    if (OB_SUCC(ret)) {
      uri.assign_ptr(uri_buf, static_cast<int32_t>(uri_len));
    }
  }
  return ret;
}

bool is_mime_token(const unsigned char c)
{
  return is_unreserved(c) || c == '!' || c == '#' || c == '$' || c == '%'
      || c == '&' || c == '\'' || c == '*' || c == '+' || c == '^'
      || c == '`' || c == '|';
}

int validate_metadata(const FileRef &ref)
{
  int ret = OB_SUCCESS;
  if (ref.size_bytes_ < -1 || ref.mime_type_.length() < 0
      || ref.mime_type_.length() > FileRefCodec::MAX_MIME_TYPE_BYTES
      || ref.object_version_.length() < 0
      || ref.object_version_.length() > FileRefCodec::MAX_OBJECT_VERSION_BYTES) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(validate_utf8(ref.object_version_))) {
  } else if (!ref.mime_type_.empty()) {
    if (OB_ISNULL(ref.mime_type_.ptr())) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      int64_t slash = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < ref.mime_type_.length(); ++i) {
        const unsigned char c = static_cast<unsigned char>(ref.mime_type_.ptr()[i]);
        if (c == '/' && slash == -1) {
          slash = i;
        } else if (!is_mime_token(c)) {
          ret = OB_INVALID_ARGUMENT;
        }
      }
      if (OB_SUCC(ret) && (slash <= 0 || slash + 1 >= ref.mime_type_.length()
          || (slash == 1 && ref.mime_type_.ptr()[0] == '*')
          || (slash + 2 == ref.mime_type_.length() && ref.mime_type_.ptr()[slash + 1] == '*'))) {
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  return ret;
}

int encode_ref(ObIAllocator &allocator, const FileRef &ref, ObString &encoded)
{
  int ret = OB_SUCCESS;
  const int64_t total = FILE_REF_HEADER_BYTES + ref.uri_.length()
      + ref.mime_type_.length() + ref.object_version_.length();
  if (OB_FAIL(validate_metadata(ref))) {
  } else if (total > FileRefCodec::MAX_REF_BYTES) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    char *buf = static_cast<char *>(allocator.alloc(total));
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(serialization::encode_i32(buf, total, pos, FILE_REF_MAGIC))
        || OB_FAIL(serialization::encode_i8(buf, total, pos, FileRefCodec::FORMAT_VERSION))
        || OB_FAIL(serialization::encode_i32(buf, total, pos, ref.uri_.length()))
        || OB_FAIL(serialization::encode_i32(buf, total, pos, ref.mime_type_.length()))
        || OB_FAIL(serialization::encode_i32(buf, total, pos, ref.object_version_.length()))
        || OB_FAIL(serialization::encode_i64(buf, total, pos, ref.size_bytes_))) {
      allocator.free(buf);
    } else {
      MEMCPY(buf + pos, ref.uri_.ptr(), ref.uri_.length());
      pos += ref.uri_.length();
      if (!ref.mime_type_.empty()) {
        MEMCPY(buf + pos, ref.mime_type_.ptr(), ref.mime_type_.length());
        pos += ref.mime_type_.length();
      }
      if (!ref.object_version_.empty()) {
        MEMCPY(buf + pos, ref.object_version_.ptr(), ref.object_version_.length());
      }
      encoded.assign_ptr(buf, static_cast<int32_t>(total));
    }
  }
  return ret;
}

int json_string(ObIJsonBase &value, const int64_t max_len, ObString &str)
{
  int ret = OB_SUCCESS;
  if (value.json_type() != ObJsonNodeType::J_STRING || value.get_data_length() == 0
      || value.get_data_length() > static_cast<uint64_t>(max_len)
      || OB_ISNULL(value.get_data())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    str.assign_ptr(value.get_data(), static_cast<int32_t>(value.get_data_length()));
  }
  return ret;
}

int json_integer(ObIJsonBase &value, int64_t &number)
{
  int ret = OB_SUCCESS;
  if (value.json_type() == ObJsonNodeType::J_INT) {
    number = value.get_int();
  } else if (value.json_type() == ObJsonNodeType::J_UINT && value.get_uint() <= INT64_MAX) {
    number = static_cast<int64_t>(value.get_uint());
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}
}

bool FileRefCodec::is_remote_uri(const ObString &uri)
{
  return OB_NOT_NULL(uri.ptr()) && (uri.prefix_match("https://") || uri.prefix_match("http://"));
}

int FileRefCodec::parse_remote_uri(const ObString &uri, ObString &origin, ObString &path)
{
  int ret = OB_SUCCESS;
  origin.reset();
  path.reset();
  const bool https = OB_NOT_NULL(uri.ptr()) && uri.prefix_match("https://");
  const int64_t authority_begin = https ? 8 : 7;
  int64_t path_begin = authority_begin;
  int64_t query_begin = uri.length();
  if (!is_remote_uri(uri) || uri.length() > MAX_URI_BYTES || OB_ISNULL(uri.ptr())) {
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < uri.length(); ++i) {
    const unsigned char c = static_cast<unsigned char>(uri.ptr()[i]);
    if (c <= 0x20 || c >= 0x7f || c == '\\' || c == '#') {
      ret = OB_INVALID_ARGUMENT;
    } else if (c == '%') {
      if (i + 2 >= uri.length() || hex_digit(uri.ptr()[i + 1]) < 0
          || hex_digit(uri.ptr()[i + 2]) < 0) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        const unsigned char decoded = hex_digit(uri.ptr()[i + 1]) * 16 + hex_digit(uri.ptr()[i + 2]);
        if (decoded < 0x20 || decoded == 0x7f) {
          ret = OB_INVALID_ARGUMENT;
        }
        i += 2;
      }
    }
  }
  while (OB_SUCC(ret) && path_begin < uri.length() && uri.ptr()[path_begin] != '/') {
    ++path_begin;
  }
  ObString host;
  int64_t port_begin = path_begin;
  if (OB_SUCC(ret)) {
    if (path_begin == uri.length() || path_begin == authority_begin) {
      ret = OB_INVALID_ARGUMENT;
    } else if (uri.ptr()[authority_begin] == '[') {
      // Numeric loopback is also useful for private, credential-free test fixtures.
      const int64_t host_end = authority_begin + 5;
      if (host_end > path_begin || MEMCMP(uri.ptr() + authority_begin, "[::1]", 5) != 0) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        host.assign_ptr(uri.ptr() + authority_begin, 5);
        port_begin = host_end;
      }
    } else {
      int64_t host_end = authority_begin;
      while (host_end < path_begin && uri.ptr()[host_end] != ':') {
        ++host_end;
      }
      host.assign_ptr(uri.ptr() + authority_begin, host_end - authority_begin);
      port_begin = host_end;
      int64_t label_begin = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i <= host.length(); ++i) {
        if (i == host.length() || host.ptr()[i] == '.') {
          const int64_t length = i - label_begin;
          if (length <= 0 || length > 63 || host.ptr()[label_begin] == '-'
              || host.ptr()[i - 1] == '-') {
            ret = OB_INVALID_ARGUMENT;
          }
          label_begin = i + 1;
        } else {
          const char c = host.ptr()[i];
          if (!(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') && c != '-') {
            ret = OB_INVALID_ARGUMENT;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && port_begin < path_begin) {
    int64_t port = 0;
    if (uri.ptr()[port_begin] != ':' || port_begin + 1 >= path_begin
        || uri.ptr()[port_begin + 1] == '0') {
      ret = OB_INVALID_ARGUMENT;
    }
    for (int64_t i = port_begin + 1; OB_SUCC(ret) && i < path_begin; ++i) {
      const char c = uri.ptr()[i];
      if (c < '0' || c > '9' || port > 6553) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        port = port * 10 + c - '0';
      }
    }
    if (OB_SUCC(ret) && (port <= 0 || port > 65535)) {
      ret = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCC(ret) && !https && host != ObString::make_string("127.0.0.1")
      && host != ObString::make_string("[::1]")) {
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = path_begin; i < uri.length(); ++i) {
      if (uri.ptr()[i] == '?') {
        query_begin = i;
        break;
      }
    }
    for (int64_t i = query_begin + 1; OB_SUCC(ret) && i < uri.length(); ++i) {
      const unsigned char c = static_cast<unsigned char>(uri.ptr()[i]);
      if (c == '%') {
        i += 2;
      } else if (!is_unreserved(c) && c != '!' && c != '$' && c != '&' && c != '\''
          && c != '(' && c != ')' && c != '*' && c != '+' && c != ',' && c != ';'
          && c != '=' && c != ':' && c != '@' && c != '/' && c != '?') {
        ret = OB_INVALID_ARGUMENT;
      }
    }
    int64_t segment_length = 0;
    bool dots_only = true;
    for (int64_t i = path_begin + 1; OB_SUCC(ret) && i <= query_begin; ++i) {
      if (i == query_begin || uri.ptr()[i] == '/') {
        if (dots_only && (segment_length == 1 || segment_length == 2)) {
          ret = OB_INVALID_ARGUMENT;
        }
        segment_length = 0;
        dots_only = true;
      } else {
        unsigned char c = static_cast<unsigned char>(uri.ptr()[i]);
        if (c == '%') {
          c = hex_digit(uri.ptr()[i + 1]) * 16 + hex_digit(uri.ptr()[i + 2]);
          i += 2;
          if (c == '/' || c == '\\') {
            ret = OB_INVALID_ARGUMENT;
          }
        } else if (!is_unreserved(c) && c != '!' && c != '$' && c != '&' && c != '\''
            && c != '(' && c != ')' && c != '*' && c != '+' && c != ',' && c != ';'
            && c != '=' && c != ':' && c != '@') {
          ret = OB_INVALID_ARGUMENT;
        }
        ++segment_length;
        dots_only = dots_only && c == '.';
      }
    }
  }
  if (OB_SUCC(ret)) {
    origin.assign_ptr(uri.ptr(), path_begin);
    path.assign_ptr(uri.ptr() + path_begin, query_begin - path_begin);
  } else {
    LOG_WARN("invalid remote FILE URL", K(ret), "length", uri.length());
  }
  return ret;
}

int FileRefCodec::validate_remote_root(const ObString &root)
{
  int ret = OB_SUCCESS;
  ObString origin;
  ObString path;
  if (root.empty()) {
  } else if (OB_FAIL(parse_remote_uri(root, origin, path))) {
  } else if (root.ptr()[root.length() - 1] != '/' ||
             origin.length() + path.length() != root.length()) {
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("invalid remote media URL prefix", K(ret), "length", root.length());
  }
  return ret;
}

int FileRefCodec::from_path(ObIAllocator &allocator, const ObString &path, ObString &encoded)
{
  int ret = OB_SUCCESS;
  const ObString input = path;
  encoded.reset();
  FileRef ref;
  char uri_buf[MAX_URI_BYTES];
  if (OB_FAIL(normalize_uri(input, uri_buf, ref.uri_))) {
  } else if (OB_FAIL(encode_ref(allocator, ref, encoded))) {
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("invalid FILE path or reference allocation failed", K(ret), K(input.length()));
  }
  return ret;
}

int FileRefCodec::from_json(ObIAllocator &allocator, ObIJsonBase &json, ObString &encoded)
{
  int ret = OB_SUCCESS;
  encoded.reset();
  FileRef ref;
  uint32_t seen = 0;
  if (json.json_type() != ObJsonNodeType::J_OBJECT || json.element_count() < 2
      || json.element_count() > 5) {
    ret = OB_INVALID_ARGUMENT;
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < json.element_count(); ++i) {
    ObString key;
    ObIJsonBase *value = nullptr;
    uint32_t field = 0;
    if (OB_FAIL(json.get_object_value(i, key, value))) {
    } else if (OB_ISNULL(value)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (key == ObString::make_string("format_version")) {
      int64_t version = 0;
      field = 1;
      if (OB_FAIL(json_integer(*value, version))) {
      } else if (version != FORMAT_VERSION) {
        ret = OB_NOT_SUPPORTED;
      }
    } else if (key == ObString::make_string("uri")) {
      field = 2;
      ret = json_string(*value, MAX_URI_BYTES, ref.uri_);
    } else if (key == ObString::make_string("mime_type")) {
      field = 4;
      ret = json_string(*value, MAX_MIME_TYPE_BYTES, ref.mime_type_);
    } else if (key == ObString::make_string("object_version")) {
      field = 8;
      ret = json_string(*value, MAX_OBJECT_VERSION_BYTES, ref.object_version_);
    } else if (key == ObString::make_string("size_bytes")) {
      field = 16;
      if (OB_FAIL(json_integer(*value, ref.size_bytes_))) {
      } else if (ref.size_bytes_ < 0) {
        ret = OB_INVALID_ARGUMENT;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCC(ret)) {
      if ((seen & field) != 0) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        seen |= field;
      }
    }
  }
  char uri_buf[MAX_URI_BYTES];
  if (OB_SUCC(ret)) {
    if ((seen & 3) != 3) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(normalize_uri(ref.uri_, uri_buf, ref.uri_))) {
    } else if (OB_FAIL(encode_ref(allocator, ref, encoded))) {
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("invalid FILE JSON descriptor or reference allocation failed", K(ret));
  }
  return ret;
}

int FileRefCodec::decode(const ObString &encoded, FileRef &ref)
{
  int ret = OB_SUCCESS;
  ref = FileRef();
  int64_t pos = 0;
  int32_t magic = 0;
  int8_t version = 0;
  int32_t uri_len = 0;
  int32_t mime_len = 0;
  int32_t object_version_len = 0;
  FileRef result;
  if (OB_ISNULL(encoded.ptr()) || encoded.length() < FILE_REF_HEADER_BYTES
      || encoded.length() > MAX_REF_BYTES) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i32(encoded.ptr(), encoded.length(), pos, &magic))
      || OB_FAIL(serialization::decode_i8(encoded.ptr(), encoded.length(), pos, &version))
      || OB_FAIL(serialization::decode_i32(encoded.ptr(), encoded.length(), pos, &uri_len))
      || OB_FAIL(serialization::decode_i32(encoded.ptr(), encoded.length(), pos, &mime_len))
      || OB_FAIL(serialization::decode_i32(encoded.ptr(), encoded.length(), pos, &object_version_len))
      || OB_FAIL(serialization::decode_i64(encoded.ptr(), encoded.length(), pos, &result.size_bytes_))) {
  } else if (magic != FILE_REF_MAGIC) {
    ret = OB_INVALID_ARGUMENT;
  } else if (version != FORMAT_VERSION) {
    ret = OB_NOT_SUPPORTED;
  } else if (uri_len <= 0 || uri_len > MAX_URI_BYTES || mime_len < 0
      || mime_len > MAX_MIME_TYPE_BYTES || object_version_len < 0
      || object_version_len > MAX_OBJECT_VERSION_BYTES
      || pos + static_cast<int64_t>(uri_len) + mime_len + object_version_len != encoded.length()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    result.uri_.assign_ptr(encoded.ptr() + pos, uri_len);
    pos += uri_len;
    result.mime_type_.assign_ptr(encoded.ptr() + pos, mime_len);
    pos += mime_len;
    result.object_version_.assign_ptr(encoded.ptr() + pos, object_version_len);
    char uri_buf[MAX_URI_BYTES];
    ObString canonical;
    if (OB_FAIL(validate_metadata(result))) {
    } else if (OB_FAIL(normalize_uri(result.uri_, uri_buf, canonical))) {
    } else if (canonical != result.uri_) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ref = result;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("invalid encoded FILE reference", K(ret), K(encoded.length()));
  }
  return ret;
}

int FileRefCodec::to_json(ObIAllocator &allocator, const ObString &encoded, ObString &json)
{
  int ret = OB_SUCCESS;
  const ObString input = encoded;
  json.reset();
  FileRef ref;
  if (OB_FAIL(decode(input, ref))) {
  } else {
    ObJsonObject object(&allocator);
    ObJsonInt format_version(FORMAT_VERSION);
    ObJsonString uri(ref.uri_);
    ObJsonString mime_type(ref.mime_type_);
    ObJsonString object_version(ref.object_version_);
    ObJsonInt size_bytes(ref.size_bytes_);
    ObJsonBuffer buffer(&allocator);
    if (OB_FAIL(object.object_add(ObString::make_string("format_version"), &format_version))
        || OB_FAIL(object.object_add(ObString::make_string("uri"), &uri))) {
    } else if (!ref.mime_type_.empty()
        && OB_FAIL(object.object_add(ObString::make_string("mime_type"), &mime_type))) {
    } else if (!ref.object_version_.empty()
        && OB_FAIL(object.object_add(ObString::make_string("object_version"), &object_version))) {
    } else if (ref.size_bytes_ >= 0
        && OB_FAIL(object.object_add(ObString::make_string("size_bytes"), &size_bytes))) {
    } else if (OB_FAIL(object.print(buffer, true))) {
    } else if (OB_FAIL(ob_write_string(allocator,
        ObString(static_cast<int32_t>(buffer.length()), buffer.ptr()), json))) {
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to export FILE JSON descriptor", K(ret));
  }
  return ret;
}

int FileRefCodec::get_uri(const ObString &encoded, ObString &uri)
{
  int ret = OB_SUCCESS;
  const ObString input = encoded;
  uri.reset();
  FileRef ref;
  if (OB_FAIL(decode(input, ref))) {
  } else {
    uri = ref.uri_;
  }
  return ret;
}
}
}
