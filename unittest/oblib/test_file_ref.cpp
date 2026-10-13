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

#include "common/object/file_ref.h"
#include "common/object/ob_object.h"
#include "common/datum/ob_datum.h"
#include "common/cell/ob_cell_reader.h"
#include "common/json_type/ob_json_parse.h"
#include "common/ob_field.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/serialization.h"
#include <cstdio>
#include <cstdlib>
#include <string>
#include <unordered_map>

using namespace oceanbase::common;

namespace
{
#define REQUIRE(condition) \
  do { \
    if (!(condition)) { \
      std::fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition); \
      return false; \
    } \
  } while (false)

class TestAllocator final : public ObIAllocator
{
public:
  ~TestAllocator() override
  {
    for (const auto &allocation : allocations_) {
      std::free(allocation.first);
    }
  }
  void *alloc(const int64_t size) override
  {
    if (size <= 0 || ++attempts_ == fail_at_) {
      return nullptr;
    }
    void *memory = std::malloc(size);
    if (memory != nullptr) {
      allocations_.emplace(memory, size);
    }
    return memory;
  }
  void *alloc(const int64_t size, const oceanbase::lib::ObMemAttr &) override
  {
    return alloc(size);
  }
  void free(void *memory) override
  {
    if (memory != nullptr) {
      if (allocations_.erase(memory) != 1) {
        std::abort();
      }
      std::free(memory);
    }
  }
  bool owns(const ObString &value) const
  {
    bool owned = false;
    for (const auto &allocation : allocations_) {
      owned |= allocation.first == value.ptr() && allocation.second >= value.length();
    }
    return owned;
  }
  void fail_at(const int64_t attempt) { attempts_ = 0; fail_at_ = attempt; }
private:
  std::unordered_map<void *, int64_t> allocations_;
  int64_t attempts_ = 0;
  int64_t fail_at_ = -1;
};

ObString string(const std::string &value)
{
  return ObString(static_cast<int32_t>(value.size()), value.data());
}

bool test_path_normalization()
{
  TestAllocator allocator;
  struct PathCase { const char *input_; const char *expected_; };
  const PathCase cases[] = {
    {"/tmp/image.png", "file:///tmp/image.png"},
    {"/tmp//album/./cover/../image.png/", "file:///tmp/album/image.png"},
    {"file:///tmp//album/%69mage%2Epng", "file:///tmp/album/image.png"},
    {"/tmp/a b%?#.png", "file:///tmp/a%20b%25%3F%23.png"},
    {"file:///tmp/a%20b%25%3f%23.png", "file:///tmp/a%20b%25%3F%23.png"},
    {"/tmp/\xE5\x9B\xBE.png", "file:///tmp/%E5%9B%BE.png"},
    {"/", "file:///"},
    {"/a/..", "file:///"},
    {"/does-not-need-to-exist/file.bin", "file:///does-not-need-to-exist/file.bin"},
  };
  for (const PathCase &path : cases) {
    ObString encoded;
    FileRef ref;
    REQUIRE(OB_SUCCESS == FileRefCodec::from_path(
        allocator, ObString::make_string(path.input_), encoded));
    REQUIRE(allocator.owns(encoded));
    REQUIRE(OB_SUCCESS == FileRefCodec::decode(encoded, ref));
    REQUIRE(ref.uri_ == ObString::make_string(path.expected_));
    REQUIRE(ref.uri_.ptr() == encoded.ptr() + 25);
    REQUIRE(ref.mime_type_.empty() && ref.object_version_.empty() && ref.size_bytes_ == -1);
    ObString uri;
    REQUIRE(OB_SUCCESS == FileRefCodec::get_uri(encoded, uri));
    REQUIRE(uri.ptr() == ref.uri_.ptr() && uri == ref.uri_);
  }
  ObString alias = ObString::make_string("/tmp/./same");
  REQUIRE(OB_SUCCESS == FileRefCodec::from_path(allocator, alias, alias));
  REQUIRE(OB_SUCCESS == FileRefCodec::get_uri(alias, alias));
  REQUIRE(alias == ObString::make_string("file:///tmp/same"));
  return true;
}

bool test_invalid_paths_and_limits()
{
  TestAllocator allocator;
  const char *invalid[] = {
    "", "relative/file", "./file", "~/file", "file:/tmp/a", "file://host/tmp/a",
    "file://localhost/tmp/a", "s3://bucket/a", "file:///tmp/a?query=1",
    "file:///tmp/a#fragment", "file:///tmp/%", "file:///tmp/%GG",
    "file:///tmp/%00a", "file:///tmp/%0Aa", "file:///tmp/%C0%AFa",
    "/../a", "/a/../../b", "/tmp/\xFF", "/tmp/\n",
  };
  for (const char *path : invalid) {
    ObString encoded = ObString::make_string("old");
    REQUIRE(OB_SUCCESS != FileRefCodec::from_path(
        allocator, ObString::make_string(path), encoded));
    REQUIRE(encoded.empty());
  }
  const char embedded_nul[] = {'/', 'a', '\0', 'b'};
  ObString encoded;
  REQUIRE(OB_SUCCESS != FileRefCodec::from_path(
      allocator, ObString(sizeof(embedded_nul), embedded_nul), encoded));
  std::string path = "/" + std::string(FileRefCodec::MAX_URI_BYTES - 8, 'a');
  REQUIRE(OB_SUCCESS == FileRefCodec::from_path(allocator, string(path), encoded));
  FileRef ref;
  REQUIRE(OB_SUCCESS == FileRefCodec::decode(encoded, ref));
  REQUIRE(ref.uri_.length() == FileRefCodec::MAX_URI_BYTES);
  path.push_back('a');
  REQUIRE(OB_SIZE_OVERFLOW == FileRefCodec::from_path(allocator, string(path), encoded));
  REQUIRE(encoded.empty());
  path.assign(FileRefCodec::MAX_URI_BYTES + 1, 'a');
  path[0] = '/';
  REQUIRE(OB_SUCCESS != FileRefCodec::from_path(allocator, string(path), encoded));
  return true;
}

bool test_remote_url_roundtrip_and_policy_parts()
{
  TestAllocator allocator;
  const char *valid[] = {
    "https://bucket.example/images/a%20b.png?X-Amz-Signature=synthetic&part=%2f%2B",
    "https://minio.example:9000/bucket//x.png?versionId=v%2B1",
    "http://127.0.0.1:9000/bucket/x.png?signature=synthetic",
    "http://[::1]:9000/bucket/x.png",
    "https://bucket.example/images/%E5%9B%BE.png",
  };
  for (const char *url : valid) {
    ObString encoded;
    REQUIRE(OB_SUCCESS == FileRefCodec::from_path(allocator, ObString::make_string(url), encoded));
    FileRef ref;
    REQUIRE(OB_SUCCESS == FileRefCodec::decode(encoded, ref));
    REQUIRE(ref.uri_ == ObString::make_string(url));
    REQUIRE(FileRefCodec::is_remote_uri(ref.uri_));
    ObString origin;
    ObString path;
    REQUIRE(OB_SUCCESS == FileRefCodec::parse_remote_uri(ref.uri_, origin, path));
    REQUIRE(origin.length() > 7 && path.prefix_match("/"));
    REQUIRE(origin.length() + path.length() <= ref.uri_.length());
    ObString json;
    REQUIRE(OB_SUCCESS == FileRefCodec::to_json(allocator, encoded, json));
    ObJsonNode *tree = nullptr;
    REQUIRE(OB_SUCCESS == ObJsonParser::get_tree(
        &allocator, json, tree, ObJsonParser::JSN_STRICT_FLAG));
    ObString roundtrip;
    REQUIRE(OB_SUCCESS == FileRefCodec::from_json(allocator, *tree, roundtrip));
    REQUIRE(roundtrip == encoded);
  }
  ObString origin;
  ObString path;
  REQUIRE(OB_SUCCESS == FileRefCodec::parse_remote_uri(
      ObString::make_string(valid[0]), origin, path));
  REQUIRE(origin == ObString::make_string("https://bucket.example"));
  REQUIRE(path == ObString::make_string("/images/a%20b.png"));
  for (const char *root : {"", "https://bucket.example/images/",
                          "http://127.0.0.1:9000/bucket/", "http://[::1]:9000/bucket/"}) {
    REQUIRE(OB_SUCCESS == FileRefCodec::validate_remote_root(ObString::make_string(root)));
  }
  for (const char *root : {"https://bucket.example", "https://bucket.example/images",
                          "https://bucket.example/images/?signature=synthetic",
                          "http://outside.example/images/"}) {
    REQUIRE(OB_SUCCESS != FileRefCodec::validate_remote_root(ObString::make_string(root)));
  }
  const char *invalid[] = {
    "http://bucket.example/a", "http://localhost/a", "http://127.1/a",
    "HTTPS://bucket.example/a", "https://BUCKET.example/a", "https://bucket.example./a",
    "https://user:password@bucket.example/a", "https://bucket.example",
    "https://bucket.example:0/a", "https://bucket.example:0443/a",
    "https://bucket.example:65536/a", "https://bucket.example:443extra/a",
    "https://bucket.example/a#fragment", "https://bucket.example/a\\b",
    "https://bucket.example/a/../b", "https://bucket.example/a/%2e%2e/b",
    "https://bucket.example/a/./b", "https://bucket.example/a/%2Fother",
    "https://bucket.example/a/%5cb", "https://bucket.example/a%00",
    "https://bucket.example/a%0a", "https://bucket.example/a%7F",
    "https://bucket.example/a?bad=%", "https://bucket.example/a?bad=%GG",
    "https://bucket.example/a?bad=raw space", "https://bucket.example/a?bad=\"raw\"",
    "https://bucket.example/\xE5\x9B\xBE.png", "https://bucket.example/a%2",
  };
  for (const char *url : invalid) {
    ObString encoded = ObString::make_string("old");
    const int ret = FileRefCodec::from_path(allocator, ObString::make_string(url), encoded);
    if (ret == OB_SUCCESS) {
      std::fprintf(stderr, "unexpectedly accepted synthetic invalid URL: %s\n", url);
    }
    REQUIRE(OB_SUCCESS != ret);
    REQUIRE(encoded.empty());
  }
  std::string maximum = "https://bucket.example/";
  maximum.append(FileRefCodec::MAX_URI_BYTES - maximum.size(), 'a');
  ObString encoded;
  REQUIRE(OB_SUCCESS == FileRefCodec::from_path(allocator, string(maximum), encoded));
  maximum.push_back('a');
  REQUIRE(OB_SUCCESS != FileRefCodec::from_path(allocator, string(maximum), encoded));
  REQUIRE(encoded.empty());
  return true;
}

bool test_json_interchange()
{
  TestAllocator allocator;
  const ObString input = ObString::make_string(
      R"({"size_bytes":4096,"object_version":"v\"\\42","mime_type":"image/png","uri":"/a/../media/x.png","format_version":1})");
  ObJsonNode *tree = nullptr;
  REQUIRE(OB_SUCCESS == ObJsonParser::get_tree(&allocator, input, tree, ObJsonParser::JSN_STRICT_FLAG));
  ObString encoded;
  REQUIRE(OB_SUCCESS == FileRefCodec::from_json(allocator, *tree, encoded));
  FileRef ref;
  REQUIRE(OB_SUCCESS == FileRefCodec::decode(encoded, ref));
  REQUIRE(ref.uri_ == ObString::make_string("file:///media/x.png"));
  REQUIRE(ref.mime_type_ == ObString::make_string("image/png"));
  REQUIRE(ref.object_version_ == ObString::make_string("v\"\\42"));
  REQUIRE(ref.size_bytes_ == 4096);
  REQUIRE(ref.mime_type_.ptr() == ref.uri_.ptr() + ref.uri_.length());
  REQUIRE(ref.object_version_.ptr() == ref.mime_type_.ptr() + ref.mime_type_.length());
  ObString json;
  REQUIRE(OB_SUCCESS == FileRefCodec::to_json(allocator, encoded, json));
  REQUIRE(allocator.owns(json));
  ObJsonNode *exported = nullptr;
  REQUIRE(OB_SUCCESS == ObJsonParser::get_tree(&allocator, json, exported, ObJsonParser::JSN_STRICT_FLAG));
  REQUIRE(exported->json_type() == ObJsonNodeType::J_OBJECT && exported->element_count() == 5);
  ObString roundtrip;
  REQUIRE(OB_SUCCESS == FileRefCodec::from_json(allocator, *exported, roundtrip));
  REQUIRE(roundtrip == encoded);
  REQUIRE(OB_SUCCESS == FileRefCodec::to_json(allocator, encoded, encoded));
  REQUIRE(encoded == json);

  const char *minimal[] = {
    R"({"format_version":1,"uri":"file:///a"})",
    R"({"format_version":1,"uri":"file:///a","size_bytes":0})",
    R"({"format_version":1,"uri":"file:///a","size_bytes":9223372036854775807})",
  };
  for (const char *text : minimal) {
    ObJsonNode *descriptor = nullptr;
    REQUIRE(OB_SUCCESS == ObJsonParser::get_tree(
        &allocator, ObString::make_string(text), descriptor, ObJsonParser::JSN_STRICT_FLAG));
    REQUIRE(OB_SUCCESS == FileRefCodec::from_json(allocator, *descriptor, roundtrip));
    REQUIRE(OB_SUCCESS == FileRefCodec::decode(roundtrip, ref));
  }
  return true;
}

bool test_invalid_json()
{
  TestAllocator allocator;
  const char *invalid[] = {
    R"([])", R"(null)", R"("file:///a")", R"({"uri":"file:///a"})",
    R"({"format_version":1})", R"({"format_version":2,"uri":"file:///a"})",
    R"({"format_version":1.0,"uri":"file:///a"})",
    R"({"format_version":true,"uri":"file:///a"})",
    R"({"format_version":1,"uri":null})",
    R"({"format_version":1,"uri":"relative"})",
    R"({"format_version":1,"uri":"file://host/a"})",
    R"({"format_version":1,"uri":"file:///a","unknown":0})",
    R"({"format_version":1,"uri":"file:///a","mime_type":""})",
    R"({"format_version":1,"uri":"file:///a","mime_type":"image"})",
    R"({"format_version":1,"uri":"file:///a","mime_type":"image/png/extra"})",
    R"({"format_version":1,"uri":"file:///a","mime_type":"image/png; charset=utf-8"})",
    R"({"format_version":1,"uri":"file:///a","mime_type":"*/*"})",
    R"({"format_version":1,"uri":"file:///a","object_version":""})",
    R"({"format_version":1,"uri":"file:///a","object_version":"a\u0000b"})",
    R"({"format_version":1,"uri":"file:///a","size_bytes":-1})",
    R"({"format_version":1,"uri":"file:///a","size_bytes":1.0})",
    R"({"format_version":1,"uri":"file:///a","size_bytes":9223372036854775808})",
    R"({"format_version":1,"uri":"file:///a","size_bytes":null})",
    R"({"format_version":1,"format_version":1,"uri":"file:///a"})",
  };
  for (const char *text : invalid) {
    ObJsonNode *descriptor = nullptr;
    REQUIRE(OB_SUCCESS == ObJsonParser::get_tree(
        &allocator, ObString::make_string(text), descriptor,
        ObJsonParser::JSN_STRICT_FLAG | ObJsonParser::JSN_PRESERVE_DUP_FLAG));
    ObString encoded = ObString::make_string("old");
    REQUIRE(OB_SUCCESS != FileRefCodec::from_json(allocator, *descriptor, encoded));
    REQUIRE(encoded.empty());
  }
  for (const char *field : {"mime_type", "object_version"}) {
    const int64_t maximum = field[0] == 'm'
        ? FileRefCodec::MAX_MIME_TYPE_BYTES : FileRefCodec::MAX_OBJECT_VERSION_BYTES;
    for (const int64_t length : {maximum, maximum + 1}) {
      const std::string value = field[0] == 'm'
          ? "a/" + std::string(length - 2, 'a') : std::string(length, 'a');
      ObJsonObject descriptor(&allocator);
      ObJsonInt format_version(1);
      ObJsonString uri(ObString::make_string("file:///a"));
      ObJsonString metadata(string(value));
      REQUIRE(OB_SUCCESS == descriptor.object_add(
          ObString::make_string("format_version"), &format_version));
      REQUIRE(OB_SUCCESS == descriptor.object_add(ObString::make_string("uri"), &uri));
      REQUIRE(OB_SUCCESS == descriptor.object_add(ObString::make_string(field), &metadata));
      ObString encoded;
      const int ret = FileRefCodec::from_json(allocator, descriptor, encoded);
      REQUIRE(length == maximum ? ret == OB_SUCCESS : ret != OB_SUCCESS);
      if (length == maximum) {
        FileRef ref;
        REQUIRE(OB_SUCCESS == FileRefCodec::decode(encoded, ref));
        REQUIRE(field[0] == 'm' ? ref.mime_type_.length() == length
                              : ref.object_version_.length() == length);
      } else {
        REQUIRE(encoded.empty());
      }
    }
  }
  return true;
}

bool test_payload_validation()
{
  TestAllocator allocator;
  ObString encoded;
  REQUIRE(OB_SUCCESS == FileRefCodec::from_path(
      allocator, ObString::make_string("/tmp/a"), encoded));
  const std::string valid(encoded.ptr(), encoded.length());
  FileRef ref;
  for (int64_t size = 0; size < encoded.length(); ++size) {
    REQUIRE(OB_SUCCESS != FileRefCodec::decode(
        ObString(static_cast<int32_t>(size), encoded.ptr()), ref));
    REQUIRE(ref.uri_.empty() && ref.size_bytes_ == -1);
  }
  std::string corrupted = valid + '\0';
  REQUIRE(OB_SUCCESS != FileRefCodec::decode(string(corrupted), ref));
  corrupted = valid;
  corrupted[0] = 0;
  REQUIRE(OB_SUCCESS != FileRefCodec::decode(string(corrupted), ref));
  corrupted = valid;
  corrupted[4] = 2;
  REQUIRE(OB_NOT_SUPPORTED == FileRefCodec::decode(string(corrupted), ref));
  for (const int32_t length : {-1, 0, INT32_MAX}) {
    corrupted = valid;
    int64_t pos = 5;
    REQUIRE(OB_SUCCESS == serialization::encode_i32(
        corrupted.data(), corrupted.size(), pos, length));
    REQUIRE(OB_SUCCESS != FileRefCodec::decode(string(corrupted), ref));
  }
  corrupted = valid;
  int64_t pos = 17;
  REQUIRE(OB_SUCCESS == serialization::encode_i64(
      corrupted.data(), corrupted.size(), pos, -2));
  REQUIRE(OB_SUCCESS != FileRefCodec::decode(string(corrupted), ref));
  corrupted = valid;
  corrupted[25] = 'h';
  REQUIRE(OB_SUCCESS != FileRefCodec::decode(string(corrupted), ref));
  corrupted = valid;
  corrupted.back() = '\0';
  REQUIRE(OB_SUCCESS != FileRefCodec::decode(string(corrupted), ref));
  corrupted.assign(FileRefCodec::MAX_REF_BYTES + 1, 'a');
  REQUIRE(OB_SUCCESS != FileRefCodec::decode(string(corrupted), ref));
  return true;
}

bool test_allocation_failures()
{
  TestAllocator allocator;
  ObString encoded;
  allocator.fail_at(1);
  REQUIRE(OB_ALLOCATE_MEMORY_FAILED == FileRefCodec::from_path(
      allocator, ObString::make_string("/a"), encoded));
  REQUIRE(encoded.empty());
  allocator.fail_at(-1);
  REQUIRE(OB_SUCCESS == FileRefCodec::from_path(
      allocator, ObString::make_string("/a"), encoded));
  allocator.fail_at(1);
  ObString json = ObString::make_string("old");
  REQUIRE(OB_SUCCESS != FileRefCodec::to_json(allocator, encoded, json));
  REQUIRE(json.empty());
  return true;
}

bool test_object_and_datum()
{
  static_assert(ObMySQLDateType == 41 && ObMySQLDateTimeType == 42);
  static_assert(ObFileType == 43 && ObMaxType == 44);
  static_assert(ObTimestampLTZType == 47 && ObTimestampNanoType == 48);
  static_assert(ObFileTC == 29 && ObMaxTC == 30);
  TestAllocator allocator;
  ObString encoded;
  REQUIRE(OB_SUCCESS == FileRefCodec::from_path(
      allocator, ObString::make_string("/tmp/image.png"), encoded));
  ObObj object;
  object.set_file(encoded);
  REQUIRE(object.is_file() && object.get_type() == ObFileType && object.get_type_class() == ObFileTC);
  REQUIRE(!object.is_json() && !object.is_string_type() && !object.is_lob_storage());
  REQUIRE(object.get_collation_type() == CS_TYPE_BINARY);
  REQUIRE(object.need_deep_copy() && object.get_deep_copy_size() == encoded.length());
  REQUIRE(object.get_data_ptr() == encoded.ptr() && object.get_data_length() == encoded.length());
  REQUIRE(object.get_tight_data_len() == encoded.length());
  REQUIRE(object.get_deep_copy_obj_ptr() == encoded.ptr());
  REQUIRE(ObDatum::get_obj_datum_map_type(ObFileType) == OBJ_DATUM_STRING);
  REQUIRE(get_vec_value_tc(ObFileType, -1, -1) == VEC_TC_STRING);
  REQUIRE(ObAccuracy::DDL_DEFAULT_ACCURACY[ObFileType].get_length() == FileRefCodec::MAX_REF_BYTES);
  REQUIRE(ObAccuracy::DML_DEFAULT_ACCURACY[ObFileType].get_length() == FileRefCodec::MAX_REF_BYTES);
  int32_t display_length = 0;
  REQUIRE(OB_SUCCESS == ObField::get_field_mb_length(
      ObFileType, ObAccuracy(), CS_TYPE_BINARY, display_length));
  REQUIRE(display_length == FileRefCodec::MAX_REF_BYTES);
  char copy_buf[FileRefCodec::MAX_REF_BYTES];
  int64_t pos = 0;
  ObObj copied;
  REQUIRE(OB_SUCCESS == copied.deep_copy(object, copy_buf, sizeof(copy_buf), pos));
  REQUIRE(copied.is_file() && copied.get_file() == encoded && copied.get_file().ptr() != encoded.ptr());
  REQUIRE(pos == encoded.length());
  int cmp = 10;
  REQUIRE(OB_SUCCESS == object.compare(copied, cmp) && cmp == 0);
  bool equal = false;
  REQUIRE(OB_SUCCESS == object.equal(copied, equal) && equal);
  uint64_t object_hash = 0;
  uint64_t copied_hash = 0;
  REQUIRE(OB_SUCCESS == object.hash(object_hash, 42));
  REQUIRE(OB_SUCCESS == copied.hash(copied_hash, 42) && object_hash == copied_hash);

  ObDatum datum;
  REQUIRE(OB_SUCCESS == datum.from_obj(object));
  REQUIRE(datum.get_string() == encoded);
  ObObj converted;
  REQUIRE(OB_SUCCESS == datum.to_obj(converted, object.get_meta()));
  REQUIRE(converted.is_file() && converted.get_file() == encoded);
  datum.set_null();
  REQUIRE(OB_SUCCESS == datum.to_obj(converted, object.get_meta()) && converted.is_null());

  char serialized[FileRefCodec::MAX_REF_BYTES + 64];
  object.set_collation_level(CS_LEVEL_EXPLICIT);
  object.set_scale(3);
  pos = 0;
  REQUIRE(OB_SUCCESS == object.serialize(serialized, sizeof(serialized), pos));
  REQUIRE(pos == object.get_serialize_size());
  int64_t read_pos = 0;
  ObObj restored;
  REQUIRE(OB_SUCCESS == restored.deserialize(serialized, pos, read_pos));
  REQUIRE(read_pos == pos && restored.is_file() && restored.get_file() == encoded);
  REQUIRE(restored.get_meta() == object.get_meta() && restored.get_scale() == object.get_scale());
  ObDatum restored_datum;
  REQUIRE(OB_SUCCESS == restored_datum.from_obj(restored));
  REQUIRE(restored_datum.get_string() == encoded);
  char printed[FileRefCodec::MAX_REF_BYTES];
  pos = 0;
  REQUIRE(OB_SUCCESS == restored.print_plain_str_literal(printed, sizeof(printed), pos));
  ObJsonNode *descriptor = nullptr;
  REQUIRE(OB_SUCCESS == ObJsonParser::get_tree(
      &allocator, ObString(static_cast<int32_t>(pos), printed), descriptor, ObJsonParser::JSN_STRICT_FLAG));
  ObString roundtrip;
  REQUIRE(OB_SUCCESS == FileRefCodec::from_json(allocator, *descriptor, roundtrip) && roundtrip == encoded);
  pos = 0;
  REQUIRE(OB_SUCCESS == restored.print_sql_literal(printed, sizeof(printed), pos));
  const std::string literal(printed, pos);
  REQUIRE(literal.starts_with("TO_FILE('") && literal.ends_with("')"));
  FileRef ref;
  REQUIRE(OB_SUCCESS == FileRefCodec::decode(encoded, ref));
  REQUIRE(literal.find(std::string(ref.uri_.ptr(), ref.uri_.length())) != std::string::npos);
  ObObj invalid;
  invalid.set_file(ObString::make_string("not a FILE payload"));
  pos = 0;
  REQUIRE(OB_SUCCESS != invalid.serialize(serialized, sizeof(serialized), pos));
  return true;
}

bool test_compact_cell_restart()
{
  TestAllocator allocator;
  ObString encoded;
  REQUIRE(OB_SUCCESS == FileRefCodec::from_path(
      allocator, ObString::make_string("/tmp/restart.png"), encoded));
  ObObj file;
  file.set_file(encoded);
  for (const ObCompactStoreType store : {DENSE, SPARSE}) {
    char buffer[FileRefCodec::MAX_REF_BYTES + 128];
    ObCellWriter writer;
    REQUIRE(OB_SUCCESS == writer.init(buffer, sizeof(buffer), store));
    ObObj cloned;
    REQUIRE(OB_SUCCESS == writer.append(17, file, &cloned));
    REQUIRE(cloned.is_file() && cloned.get_file() == encoded && cloned.get_file().ptr() != encoded.ptr());
    REQUIRE(OB_SUCCESS == writer.row_finish());
    ObCellReader reader;
    REQUIRE(OB_SUCCESS == reader.init(buffer, writer.size(), store));
    REQUIRE(OB_SUCCESS == reader.next_cell());
    uint64_t column_id = 0;
    ObObj restored;
    REQUIRE(OB_SUCCESS == reader.get_cell(column_id, restored));
    REQUIRE(restored.is_file() && restored.get_file() == encoded);
    REQUIRE(store != SPARSE || column_id == 17);
    FileRef ref;
    REQUIRE(OB_SUCCESS == FileRefCodec::decode(restored.get_file(), ref));
    REQUIRE(ref.uri_ == ObString::make_string("file:///tmp/restart.png"));

    std::string snapshot(buffer, writer.size());
    ObCellReader restarted;
    REQUIRE(OB_SUCCESS == restarted.init(snapshot.data(), snapshot.size(), store));
    REQUIRE(OB_SUCCESS == restarted.next_cell());
    REQUIRE(OB_SUCCESS == restarted.get_cell(column_id, restored));
    REQUIRE(restored.is_file() && restored.get_file() == encoded);
    const int64_t value_offset = restored.get_file().ptr() - snapshot.data();
    snapshot[value_offset + 4] = 2;
    ObCellReader corrupt_reader;
    REQUIRE(OB_SUCCESS == corrupt_reader.init(snapshot.data(), snapshot.size(), store));
    REQUIRE(OB_SUCCESS != corrupt_reader.next_cell());
  }
  return true;
}

bool test_sql_literal_metadata_escaping()
{
  TestAllocator allocator;
  const char *text = R"({"format_version":1,"uri":"file:///tmp/quote%27.png","mime_type":"image/png","object_version":"v'\\\"suffix","size_bytes":42})";
  ObJsonNode *descriptor = nullptr;
  REQUIRE(OB_SUCCESS == ObJsonParser::get_tree(
      &allocator, ObString::make_string(text), descriptor, ObJsonParser::JSN_STRICT_FLAG));
  ObString encoded;
  ObString json;
  REQUIRE(OB_SUCCESS == FileRefCodec::from_json(allocator, *descriptor, encoded));
  REQUIRE(OB_SUCCESS == FileRefCodec::to_json(allocator, encoded, json));
  ObObj file;
  file.set_file(encoded);
  ObObj json_text;
  json_text.set_varchar(json);
  json_text.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  for (const bool skip_escape : {false, true}) {
    ObObjPrintParams params;
    params.skip_escape_ = skip_escape;
    char expected[FileRefCodec::MAX_REF_BYTES];
    int64_t expected_length = 0;
    REQUIRE(OB_SUCCESS == json_text.print_sql_literal(
        expected, sizeof(expected), expected_length, params));
    char actual[FileRefCodec::MAX_REF_BYTES];
    int64_t actual_length = 0;
    REQUIRE(OB_SUCCESS == file.print_sql_literal(actual, sizeof(actual), actual_length, params));
    REQUIRE(std::string(actual, actual_length)
        == "TO_FILE(" + std::string(expected, expected_length) + ")");
    REQUIRE(std::string(actual, actual_length).find("object_version") != std::string::npos);
    REQUIRE(std::string(actual, actual_length).find("size_bytes") != std::string::npos);
  }
  return true;
}
}

int main()
{
  struct TestCase { const char *name_; bool (*run_)(); };
  const TestCase tests[] = {
    {"path normalization and borrowed references", test_path_normalization},
    {"invalid paths and URI boundaries", test_invalid_paths_and_limits},
    {"remote URL byte-preserving interchange and policy parts", test_remote_url_roundtrip_and_policy_parts},
    {"canonical JSON interchange", test_json_interchange},
    {"strict JSON descriptor validation", test_invalid_json},
    {"versioned payload validation", test_payload_validation},
    {"allocator failures", test_allocation_failures},
    {"native object and datum", test_object_and_datum},
    {"compact row persistence and restart", test_compact_cell_restart},
    {"SQL literal metadata and escaping", test_sql_literal_metadata_escaping},
  };
  for (const TestCase &test : tests) {
    if (!test.run_()) {
      std::fprintf(stderr, "FAIL %s\n", test.name_);
      return 1;
    }
    std::printf("PASS %s\n", test.name_);
  }
  return 0;
}
