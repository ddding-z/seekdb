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
#include "lib/allocator/ob_allocator.h"
#include "share/datum/ob_datum_funcs.h"
#include "share/object/ob_obj_cast.h"
#include "share/vector/ob_bit_vector.h"
#include "query/protocol/ob_mysql_protocol_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/blocksstable/encoding/ob_micro_block_encoder.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/blocksstable/index_block/ob_index_block_util.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/blocksstable/ob_row_writer.h"
#include <cstdio>
#include <new>
#include <string>
#include <vector>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share::schema;

namespace
{
#define REQUIRE(condition) \
  do { \
    if (!(condition)) { \
      std::fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition); \
      return false; \
    } \
  } while (false)

bool test_datum_basic_functions()
{
  ObArenaAllocator allocator("FileTest");
  ObString encoded[2];
  REQUIRE(OB_SUCCESS == FileRefCodec::from_path(
      allocator, ObString::make_string("/tmp/a"), encoded[0]));
  REQUIRE(OB_SUCCESS == FileRefCodec::from_path(
      allocator, ObString::make_string("/tmp/b"), encoded[1]));
  ObDatum datums[3];
  datums[0].set_string(encoded[0]);
  datums[1].set_string(encoded[1]);
  datums[2].set_null();
  ObDatumBasicFuncs *functions = ObDatumFuncs::get_basic_func(ObFileType, CS_TYPE_BINARY);
  REQUIRE(functions != nullptr);
  REQUIRE(ObDatumFuncs::is_null_aware_hash_type(ObFileType));
  int cmp = 42;
  REQUIRE(OB_SUCCESS == functions->null_first_cmp_(datums[0], datums[1], cmp, nullptr) && cmp == -1);
  REQUIRE(OB_SUCCESS == functions->null_last_cmp_(datums[1], datums[0], cmp, nullptr) && cmp == 1);
  REQUIRE(OB_SUCCESS == functions->null_first_cmp_(datums[0], datums[0], cmp, nullptr) && cmp == 0);
  REQUIRE(OB_SUCCESS == functions->null_first_cmp_(datums[2], datums[0], cmp, nullptr) && cmp == -1);
  REQUIRE(OB_SUCCESS == functions->null_last_cmp_(datums[2], datums[0], cmp, nullptr) && cmp == 1);
  REQUIRE(OB_SUCCESS == functions->null_first_cmp_(datums[2], datums[2], cmp, nullptr) && cmp == 0);

  int64_t boundary_value = 0;
  ObDatum boundary;
  boundary.ptr_ = reinterpret_cast<const char *>(&boundary_value);
  boundary.set_int(ObObj::MIN_OBJECT_VALUE);
  ObDatumCmpFuncType to_boundary = ObDatumFuncs::get_nullsafe_cmp_func(
      ObFileType, ObExtendType, NULL_LAST, CS_TYPE_BINARY, -1, false);
  ObDatumCmpFuncType from_boundary = ObDatumFuncs::get_nullsafe_cmp_func(
      ObExtendType, ObFileType, NULL_LAST, CS_TYPE_BINARY, -1, false);
  REQUIRE(to_boundary != nullptr && from_boundary != nullptr);
  REQUIRE(OB_SUCCESS == to_boundary(datums[0], boundary, cmp, nullptr) && cmp == 1);
  REQUIRE(OB_SUCCESS == from_boundary(boundary, datums[0], cmp, nullptr) && cmp == -1);
  boundary.set_int(ObObj::MAX_OBJECT_VALUE);
  REQUIRE(OB_SUCCESS == to_boundary(datums[0], boundary, cmp, nullptr) && cmp == -1);
  REQUIRE(OB_SUCCESS == from_boundary(boundary, datums[0], cmp, nullptr) && cmp == 1);

  const ObDatumHashFuncType scalar[] = {
    functions->default_hash_, functions->murmur_hash_, functions->xx_hash_,
    functions->wy_hash_, functions->murmur_hash_v2_,
  };
  const ObBatchDatumHashFunc batch[] = {
    functions->default_hash_batch_, functions->murmur_hash_batch_, functions->xx_hash_batch_,
    functions->wy_hash_batch_, functions->murmur_hash_v2_batch_,
  };
  alignas(sql::ObBitVector::DATA_ALIGN_SIZE) char skip_buf[sizeof(uint64_t)];
  sql::ObBitVector &skip = *new (skip_buf) sql::ObBitVector;
  skip.init(3);
  const uint64_t seeds[] = {42, 43, 44};
  for (int64_t kind = 0; kind < 5; ++kind) {
    REQUIRE(scalar[kind] != nullptr && batch[kind] != nullptr);
    uint64_t hashes[3] = {};
    batch[kind](hashes, datums, true, skip, 3, seeds, true, nullptr);
    for (int64_t i = 0; i < 3; ++i) {
      uint64_t expected = 0;
      REQUIRE(OB_SUCCESS == scalar[kind](datums[i], seeds[i], expected, nullptr));
      REQUIRE(hashes[i] == expected);
    }
    skip.set(1);
    hashes[1] = 123;
    batch[kind](hashes, datums, true, skip, 3, seeds, false, nullptr);
    REQUIRE(hashes[1] == 123);
    skip.unset(1);
  }
  return true;
}

bool test_cast_restrictions()
{
  ObArenaAllocator allocator("FileTest");
  ObString encoded;
  REQUIRE(OB_SUCCESS == FileRefCodec::from_path(
      allocator, ObString::make_string("/tmp/cast"), encoded));
  ObObj file;
  file.set_file(encoded);
  ObCastCtx context;
  context.allocator_v2_ = &allocator;
  ObObj output;
  REQUIRE(OB_SUCCESS == ObObjCaster::to_type(ObFileType, context, file, output));
  REQUIRE(output.is_file() && output.get_file() == encoded);
  ObObj null;
  null.set_null();
  REQUIRE(OB_SUCCESS == ObObjCaster::to_type(ObFileType, context, null, output));
  REQUIRE(output.is_null());
  for (int64_t type = ObNullType; type < ObMaxType; ++type) {
    const ObObjType candidate = static_cast<ObObjType>(type);
    REQUIRE(cast_supported(candidate, CS_TYPE_BINARY, ObFileType, CS_TYPE_BINARY)
        == (candidate == ObNullType || candidate == ObFileType));
    REQUIRE(cast_supported(ObFileType, CS_TYPE_BINARY, candidate, CS_TYPE_BINARY)
        == (candidate == ObFileType));
  }
  ObObj text;
  text.set_varchar(ObString::make_string("file:///tmp/cast"));
  text.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  REQUIRE(OB_SUCCESS != ObObjCaster::to_type(ObFileType, context, text, output));
  REQUIRE(OB_SUCCESS != ObObjCaster::to_type(ObVarcharType, context, file, output));
  REQUIRE(OB_SUCCESS != ObObjCaster::to_type(ObJsonType, context, file, output));
  bool monotonic = false;
  REQUIRE(OB_SUCCESS == ObObjCaster::is_cast_monotonic(ObFileType, ObFileType, monotonic));
  REQUIRE(monotonic);
  REQUIRE(OB_SUCCESS == ObObjCaster::is_cast_monotonic(ObFileType, ObVarcharType, monotonic));
  REQUIRE(!monotonic);
  return true;
}

bool test_encoded_restart(const ObColumnHeader::Type encoding)
{
  ObArenaAllocator allocator("FileTest");
  ObArray<ObColDesc> columns;
  ObColDesc key;
  key.col_id_ = 16;
  key.col_type_.set_int();
  REQUIRE(OB_SUCCESS == columns.push_back(key));
  ObColDesc file;
  file.col_id_ = 17;
  file.col_type_.set_file();
  REQUIRE(OB_SUCCESS == columns.push_back(file));
  bool enabled[ObColumnHeader::MAX_TYPE] = {};
  enabled[ObColumnHeader::RAW] = true;
  enabled[encoding] = true;
  int64_t forced[] = {ObColumnHeader::RAW, encoding};
  ObMicroBlockEncodingCtx context;
  context.macro_block_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
  context.micro_block_size_ = 64 * 1024;
  context.rowkey_column_cnt_ = 1;
  context.column_cnt_ = 2;
  context.col_descs_ = &columns;
  context.encoder_opt_.encodings_ = enabled;
  context.column_encodings_ = forced;
  context.need_calc_column_chksum_ = true;
  context.compressor_type_ = NONE_COMPRESSOR;
  REQUIRE(context.is_valid());
  std::vector<ObString> references;
  for (int64_t i = 0; i < 7; ++i) {
    const std::string path = "/tmp/encoded/file-" + std::to_string(i) + ".png";
    ObString encoded;
    REQUIRE(OB_SUCCESS == FileRefCodec::from_path(
        allocator, ObString(static_cast<int32_t>(path.size()), path.data()), encoded));
    references.push_back(encoded);
  }
  constexpr int64_t ROWS = 96;
  std::vector<int64_t> expected;
  std::string snapshot;
  {
    ObMicroBlockEncoder encoder;
    REQUIRE(OB_SUCCESS == encoder.init(context));
    ObDatumRow row;
    REQUIRE(OB_SUCCESS == row.init(allocator, 2));
    for (int64_t i = 0; i < ROWS; ++i) {
      row.storage_datums_[0].set_int(i);
      const int64_t index = i % 11 == 0 ? -1
          : encoding == ObColumnHeader::CONST ? 0 : i % references.size();
      expected.push_back(index);
      if (index < 0) {
        row.storage_datums_[1].set_null();
      } else {
        row.storage_datums_[1].set_string(references[index]);
      }
      REQUIRE(OB_SUCCESS == encoder.append_row(row));
    }
    char *block = nullptr;
    int64_t size = 0;
    REQUIRE(OB_SUCCESS == encoder.build_block(block, size));
    REQUIRE(block != nullptr && size > 0);
    const ObMicroBlockHeader *header = reinterpret_cast<const ObMicroBlockHeader *>(block);
    const ObColumnHeader *column_headers =
        reinterpret_cast<const ObColumnHeader *>(block + header->header_size_);
    REQUIRE(column_headers[1].obj_type_ == ObFileType);
    REQUIRE(column_headers[1].type_ == encoding);
    snapshot.assign(block, size);
    encoder.reset();
  }
  ObMicroBlockDecoder decoder;
  ObMicroBlockData block(snapshot.data(), snapshot.size());
  REQUIRE(OB_SUCCESS == decoder.init(block, static_cast<const ObStorageDatumUtils *>(nullptr)));
  int64_t row_count = 0;
  REQUIRE(OB_SUCCESS == decoder.get_row_count(row_count) && row_count == ROWS);
  ObDatumRow row;
  REQUIRE(OB_SUCCESS == row.init(allocator, 2));
  for (int64_t i = 0; i < ROWS; ++i) {
    REQUIRE(OB_SUCCESS == decoder.get_row(i, row));
    REQUIRE(row.storage_datums_[0].get_int() == i);
    ObObj restored;
    REQUIRE(OB_SUCCESS == row.storage_datums_[1].to_obj(restored, file.col_type_));
    if (expected[i] < 0) {
      REQUIRE(restored.is_null());
    } else {
      REQUIRE(restored.is_file() && restored.get_file() == references[expected[i]]);
      FileRef ref;
      REQUIRE(OB_SUCCESS == FileRefCodec::decode(restored.get_file(), ref));
    }
  }
  return true;
}

bool test_flat_row_restart()
{
  ObArenaAllocator allocator("FileTest");
  ObString encoded;
  REQUIRE(OB_SUCCESS == FileRefCodec::from_path(
      allocator, ObString::make_string("/tmp/flat-row"), encoded));
  ObObjMeta meta;
  meta.set_file();
  for (const bool is_null : {false, true}) {
    std::string snapshot;
    {
      ObDatumRow row;
      REQUIRE(OB_SUCCESS == row.init(allocator, 2));
      row.storage_datums_[0].set_int(42);
      if (is_null) {
        row.storage_datums_[1].set_null();
      } else {
        row.storage_datums_[1].set_string(encoded);
      }
      ObRowWriter writer;
      char *bytes = nullptr;
      int64_t length = 0;
      REQUIRE(OB_SUCCESS == writer.write(1, row, bytes, length));
      snapshot.assign(bytes, length);
    }
    ObRowReader reader;
    ObDatumRow restored_row;
    REQUIRE(OB_SUCCESS == restored_row.init(allocator, 2));
    REQUIRE(OB_SUCCESS == reader.read_row(
        snapshot.data(), snapshot.size(), nullptr, restored_row));
    REQUIRE(restored_row.storage_datums_[0].get_int() == 42);
    ObObj restored;
    REQUIRE(OB_SUCCESS == restored_row.storage_datums_[1].to_obj(restored, meta));
    REQUIRE(is_null ? restored.is_null() : restored.is_file() && restored.get_file() == encoded);
  }
  REQUIRE(is_skip_index_black_list_type(ObFileType));
  REQUIRE(!is_skip_index_while_list_type(ObFileType));
  REQUIRE(!can_agg_sum(ObFileType));
  return true;
}

bool test_mysql_export()
{
  ObArenaAllocator allocator("FileTest");
  ObString encoded;
  REQUIRE(OB_SUCCESS == FileRefCodec::from_path(
      allocator, ObString::make_string("/tmp/mysql-export"), encoded));
  ObObj file;
  file.set_file(encoded);
  obmysql::EMySQLFieldType wire_type = obmysql::EMySQLFieldType::MYSQL_TYPE_NULL;
  uint16_t flags = 0;
  ObScale decimals = 0;
  REQUIRE(OB_SUCCESS == ObSMUtils::get_mysql_type(ObFileType, wire_type, flags, decimals));
  REQUIRE(wire_type == obmysql::EMySQLFieldType::MYSQL_TYPE_JSON);
  REQUIRE((flags & (BLOB_FLAG | BINARY_FLAG)) == (BLOB_FLAG | BINARY_FLAG));
  sql::ObSQLSessionInfo session;
  ObDataTypeCastParams parameters;
  ObString expected;
  REQUIRE(OB_SUCCESS == FileRefCodec::to_json(allocator, encoded, expected));
  for (const obmysql::MYSQL_PROTOCOL_TYPE protocol : {obmysql::TEXT, obmysql::BINARY}) {
    obmysql::ObMySQLCellValue cell;
    REQUIRE(OB_SUCCESS == ObSMUtils::build_cell_value(
        file, protocol, allocator, cell, parameters, nullptr, session));
    REQUIRE(cell.kind_ == obmysql::ObMySQLCellValueKind::LENENC_BYTES);
    REQUIRE(ObString(static_cast<int32_t>(cell.get_bytes_len()), cell.get_bytes()) == expected);
    REQUIRE(cell.get_bytes() != encoded.ptr());
    ObObj invalid;
    invalid.set_file(ObString::make_string("invalid FILE payload"));
    REQUIRE(OB_SUCCESS != ObSMUtils::build_cell_value(
        invalid, protocol, allocator, cell, parameters, nullptr, session));
    REQUIRE(cell.kind_ == obmysql::ObMySQLCellValueKind::NULL_VALUE);
  }
  return true;
}
}

int main()
{
  if (!test_datum_basic_functions() || !test_cast_restrictions()
      || !test_flat_row_restart() || !test_mysql_export()) {
    return 1;
  }
  std::puts("PASS FILE datum functions, casts, flat-row restart, and MySQL text/binary export");
  for (const ObColumnHeader::Type encoding : {
      ObColumnHeader::RAW, ObColumnHeader::DICT, ObColumnHeader::CONST,
      ObColumnHeader::RLE, ObColumnHeader::STRING_PREFIX}) {
    if (!test_encoded_restart(encoding)) {
      return 1;
    }
    std::printf("PASS FILE encoded microblock restart: %d\n", encoding);
  }
  return 0;
}
