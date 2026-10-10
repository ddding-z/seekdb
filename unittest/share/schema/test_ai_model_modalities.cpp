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

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include "common/json_type/ob_json_base.h"
#include "observer/schema/ob_schema_retrieve_utils.h"
#include "observer/schema/ob_schema_service_sql_impl.h"
#include "share/ai_service/ob_ai_model_info.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_ai_model_mgr.h"
#include "share/schema/ob_ai_model_sql_service.h"
#include "share/schema/ob_multi_version_schema_service.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace
{
void require(bool condition, const std::string &message)
{
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void require_equal(int64_t actual, int64_t expected, const std::string &message)
{
  if (actual != expected) {
    std::ostringstream diagnostic;
    diagnostic << message << ": expected " << expected << ", got " << actual;
    throw std::runtime_error(diagnostic.str());
  }
}

int parse_model(ObArenaAllocator &allocator, const std::string &json, ObAiServiceModelInfo &model)
{
  ObIJsonBase *base = nullptr;
  require_equal(ObJsonBaseFactory::get_json_base(
      &allocator, ObString(json.size(), json.data()), ObJsonInType::JSON_TREE,
      ObJsonInType::JSON_TREE, base), OB_SUCCESS, "parse JSON fixture");
  require(base != nullptr, "JSON fixture has a root");
  return model.parse_from_json_base(ObString::make_string("modality_model"), *base);
}

std::string model_json(const std::string &type, const std::string &modalities = "")
{
  return "{\"model_name\":\"mock-model\",\"type\":\"" + type + "\""
      + (modalities.empty() ? "" : ",\"input_modalities\":" + modalities) + "}";
}

void make_schema(ObAiModelSchema &schema, const ObAiServiceModelInfo &model)
{
  require_equal(schema.assign(model), OB_SUCCESS, "assign model information");
  schema.set_model_id(3010);
  schema.set_schema_version(42);
  schema.set_case_mode(OB_LOWERCASE_AND_INSENSITIVE);
  require(schema.is_valid(), "assigned model schema is valid");
}

template<typename T>
std::vector<char> serialize(const T &value)
{
  std::vector<char> buffer(value.get_serialize_size());
  int64_t pos = 0;
  require_equal(value.serialize(buffer.data(), buffer.size(), pos), OB_SUCCESS, "serialize");
  require_equal(pos, buffer.size(), "serialized size");
  return buffer;
}

template<typename T>
int deserialize(const std::vector<char> &buffer, T &value)
{
  int64_t pos = 0;
  const int ret = value.deserialize(buffer.data(), buffer.size(), pos);
  if (ret == OB_SUCCESS) {
    require_equal(pos, buffer.size(), "deserializer consumes the complete v1 envelope");
  }
  return ret;
}

class LegacyModelInfo
{
  OB_UNIS_VERSION(1);
public:
  ObString name_ = ObString::make_string("legacy_model");
  EndpointType::TYPE type_ = EndpointType::COMPLETION;
  ObString model_name_ = ObString::make_string("legacy-provider-model");
};
OB_SERIALIZE_MEMBER(LegacyModelInfo, name_, type_, model_name_);

class LegacyModelSchema
{
  OB_UNIS_VERSION(1);
public:
  uint64_t model_id_ = 3020;
  ObString name_ = ObString::make_string("legacy_model");
  EndpointType::TYPE type_ = EndpointType::COMPLETION;
  ObString model_name_ = ObString::make_string("legacy-provider-model");
  int64_t schema_version_ = 43;
  ObNameCaseMode case_mode_ = OB_LOWERCASE_AND_INSENSITIVE;
};
OB_SERIALIZE_MEMBER(LegacyModelSchema,
                    model_id_, name_, type_, model_name_, schema_version_, case_mode_);

void defaults_and_reset()
{
  ObArenaAllocator allocator("ModalityTest");
  ObAiServiceModelInfo model;
  ObAiModelSchema schema;
  require_equal(model.get_input_modalities(), AiInputModality::TEXT, "default model capability");
  require_equal(schema.get_input_modalities(), AiInputModality::TEXT, "default schema capability");
  require_equal(parse_model(allocator, model_json("COMPLETION", "[\"text\",\"image\",\"audio\"]"), model),
                OB_SUCCESS, "parse all modalities");
  make_schema(schema, model);
  model.reset();
  schema.reset();
  require_equal(model.get_input_modalities(), AiInputModality::TEXT, "reset model capability");
  require_equal(schema.get_input_modalities(), AiInputModality::TEXT, "reset schema capability");
}

void valid_declarations()
{
  ObArenaAllocator allocator("ModalityTest");
  ObAiServiceModelInfo model;
  const struct {
    const char *json_;
    int64_t mask_;
  } declarations[] = {
    {"[\"text\"]", 1},
    {"[\"image\",\"text\"]", 3},
    {"[\"audio\",\"text\"]", 5},
    {"[\"audio\",\"image\",\"text\"]", 7},
  };
  for (const auto &declaration : declarations) {
    require_equal(parse_model(allocator, model_json("COMPLETION", declaration.json_), model),
                  OB_SUCCESS, "valid completion declaration");
    require_equal(model.get_input_modalities(), declaration.mask_, "declared bitmask");
  }
  for (const char *type : {"COMPLETION", "DENSE_EMBEDDING", "SPARSE_EMBEDDING", "RERANK"}) {
    require_equal(parse_model(allocator, model_json(type), model), OB_SUCCESS, "legacy declaration");
    require_equal(model.get_input_modalities(), AiInputModality::TEXT, "legacy declaration defaults to text");
    require_equal(parse_model(allocator, model_json(type, "[\"text\"]"), model),
                  OB_SUCCESS, "explicit text declaration");
  }
}

void invalid_declarations()
{
  ObArenaAllocator allocator("ModalityTest");
  ObAiServiceModelInfo model;
  for (const char *modalities : {
      "null", "true", "1", "\"text\"", "{}", "[]",
      "[\"text\",1]", "[\"text\",null]", "[\"text\",true]", "[\"text\",[]]", "[\"text\",{}]",
      "[\"image\"]", "[\"audio\"]", "[\"image\",\"audio\"]",
      "[\"TEXT\"]", "[\"text\",\"Image\"]", "[\"text\",\"AUDIO\"]", "[\" text \"]",
      "[\"text\",\"video\"]", "[\"text\",\"\"]", "[\"text\",\"text\"]",
      "[\"text\",\"image\",\"image\"]", "[\"text\",\"audio\",\"audio\"]",
      "[\"text\",\"image\",\"audio\",\"text\"]"}) {
    require_equal(parse_model(allocator, model_json("COMPLETION", modalities), model),
                  OB_AI_FUNC_PARAM_VALUE_INVALID, std::string("reject ") + modalities);
  }
}

void media_type_restrictions()
{
  ObArenaAllocator allocator("ModalityTest");
  ObAiServiceModelInfo model;
  for (const char *type : {"DENSE_EMBEDDING", "SPARSE_EMBEDDING", "RERANK"}) {
    for (const char *modalities : {"[\"text\",\"image\"]", "[\"text\",\"audio\"]",
                                  "[\"text\",\"image\",\"audio\"]"}) {
      require_equal(parse_model(allocator, model_json(type, modalities), model),
                    OB_AI_FUNC_PARAM_VALUE_INVALID, std::string("reject media on ") + type);
    }
  }
}

void assignment_and_manager_copy()
{
  ObArenaAllocator allocator("ModalityTest");
  ObAiServiceModelInfo model;
  require_equal(parse_model(allocator, model_json("COMPLETION", "[\"text\",\"image\",\"audio\"]"), model),
                OB_SUCCESS, "parse multimodal model");
  const ObAiServiceModelInfo model_copy = model;
  require_equal(model_copy.get_input_modalities(), AiInputModality::ALL, "plain info copy");
  ObAiModelSchema source;
  ObAiModelSchema copy;
  make_schema(source, model);
  require_equal(copy.assign(source), OB_SUCCESS, "assign schema");
  require_equal(copy.get_input_modalities(), AiInputModality::ALL, "schema assignment capability");
  require(copy.get_name() == source.get_name(), "schema assignment name");
  require(copy.get_name().ptr() != source.get_name().ptr(), "schema assignment deep-copies strings");
  require_equal(copy.assign(copy), OB_SUCCESS, "self assignment");
  require_equal(copy.get_input_modalities(), AiInputModality::ALL, "self assignment capability");

  ObAiModelMgr manager;
  ObAiModelMgr manager_copy;
  require_equal(manager.init(), OB_SUCCESS, "initialize model manager");
  require_equal(manager_copy.init(), OB_SUCCESS, "initialize copy manager");
  require_equal(manager.add_ai_model(source, OB_LOWERCASE_AND_INSENSITIVE), OB_SUCCESS, "add model");
  require_equal(manager_copy.deep_copy(manager), OB_SUCCESS, "deep-copy model manager");
  const ObAiModelSchema *cached = nullptr;
  require_equal(manager_copy.get_ai_model_schema(source.get_ai_model_id(), cached),
                OB_SUCCESS, "lookup copied model");
  require(cached != nullptr, "copied manager retains model");
  require_equal(cached->get_input_modalities(), AiInputModality::ALL, "manager deep-copy capability");

  require_equal(parse_model(allocator, model_json("COMPLETION"), model), OB_SUCCESS, "parse legacy model");
  require_equal(copy.assign(model), OB_SUCCESS, "assign text-only info over multimodal schema");
  require_equal(copy.get_input_modalities(), AiInputModality::TEXT, "assignment resets stale capability");
}

void invalid_mask_validation()
{
  ObArenaAllocator allocator("ModalityTest");
  ObAiServiceModelInfo model;
  require_equal(parse_model(allocator, model_json("COMPLETION"), model), OB_SUCCESS, "parse text model");
  ObAiModelSchema schema;
  ObAiModelSchema copy;
  make_schema(schema, model);
  for (const int64_t mask : {0, 2, 4, 6, 8, 9, -1}) {
    model.set_input_modalities(mask);
    schema.set_input_modalities(mask);
    require_equal(model.check_valid(), OB_AI_FUNC_PARAM_VALUE_INVALID, "reject malformed model mask");
    require(!schema.is_valid(), "reject malformed schema mask");
    require_equal(copy.assign(schema), OB_INVALID_ARGUMENT, "reject malformed schema assignment");
    require_equal(copy.assign(model), OB_INVALID_ARGUMENT, "reject malformed info assignment");
  }
  for (const int64_t mask : {1, 3, 5, 7}) {
    model.set_input_modalities(mask);
    schema.set_input_modalities(mask);
    require_equal(model.check_valid(), OB_SUCCESS, "accept completion capability mask");
    require(schema.is_valid(), "valid completion schema mask");
  }
  schema.set_type(EndpointType::RERANK);
  require(!schema.is_valid(), "non-completion schema rejects media capability");
}

void info_serialization()
{
  ObArenaAllocator allocator("ModalityTest");
  ObAiServiceModelInfo source;
  require_equal(parse_model(allocator, model_json("COMPLETION", "[\"text\",\"image\",\"audio\"]"), source),
                OB_SUCCESS, "parse multimodal model");
  const auto source_bytes = serialize(source);
  ObAiServiceModelInfo decoded;
  require_equal(deserialize(source_bytes, decoded), OB_SUCCESS, "round-trip model info");
  require_equal(decoded.get_input_modalities(), AiInputModality::ALL, "round-trip info capability");
  require(decoded.get_name() == source.get_name(), "round-trip info name");
  require(decoded.get_model_name() == source.get_model_name(), "round-trip provider name");
  require_equal(decoded.get_type(), source.get_type(), "round-trip info type");

  LegacyModelInfo old_decoder;
  require_equal(deserialize(source_bytes, old_decoder), OB_SUCCESS, "old reader accepts appended tail");
  require(old_decoder.model_name_ == source.get_model_name(), "appending does not shift old fields");
  LegacyModelInfo legacy;
  const auto legacy_bytes = serialize(legacy);
  require_equal(deserialize(legacy_bytes, decoded), OB_SUCCESS, "read old model info into reused destination");
  require_equal(decoded.get_input_modalities(), AiInputModality::TEXT, "missing legacy info tail defaults to text");
  require(decoded.get_name() == legacy.name_, "legacy info name");

  source.set_input_modalities(9);
  const auto invalid_bytes = serialize(source);
  require_equal(deserialize(invalid_bytes, decoded), OB_AI_FUNC_PARAM_VALUE_INVALID,
                "malformed serialized info mask is not defaulted");
}

void schema_serialization()
{
  ObArenaAllocator allocator("ModalityTest");
  ObAiServiceModelInfo model;
  require_equal(parse_model(allocator, model_json("COMPLETION", "[\"text\",\"image\",\"audio\"]"), model),
                OB_SUCCESS, "parse multimodal model");
  ObAiModelSchema source;
  ObAiModelSchema decoded;
  make_schema(source, model);
  const auto source_bytes = serialize(source);
  require_equal(deserialize(source_bytes, decoded), OB_SUCCESS, "round-trip schema");
  require_equal(decoded.get_input_modalities(), AiInputModality::ALL, "round-trip schema capability");
  require_equal(decoded.get_ai_model_id(), source.get_ai_model_id(), "round-trip model ID");
  require_equal(decoded.get_schema_version(), source.get_schema_version(), "round-trip schema version");
  require_equal(decoded.get_case_mode(), source.get_case_mode(), "round-trip case mode");
  require(decoded.get_model_name() == source.get_model_name(), "round-trip schema provider name");

  LegacyModelSchema old_decoder;
  require_equal(deserialize(source_bytes, old_decoder), OB_SUCCESS, "old schema reader accepts tail");
  require_equal(old_decoder.case_mode_, source.get_case_mode(), "appending preserves last old field");
  LegacyModelSchema legacy;
  const auto legacy_bytes = serialize(legacy);
  require_equal(deserialize(legacy_bytes, decoded), OB_SUCCESS, "read old schema into reused destination");
  require_equal(decoded.get_input_modalities(), AiInputModality::TEXT, "missing legacy schema tail defaults to text");
  require_equal(decoded.get_ai_model_id(), legacy.model_id_, "legacy schema ID");

  source.set_input_modalities(0);
  const auto invalid_bytes = serialize(source);
  require_equal(deserialize(invalid_bytes, decoded), OB_ERR_UNEXPECTED,
                "malformed serialized schema mask is not defaulted");
  source.set_input_modalities(AiInputModality::ALL);
  const auto complete = serialize(source);
  const std::vector<char> truncated(complete.begin(), complete.end() - 1);
  require(deserialize(truncated, decoded) != OB_SUCCESS, "truncated envelope is not a legacy tail");
}

class ModelRow
{
public:
  int get_int(const char *name, int64_t &value) const
  {
    int ret = OB_SUCCESS;
    if (std::strcmp(name, "model_id") == 0) {
      value = 3010;
    } else if (std::strcmp(name, "schema_version") == 0) {
      value = 42;
    } else if (std::strcmp(name, "is_deleted") == 0) {
      value = 0;
    } else if (std::strcmp(name, "type") == 0) {
      value = type_;
    } else if (std::strcmp(name, "input_modalities") == 0) {
      ret = modality_error_;
      if (ret == OB_SUCCESS) {
        value = mask_;
      }
    } else {
      ret = OB_ERR_COLUMN_NOT_FOUND;
    }
    return ret;
  }

  int get_varchar(const char *name, ObString &value) const
  {
    const bool known = std::strcmp(name, "name") == 0 || std::strcmp(name, "model_name") == 0;
    if (known) {
      value = ObString::make_string("stored-model");
    }
    return known ? OB_SUCCESS : OB_ERR_COLUMN_NOT_FOUND;
  }

  int64_t type_ = EndpointType::COMPLETION;
  int64_t mask_ = AiInputModality::ALL;
  int modality_error_ = OB_SUCCESS;
};

void stored_mask_retrieval()
{
  ModelRow row;
  ObAiModelSchema schema;
  bool deleted = true;
  require_equal(ObSchemaRetrieveUtils::fill_ai_model_schema(row, schema, deleted),
                OB_SUCCESS, "retrieve stored multimodal mask");
  require(!deleted && schema.is_valid(), "retrieved live schema is valid");
  require_equal(schema.get_input_modalities(), AiInputModality::ALL, "stored mask is preserved");

  row.modality_error_ = OB_ERR_COLUMN_NOT_FOUND;
  require_equal(ObSchemaRetrieveUtils::fill_ai_model_schema(row, schema, deleted),
                OB_SUCCESS, "retrieve absent legacy column");
  require_equal(schema.get_input_modalities(), AiInputModality::TEXT, "only absent legacy column defaults to text");
  for (const int error : {OB_ERR_NULL_VALUE, OB_INVALID_DATA, OB_ERR_UNEXPECTED}) {
    row.modality_error_ = error;
    require_equal(ObSchemaRetrieveUtils::fill_ai_model_schema(row, schema, deleted),
                  error, "present unreadable column preserves failure");
  }
  row.modality_error_ = OB_SUCCESS;
  for (const int64_t mask : {0, 2, 4, 6, 8, 9, -1}) {
    row.mask_ = mask;
    require_equal(ObSchemaRetrieveUtils::fill_ai_model_schema(row, schema, deleted),
                  OB_ERR_UNEXPECTED, "malformed stored mask is not defaulted");
  }
  row.mask_ = AiInputModality::ALL;
  row.type_ = EndpointType::DENSE_EMBEDDING;
  require_equal(ObSchemaRetrieveUtils::fill_ai_model_schema(row, schema, deleted),
                OB_ERR_UNEXPECTED, "stored embedding media capability is corruption");
}

void check_column(const ObColumnSchemaV2 *column, uint64_t expected_id, bool nullable)
{
  require(column != nullptr, "input_modalities exists");
  require_equal(column->get_column_id(), expected_id, "stable appended column ID");
  require_equal(column->get_data_type(), ObIntType, "integer capability column");
  require(column->is_nullable() == nullable, "capability follows the table's nullability policy");
  require(column->get_orig_default_value().is_int(), "original default is an integer");
  require(column->get_cur_default_value().is_int(), "current default is an integer");
  require_equal(column->get_orig_default_value().get_int(), AiInputModality::TEXT, "original text-only default");
  require_equal(column->get_cur_default_value().get_int(), AiInputModality::TEXT, "current text-only default");
}

const schema_create_func MODEL_TABLE_CREATORS[] = {
  ObInnerTableSchema::all_ai_model_schema,
  ObInnerTableSchema::all_ai_model_history_schema,
};

void persistent_schema_defaults()
{
  for (int64_t i = 0; i < ARRAYSIZEOF(MODEL_TABLE_CREATORS); ++i) {
    ObTableSchema table;
    require_equal(MODEL_TABLE_CREATORS[i](table), OB_SUCCESS, "construct persistent model schema");
    const uint64_t expected_id = OB_APP_MIN_COLUMN_ID + (i == 0 ? 6 : 8);
    check_column(table.get_column_schema("input_modalities"), expected_id, i == 1);
    require_equal(table.get_max_used_column_id(), expected_id, "new capability is the final column");
    require_equal(table.get_column_schema("model_name")->get_column_id(), expected_id - 1,
                  "existing provider-name column retains its ID");
  }
}

void upgrade_arguments()
{
  for (const auto creator : MODEL_TABLE_CREATORS) {
    ObTableSchema table;
    require_equal(creator(table), OB_SUCCESS, "construct persistent model schema");
    const uint64_t column_id = table.get_column_schema("input_modalities")->get_column_id();
    const bool nullable = table.get_table_id() == OB_ALL_AI_MODEL_HISTORY_TID;
    oceanbase::obcall::ObAlterTableArg no_op;
    bool needed = true;
    require_equal(ObAiModelSqlService::prepare_input_modalities_upgrade(table, no_op, needed),
                  OB_SUCCESS, "check up-to-date table");
    require(!needed, "up-to-date table needs no DDL");
    require_equal(no_op.alter_table_schema_.get_column_count(), 0, "no-op does not fabricate DDL");

    require_equal(table.delete_column(ObString::make_string("input_modalities")),
                  OB_SUCCESS, "construct legacy schema");
    table.set_max_used_column_id(column_id - 1);
    oceanbase::obcall::ObAlterTableArg upgrade;
    require_equal(ObAiModelSqlService::prepare_input_modalities_upgrade(table, upgrade, needed),
                  OB_SUCCESS, "prepare legacy schema upgrade");
    require(needed && upgrade.is_valid(), "legacy upgrade is explicit and valid");
    require(upgrade.is_inner_ && upgrade.skip_sys_table_check_ && upgrade.is_alter_columns_
                && !upgrade.is_alter_options_,
            "upgrade uses internal ADD_COLUMN DDL without changing the global system-table gate");
    require_equal(upgrade.table_id_, table.get_table_id(), "upgrade target ID");
    require_equal(upgrade.alter_table_schema_.get_column_count(), 1, "upgrade adds exactly one column");
    const auto *column = *upgrade.alter_table_schema_.column_begin();
    check_column(column, column_id, nullable);
    const auto *alter_column = static_cast<const AlterColumnSchema *>(column);
    require_equal(alter_column->alter_type_, OB_DDL_ADD_COLUMN, "upgrade operation");
    require(alter_column->is_set_default_ && alter_column->is_set_nullable_, "DDL specifies default and nullability");
    require_equal(table.add_column(*column), OB_SUCCESS, "apply upgraded column schema");
    table.set_max_used_column_id(column_id);
    oceanbase::obcall::ObAlterTableArg restarted;
    require_equal(ObAiModelSqlService::prepare_input_modalities_upgrade(table, restarted, needed),
                  OB_SUCCESS, "check upgraded schema after restart");
    require(!needed, "upgrade is idempotent");

    table.get_column_schema("input_modalities")->set_nullable(!nullable);
    require_equal(ObAiModelSqlService::prepare_input_modalities_upgrade(table, restarted, needed),
                  OB_ERR_UNEXPECTED, "do not silently accept corrupt persisted column declaration");
  }
}

class RecordingSQLClient final : public ObISQLClient
{
public:
  int escape(const char *, const int64_t, char *, const int64_t, int64_t &) override
  {
    return OB_NOT_SUPPORTED;
  }
  int read(ReadResult &, const char *, const int32_t) override { return OB_NOT_SUPPORTED; }
  sqlclient::ObISQLConnection *get_connection() override { return nullptr; }
  int write(const char *sql, const int32_t, int64_t &affected_rows) override
  {
    writes_.emplace_back(sql);
    affected_rows = 1;
    return OB_SUCCESS;
  }
  std::vector<std::string> writes_;
};

class TestSchemaService final : public ObMultiVersionSchemaService
{
public:
  TestSchemaService() = default;
  ~TestSchemaService() override = default;
};

std::vector<std::string> split_fields(const std::string &text)
{
  std::vector<std::string> fields;
  std::istringstream input(text);
  std::string field;
  while (std::getline(input, field, ',')) {
    const auto first = field.find_first_not_of(" `");
    const auto last = field.find_last_not_of(" `");
    require(first != std::string::npos, "SQL fixture has a nonempty field");
    fields.push_back(field.substr(first, last - first + 1));
  }
  return fields;
}

int64_t stored_mask(const std::string &sql)
{
  const auto columns_begin = sql.find('(');
  const auto columns_end = sql.find(')', columns_begin);
  const auto values_begin = sql.find('(', columns_end);
  const auto values_end = sql.find(')', values_begin);
  require(columns_begin != std::string::npos && columns_end != std::string::npos
          && values_begin != std::string::npos && values_end != std::string::npos,
          "persistence uses a complete explicit column/value insert");
  const auto columns = split_fields(sql.substr(columns_begin + 1, columns_end - columns_begin - 1));
  const auto values = split_fields(sql.substr(values_begin + 1, values_end - values_begin - 1));
  require_equal(columns.size(), values.size(), "persistent insert column/value arity");
  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i] == "input_modalities") {
      return std::stoll(values[i]);
    }
  }
  throw std::runtime_error("persistent insert is missing input_modalities");
}

void persistence_writes()
{
  ObArenaAllocator allocator("ModalityTest");
  ObAiServiceModelInfo model;
  require_equal(parse_model(allocator, model_json("COMPLETION", "[\"text\",\"image\",\"audio\"]"), model),
                OB_SUCCESS, "parse multimodal model");
  ObAiModelSchema schema;
  make_schema(schema, model);
  ObMySQLProxy proxy;
  TestSchemaService schema_service;
  ObSchemaServiceSQLImpl backend(nullptr, proxy, schema_service);
  ObAiModelSqlService service(backend);
  RecordingSQLClient client;
  require_equal(service.create_ai_model(schema, ObString(), client), OB_SUCCESS, "persist model creation");
  require_equal(client.writes_.size(), 3, "creation writes current/history/DDL operation");
  require(client.writes_[0].find(OB_ALL_AI_MODEL_TNAME) != std::string::npos, "current model insert");
  require(client.writes_[1].find(OB_ALL_AI_MODEL_HISTORY_TNAME) != std::string::npos, "history model insert");
  require_equal(stored_mask(client.writes_[0]), AiInputModality::ALL, "current table stores the declared mask");
  require_equal(stored_mask(client.writes_[1]), AiInputModality::ALL, "history table stores the declared mask");
  client.writes_.clear();
  require_equal(service.drop_ai_model(schema, 44, ObString(), client), OB_SUCCESS, "persist model deletion");
  require_equal(client.writes_.size(), 3, "deletion writes current delete/history/DDL operation");
  require_equal(stored_mask(client.writes_[1]), AiInputModality::ALL, "deletion history preserves capability");
}

void media_system_variable_defaults()
{
  ObSysVariableSchema variables;
  variables.set_schema_version(42);
  const ObSysVarClassType ids[] = {
      SYS_VAR_AI_MEDIA_ROOT, SYS_VAR_AI_MEDIA_MAX_BYTES, SYS_VAR_AI_MEDIA_PARALLEL};
  const char *values[] = {"", "2097152", "4"};
  for (int64_t index = 0; index < 3; ++index) {
    const ObSysVarSchema *variable = nullptr;
    require_equal(variables.get_sysvar_schema(ids[index], variable),
                  OB_ERR_SYS_VARIABLE_UNKNOWN, "unpersisted media variable is absent");
    require_equal(variables.add_default_system_variable(ids[index]),
                  OB_SUCCESS, "load an explicitly selected media default");
    require_equal(variables.get_sysvar_schema(ids[index], variable),
                  OB_SUCCESS, "loaded media default is present");
    require(variable != nullptr && variable->get_value() == ObString::make_string(values[index]),
            "media default matches the generated definition");
    require_equal(variable->get_schema_version(), 42, "media default preserves schema version");
  }
  require_equal(variables.get_real_sysvar_count(), 3, "only selected variables are added");
  require_equal(variables.add_default_system_variable(SYS_VAR_AI_MEDIA_PARALLEL),
                OB_ERR_UNEXPECTED, "default loading cannot overwrite an existing policy");
  require_equal(variables.get_real_sysvar_count(), 3, "duplicate defaults preserve existing schema");
  const ObSysVarSchema *parallel = nullptr;
  require_equal(variables.get_sysvar_schema(SYS_VAR_AI_MEDIA_PARALLEL, parallel),
                OB_SUCCESS, "existing parallel policy is still present");
  require(parallel != nullptr && parallel->get_value() == ObString::make_string("4"),
          "duplicate defaults do not replace an existing value");
  ObSysVariableSchema all_variables;
  require_equal(all_variables.load_default_system_variable(), OB_SUCCESS,
                "complete default loading preserves existing bootstrap behavior");
  require_equal(all_variables.get_real_sysvar_count(), all_variables.get_sysvar_count(),
                "complete defaults include every generated variable");
  require_equal(all_variables.get_name_case_mode(), OB_ORIGIN_AND_INSENSITIVE,
                "bootstrap keeps its existing lower_case_table_names default");
  ObSysVariableSchema invalid;
  require(invalid.add_default_system_variable(SYS_VAR_INVALID) != OB_SUCCESS,
          "invalid variable ID is an explicit error");
  require_equal(invalid.get_real_sysvar_count(), 0, "invalid default does not change schema");
}

const struct {
  const char *name_;
  void (*run_)();
} TESTS[] = {
  {"defaults_and_reset", defaults_and_reset},
  {"valid_declarations", valid_declarations},
  {"invalid_declarations", invalid_declarations},
  {"media_type_restrictions", media_type_restrictions},
  {"assignment_and_manager_copy", assignment_and_manager_copy},
  {"invalid_mask_validation", invalid_mask_validation},
  {"info_serialization", info_serialization},
  {"schema_serialization", schema_serialization},
  {"stored_mask_retrieval", stored_mask_retrieval},
  {"persistent_schema_defaults", persistent_schema_defaults},
  {"upgrade_arguments", upgrade_arguments},
  {"persistence_writes", persistence_writes},
  {"media_system_variable_defaults", media_system_variable_defaults},
};
}

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_file_name("model_modalities.log", true);
  ObLogger::get_logger().set_log_level("WARN");
  ObLogger::get_logger().set_enable_async_log(false);
  try {
    int64_t repeats = 1;
    std::string selected;
    for (int i = 1; i < argc; i += 2) {
      require(i + 1 < argc, "native test options require a value");
      const std::string option(argv[i]);
      if (option == "--repeat") {
        char *end = nullptr;
        repeats = std::strtoll(argv[i + 1], &end, 10);
        require(end != argv[i + 1] && *end == '\0' && repeats >= 1 && repeats <= 100,
                "--repeat must be in [1, 100]");
      } else if (option == "--case") {
        selected = argv[i + 1];
      } else {
        require(false, "usage: ai_model_modality_tests [--repeat COUNT] [--case NAME]");
      }
    }
    int64_t completed = 0;
    for (int64_t repeat = 0; repeat < repeats; ++repeat) {
      for (const auto &test : TESTS) {
        if (selected.empty() || selected == test.name_) {
          test.run_();
          std::cout << "PASS " << test.name_ << " repeat=" << repeat + 1 << std::endl;
          ++completed;
        }
      }
    }
    require(completed > 0, "unknown native model modality test case: " + selected);
    std::cout << "AI model modality native tests passed: " << completed << " cases" << std::endl;
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "FAIL AI model modality native tests: " << error.what() << std::endl;
    return 1;
  }
}
