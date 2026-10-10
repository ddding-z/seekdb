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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_ai_model_sql_service.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_ai_model_mgr.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
int ObAiModelSqlService::prepare_input_modalities_upgrade(
    const ObTableSchema &table_schema,
    obcall::ObAlterTableArg &arg,
    bool &needs_upgrade)
{
  int ret = OB_SUCCESS;
  needs_upgrade = false;
  ObTableSchema expected_schema;
  const uint64_t table_id = table_schema.get_table_id();
  const ObColumnSchemaV2 *expected_column = nullptr;
  if (table_id != OB_ALL_AI_MODEL_TID && table_id != OB_ALL_AI_MODEL_HISTORY_TID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not an AI model system table", K(ret), K(table_id));
  } else if (arg.alter_table_schema_.get_column_count() != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("AI model upgrade requires an empty alter argument", K(ret), K(arg));
  } else if (OB_FAIL(table_id == OB_ALL_AI_MODEL_TID
      ? ObInnerTableSchema::all_ai_model_schema(expected_schema)
      : ObInnerTableSchema::all_ai_model_history_schema(expected_schema))) {
    LOG_WARN("failed to construct AI model system table schema", K(ret), K(table_id));
  } else if (OB_ISNULL(expected_column = expected_schema.get_column_schema("input_modalities"))
      || table_schema.get_table_name_str() != expected_schema.get_table_name_str()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid AI model system table definition", K(ret), K(table_id), K(table_schema));
  } else {
    const ObColumnSchemaV2 *column = table_schema.get_column_schema("input_modalities");
    if (OB_NOT_NULL(column)) {
      const ObObj &orig_default = column->get_orig_default_value();
      const ObObj &cur_default = column->get_cur_default_value();
      if (column->get_column_id() != expected_column->get_column_id()
          || column->get_data_type() != ObIntType
          || column->is_nullable() != expected_column->is_nullable()
          || !orig_default.is_int() || orig_default.get_int() != AiInputModality::TEXT
          || !cur_default.is_int() || cur_default.get_int() != AiInputModality::TEXT) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid persisted input_modalities column", K(ret), K(table_id), KPC(column));
      }
    } else if (expected_column->get_column_id() != table_schema.get_max_used_column_id() + 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("AI model upgrade must preserve existing column identifiers",
               K(ret), K(table_id), KPC(expected_column), K(table_schema.get_max_used_column_id()));
    } else {
      AlterColumnSchema alter_column;
      if (OB_FAIL(alter_column.assign(*expected_column))) {
        LOG_WARN("failed to copy input_modalities column", K(ret), K(table_id));
      } else if (OB_FAIL(arg.alter_table_schema_.set_origin_database_name(OB_SYS_DATABASE_NAME))) {
        LOG_WARN("failed to set AI model upgrade database", K(ret));
      } else if (OB_FAIL(arg.alter_table_schema_.set_origin_table_name(table_schema.get_table_name()))) {
        LOG_WARN("failed to set AI model upgrade table", K(ret), K(table_id));
      } else {
        alter_column.alter_type_ = OB_DDL_ADD_COLUMN;
        alter_column.is_set_default_ = true;
        alter_column.is_set_nullable_ = true;
        if (OB_FAIL(arg.alter_table_schema_.add_alter_column(alter_column, true /* need allocate */))) {
          LOG_WARN("failed to add input_modalities upgrade column", K(ret), K(table_id));
        } else {
          arg.alter_table_schema_.set_table_id(table_id);
          arg.alter_table_schema_.set_database_id(table_schema.get_database_id());
          arg.table_id_ = table_id;
          arg.is_alter_columns_ = true;
          arg.is_alter_options_ = false;
          arg.is_inner_ = true;
          arg.skip_sys_table_check_ = true;
          arg.ddl_stmt_str_ = ObString::make_string(table_id == OB_ALL_AI_MODEL_TID
              ? "ALTER TABLE oceanbase.__all_ai_model ADD COLUMN input_modalities BIGINT NOT NULL DEFAULT 1"
              : "ALTER TABLE oceanbase.__all_ai_model_history ADD COLUMN input_modalities BIGINT DEFAULT 1");
          needs_upgrade = true;
        }
      }
    }
  }
  return ret;
}

int ObAiModelSqlService::create_ai_model(const ObAiModelSchema &new_schema,
                                         const ObString &ddl_stmt,
                                         common::ObISQLClient &sql_client)
{
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  int ret = OB_SUCCESS;
  
  
  int64_t affected_rows = 0;

  if (!new_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_schema));
  } else if (OB_FAIL(sql.add_pk_column("model_id", new_schema.get_ai_model_id()))) {
  } else if (OB_FAIL(sql.add_column("name", ObHexEscapeSqlStr(new_schema.get_name())))) {
  } else if (OB_FAIL(sql.add_column("type", static_cast<int64_t>(new_schema.get_type())))) {
  } else if (OB_FAIL(sql.add_column("model_name", ObHexEscapeSqlStr(new_schema.get_model_name())))) {
  } else if (OB_FAIL(sql.add_column("input_modalities", new_schema.get_input_modalities()))) {
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_AI_MODEL_TNAME, buffer))) {
  } else if (OB_FAIL(sql_client.write(buffer.ptr(), affected_rows))) {
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  } else if (OB_FAIL(sql.add_pk_column("schema_version", new_schema.get_schema_version()))) {
  } else if (OB_FAIL(sql.add_column("is_deleted", 0))) {
  } else if (FALSE_IT(buffer.reuse())) {
  } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_AI_MODEL_HISTORY_TNAME, buffer))) {
  } else if (OB_FAIL(sql_client.write(buffer.ptr(), affected_rows))) {
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  } else {
    ObSchemaOperation opt;
    
    opt.ai_model_id_ = new_schema.get_ai_model_id();
    opt.op_type_ = OB_DDL_CREATE_AI_MODEL;
    opt.schema_version_ = new_schema.get_schema_version();
    opt.ai_model_name_ = new_schema.get_name();
    opt.ddl_stmt_str_ = ddl_stmt;

    if (OB_FAIL(log_operation(opt, sql_client))) {
    }
  }
  return ret;
}

int ObAiModelSqlService::drop_ai_model(const ObAiModelSchema &schema,
                                       const int64_t new_schema_version,
                                       const ObString &ddl_stmt,
                                       common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer sql;
  ObSqlString buffer;
  int64_t affected_rows = 0;
  
  

  if (!schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema));
  } else if (OB_FAIL(sql.add_pk_column("model_id", schema.get_ai_model_id()))) {
  } else if (OB_FAIL(sql.splice_delete_sql(OB_ALL_AI_MODEL_TNAME, buffer))) {
  } else if (OB_FAIL(sql_client.write(buffer.ptr(), affected_rows))) {
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
  } else {
    buffer.reuse();
    sql.reuse();
    if (OB_FAIL(sql.add_pk_column("model_id", schema.get_ai_model_id()))) {
    } else if (OB_FAIL(sql.add_pk_column("schema_version", new_schema_version))) {
    } else if (OB_FAIL(sql.add_column("is_deleted", 1))) {
    } else if (OB_FAIL(sql.add_column("name", ObHexEscapeSqlStr(schema.get_name())))) {
    } else if (OB_FAIL(sql.add_column("type", static_cast<int64_t>(schema.get_type())))) {
    } else if (OB_FAIL(sql.add_column("model_name", ObHexEscapeSqlStr(schema.get_model_name())))) {
    } else if (OB_FAIL(sql.add_column("input_modalities", schema.get_input_modalities()))) {
    } else if (OB_FAIL(sql.splice_insert_sql(OB_ALL_AI_MODEL_HISTORY_TNAME, buffer))) {
    } else if (OB_FAIL(sql_client.write(buffer.ptr(), affected_rows))) {
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(affected_rows), K(buffer), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaOperation opt;
    
    opt.ai_model_id_ = schema.get_ai_model_id();
    opt.op_type_ = OB_DDL_DROP_AI_MODEL;
    opt.schema_version_ = new_schema_version;
    opt.ai_model_name_ = schema.get_name();
    opt.ddl_stmt_str_ = ddl_stmt;

    if (OB_FAIL(log_operation(opt, sql_client))) {
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
