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

#include "ob_temp_table_transformation_op.h"
#include "sql/engine/basic/ob_temp_table_insert_op.h"
#include "sql/engine/basic/ob_temp_table_access_op.h"
#include "sql/engine/basic/ob_semantic_op.h"
#include <chrono>
#include <thread>
#include "share/rc/ob_server_runtime.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql
{
DEF_TO_STRING(ObTempTableTransformationOpSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("op_spec");
  J_COLON();
  pos += ObOpSpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER_INHERIT(ObTempTableTransformationOpSpec, ObOpSpec,
                            branch_async_, semantic_producers_, branch_dependencies_);

namespace
{
bool branch_semantic_type(ObPhyOperatorType type)
{
  return type == PHY_SEMANTIC_MAP || type == PHY_SEMANTIC_FILTER ||
      type == PHY_SEMANTIC_RANK;
}

int inspect_branch(ObOpSpec &node, const ObIArray<uint64_t> &tables,
                   uint64_t &dependencies, bool &semantic)
{
  int ret = OB_SUCCESS;
  semantic = branch_semantic_type(node.type_);
  if (!node.is_vectorized() || node.type_ == PHY_TEMP_TABLE_TRANSFORMATION ||
      node.type_ == PHY_SEMANTIC_JOIN || node.type_ == PHY_SEMANTIC_AGGREGATE ||
      node.type_ == PHY_AI_FUNC) {
    ret = OB_NOT_SUPPORTED;
  } else if (node.type_ == PHY_TEMP_TABLE_ACCESS) {
    const auto &access = static_cast<const ObTempTableAccessOpSpec &>(node);
    int64_t index = 0;
    while (index < tables.count() && tables.at(index) != access.temp_table_id_) {
      ++index;
    }
    if (access.is_distributed_ || index == tables.count()) {
      ret = OB_NOT_SUPPORTED;
    } else {
      dependencies |= uint64_t(1) << index;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < node.get_child_cnt(); ++i) {
    bool child_semantic = false;
    if (OB_ISNULL(node.get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(SMART_CALL(inspect_branch(
        *node.get_child(i), tables, dependencies, child_semantic)))) {
    } else {
      semantic |= child_semantic;
    }
  }
  if (OB_SUCC(ret) && semantic && !branch_semantic_type(node.type_) &&
      node.type_ != PHY_TEMP_TABLE_INSERT && node.type_ != PHY_SUBPLAN_SCAN) {
    ret = OB_NOT_SUPPORTED;
  }
  if (OB_FAIL(ret)) {
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
        "semantic branch producers require vectorized MAP/FILTER/RANK pipelines over local scans; joins, sets and aggregation above a pending semantic call are not supported");
    LOG_WARN("unsupported semantic CTE producer", K(ret), "operator_type", node.type_);
  }
  return ret;
}

int configure_transformation(ObTempTableTransformationOpSpec &spec)
{
  int ret = OB_SUCCESS;
  const int64_t count = spec.get_child_cnt() - 1;
  ObSEArray<uint64_t, 8> tables;
  if (!spec.is_vectorized() || count < 1 || count > 63) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(spec.branch_dependencies_.init(count))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObOpSpec *child = spec.get_child(i);
    if (nullptr == child || child->type_ != PHY_TEMP_TABLE_INSERT ||
        static_cast<const ObTempTableInsertOpSpec *>(child)->is_distributed_) {
      ret = OB_NOT_SUPPORTED;
    } else if (OB_FAIL(tables.push_back(
        static_cast<const ObTempTableInsertOpSpec *>(child)->temp_table_id_))) {
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
      if (tables.at(j) == tables.at(i)) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    uint64_t dependencies = 0;
    bool semantic = false;
    if (OB_FAIL(inspect_branch(*spec.get_child(i), tables, dependencies, semantic))) {
    } else if (OB_FAIL(spec.branch_dependencies_.push_back(dependencies))) {
    } else if (semantic) {
      spec.semantic_producers_ |= uint64_t(1) << i;
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t dependencies = 0;
    bool semantic = false;
    if (OB_FAIL(inspect_branch(
        *spec.get_child(count), tables, dependencies, semantic))) {
    } else if (semantic) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
          "semantic calls must be inside materialized producers when branch scheduling is enabled");
    }
  }
  uint64_t done = 0;
  const uint64_t all = count > 0 && count <= 63 ? (uint64_t(1) << count) - 1 : 0;
  for (int64_t pass = 0; OB_SUCC(ret) && pass < count && done != all; ++pass) {
    const uint64_t before = done;
    for (int64_t i = 0; i < count; ++i) {
      if ((spec.branch_dependencies_.at(i) & ~done) == 0) {
        done |= uint64_t(1) << i;
      }
    }
    if (done == before) {
      ret = OB_NOT_SUPPORTED;
    }
  }
  if (OB_SUCC(ret)) {
    spec.branch_async_ = true;
  } else {
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
        "semantic branch scheduling requires 1 to 63 local nonrecursive materialized CTE producers with an acyclic dependency graph");
    LOG_WARN("invalid semantic CTE dependency graph", K(ret), K(count), K(tables));
  }
  return ret;
}

int collect_branch_plan(ObOpSpec &node, ObIArray<SemanticSpec *> &stages,
                        int64_t &transformations, bool &other_semantic,
                        bool enabled, bool in_producer = false)
{
  int ret = OB_SUCCESS;
  if (branch_semantic_type(node.type_)) {
    if (enabled && !in_producer) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "semantic branch scheduling requires materialized semantic producers");
    } else {
      ret = stages.push_back(static_cast<SemanticSpec *>(&node));
    }
  } else if (node.type_ == PHY_SEMANTIC_JOIN || node.type_ == PHY_SEMANTIC_AGGREGATE) {
    other_semantic = true;
    if (enabled) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "semantic branch scheduling does not support semantic JOIN or AGG");
    }
  } else if (node.type_ == PHY_TEMP_TABLE_TRANSFORMATION) {
    ++transformations;
    if (enabled) {
      ret = configure_transformation(static_cast<ObTempTableTransformationOpSpec &>(node));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < node.get_child_cnt(); ++i) {
    if (OB_ISNULL(node.get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = SMART_CALL(collect_branch_plan(
          *node.get_child(i), stages, transformations, other_semantic, enabled,
          in_producer || (node.type_ == PHY_TEMP_TABLE_TRANSFORMATION &&
                          i < node.get_child_cnt() - 1)));
    }
  }
  return ret;
}
}

int configure_semantic_branch_plan(ObOpSpec &root, bool enabled)
{
  int ret = OB_SUCCESS;
  int64_t transformations = 0;
  bool other_semantic = false;
  ObSEArray<SemanticSpec *, 8> stages;
  if (OB_FAIL(collect_branch_plan(root, stages, transformations, other_semantic, enabled))) {
  } else if (enabled && (transformations != 1 || stages.empty())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
        "semantic branch scheduling requires one materialization graph with native semantic producers");
  } else if (transformations == 1 && !other_semantic) {
    int64_t calls = 0;
    for (SemanticSpec *stage : stages) {
      calls += stage->semantic_exprs_.count();
    }
    for (SemanticSpec *stage : stages) {
      stage->query_task_count_ = calls;
    }
  }
  return ret;
}

int ObTempTableTransformationOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else { /*do nothing.*/ }
  return ret;
}

int ObTempTableTransformationOp::inner_open()
{
  int ret = OB_SUCCESS;
  const auto &spec = static_cast<const ObTempTableTransformationOpSpec &>(spec_);
  if (spec.branch_async_ && (!is_vectorized() || nullptr != ctx_.get_sqc_handler() ||
      spec.branch_dependencies_.count() != get_child_cnt() - 1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "semantic branch scheduling requires a serial local materialization graph");
  }
  return ret;
}

int ObTempTableTransformationOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  ObExecContext &ctx = get_exec_ctx();
  if (init_temp_table_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_child_cnt() - 1; ++i) {
      int64_t temp_table_count = ctx.get_temp_table_ctx().count();
      if (OB_ISNULL(children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child op is null");
      } else if (OB_FALSE_IT(ret = children_[i]->get_next_row())) {
      } else if (ret != OB_ITER_END) {
        LOG_WARN("failed to get next row.", K(ret));
      } else {
        ret = OB_SUCCESS;
        while(OB_SUCC(ret) && ctx.get_temp_table_ctx().count() <= temp_table_count) {
          if (OB_FAIL(check_status())) {
          } else {
            ob_usleep(1000);
          }
        }
      }
    }
    init_temp_table_ = false;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(children_[get_child_cnt() - 1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null");
  } else if (OB_FAIL(children_[get_child_cnt() - 1]->get_next_row())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row.", K(ret));
    } else { /*do nothing.*/ }
  } else { /*do nothing.*/ }
  return ret;
}

int ObTempTableTransformationOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const ObBatchRows *child_brs = nullptr;
  clear_evaluated_flag();
  clear_datum_eval_flag();
  if (init_temp_table_ &&
      static_cast<const ObTempTableTransformationOpSpec &>(spec_).branch_async_) {
    ret = materialize_semantic_branches(max_row_cnt);
  } else if (init_temp_table_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_child_cnt() - 1; ++i) {
      if (OB_ISNULL(children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child op is null");
      } else if (OB_FAIL(children_[i]->get_next_batch(max_row_cnt, child_brs))) {
      }
    }
    init_temp_table_ = false;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(children_[get_child_cnt() - 1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null");
  } else if (OB_FAIL(children_[get_child_cnt() - 1]->get_next_batch(
                 max_row_cnt, child_brs))) {
  } else { /*do nothing.*/
  }
  if (OB_SUCC(ret) && nullptr != child_brs) {
    (void)brs_.copy(child_brs);
  }
  return ret;
}

int ObTempTableTransformationOp::materialize_semantic_branches(int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const auto &spec = static_cast<const ObTempTableTransformationOpSpec &>(spec_);
  const int64_t count = spec.branch_dependencies_.count();
  const uint64_t all = (uint64_t(1) << count) - 1;
  while (OB_SUCC(ret) && completed_branches_ != all) {
    bool progress = false;
    if (OB_FAIL(ctx_.check_status())) {
      break;
    }
    for (int64_t offset = 0; OB_SUCC(ret) && offset < count; ++offset) {
      const int64_t index = (next_branch_ + offset) % count;
      const uint64_t bit = uint64_t(1) << index;
      if ((completed_branches_ & bit) != 0 ||
          (spec.branch_dependencies_.at(index) & ~completed_branches_) != 0) {
        continue;
      }
      bool done = false;
      if ((spec.semantic_producers_ & bit) != 0) {
        ret = static_cast<ObTempTableInsertOp *>(children_[index])->advance_semantic_branch(
            max_row_cnt, done, progress);
      } else {
        const ObBatchRows *rows = nullptr;
        if (OB_FAIL(children_[index]->get_next_batch(max_row_cnt, rows))) {
        } else if (OB_ISNULL(rows) || !rows->end_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("relational CTE producer did not materialize completely", K(ret), K(index));
        } else {
          done = true;
          progress = true;
        }
      }
      if (OB_SUCC(ret) && done) {
        completed_branches_ |= bit;
      }
    }
    next_branch_ = (next_branch_ + 1) % count;
    if (OB_SUCC(ret) && !progress) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
  if (OB_SUCC(ret)) {
    init_temp_table_ = false;
  }
  return ret;
}

int ObTempTableTransformationOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(destory_interm_results())) {
  }
  return ret;
}

int ObTempTableTransformationOp::destory_interm_results()
{
  int ret = OB_SUCCESS;
  ObExecContext &ctx = get_exec_ctx();
  const int64_t temp_table_count = ctx.get_temp_table_ctx().count();
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_count; ++i) {
    ObSqlTempTableCtx &temp_table_ctx = ctx.get_temp_table_ctx().at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < temp_table_ctx.interm_result_infos_.count(); ++j) {
      ObTempTableResultInfo &result_info = temp_table_ctx.interm_result_infos_.at(j);
      if (OB_FAIL(destory_local_interm_results(result_info.interm_result_ids_))) {
      }
    }
  }
  return ret;
}


int ObTempTableTransformationOp::destory_local_interm_results(ObIArray<uint64_t> &result_ids)
{
  int ret = OB_SUCCESS;
  dtl::ObDTLIntermResultKey dtl_int_key;
  LOG_TRACE("destory interm results", K(get_exec_ctx().get_addr()), K(result_ids));
  for (int64_t i = 0; OB_SUCC(ret) && i < result_ids.count(); ++i) {
    dtl_int_key.channel_id_ = result_ids.at(i);
    if (OB_FAIL(::oceanbase::share::server_service<::oceanbase::sql::dtl::ObDTLIntermResultManager>()->erase_interm_result_info(
                                                                            dtl_int_key))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_WARN("interm result may erased by DM", K(ret));
      } else {
        LOG_WARN("failed to erase interm result info in manager.", K(ret));
      }
    }
  }
  return ret;
}

void ObTempTableTransformationOp::destroy()
{
  ObOperator::destroy();
}

} // end namespace sql
} // end namespace oceanbase
