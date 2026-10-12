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

#ifndef OCEANBASE_SQL_ENGINE_SEMANTIC_AGGREGATE_OP_H_
#define OCEANBASE_SQL_ENGINE_SEMANTIC_AGGREGATE_OP_H_

#include "sql/engine/aggregate/ob_groupby_op.h"
#include "sql/engine/basic/ob_semantic_runtime.h"

namespace oceanbase
{
namespace sql
{
class SemanticAggregateSpec : public ObGroupBySpec
{
  OB_UNIS_VERSION_V(1);
public:
  SemanticAggregateSpec(common::ObIAllocator &allocator, ObPhyOperatorType type)
      : ObGroupBySpec(allocator, type), group_exprs_(allocator),
        cmp_funcs_(allocator), ordinary_aggr_infos_(allocator),
        semantic_aggr_infos_(allocator), query_task_count_(1) {}
  ExprFixedArray group_exprs_;
  ObCmpFuncs cmp_funcs_;
  AggrInfoFixedArray ordinary_aggr_infos_;
  AggrInfoFixedArray semantic_aggr_infos_;
  int64_t query_task_count_;
};

class SemanticAggregateOp : public ObOperator
{
public:
  SemanticAggregateOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input);
  int inner_open() override;
  int inner_get_next_row() override;
  int inner_get_next_batch(int64_t max_row_cnt) override;
  int rescan() override;
  int inner_rescan() override;
  int inner_close() override;
  void destroy() override;
  bool supports_semantic_suspend() const override { return true; }
private:
  struct State;
  static bool can_resume(const void *operation);
  static int poll_ready_tasks(void *operation, bool &progress);
  int wait_for_work(bool &suspended);
  const SemanticAggregateSpec &aggregate_spec() const
  { return static_cast<const SemanticAggregateSpec &>(spec_); }
  State *state_;
  std::shared_ptr<SemanticExecutionState> execution_;
  bool registered_;
  DISALLOW_COPY_AND_ASSIGN(SemanticAggregateOp);
};
}
}

#endif
