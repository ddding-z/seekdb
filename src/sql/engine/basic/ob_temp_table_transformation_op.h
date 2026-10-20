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

#ifndef OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_TRANSFORMATION_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_TRANSFORMATION_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/ob_sql_memory_manager.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/engine/ob_physical_plan_ctx.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObTempTableTransformationOpSpec : public ObOpSpec
{
public:
  OB_UNIS_VERSION_V(1);
public:
  ObTempTableTransformationOpSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), branch_async_(false), semantic_producers_(0),
      branch_dependencies_(alloc) {}
  virtual ~ObTempTableTransformationOpSpec() {}
  DECLARE_VIRTUAL_TO_STRING;
  bool branch_async_;
  uint64_t semantic_producers_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> branch_dependencies_;
};

int configure_semantic_branch_plan(ObOpSpec &root, bool enabled);

class ObTempTableTransformationOp : public ObOperator
{
public:
  ObTempTableTransformationOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input), init_temp_table_(true),
      completed_branches_(0), next_branch_(0) {}
  ~ObTempTableTransformationOp() {}
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual void destroy() override;
  int destory_interm_results();
  int destory_local_interm_results(ObIArray<uint64_t> &result_ids);

  bool init_temp_table_;
private:
  int materialize_semantic_branches(int64_t max_row_cnt);
  uint64_t completed_branches_;
  int64_t next_branch_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_TRANSFORMATION_OP_H_ */
