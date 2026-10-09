#ifndef OCEANBASE_SQL_ENGINE_SEMANTIC_OP_H_
#define OCEANBASE_SQL_ENGINE_SEMANTIC_OP_H_

#include "query/engine/ob_operator.h"
#include "query/engine/basic/ob_batch_result_holder.h"
#include "sql/engine/basic/ob_semantic_runtime.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_semantic.h"

namespace oceanbase
{
namespace sql
{
class SemanticSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  SemanticSpec(common::ObIAllocator &allocator, ObPhyOperatorType type)
      : ObOpSpec(allocator, type), expressions_(allocator), semantic_exprs_(allocator),
        owned_exprs_(allocator), local_exprs_(allocator),
        shared_exprs_(allocator), carried_exprs_(allocator),
        query_task_count_(1), filter_expr_count_(0) {}
  common::ObFixedArray<ObExpr *, common::ObIAllocator> expressions_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> semantic_exprs_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> owned_exprs_;
  // Deferred CPU recipe branches, excluding completed child inputs.
  common::ObFixedArray<ObExpr *, common::ObIAllocator> local_exprs_;
  // Shared inputs carry either a non-NULL semantic result or a private, undemanded NULL tag.
  common::ObFixedArray<ObExpr *, common::ObIAllocator> shared_exprs_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> carried_exprs_;
  int64_t query_task_count_;
  int64_t filter_expr_count_;
};

class SemanticMapSpec : public SemanticSpec
{
  OB_UNIS_VERSION_V(1);
public:
  SemanticMapSpec(common::ObIAllocator &allocator, ObPhyOperatorType type)
      : SemanticSpec(allocator, type) {}
};

class SemanticFilterSpec : public SemanticSpec
{
  OB_UNIS_VERSION_V(1);
public:
  SemanticFilterSpec(common::ObIAllocator &allocator, ObPhyOperatorType type)
      : SemanticSpec(allocator, type) {}
};

class SemanticOp : public ObOperator, public ISemanticExprRuntime
{
public:
  SemanticOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input);
  int inner_open() override;
  int inner_get_next_row() override;
  int inner_get_next_batch(int64_t max_row_cnt) override;
  int inner_rescan() override;
  int inner_close() override;
  void destroy() override;
  int evaluate(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result) override;
  bool supports_semantic_suspend() const override { return true; }
private:
  struct Task;
  struct Slot;
  const SemanticSpec &semantic_spec() const { return static_cast<const SemanticSpec &>(spec_); }
  bool is_filter() const { return spec_.type_ == PHY_SEMANTIC_FILTER; }
  int validate_shared_expressions() const;
  int reserve(Slot &slot, int64_t bytes);
  int capture(Slot &slot);
  int prepare(Slot &slot, bool &progress);
  int submit_tasks(Slot &slot, bool &progress);
  int poll_tasks(Slot &slot, bool &progress);
  int restore_input(Slot &slot);
  void clear_owned_flags(int64_t size);
  int output(Slot &slot, int64_t max_row_cnt);
  void reset_task(Slot &slot, Task &task);
  void reset_slot(Slot &slot);
  void reset_pipeline();
  static bool can_resume(const void *operation);
  static int poll_ready_tasks(void *operation, bool &progress);
  int wait_for_work(bool &suspended);
  ObBatchResultHolder child_frame_;
  std::shared_ptr<SemanticExecutionState> execution_;
  Slot **slots_;
  int64_t slot_count_;
  int64_t head_;
  int64_t count_;
  bool input_end_;
  const ObBatchRows *pending_input_;
  int64_t waiting_memory_;
  Slot *evaluating_slot_;
  bool registered_;
  bool first_prepare_logged_;
  DISALLOW_COPY_AND_ASSIGN(SemanticOp);
};

class SemanticMapOp : public SemanticOp
{
public:
  SemanticMapOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
      : SemanticOp(ctx, spec, input) {}
};

class SemanticFilterOp : public SemanticOp
{
public:
  SemanticFilterOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
      : SemanticOp(ctx, spec, input) {}
};
}
}
#endif
