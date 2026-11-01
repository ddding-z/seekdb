#ifndef OCEANBASE_SQL_ENGINE_SEMANTIC_OP_H_
#define OCEANBASE_SQL_ENGINE_SEMANTIC_OP_H_

#include "query/engine/ob_operator.h"
#include "query/engine/basic/ob_batch_result_holder.h"
#include "sql/engine/basic/ob_semantic_runtime.h"
#include "sql/engine/basic/ob_semantic_proxy.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_semantic.h"
#include <algorithm>

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
        query_task_count_(1), filter_expr_count_(0), row_async_enabled_(false),
        topn_lifted_(false), memoized_exprs_(allocator),
        proxy_configs_(allocator), output_order_(SemanticOutputOrder::INPUT_ORDER),
        execution_scope_(SemanticExecutionScope::SERIAL), dispatch_rows_(0), request_limit_(0) {}
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
  bool row_async_enabled_;
  bool topn_lifted_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> memoized_exprs_;
  common::ObFixedArray<SemanticProxyConfig, common::ObIAllocator> proxy_configs_;
  SemanticOutputOrder output_order_;
  SemanticExecutionScope execution_scope_;
  int64_t dispatch_rows_;
  int64_t request_limit_;
  int64_t task_slots(int64_t session_slots) const
  {
    // Output frames pinned by a slow row do not need additional request credits.
    return dispatch_rows_ > 0 ? std::max(session_slots, 2 * request_limit_ + 1) : session_slots;
  }
  bool unordered_output() const { return output_order_ == SemanticOutputOrder::READY_ORDER; }
  bool has_embedding_proxy() const
  {
    bool enabled = false;
    for (int64_t index = 0; !enabled && index < proxy_configs_.count(); ++index) {
      enabled = proxy_configs_.at(index).enabled_;
    }
    return enabled;
  }
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

class SemanticRankSpec : public SemanticSpec
{
  OB_UNIS_VERSION_V(1);
public:
  SemanticRankSpec(common::ObIAllocator &allocator, ObPhyOperatorType type)
      : SemanticSpec(allocator, type) {}
};

class SemanticJoinSpec : public SemanticSpec
{
  OB_UNIS_VERSION_V(1);
public:
  SemanticJoinSpec(common::ObIAllocator &allocator, ObPhyOperatorType type)
      : SemanticSpec(allocator, type), join_exprs_(allocator) {}
  int validate_bindings() const;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> join_exprs_;
};

class SemanticOp : public ObOperator, public ISemanticExprRuntime
{
public:
  SemanticOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input);
  int inner_open() override;
  int inner_get_next_row() override;
  int inner_get_next_batch(int64_t max_row_cnt) override;
  int rescan() override;
  int inner_rescan() override;
  int inner_close() override;
  void destroy() override;
  int evaluate(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result) override;
  bool supports_semantic_suspend() const override { return true; }
private:
  struct Dispatch;
  struct Task;
  struct Slot;
  const SemanticSpec &semantic_spec() const { return static_cast<const SemanticSpec &>(spec_); }
  bool is_filter() const
  { return spec_.type_ == PHY_SEMANTIC_FILTER || spec_.type_ == PHY_SEMANTIC_JOIN; }
  int validate_shared_expressions() const;
  int validate_proxy_configs() const;
  int reserve(Slot &slot, int64_t bytes);
  int64_t slot_metadata_bytes(int64_t size) const;
  int allocate_slot(Slot &slot, int64_t size);
  int capture(Slot &slot, int64_t max_row_cnt);
  int capture_join_candidates(Slot &slot);
  int prepare(Slot &slot, bool &progress);
  void mark_row_ready(Slot &slot, int64_t row);
  bool slot_drained(const Slot &slot) const;
  bool quota_cutoff_enabled(const Slot &slot) const;
  bool quota_cruise_brake_enabled() const;
  int64_t slot_cutoff_row(const Slot &slot) const;
  bool row_started(const Slot &slot, int64_t row) const;
  int64_t quota_new_row_allowance(const Slot &slot) const;
  void enqueue_ready_slot(Slot &slot);
  void reclaim_unordered_slots(bool &progress);
  int output_ready_slot(int64_t max_row_cnt);
  int advance_media(Task &task, bool &progress);
  bool input_ready(const Task &task, int64_t position) const;
  bool has_pending_requests() const;
  int build_request_parts(Task &task, Dispatch &dispatch, int64_t position);
  int submit_tasks(Slot &slot, bool &progress);
  int submit_cached_tasks(Slot &slot, Task &task, bool &progress);
  int poll_tasks(Slot &slot, bool &progress);
  int restore_input(Slot &slot);
  void clear_owned_flags(int64_t size);
  int output(Slot &slot, int64_t max_row_cnt);
  void reset_dispatch(Slot &slot, Dispatch &dispatch);
  void reset_task(Slot &slot, Task &task);
  void reset_slot(Slot &slot);
  void reset_pipeline();
  static bool can_resume(const void *operation);
  static int poll_ready_tasks(void *operation, bool &progress);
  int wait_for_work(bool &suspended);
  ObBatchResultHolder child_frame_;
  std::shared_ptr<SemanticExecutionState> execution_;
  class SemanticFunctionCache *function_cache_;
  Slot **slots_;
  int64_t slot_count_;
  int64_t head_;
  int64_t count_;
  Slot *ready_head_;
  Slot *ready_tail_;
  Slot *last_output_slot_;
  bool input_end_;
  const ObBatchRows *pending_input_;
  int64_t waiting_memory_;
  Slot *evaluating_slot_;
  bool registered_;
  bool first_prepare_logged_;
  std::shared_ptr<common::AIRequestTransport> transport_;
  int64_t pending_rows_;
  int64_t slot_storage_bytes_;
  int64_t captured_batches_;
  int64_t max_captured_rows_;
  int64_t output_batches_;
  int64_t output_rows_;
  int64_t max_output_rows_;
  int64_t peak_pending_rows_;
  int64_t peak_frames_;
  int64_t demand_evaluated_;
  int64_t demand_matched_;
  int64_t active_pull_rows_;
  int64_t async_evaluated_rows_;
  int64_t async_matched_rows_;
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

class SemanticRankOp : public SemanticOp
{
public:
  SemanticRankOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
      : SemanticOp(ctx, spec, input) {}
};

class SemanticJoinOp : public SemanticOp
{
public:
  SemanticJoinOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
      : SemanticOp(ctx, spec, input) {}
};
}
}
#endif
