#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_semantic_op.h"
#include "sql/engine/basic/ob_semantic_cache.h"
#include "sql/engine/basic/ob_semantic_media.h"
#include "common/object/file_ref.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_input.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include <algorithm>
#include <chrono>
#include <thread>

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{
bool contains_expression(const ObIArray<ObExpr *> &expressions, const ObExpr *expr)
{
  bool contains = false;
  for (int64_t index = 0; !contains && index < expressions.count(); ++index) {
    contains = expressions.at(index) == expr;
  }
  return contains;
}

struct SemanticRequestMemory
{
  int64_t body_;
  int64_t wire_;
};

SemanticRequestMemory request_memory(const SemanticInput &input)
{
  SemanticRequestMemory bytes{input.parts_.count() * 1024, input.parts_.count() * 512};
  if (input.has_files_ || input.parts_.count() > 1) {
    bytes.body_ += 2 * input.owned_bytes_;
  }
  for (const auto &part : input.parts_) {
    if (part.kind_ == SemanticInputPart::TEXT) {
      bytes.wire_ += 6 * part.value_.length();
    } else if (part.media_) {
      bytes.body_ += 3 * part.media_->base64().length();
      bytes.wire_ += part.media_->base64().length();
    }
  }
  return bytes;
}
}

OB_SERIALIZE_MEMBER((SemanticSpec, ObOpSpec), expressions_, semantic_exprs_, owned_exprs_,
                    local_exprs_, shared_exprs_, carried_exprs_, query_task_count_, filter_expr_count_,
                    memoized_exprs_, proxy_configs_, row_async_enabled_, output_order_, execution_scope_);
OB_SERIALIZE_MEMBER((SemanticMapSpec, SemanticSpec));
OB_SERIALIZE_MEMBER((SemanticFilterSpec, SemanticSpec));
OB_SERIALIZE_MEMBER((SemanticRankSpec, SemanticSpec));
OB_SERIALIZE_MEMBER((SemanticJoinSpec, SemanticSpec), join_exprs_);

int SemanticJoinSpec::validate_bindings() const
{
  int ret = OB_SUCCESS;
  if (join_exprs_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("semantic join has no pair predicate binding", K(ret));
  }
  for (int64_t index = 0; OB_SUCC(ret) && index < join_exprs_.count(); ++index) {
    const ObExpr *expr = join_exprs_.at(index);
    if (OB_ISNULL(expr) || expr->type_ != T_FUN_SYS_AI_JOIN ||
        (expr->arg_cnt_ != 4 && expr->arg_cnt_ != 5) ||
        OB_ISNULL(expr->args_) ||
        !contains_expression(semantic_exprs_, expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid semantic join pair predicate binding", K(ret), KP(expr));
    } else {
      for (uint32_t argument = 0; OB_SUCC(ret) && argument < expr->arg_cnt_; ++argument) {
        if (OB_ISNULL(expr->args_[argument])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("semantic join argument binding is null", K(ret), K(argument));
        }
      }
    }
  }
  for (int64_t index = 0; OB_SUCC(ret) && index < semantic_exprs_.count(); ++index) {
    const ObExpr *expr = semantic_exprs_.at(index);
    if (OB_ISNULL(expr) ||
        (expr->type_ == T_FUN_SYS_AI_JOIN && !contains_expression(join_exprs_, expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semantic join is missing a pair predicate binding", K(ret), KP(expr));
    }
  }
  return ret;
}

struct SemanticOp::Dispatch
{
  Dispatch()
      : arena_("SemanticBody"), allocator_(arena_), batch_(allocator_),
        next_(nullptr), retained_bytes_(0), completed_count_(0) {}
  ~Dispatch() { batch_.cancel(); }
  ObArenaAllocator arena_;
  MultimodeAlloctor allocator_;
  AIFuncBatch batch_;
  ObArray<int64_t> positions_;
  ObArray<ObString> prompts_;
  ObArray<ObJsonArray *> media_parts_;
  Dispatch *next_;
  int64_t retained_bytes_;
  int64_t completed_count_;
};

struct SemanticOp::Task
{
  enum RowState : uint8_t { UNREQUESTED, QUEUED, IN_FLIGHT, COMPLETE, CACHE_WAIT };
  Task(const ObExpr *expr, int64_t max_parallel, bool memoized,
       const SemanticProxyConfig *proxy, const SemanticTaskGroup &task_group,
       int64_t task_group_limit)
      : expression_(expr), request_arena_("SemanticReq"), request_allocator_(request_arena_),
        dispatches_(nullptr), states_(nullptr), results_(nullptr), config_(nullptr), info_(nullptr),
        input_bytes_(0), in_flight_rows_(0), max_parallel_(max_parallel), credit_(false),
        task_group_(task_group), task_group_limit_(task_group_limit),
        memoized_(memoized), cache_keys_(nullptr), cache_prompts_(nullptr),
        cache_handles_(nullptr), cache_binding_id_(0), proxy_config_(proxy),
        join_proxy_(nullptr != proxy && expr->type_ == T_FUN_SYS_AI_JOIN),
        proxy_lefts_(nullptr), proxy_rights_(nullptr) {}
  ~Task()
  {
    while (nullptr != dispatches_) {
      Dispatch *dispatch = dispatches_;
      dispatches_ = dispatch->next_;
      dispatch->~Dispatch();
      ob_free(dispatch);
    }
    for (SemanticInput *input : inputs_) {
      input->~SemanticInput();
    }
    for (int64_t row = 0; nullptr != cache_handles_ && row < max_parallel_; ++row) {
      cache_handles_[row].~shared_ptr();
    }
  }
  const ObExpr *expression_;
  ObArenaAllocator request_arena_;
  MultimodeAlloctor request_allocator_;
  Dispatch *dispatches_;
  uint8_t *states_;
  ObString *results_;
  ObArray<int64_t> rows_;
  ObArray<SemanticInput *> inputs_;
  ObString model_;
  ObJsonObject *config_;
  ObAIFuncExprInfo *info_;
  share::ObAiModelEndpointInfo endpoint_;
  std::shared_ptr<SemanticMediaRuntime> media_;
  int64_t input_bytes_;
  int64_t in_flight_rows_;
  int64_t max_parallel_;
  bool credit_;
  SemanticTaskGroup task_group_;
  int64_t task_group_limit_;
  bool memoized_;
  ObString *cache_keys_;
  ObString *cache_prompts_;
  SemanticFunctionCache::Handle *cache_handles_;
  uint64_t cache_binding_id_;
  const SemanticProxyConfig *proxy_config_;
  bool join_proxy_;
  ObString *proxy_lefts_;
  ObString *proxy_rights_;
};

struct SemanticOp::Slot
{
  enum RowState : uint8_t { WAITING, ACCEPTED, REJECTED };
  Slot() : allocator_("SemanticRows"), inputs_(nullptr), values_(nullptr), evaluated_(nullptr),
           kept_rows_(nullptr), tasks_(nullptr), task_count_(0), size_(0), kept_count_(0),
           offset_(0), retained_bytes_(0), ready_(false), needs_prepare_(false),
           row_states_(nullptr), dirty_(nullptr), dirty_rows_(nullptr),
           dirty_head_(0), dirty_tail_(0), dirty_count_(0), prefix_(0),
           terminal_count_(0), ready_next_(nullptr), output_queued_(false) {}
  ObArenaAllocator allocator_;
  ObDatum *inputs_;
  ObDatum *values_;
  uint8_t *evaluated_;
  int64_t *kept_rows_;
  Task **tasks_;
  int64_t task_count_;
  int64_t size_;
  int64_t kept_count_;
  int64_t offset_;
  int64_t retained_bytes_;
  bool ready_;
  bool needs_prepare_;
  uint8_t *row_states_;
  uint8_t *dirty_;
  int64_t *dirty_rows_;
  int64_t dirty_head_;
  int64_t dirty_tail_;
  int64_t dirty_count_;
  int64_t prefix_;
  int64_t terminal_count_;
  Slot *ready_next_;
  bool output_queued_;
};

SemanticOp::SemanticOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(ctx, spec, input), function_cache_(nullptr),
      slots_(nullptr), slot_count_(0), head_(0), count_(0),
      ready_head_(nullptr), ready_tail_(nullptr), last_output_slot_(nullptr),
      input_end_(false), pending_input_(nullptr), waiting_memory_(0),
      evaluating_slot_(nullptr), registered_(false), first_prepare_logged_(false)
{
}

int SemanticOp::validate_shared_expressions() const
{
  int ret = OB_SUCCESS;
  const SemanticSpec &spec = semantic_spec();
  for (int64_t index = 0; OB_SUCC(ret) && index < spec.shared_exprs_.count(); ++index) {
    const ObExpr *expr = spec.shared_exprs_.at(index);
    if (OB_ISNULL(expr) || !SemanticExprUtils::is_semantic(expr->type_) ||
        !contains_expression(spec.semantic_exprs_, expr) ||
        !contains_expression(child_->get_spec().output_, expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semantic shared expression is not a carried child input", K(ret), KP(expr));
    }
  }
  for (int64_t index = 0; OB_SUCC(ret) && index < spec.carried_exprs_.count(); ++index) {
    const ObExpr *expr = spec.carried_exprs_.at(index);
    if (OB_ISNULL(expr) || !SemanticExprUtils::is_semantic(expr->type_) ||
        !contains_expression(spec.semantic_exprs_, expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semantic carried expression is not a stage dependency", K(ret), KP(expr));
    }
  }
  return ret;
}

int SemanticOp::validate_proxy_configs() const
{
  int ret = OB_SUCCESS;
  const SemanticSpec &spec = semantic_spec();
  if (!spec.proxy_configs_.empty() &&
      spec.proxy_configs_.count() != spec.semantic_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("embedding proxy bindings do not match runtime calls", K(ret));
  }
  for (int64_t index = 0; OB_SUCC(ret) && index < spec.proxy_configs_.count(); ++index) {
    if (!spec.proxy_configs_.at(index).enabled_) {
      continue;
    }
    const ObExpr *expr = spec.semantic_exprs_.at(index);
    const bool join = spec_.type_ == PHY_SEMANTIC_JOIN;
    if ((spec_.type_ != PHY_SEMANTIC_FILTER && !join) || nullptr == expr ||
        expr->type_ != (join ? T_FUN_SYS_AI_JOIN : T_FUN_SYS_AI_FILTER) ||
        !contains_expression(spec.memoized_exprs_, expr) ||
        (join && (spec.semantic_exprs_.count() != 1 || spec.query_task_count_ != 1))) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "embedding proxy requires memoized text FILTER calls or one AI_JOIN");
      LOG_WARN("unsupported embedding proxy runtime binding", K(ret));
    }
  }
  return ret;
}

int SemanticOp::inner_open()
{
  int ret = OB_SUCCESS;
  int64_t memory_limit = 0;
  const SemanticSpec &spec = semantic_spec();
  if (OB_ISNULL(child_) || OB_ISNULL(ctx_.get_my_session()) ||
      spec.expressions_.empty() || spec.semantic_exprs_.empty() || spec.query_task_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid semantic operator specification", K(ret));
  } else if ((is_filter() && (spec.filter_expr_count_ <= 0 ||
                             spec.filter_expr_count_ != spec.expressions_.count())) ||
             (!is_filter() && spec.filter_expr_count_ != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid semantic filter condition count", K(ret), K(spec.filter_expr_count_));
  } else if ((spec.output_order_ != SemanticOutputOrder::INPUT_ORDER &&
              spec.output_order_ != SemanticOutputOrder::READY_ORDER) ||
             (spec.unordered_output() &&
              (!spec.row_async_enabled_ || spec_.type_ == PHY_SEMANTIC_JOIN))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid semantic output order specification", K(ret), K(spec.output_order_),
             K(spec.row_async_enabled_));
  } else if (spec.execution_scope_ != SemanticExecutionScope::SERIAL &&
             spec.execution_scope_ != SemanticExecutionScope::LOCAL_PX_WORKER) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid semantic execution scope", K(ret), K(spec.execution_scope_));
  } else if (spec.execution_scope_ == SemanticExecutionScope::LOCAL_PX_WORKER &&
             (!spec.row_async_enabled_ || !spec.unordered_output() || spec.has_embedding_proxy() ||
              nullptr == ctx_.get_sqc_handler() || ctx_.get_px_task_id() < 0 ||
              nullptr == ctx_.get_physical_plan_ctx() ||
              nullptr == ctx_.get_physical_plan_ctx()->get_phy_plan() ||
              nullptr == lib::RequestAwait::current() ||
              !lib::RequestAwait::current()->owns(&ctx_) ||
              (spec_.type_ != PHY_SEMANTIC_MAP && spec_.type_ != PHY_SEMANTIC_FILTER &&
               spec_.type_ != PHY_SEMANTIC_RANK))) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "local PX row asynchronous execution requires an owned resumable TEXT MAP/FILTER/RANK task");
    LOG_WARN("invalid local PX semantic execution binding", K(ret), K(spec.execution_scope_),
             "px_task_id", ctx_.get_px_task_id());
  } else if (spec.row_async_enabled_ && spec.execution_scope_ == SemanticExecutionScope::SERIAL &&
             nullptr != ctx_.get_sqc_handler()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "serial row asynchronous stages cannot execute inside PX tasks");
    LOG_WARN("serial semantic scope entered a PX task", K(ret));
  } else if (OB_FAIL(validate_proxy_configs())) {
  } else if (OB_FAIL(validate_shared_expressions())) {
  } else if (spec_.type_ == PHY_SEMANTIC_JOIN &&
             OB_FAIL(static_cast<const SemanticJoinSpec &>(spec_).validate_bindings())) {
  } else if (!spec_.is_vectorized()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "AI_MAP, AI_FILTER, AI_JOIN and AI_RANK require a vectorized semantic operator");
  } else if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(
                         share::SYS_VAR_AI_PIPELINE_SLOTS, slot_count_))) {
  } else if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(
                         share::SYS_VAR_AI_PIPELINE_MEMORY_LIMIT, memory_limit))) {
  } else if (slot_count_ < 1 || slot_count_ > 1024 || memory_limit <= 0 ||
             spec.query_task_count_ > INT64_MAX / slot_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid semantic query limits", K(ret), K(slot_count_), K(memory_limit));
  } else if (OB_FAIL(SemanticExecutionState::acquire(ctx_, memory_limit,
                         slot_count_ * spec.query_task_count_, execution_, spec.row_async_enabled_))) {
  } else if (!spec.memoized_exprs_.empty() &&
             OB_FAIL(execution_->function_cache(function_cache_))) {
  } else if (OB_FAIL(child_frame_.init(child_->get_spec().output_, eval_ctx_,
                                      &ctx_.get_allocator()))) {
  } else if (OB_ISNULL(slots_ = static_cast<Slot **>(
                         ctx_.get_allocator().alloc(slot_count_ * sizeof(Slot *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMSET(slots_, 0, slot_count_ * sizeof(Slot *));
    for (int64_t index = 0; OB_SUCC(ret) && index < slot_count_; ++index) {
      if (OB_ISNULL(slots_[index] = OB_NEWx(Slot, &ctx_.get_allocator()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(execution_->register_operator(
        this, &SemanticOp::can_resume, &SemanticOp::poll_ready_tasks))) {
    } else {
      registered_ = true;
    }
  }
  return ret;
}

int SemanticOp::reserve(Slot &slot, int64_t bytes)
{
  const int ret = execution_->budget().reserve(bytes);
  if (ret == OB_SUCCESS) {
    slot.retained_bytes_ += bytes;
  }
  return ret;
}

void SemanticOp::reset_dispatch(Slot &slot, Dispatch &dispatch)
{
  const int64_t bytes = dispatch.retained_bytes_;
  dispatch.~Dispatch();
  ob_free(&dispatch);
  if (bytes > 0) {
    execution_->budget().release(bytes);
    slot.retained_bytes_ -= bytes;
  }
}

void SemanticOp::reset_task(Slot &slot, Task &task)
{
  while (nullptr != task.dispatches_) {
    Dispatch *dispatch = task.dispatches_;
    task.dispatches_ = dispatch->next_;
    reset_dispatch(slot, *dispatch);
  }
  task.in_flight_rows_ = 0;
  if (task.credit_) {
    execution_->budget().release_task(task.task_group_);
    task.credit_ = false;
  }
  for (SemanticInput *input : task.inputs_) {
    input->~SemanticInput();
  }
  task.inputs_.reset();
  if (nullptr != task.cache_handles_) {
    for (int64_t row : task.rows_) {
      task.cache_handles_[row].reset();
    }
  }
  task.media_.reset();
  task.request_allocator_.reset();
  task.rows_.reset();
  task.model_.reset();
  task.config_ = nullptr;
  task.info_ = nullptr;
  task.endpoint_.reset();
  if (task.input_bytes_ > 0) {
    execution_->budget().release(task.input_bytes_);
    slot.retained_bytes_ -= task.input_bytes_;
  }
  task.input_bytes_ = 0;
}

void SemanticOp::reset_slot(Slot &slot)
{
  for (int64_t index = 0; index < slot.task_count_; ++index) {
    if (nullptr != slot.tasks_[index]) {
      reset_task(slot, *slot.tasks_[index]);
      OB_DELETEx(Task, &slot.allocator_, slot.tasks_[index]);
    }
  }
  slot.allocator_.reset();
  if (execution_) {
    execution_->budget().release(slot.retained_bytes_);
  }
  slot.inputs_ = slot.values_ = nullptr;
  slot.evaluated_ = nullptr;
  slot.kept_rows_ = nullptr;
  slot.tasks_ = nullptr;
  slot.task_count_ = slot.size_ = slot.kept_count_ = slot.offset_ = slot.retained_bytes_ = 0;
  slot.ready_ = false;
  slot.needs_prepare_ = false;
  slot.row_states_ = slot.dirty_ = nullptr;
  slot.dirty_rows_ = nullptr;
  slot.dirty_head_ = slot.dirty_tail_ = slot.dirty_count_ = slot.prefix_ = 0;
  slot.terminal_count_ = 0;
  slot.ready_next_ = nullptr;
  slot.output_queued_ = false;
}

void SemanticOp::reset_pipeline()
{
  ready_head_ = ready_tail_ = last_output_slot_ = nullptr;
  for (int64_t index = 0; nullptr != slots_ && index < slot_count_; ++index) {
    if (nullptr != slots_[index]) {
      reset_slot(*slots_[index]);
    }
  }
  head_ = count_ = 0;
  input_end_ = false;
  pending_input_ = nullptr;
  waiting_memory_ = 0;
  evaluating_slot_ = nullptr;
  first_prepare_logged_ = false;
}

int SemanticOp::inner_close()
{
  const int ret = child_frame_.restore();
  child_frame_.reset();
  reset_pipeline();
  if (registered_) {
    execution_->unregister_operator(this);
    registered_ = false;
  }
  if (function_cache_ && execution_) {
    LOG_DEBUG("semantic cached producer closed", "execution_id", execution_->budget().execution_id(),
              "operator_id", spec_.id_, "operator_type", spec_.type_);
  }
  execution_.reset();
  function_cache_ = nullptr;
  return ret;
}

int SemanticOp::rescan()
{
  int ret = child_frame_.restore();
  child_frame_.reset();
  reset_pipeline();
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to restore semantic input frame before child rescan", K(ret));
  } else if (OB_FAIL(ObOperator::rescan())) {
    LOG_WARN("failed to rescan semantic operator", K(ret));
  }
  return ret;
}

int SemanticOp::inner_rescan()
{
  return ObOperator::inner_rescan();
}

void SemanticOp::destroy()
{
  reset_pipeline();
  if (registered_) {
    execution_->unregister_operator(this);
    registered_ = false;
  }
  execution_.reset();
  function_cache_ = nullptr;
  for (int64_t index = 0; nullptr != slots_ && index < slot_count_; ++index) {
    OB_DELETEx(Slot, &ctx_.get_allocator(), slots_[index]);
  }
  ctx_.get_allocator().free(slots_);
  slots_ = nullptr;
  slot_count_ = 0;
  child_frame_.destroy();
  child_frame_.~ObBatchResultHolder();
  execution_.~shared_ptr();
  ObOperator::destroy();
}

int SemanticOp::inner_get_next_row()
{
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "semantic operators require vectorized execution");
  return OB_NOT_SUPPORTED;
}

int64_t SemanticOp::slot_metadata_bytes(int64_t size) const
{
  const int64_t columns = child_->get_spec().output_.count();
  const int64_t expressions = semantic_spec().expressions_.count();
  const int64_t tasks = semantic_spec().semantic_exprs_.count();
  const int64_t memoized = semantic_spec().memoized_exprs_.count();
  return size * (columns * sizeof(ObDatum) +
      expressions * (sizeof(ObDatum) + sizeof(uint8_t)) + sizeof(int64_t) +
      tasks * (sizeof(uint8_t) + sizeof(ObString))) +
      tasks * (sizeof(Task) + sizeof(Task *)) +
      size * memoized * (2 * sizeof(ObString) + sizeof(SemanticFunctionCache::Handle)) +
      (semantic_spec().row_async_enabled_ ? size * (2 * sizeof(uint8_t) + sizeof(int64_t)) : 0) +
      (semantic_spec().unordered_output() ? sizeof(Slot *) + sizeof(int64_t) + sizeof(bool) : 0) +
      (semantic_spec().has_embedding_proxy() && spec_.type_ == PHY_SEMANTIC_JOIN
          ? size * 2 * sizeof(ObString) : 0);
}

int SemanticOp::allocate_slot(Slot &slot, int64_t size)
{
  int ret = OB_SUCCESS;
  auto &allocator = slot.allocator_;
  const int64_t columns = child_->get_spec().output_.count();
  const int64_t outputs = semantic_spec().expressions_.count();
  const int64_t tasks = semantic_spec().semantic_exprs_.count();
  if (columns > 0 && OB_ISNULL(slot.inputs_ = static_cast<ObDatum *>(
      allocator.alloc(size * columns * sizeof(ObDatum))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(slot.values_ = static_cast<ObDatum *>(
      allocator.alloc(size * outputs * sizeof(ObDatum)))) ||
      OB_ISNULL(slot.evaluated_ = static_cast<uint8_t *>(allocator.alloc(size * outputs))) ||
      OB_ISNULL(slot.kept_rows_ = static_cast<int64_t *>(allocator.alloc(size * sizeof(int64_t)))) ||
      OB_ISNULL(slot.tasks_ = static_cast<Task **>(allocator.alloc(tasks * sizeof(Task *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMSET(slot.evaluated_, 0, size * outputs);
    MEMSET(slot.tasks_, 0, tasks * sizeof(Task *));
    slot.task_count_ = tasks;
    for (int64_t index = 0; index < size * outputs; ++index) {
      new (&slot.values_[index]) ObDatum();
    }
    if (semantic_spec().row_async_enabled_) {
      slot.row_states_ = static_cast<uint8_t *>(allocator.alloc(size));
      slot.dirty_ = static_cast<uint8_t *>(allocator.alloc(size));
      slot.dirty_rows_ = static_cast<int64_t *>(allocator.alloc(size * sizeof(int64_t)));
      if (OB_ISNULL(slot.row_states_) || OB_ISNULL(slot.dirty_) || OB_ISNULL(slot.dirty_rows_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMSET(slot.row_states_, Slot::WAITING, size);
        MEMSET(slot.dirty_, 0, size);
        for (int64_t row = 0; row < size; ++row) {
          mark_row_ready(slot, row);
        }
      }
    }
    for (int64_t index = 0; OB_SUCC(ret) && index < tasks; ++index) {
      const ObExpr *expr = semantic_spec().semantic_exprs_.at(index);
      const SemanticProxyConfig *proxy = semantic_spec().proxy_configs_.empty() ||
          !semantic_spec().proxy_configs_.at(index).enabled_
          ? nullptr : &semantic_spec().proxy_configs_.at(index);
      const bool local_px = semantic_spec().execution_scope_ == SemanticExecutionScope::LOCAL_PX_WORKER;
      auto *task = OB_NEWx(Task, &allocator, expr, size,
          contains_expression(semantic_spec().memoized_exprs_, expr), proxy,
          local_px ? SemanticTaskGroup(spec_.id_, index) : SemanticTaskGroup(-1, -1),
          local_px ? slot_count_ : 0);
      if (OB_ISNULL(task)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        slot.tasks_[index] = task;
        task->states_ = static_cast<uint8_t *>(allocator.alloc(size));
        task->results_ = static_cast<ObString *>(allocator.alloc(size * sizeof(ObString)));
        if (OB_ISNULL(task->states_) || OB_ISNULL(task->results_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMSET(task->states_, Task::UNREQUESTED, size);
          for (int64_t row = 0; row < size; ++row) {
            new (&task->results_[row]) ObString();
          }
          if (task->memoized_) {
            task->cache_keys_ = static_cast<ObString *>(allocator.alloc(size * sizeof(ObString)));
            task->cache_prompts_ = static_cast<ObString *>(allocator.alloc(size * sizeof(ObString)));
            if (nullptr == task->cache_keys_ || nullptr == task->cache_prompts_) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              for (int64_t row = 0; row < size; ++row) {
                new (&task->cache_keys_[row]) ObString();
                new (&task->cache_prompts_[row]) ObString();
              }
              task->cache_handles_ = static_cast<SemanticFunctionCache::Handle *>(
                  allocator.alloc(size * sizeof(SemanticFunctionCache::Handle)));
              if (nullptr == task->cache_handles_) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                for (int64_t row = 0; row < size; ++row) {
                  new (&task->cache_handles_[row]) SemanticFunctionCache::Handle();
                }
              }
              if (OB_SUCC(ret) && task->join_proxy_) {
                task->proxy_lefts_ = static_cast<ObString *>(allocator.alloc(size * sizeof(ObString)));
                task->proxy_rights_ = static_cast<ObString *>(allocator.alloc(size * sizeof(ObString)));
                if (nullptr == task->proxy_lefts_ || nullptr == task->proxy_rights_) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                } else {
                  for (int64_t row = 0; row < size; ++row) {
                    new (&task->proxy_lefts_[row]) ObString();
                    new (&task->proxy_rights_[row]) ObString();
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to allocate semantic batch storage", K(ret), K(size));
  }
  return ret;
}

int SemanticOp::capture(Slot &slot)
{
  if (spec_.type_ == PHY_SEMANTIC_JOIN) {
    return capture_join_candidates(slot);
  }
  int ret = OB_SUCCESS;
  const auto &columns = child_->get_spec().output_;
  if (OB_FAIL(child_frame_.restore())) {
  } else if (nullptr == pending_input_ &&
      OB_FAIL(child_->get_next_batch(spec_.max_batch_size_, pending_input_))) {
  }
  if (OB_SUCC(ret)) {
    const ObBatchRows &rows = *pending_input_;
    // Prefix sort and joins can retain a live input tail beyond the returned rows.
    const int64_t size = rows.size_ - rows.skip_->accumulate_bit_cnt(rows.size_);
    int64_t bytes = slot_metadata_bytes(size);
    if (size == 0) {
      input_end_ = rows.end_;
      pending_input_ = nullptr;
    } else if (OB_FAIL(child_frame_.save(eval_ctx_.max_batch_size_))) {
    } else {
      for (int64_t row = 0; OB_SUCC(ret) && row < rows.size_; ++row) {
        if (!rows.skip_->at(row)) {
          for (int64_t column = 0; OB_SUCC(ret) && column < columns.count(); ++column) {
            const int64_t extra = columns.at(column)->locate_expr_datumvector(eval_ctx_)
                                      .at(row)->get_deep_copy_size();
            if (extra > execution_->budget().memory_limit() - bytes) {
              ret = OB_SIZE_OVERFLOW;
            } else {
              bytes += extra;
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (bytes > execution_->budget().memory_limit() ||
                 bytes > ObAIFuncClient::pipeline_buffer_limit()) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_FAIL(reserve(slot, bytes))) {
        if (ret == OB_SIZE_OVERFLOW && count_ == 0 &&
            (semantic_spec().execution_scope_ != SemanticExecutionScope::LOCAL_PX_WORKER ||
             execution_->budget().active_tasks() == 0) &&
            bytes > execution_->budget().memory_limit() - execution_->budget().memory_used()) {
          LOG_WARN("semantic input cannot fit while its upstream batch is pinned",
                   K(ret), K(bytes), "memory_used", execution_->budget().memory_used());
        } else if (ret == OB_SIZE_OVERFLOW) {
          waiting_memory_ = bytes;
          ret = OB_EAGAIN;
        }
      } else {
        waiting_memory_ = 0;
        slot.size_ = size;
        if (OB_FAIL(allocate_slot(slot, size))) {
        } else {
          int64_t target = 0;
          for (int64_t row = 0; OB_SUCC(ret) && row < rows.size_; ++row) {
            if (!rows.skip_->at(row)) {
              for (int64_t column = 0; OB_SUCC(ret) && column < columns.count(); ++column) {
                if (OB_FAIL(slot.inputs_[target * columns.count() + column].deep_copy(
                    *columns.at(column)->locate_expr_datumvector(eval_ctx_).at(row), slot.allocator_))) {
                }
              }
              ++target;
            }
          }
        }
        if (OB_SUCC(ret)) {
          slot.needs_prepare_ = true;
          input_end_ = rows.end_;
          pending_input_ = nullptr;
          ++count_;
        }
      }
    }
  }
  return ret;
}

int SemanticOp::capture_join_candidates(Slot &slot)
{
  int ret = OB_SUCCESS;
  const auto &columns = child_->get_spec().output_;
  const int64_t capacity = MIN(spec_.max_batch_size_, eval_ctx_.max_batch_size_);
  if (capacity <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid semantic join candidate batch capacity", K(ret), K(capacity));
  }
  // Native NLJ output can be one row per fragment, independently of its ROWSET.
  while (OB_SUCC(ret) && !input_end_ && slot.size_ < capacity) {
    if (OB_FAIL(ctx_.check_status())) {
    } else if (OB_FAIL(child_frame_.restore())) {
    } else if (nullptr == pending_input_ &&
               OB_FAIL(child_->get_next_batch(capacity - slot.size_, pending_input_))) {
      if (ret == OB_EAGAIN && slot.size_ > 0) {
        ret = OB_SUCCESS;
        break;
      }
    }
    if (OB_FAIL(ret)) {
      break;
    } else if (OB_ISNULL(pending_input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semantic join candidate generator returned no batch", K(ret));
      break;
    }
    const ObBatchRows &rows = *pending_input_;
    const int64_t size = rows.size_ - rows.skip_->accumulate_bit_cnt(rows.size_);
    if (size > capacity - slot.size_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semantic join candidate fragment exceeds requested capacity",
               K(ret), K(size), K(capacity), K(slot.size_));
    } else if (size == 0) {
      input_end_ = rows.end_;
      pending_input_ = nullptr;
    } else if (OB_FAIL(child_frame_.save(eval_ctx_.max_batch_size_))) {
    } else {
      int64_t bytes = slot.task_count_ == 0 ? slot_metadata_bytes(capacity) : 0;
      for (int64_t row = 0; OB_SUCC(ret) && row < rows.size_; ++row) {
        if (!rows.skip_->at(row)) {
          for (int64_t column = 0; OB_SUCC(ret) && column < columns.count(); ++column) {
            const int64_t extra = columns.at(column)->locate_expr_datumvector(eval_ctx_)
                                      .at(row)->get_deep_copy_size();
            if (extra > execution_->budget().memory_limit() - bytes) {
              ret = OB_SIZE_OVERFLOW;
            } else {
              bytes += extra;
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (bytes > execution_->budget().memory_limit() ||
                 bytes > ObAIFuncClient::pipeline_buffer_limit()) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_FAIL(reserve(slot, bytes))) {
        if (ret == OB_SIZE_OVERFLOW && execution_->budget().failure() == OB_SUCCESS) {
          if (slot.size_ > 0) {
            LOG_DEBUG("semantic join emits a partial batch under memory pressure",
                      K(bytes), K(slot.size_));
            waiting_memory_ = 0;
            ret = OB_SUCCESS;
            break;
          } else if (count_ > 0) {
            waiting_memory_ = bytes;
            ret = OB_EAGAIN;
          } else {
            LOG_WARN("semantic join input cannot fit while its upstream batch is pinned",
                     K(ret), K(bytes), "memory_used", execution_->budget().memory_used());
          }
        }
      } else {
        waiting_memory_ = 0;
        if (slot.task_count_ == 0 && OB_FAIL(allocate_slot(slot, capacity))) {
        } else {
          int64_t target = slot.size_;
          for (int64_t row = 0; OB_SUCC(ret) && row < rows.size_; ++row) {
            if (!rows.skip_->at(row)) {
              for (int64_t column = 0; OB_SUCC(ret) && column < columns.count(); ++column) {
                if (OB_FAIL(slot.inputs_[target * columns.count() + column].deep_copy(
                    *columns.at(column)->locate_expr_datumvector(eval_ctx_).at(row), slot.allocator_))) {
                }
              }
              ++target;
            }
          }
          if (OB_SUCC(ret)) {
            slot.size_ = target;
            input_end_ = rows.end_;
            pending_input_ = nullptr;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && slot.size_ > 0) {
    slot.needs_prepare_ = true;
    ++count_;
  }
  return ret;
}

int SemanticOp::restore_input(Slot &slot)
{
  clear_evaluated_flag();
  clear_owned_flags(slot.size_);
  const auto &columns = child_->get_spec().output_;
  for (int64_t column = 0; column < columns.count(); ++column) {
    ObExpr &expr = *columns.at(column);
    if (contains_expression(semantic_spec().local_exprs_, &expr)) {
      continue;
    }
    const bool shared = contains_expression(semantic_spec().shared_exprs_, &expr);
    expr.get_eval_info(eval_ctx_).evaluated_ = true;
    if (shared) {
      expr.get_eval_info(eval_ctx_).projected_ = false;
    }
    for (int64_t row = 0; row < slot.size_; ++row) {
      *expr.locate_expr_datumvector(eval_ctx_).at(row) = slot.inputs_[row * columns.count() + column];
      if (shared && slot.inputs_[row * columns.count() + column].is_null()) {
        if (expr.is_batch_result()) {
          // Keep the batch initialized so evaluating a missing row does not clear its ready peers.
          expr.get_evaluated_flags(eval_ctx_).unset(row);
        } else {
          expr.get_eval_info(eval_ctx_).evaluated_ = false;
        }
      } else if (expr.is_batch_result()) {
        expr.get_evaluated_flags(eval_ctx_).set(row);
      }
    }
  }
  return OB_SUCCESS;
}

void SemanticOp::clear_owned_flags(int64_t size)
{
  for (int64_t index = 0; index < semantic_spec().owned_exprs_.count(); ++index) {
    const ObExpr &expr = *semantic_spec().owned_exprs_.at(index);
    expr.get_eval_info(eval_ctx_).clear_evaluated_flag();
    if (expr.is_batch_result()) {
      expr.get_evaluated_flags(eval_ctx_).reset(size);
    }
  }
}

int SemanticOp::evaluate(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  int ret = OB_SUCCESS;
  Task *task = nullptr;
  Slot *slot = evaluating_slot_;
  const int64_t row = ctx.get_batch_idx();
  if (OB_ISNULL(slot) || &ctx.exec_ctx_ != &ctx_ || row < 0 || row >= slot->size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("semantic expression has no active input row", K(ret), K(row));
  } else {
    for (int64_t index = 0; nullptr == task && index < slot->task_count_; ++index) {
      if (slot->tasks_[index]->expression_ == &expr) {
        task = slot->tasks_[index];
      }
    }
    if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semantic expression is not owned by its operator", K(ret), K(expr));
    } else if (task->states_[row] == Task::COMPLETE) {
      ret = SemanticExprUtils::set_result(expr, ctx, result, task->results_[row]);
    } else if (task->states_[row] != Task::UNREQUESTED) {
      ret = OB_EAGAIN;
    } else {
      ObEvalCtx::TempAllocGuard input_guard(ctx);
      MultimodeAlloctor input_allocator(input_guard.get_allocator());
      ObString model;
      ObString cache_key;
      ObString proxy_left;
      ObString proxy_right;
      SemanticInput input;
      ObJsonObject *config = nullptr;
      const bool first_request = task->rows_.empty();
      int64_t bytes = 0;
      if (OB_FAIL(SemanticExprUtils::prepare_request_input(expr, ctx, input_allocator,
                                                          model, input, config, first_request))) {
      } else if (input.has_files_ &&
                 semantic_spec().execution_scope_ == SemanticExecutionScope::LOCAL_PX_WORKER) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "local PX row asynchronous execution supports TEXT inputs only");
        LOG_WARN("FILE input in local PX row asynchronous task", K(ret));
      } else if (task->memoized_ && OB_FAIL(SemanticExprUtils::prepare_cache_key(
          expr, ctx, input_allocator, cache_key))) {
      } else if (task->join_proxy_ && OB_FAIL(SemanticExprUtils::prepare_join_proxy_input(
          expr, ctx, input_allocator, proxy_left, proxy_right))) {
      } else {
        const uint64_t config_bytes = nullptr == config ? 0 : config->get_serialize_size();
        bytes = input.owned_bytes_ + sizeof(SemanticInput) +
                input.parts_.count() * sizeof(SemanticInputPart) +
                (first_request ? model.length() : 0) + cache_key.length() +
                (task->memoized_ && input.parts_.count() > 1 ? input.owned_bytes_ : 0) +
                proxy_left.length() + proxy_right.length();
        if (bytes > ObAIFuncClient::MAX_BATCH_BYTES ||
            config_bytes > static_cast<uint64_t>(ObAIFuncClient::MAX_BATCH_BYTES - bytes)) {
          ret = OB_SIZE_OVERFLOW;
        } else {
          bytes += config_bytes;
        }
      }
      if (OB_SUCC(ret) && (bytes > ObAIFuncClient::MAX_BATCH_BYTES ||
                           task->input_bytes_ > ObAIFuncClient::MAX_BATCH_BYTES - bytes)) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_SUCC(ret) && OB_FAIL(reserve(*slot, bytes))) {
      } else if (OB_SUCC(ret)) {
        task->input_bytes_ += bytes;
        auto *owned_input = OB_NEWx(SemanticInput, &task->request_allocator_);
        if (nullptr == owned_input) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(task->inputs_.push_back(owned_input))) {
          owned_input->~SemanticInput();
        }
        for (int64_t part = 0; OB_SUCC(ret) && part < input.parts_.count(); ++part) {
          const auto &value = input.parts_.at(part);
          ret = owned_input->append(task->request_allocator_, value.kind_, value.value_);
        }
        if (OB_SUCC(ret) && task->memoized_) {
          if (OB_FAIL(ob_write_string(task->request_allocator_, cache_key, task->cache_keys_[row]))) {
          } else {
            ret = owned_input->render_text(task->request_allocator_, task->cache_prompts_[row]);
          }
        }
        if (OB_SUCC(ret) && task->join_proxy_) {
          if (OB_FAIL(ob_write_string(task->request_allocator_, proxy_left, task->proxy_lefts_[row]))) {
          } else {
            ret = ob_write_string(task->request_allocator_, proxy_right, task->proxy_rights_[row]);
          }
        }
        if (OB_FAIL(ret)) {
        } else if (first_request &&
                   OB_FAIL(ob_write_string(task->request_allocator_, model, task->model_))) {
        } else if (first_request && nullptr != config &&
                   OB_ISNULL(task->config_ = static_cast<ObJsonObject *>(
                       config->clone(&task->request_allocator_, true)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(task->rows_.push_back(row))) {
        } else {
          task->states_[row] = Task::QUEUED;
          ret = OB_EAGAIN;
        }
      }
    }
  }
  return ret;
}

int SemanticOp::prepare(Slot &slot, bool &progress)
{
  int ret = OB_SUCCESS;
  const bool first_prepare = !first_prepare_logged_;
  if (first_prepare) {
    first_prepare_logged_ = true;
    LOG_INFO("semantic first prepare begin", "execution_id", execution_->budget().execution_id(),
             "operator_id", spec_.get_id(), "px_task_id", ctx_.get_px_task_id(),
             "thread_id", GETTID(), "rows", slot.size_);
  }
  const auto &expressions = semantic_spec().expressions_;
  ObEvalCtx::BatchInfoScopeGuard batch_guard(eval_ctx_);
  batch_guard.set_batch_size(slot.size_);
  if (OB_FAIL(restore_input(slot))) {
  } else {
    SemanticExprRuntimeGuard runtime_guard(this);
    evaluating_slot_ = &slot;
    int64_t finished = 0;
    int64_t kept = 0;
    const bool incremental = semantic_spec().row_async_enabled_;
    const int64_t pending = incremental ? slot.dirty_count_ : slot.size_;
    for (int64_t position = 0; OB_SUCC(ret) && position < pending; ++position) {
      int64_t row = position;
      if (incremental) {
        row = slot.dirty_rows_[slot.dirty_head_];
        slot.dirty_head_ = (slot.dirty_head_ + 1) % slot.size_;
        --slot.dirty_count_;
        slot.dirty_[row] = 0;
      }
      batch_guard.set_batch_idx(row);
      bool complete = true;
      bool accepted = true;
      for (int64_t index = 0; OB_SUCC(ret) && accepted && index < expressions.count(); ++index) {
        const int64_t offset = row * expressions.count() + index;
        const bool condition = is_filter() && index < semantic_spec().filter_expr_count_;
        if (!slot.evaluated_[offset]) {
          ObDatum *value = nullptr;
          const int evaluated = expressions.at(index)->eval(eval_ctx_, value);
          if (evaluated == OB_EAGAIN) {
            complete = false;
            if (condition) {
              break;
            }
          } else if (evaluated != OB_SUCCESS) {
            ret = evaluated;
          } else if (OB_FAIL(reserve(slot, value->get_deep_copy_size()))) {
          } else if (OB_FAIL(slot.values_[offset].deep_copy(*value, slot.allocator_))) {
          } else {
            slot.evaluated_[offset] = 1;
            progress = true;
          }
        }
        if (condition && slot.evaluated_[offset]) {
          accepted = !slot.values_[offset].is_null() && slot.values_[offset].get_int() != 0;
        }
      }
      if (OB_SUCC(ret) && complete) {
        if (incremental) {
          OB_ASSERT(slot.row_states_[row] == Slot::WAITING);
          slot.row_states_[row] = accepted ? Slot::ACCEPTED : Slot::REJECTED;
          ++slot.terminal_count_;
          if (semantic_spec().unordered_output() && accepted) {
            slot.kept_rows_[slot.kept_count_++] = row;
          }
        } else if (accepted) {
          slot.kept_rows_[kept++] = row;
        }
        ++finished;
      }
    }
    evaluating_slot_ = nullptr;
    slot.needs_prepare_ = false;
    if (OB_SUCC(ret) && incremental && semantic_spec().unordered_output()) {
      slot.ready_ = slot.terminal_count_ == slot.size_;
      enqueue_ready_slot(slot);
    } else if (OB_SUCC(ret) && incremental) {
      while (slot.prefix_ < slot.size_ && slot.row_states_[slot.prefix_] != Slot::WAITING) {
        if (slot.row_states_[slot.prefix_] == Slot::ACCEPTED) {
          slot.kept_rows_[slot.kept_count_++] = slot.prefix_;
        }
        ++slot.prefix_;
        progress = true;
      }
      slot.ready_ = slot.prefix_ == slot.size_;
    } else if (OB_SUCC(ret) && finished == slot.size_) {
      slot.kept_count_ = kept;
      slot.ready_ = true;
    }
  }
  if (first_prepare) {
    LOG_INFO("semantic first prepare end", K(ret), "execution_id", execution_->budget().execution_id(),
             "operator_id", spec_.get_id(), "px_task_id", ctx_.get_px_task_id(),
             "thread_id", GETTID(), "rows", slot.size_);
  }
  return ret;
}

void SemanticOp::mark_row_ready(Slot &slot, int64_t row)
{
  if (semantic_spec().row_async_enabled_ &&
      slot.row_states_[row] == Slot::WAITING && !slot.dirty_[row]) {
    OB_ASSERT(slot.dirty_count_ < slot.size_);
    slot.dirty_[row] = 1;
    slot.dirty_rows_[slot.dirty_tail_] = row;
    slot.dirty_tail_ = (slot.dirty_tail_ + 1) % slot.size_;
    ++slot.dirty_count_;
  }
  slot.needs_prepare_ = true;
}

bool SemanticOp::slot_drained(const Slot &slot) const
{
  return slot.ready_ && slot.offset_ == slot.kept_count_;
}

void SemanticOp::enqueue_ready_slot(Slot &slot)
{
  if (!slot.output_queued_ && slot.offset_ < slot.kept_count_) {
    slot.ready_next_ = nullptr;
    slot.output_queued_ = true;
    if (nullptr == ready_tail_) {
      ready_head_ = &slot;
    } else {
      ready_tail_->ready_next_ = &slot;
    }
    ready_tail_ = &slot;
  }
}

void SemanticOp::reclaim_unordered_slots(bool &progress)
{
  for (int64_t index = 0; index < count_;) {
    Slot *slot = slots_[(head_ + index) % slot_count_];
    if (slot != last_output_slot_ && slot_drained(*slot)) {
      OB_ASSERT(!slot->output_queued_);
      reset_slot(*slot);
      // Keep the active segment compact without moving the slot objects or their datums.
      std::swap(slots_[(head_ + index) % slot_count_],
                slots_[(head_ + count_ - 1) % slot_count_]);
      --count_;
      progress = true;
    } else {
      ++index;
    }
  }
}

int SemanticOp::output_ready_slot(int64_t max_row_cnt)
{
  OB_ASSERT(nullptr != ready_head_);
  Slot &slot = *ready_head_;
  ready_head_ = slot.ready_next_;
  if (nullptr == ready_head_) {
    ready_tail_ = nullptr;
  }
  slot.ready_next_ = nullptr;
  slot.output_queued_ = false;
  const int ret = output(slot, max_row_cnt);
  if (ret == OB_SUCCESS) {
    last_output_slot_ = &slot;
    enqueue_ready_slot(slot);
  }
  return ret;
}

int SemanticOp::advance_media(Task &task, bool &progress)
{
  int ret = OB_SUCCESS;
  if (nullptr == task.info_) {
    auto *resolver = share::server_service<query::ObIAiEndpointResolver>();
    if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(task.request_allocator_, task.model_, task.info_))) {
    } else if (OB_ISNULL(resolver)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(resolver->resolve_by_model_name(task.model_, task.request_allocator_, task.endpoint_))) {
    }
  }
  for (int64_t position = 0; OB_SUCC(ret) && position < task.rows_.count(); ++position) {
    if (task.states_[task.rows_.at(position)] != Task::QUEUED) {
      continue;
    }
    SemanticInput &input = *task.inputs_.at(position);
    if (input.has_files_ && task.info_->type_ != share::EndpointType::COMPLETION) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "FILE inputs require a completion model");
    } else if (input.has_files_ && (task.info_->input_modalities_ &
               (share::AiInputModality::IMAGE | share::AiInputModality::AUDIO)) == 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "the model was registered for text-only inputs");
    } else if (input.has_files_ && !ObAIFuncUtils::ob_provider_check(task.endpoint_.get_provider(),
                                                                  ObAIFuncProviderUtils::OPENAI)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "FILE inputs currently require the openai provider");
    }
    for (int64_t part = 0; OB_SUCC(ret) && part < input.parts_.count(); ++part) {
      auto &value = input.parts_.at(part);
      if (value.kind_ != SemanticInputPart::FILE) {
        continue;
      }
      if (!value.media_) {
        FileRef reference;
        if (OB_FAIL(FileRefCodec::decode(value.value_, reference))) {
        } else if ((reference.mime_type_.prefix_match("image/") &&
                    (task.info_->input_modalities_ & share::AiInputModality::IMAGE) == 0) ||
                   (reference.mime_type_.prefix_match("audio/") &&
                    (task.info_->input_modalities_ & share::AiInputModality::AUDIO) == 0)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "the model was not registered for this media modality");
        } else if (!task.media_ && OB_FAIL(execution_->media_runtime(task.media_))) {
        } else {
          const int demanded = task.media_->demand(value.value_, value.media_);
          if (demanded == OB_EAGAIN) {
          } else if (demanded != OB_SUCCESS) {
            ret = demanded;
          } else {
            progress = true;
          }
        }
      }
      if (OB_SUCC(ret) && value.media_ && value.media_->ready()) {
        if (OB_FAIL(value.media_->result())) {
        } else if ((task.info_->input_modalities_ & (value.media_->is_audio()
                   ? share::AiInputModality::AUDIO : share::AiInputModality::IMAGE)) == 0) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "the model was not registered for this media modality");
        }
      }
    }
  }
  return ret;
}

bool SemanticOp::input_ready(const Task &task, int64_t position) const
{
  bool ready = task.states_[task.rows_.at(position)] == Task::QUEUED;
  const SemanticInput &input = *task.inputs_.at(position);
  for (int64_t part = 0; ready && part < input.parts_.count(); ++part) {
    const auto &value = input.parts_.at(part);
    ready = value.kind_ != SemanticInputPart::FILE ||
            (value.media_ && value.media_->ready() && value.media_->result() == OB_SUCCESS);
  }
  return ready;
}

bool SemanticOp::has_pending_requests() const
{
  int64_t idle_credits = 0;
  bool pending = false;
  for (int64_t index = 0; !pending && index < count_; ++index) {
    const Slot &slot = *slots_[(head_ + index) % slot_count_];
    for (int64_t task_index = 0; !pending && task_index < slot.task_count_; ++task_index) {
      const Task &task = *slot.tasks_[task_index];
      pending = nullptr != task.dispatches_;
      idle_credits += task.credit_ && !pending ? 1 : 0;
    }
  }
  // A held wave credit without a dispatch cannot free request memory by itself.
  return pending || execution_->budget().function_cache_pending() ||
      execution_->budget().active_tasks() > idle_credits;
}

int SemanticOp::build_request_parts(Task &task, Dispatch &dispatch, int64_t position)
{
  int ret = OB_SUCCESS;
  SemanticInput &input = *task.inputs_.at(position);
  ObString prompt;
  ObJsonArray *parts = nullptr;
  if (!input.has_files_) {
    ret = input.render_text(dispatch.allocator_, prompt);
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_array(dispatch.allocator_, parts))) {
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < input.parts_.count(); ++index) {
      auto &part = input.parts_.at(index);
      if (part.kind_ == SemanticInputPart::FILE) {
        if (!part.media_) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(part.media_->result())) {
        } else if (part.media_->base64().empty() || part.media_->mime_type().empty()) {
          ret = OB_INVALID_DATA;
          LOG_WARN("successful media load has no usable content", K(ret));
        } else if ((task.info_->input_modalities_ & (part.media_->is_audio()
                   ? share::AiInputModality::AUDIO : share::AiInputModality::IMAGE)) == 0) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "the model was not registered for this media modality");
        }
      }
      if (OB_FAIL(ret)) {
        break;
      }
      ObJsonObject *object = nullptr;
      ObJsonString *type = nullptr;
      ObJsonString *data = nullptr;
      ObString kind = part.kind_ == SemanticInputPart::TEXT ? ObString("text") :
                      part.media_->is_audio() ? ObString("audio") : ObString("image");
      ObString value = part.kind_ == SemanticInputPart::TEXT ? part.value_ : part.media_->base64();
      if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(dispatch.allocator_, object))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(dispatch.allocator_, kind, type))) {
      } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(dispatch.allocator_, value, data))) {
      } else if (OB_FAIL(object->add("type", type))) {
      } else if (OB_FAIL(object->add("data", data))) {
      } else if (OB_FAIL(parts->append(object))) {
      } else if (part.kind_ == SemanticInputPart::FILE) {
        ObJsonString *metadata = nullptr;
        ObString detail = part.media_->is_audio() ? part.media_->audio_format() : part.media_->mime_type();
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(dispatch.allocator_, detail, metadata))) {
        } else if (OB_FAIL(object->add(part.media_->is_audio() ? "format" : "mime_type", metadata))) {
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(dispatch.prompts_.push_back(prompt))) {
  } else if (OB_SUCC(ret) && OB_FAIL(dispatch.media_parts_.push_back(parts))) {
  } else if (OB_SUCC(ret) && OB_FAIL(dispatch.positions_.push_back(position))) {
  }
  return ret;
}

int SemanticOp::submit_cached_tasks(Slot &slot, Task &task, bool &progress)
{
  UNUSED(slot);
  int ret = OB_SUCCESS;
  ObArray<int64_t> rows;
  ObArray<ObString> keys;
  ObArray<ObString> prompts;
  ObArray<ObString> documents;
  ObArray<ObString> right_documents;
  const SemanticProxyConfig *proxy = task.proxy_config_;
  std::vector<SemanticFunctionCache::Handle> handles;
  for (int64_t position = 0; OB_SUCC(ret) && position < task.rows_.count(); ++position) {
    const int64_t row = task.rows_.at(position);
    if (OB_FAIL(ret) || task.states_[row] != Task::QUEUED) {
    } else if (OB_FAIL(rows.push_back(row))) {
    } else if (OB_FAIL(keys.push_back(task.cache_keys_[row]))) {
    } else {
      ret = prompts.push_back(task.cache_prompts_[row]);
    }
    if (OB_SUCC(ret) && nullptr != proxy && task.states_[row] == Task::QUEUED) {
      const SemanticInput &input = *task.inputs_.at(position);
      if (task.join_proxy_) {
        if (OB_FAIL(documents.push_back(task.proxy_lefts_[row]))) {
        } else {
          ret = right_documents.push_back(task.proxy_rights_[row]);
        }
      } else if (input.has_files_ || input.parts_.count() != 2 ||
          input.parts_.at(1).kind_ != SemanticInputPart::TEXT) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("embedding proxy lost its ordered text input binding", K(ret));
      } else if (input.parts_.at(1).value_.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "embedding proxy document must not be empty");
        LOG_WARN("empty embedding proxy document", K(ret));
      } else {
        ret = documents.push_back(input.parts_.at(1).value_);
      }
    }
  }
  if (OB_FAIL(ret) || rows.empty()) {
  } else if (nullptr == function_cache_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(function_cache_->submit(task.expression_->type_, task.model_, task.config_,
      keys, prompts, task.max_parallel_, task.cache_binding_id_, handles, progress,
      proxy, nullptr != proxy ? &documents : nullptr,
      task.join_proxy_ ? &right_documents : nullptr, task.task_group_, task.task_group_limit_))) {
  } else {
    for (int64_t index = 0; index < rows.count(); ++index) {
      if (handles.at(index)) {
        const int64_t row = rows.at(index);
        task.cache_handles_[row] = handles.at(index);
        task.states_[row] = Task::CACHE_WAIT;
        ++task.in_flight_rows_;
        progress = true;
      }
    }
  }
  return ret;
}

int SemanticOp::submit_tasks(Slot &slot, bool &progress)
{
  int ret = OB_SUCCESS;
  for (int64_t index = 0; OB_SUCC(ret) && index < slot.task_count_; ++index) {
    Task &task = *slot.tasks_[index];
    if (task.rows_.empty()) {
      continue;
    } else if (task.memoized_) {
      ret = submit_cached_tasks(slot, task, progress);
      continue;
    } else if (OB_FAIL(advance_media(task, progress))) {
      break;
    } else if (task.in_flight_rows_ >= task.max_parallel_) {
      continue;
    }
    const int64_t config_bytes = nullptr == task.config_ ? 0 : task.config_->get_serialize_size();
    const int64_t overhead = sizeof(Dispatch) + 2 * config_bytes;
    int64_t bytes = overhead;
    int64_t wire_bytes = 0;
    int64_t minimum = INT64_MAX;
    ObArray<int64_t> positions;
    const int64_t available_rows = task.max_parallel_ - task.in_flight_rows_;
    for (int64_t position = 0; OB_SUCC(ret) && position < task.rows_.count() &&
         positions.count() < available_rows; ++position) {
      if (!input_ready(task, position)) {
        continue;
      }
      const SemanticInput &input = *task.inputs_.at(position);
      const SemanticRequestMemory extra = request_memory(input);
      const int64_t wire = extra.wire_ + 6 * config_bytes;
      minimum = std::min(minimum, extra.body_ + wire + overhead);
      if (extra.body_ + wire + overhead > execution_->budget().memory_limit() ||
          extra.body_ + wire > ObAIFuncClient::MAX_BATCH_BYTES) {
        ret = OB_SIZE_OVERFLOW;
      } else if (extra.body_ + wire <= execution_->budget().memory_limit() -
                                      execution_->budget().memory_used() - bytes - wire_bytes &&
                 extra.body_ + wire <= ObAIFuncClient::MAX_BATCH_BYTES - bytes - wire_bytes) {
        bytes += extra.body_;
        wire_bytes += wire;
        ret = positions.push_back(position);
      }
    }
    if (OB_SUCC(ret) && positions.empty() && minimum != INT64_MAX &&
        (semantic_spec().execution_scope_ != SemanticExecutionScope::LOCAL_PX_WORKER ||
         execution_->budget().active_tasks() == 0) &&
        !has_pending_requests() &&
        (!task.media_ || !task.media_->has_pending())) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("prepared semantic inputs leave no room for request construction", K(ret), K(minimum));
    }
    if (OB_SUCC(ret) && !positions.empty()) {
      const bool owned_credit = task.credit_;
      const int admission = owned_credit ? OB_SUCCESS :
          execution_->budget().acquire_task(task.task_group_, task.task_group_limit_);
      if (admission == OB_EAGAIN) {
      } else if (admission != OB_SUCCESS) {
        ret = admission;
      } else {
        task.credit_ = true;
        Dispatch *dispatch = nullptr;
        const int reserved = reserve(slot, bytes);
        if (reserved == OB_EAGAIN) {
        } else if (reserved != OB_SUCCESS) {
          ret = reserved;
        } else {
          void *memory = ob_malloc(sizeof(Dispatch), ObMemAttr("SemanticBody"));
          if (OB_ISNULL(memory)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            execution_->budget().release(bytes);
            slot.retained_bytes_ -= bytes;
          } else {
            dispatch = new (memory) Dispatch();
            dispatch->retained_bytes_ = bytes;
            dispatch->batch_.set_shared_buffer_usage(*execution_->budget().memory_counter(),
                                                      execution_->budget().memory_limit());
            dispatch->batch_.set_nonblocking_admission();
            if (semantic_spec().row_async_enabled_) {
              dispatch->batch_.set_incremental_results();
            }
            dispatch->batch_.set_max_parallel(task.max_parallel_);
            for (int64_t position : positions) {
              if (OB_FAIL(build_request_parts(task, *dispatch, position))) {
                break;
              }
            }
          }
          if (OB_SUCC(ret)) {
            ObJsonObject *config = nullptr;
            if (nullptr != task.config_ &&
                OB_ISNULL(config = static_cast<ObJsonObject *>(
                    task.config_->clone(&dispatch->allocator_, true)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            }
            ObAIFuncModel model(dispatch->allocator_, *task.info_, task.endpoint_);
            if (OB_SUCC(ret)) {
              ret = model.start_completion_batch(dispatch->prompts_, config, dispatch->batch_,
                                                  &dispatch->media_parts_);
            }
          }
          if (OB_SUCC(ret)) {
            dispatch->next_ = task.dispatches_;
            task.dispatches_ = dispatch;
            task.in_flight_rows_ += dispatch->positions_.count();
            for (int64_t position : dispatch->positions_) {
              task.states_[task.rows_.at(position)] = Task::IN_FLIGHT;
            }
            progress = true;
          } else if (nullptr != dispatch) {
            reset_dispatch(slot, *dispatch);
          }
        }
        if (!owned_credit && nullptr == task.dispatches_) {
          execution_->budget().release_task(task.task_group_);
          task.credit_ = false;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to submit semantic requests", K(ret));
  }
  return ret;
}

int SemanticOp::poll_tasks(Slot &slot, bool &progress)
{
  int ret = OB_SUCCESS;
  for (int64_t index = 0; OB_SUCC(ret) && index < slot.task_count_; ++index) {
    Task &task = *slot.tasks_[index];
    if (task.memoized_) {
      bool complete = !task.rows_.empty();
      for (int64_t row : task.rows_) {
        if (OB_FAIL(ret)) {
        } else if (task.states_[row] == Task::CACHE_WAIT) {
          const auto &entry = task.cache_handles_[row];
          if (!entry) {
            ret = OB_ERR_UNEXPECTED;
          } else if (entry->state() == SemanticFunctionCache::Entry::FAILED) {
            ret = entry->error();
            LOG_WARN("memoized semantic call failed", K(ret), K(row));
          } else if (entry->state() == SemanticFunctionCache::Entry::COMPLETE) {
            task.results_[row] = entry->result();
            task.states_[row] = Task::COMPLETE;
            --task.in_flight_rows_;
            mark_row_ready(slot, row);
            progress = true;
          }
        }
        complete = complete && task.states_[row] == Task::COMPLETE;
      }
      if (OB_SUCC(ret) && complete) {
        reset_task(slot, task);
      }
      continue;
    }
    Dispatch **link = &task.dispatches_;
    while (OB_SUCC(ret) && nullptr != *link) {
      Dispatch &dispatch = **link;
      bool finished = false;
      ObArray<ObString> results;
      ObArray<int64_t> positions;
      ObArenaAllocator ready_allocator("SemanticReady");
      if (dispatch.batch_.incremental_results()) {
        ret = dispatch.batch_.get_ready_results(ready_allocator, positions, results, finished);
      } else if (OB_FAIL(dispatch.batch_.poll(finished))) {
      } else if (finished && OB_FAIL(dispatch.batch_.get_results(results))) {
      } else if (finished) {
        for (int64_t position = 0; OB_SUCC(ret) && position < results.count(); ++position) {
          ret = positions.push_back(position);
        }
      }
      if (OB_SUCC(ret) && positions.count() != results.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("semantic ready positions differ from results", K(ret));
      }
      for (int64_t index = 0; OB_SUCC(ret) && index < results.count(); ++index) {
        const int64_t position = positions.at(index);
        if (position < 0 || position >= dispatch.positions_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("semantic ready result has invalid request position", K(ret), K(position));
        } else {
          const int64_t source_row = task.rows_.at(dispatch.positions_.at(position));
          if (task.states_[source_row] != Task::IN_FLIGHT) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("semantic result row is not in flight", K(ret), K(source_row));
          } else if (OB_FAIL(reserve(slot, results.at(index).length()))) {
          } else if (OB_FAIL(ob_write_string(slot.allocator_, results.at(index),
                                            task.results_[source_row]))) {
          } else {
            task.states_[source_row] = Task::COMPLETE;
            --task.in_flight_rows_;
            ++dispatch.completed_count_;
            mark_row_ready(slot, source_row);
            progress = true;
          }
        }
      }
      if (OB_SUCC(ret) && finished) {
        if (dispatch.completed_count_ != dispatch.positions_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("semantic result count differs from submitted rows", K(ret));
        }
        if (OB_SUCC(ret)) {
          dispatch.batch_.cancel();
          for (int64_t position : dispatch.positions_) {
            for (auto &part : task.inputs_.at(position)->parts_) {
              part.media_.reset();
            }
          }
          *link = dispatch.next_;
          reset_dispatch(slot, dispatch);
          progress = true;
        }
      } else {
        link = &dispatch.next_;
      }
    }
    if (OB_SUCC(ret) && task.credit_ && nullptr == task.dispatches_) {
      bool complete = true;
      for (int64_t row : task.rows_) {
        complete = complete && task.states_[row] == Task::COMPLETE;
      }
      if (complete) {
        reset_task(slot, task);
      }
    }
  }
  return ret;
}

int SemanticOp::output(Slot &slot, int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  brs_.size_ = std::min(max_row_cnt, slot.kept_count_ - slot.offset_);
  clear_owned_flags(brs_.size_);
  brs_.skip_->reset(brs_.size_);
  brs_.all_rows_active_ = true;
  const auto &columns = child_->get_spec().output_;
  const auto &expressions = semantic_spec().expressions_;
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  guard.set_batch_size(brs_.size_);
  for (int64_t row = 0; OB_SUCC(ret) && row < brs_.size_; ++row) {
    guard.set_batch_idx(row);
    const int64_t source_row = slot.kept_rows_[slot.offset_ + row];
    for (int64_t column = 0; column < columns.count(); ++column) {
      ObExpr &expr = *columns.at(column);
      if (contains_expression(semantic_spec().local_exprs_, &expr)) {
        continue;
      }
      *expr.locate_expr_datumvector(eval_ctx_).at(row) = slot.inputs_[source_row * columns.count() + column];
      const bool missing = contains_expression(semantic_spec().shared_exprs_, &expr) &&
          slot.inputs_[source_row * columns.count() + column].is_null();
      expr.get_eval_info(eval_ctx_).evaluated_ = expr.is_batch_result() || !missing;
      if (contains_expression(semantic_spec().shared_exprs_, &expr)) {
        expr.get_eval_info(eval_ctx_).projected_ = false;
      }
      if (expr.is_batch_result() && missing) {
        expr.get_evaluated_flags(eval_ctx_).unset(row);
      } else if (expr.is_batch_result()) {
        expr.get_evaluated_flags(eval_ctx_).set(row);
      }
    }
    for (int64_t index = 0; index < expressions.count(); ++index) {
      const int64_t offset = source_row * expressions.count() + index;
      if (slot.evaluated_[offset]) {
        ObExpr &expr = *expressions.at(index);
        *expr.locate_expr_datumvector(eval_ctx_).at(row) = slot.values_[offset];
        expr.get_eval_info(eval_ctx_).evaluated_ = true;
        if (expr.is_batch_result()) {
          expr.get_evaluated_flags(eval_ctx_).set(row);
        }
      }
    }
    for (int64_t index = 0; OB_SUCC(ret) && index < slot.task_count_; ++index) {
      const Task &task = *slot.tasks_[index];
      if (task.states_[source_row] == Task::COMPLETE) {
        const ObExpr &expr = *task.expression_;
        if (OB_FAIL(SemanticExprUtils::set_result(expr, eval_ctx_,
             *expr.locate_expr_datumvector(eval_ctx_).at(row), task.results_[source_row]))) {
        } else {
          expr.get_eval_info(eval_ctx_).evaluated_ = true;
          if (expr.is_batch_result()) {
            expr.get_evaluated_flags(eval_ctx_).set(row);
          }
        }
      } else if (contains_expression(semantic_spec().carried_exprs_, task.expression_)) {
        const ObExpr &expr = *task.expression_;
        const bool evaluated = expr.is_batch_result()
            ? expr.get_evaluated_flags(eval_ctx_).at(row) : expr.get_eval_info(eval_ctx_).evaluated_;
        if (!evaluated) {
          // Semantic results cannot be SQL NULL; this internal tag carries an undemanded dependency.
          expr.locate_expr_datumvector(eval_ctx_).at(row)->set_null();
          expr.get_eval_info(eval_ctx_).evaluated_ = true;
          expr.get_eval_info(eval_ctx_).notnull_ = false;
          if (expr.is_batch_result()) {
            expr.get_evaluated_flags(eval_ctx_).set(row);
          }
        }
      }
    }
  }
  slot.offset_ += brs_.size_;
  return ret;
}

int SemanticOp::poll_ready_tasks(void *data, bool &progress)
{
  int ret = OB_SUCCESS;
  auto &operation = *static_cast<SemanticOp *>(data);
  for (int64_t index = 0; OB_SUCC(ret) && index < operation.count_; ++index) {
    Slot &slot = *operation.slots_[(operation.head_ + index) % operation.slot_count_];
    if (OB_FAIL(operation.poll_tasks(slot, progress))) {
    } else if (OB_FAIL(operation.submit_tasks(slot, progress))) {
    }
  }
  return ret;
}

bool SemanticOp::can_resume(const void *data)
{
  const auto &operation = *static_cast<const SemanticOp *>(data);
  bool ready = false;
  if (operation.waiting_memory_ > 0) {
    const auto &budget = operation.execution_->budget();
    ready = operation.waiting_memory_ <= budget.memory_limit() - budget.memory_used() &&
        operation.waiting_memory_ <= ObAIFuncClient::pipeline_buffer_limit() -
                                     ObAIFuncClient::pipeline_buffer_usage();
    ready = ready || (operation.semantic_spec().execution_scope_ == SemanticExecutionScope::LOCAL_PX_WORKER &&
        budget.active_tasks() == 0 &&
        operation.waiting_memory_ > budget.memory_limit() - budget.memory_used());
  }
  for (int64_t index = 0; !ready && index < operation.count_; ++index) {
    const Slot &slot = *operation.slots_[(operation.head_ + index) % operation.slot_count_];
    ready = !slot.ready_ && slot.needs_prepare_;
    for (int64_t task_index = 0; !ready && task_index < slot.task_count_; ++task_index) {
      const Task &task = *slot.tasks_[task_index];
      for (const Dispatch *dispatch = task.dispatches_; !ready && nullptr != dispatch;
           dispatch = dispatch->next_) {
        ready = dispatch->batch_.is_ready();
      }
      if (!ready && !task.rows_.empty()) {
        for (int64_t position = 0; !ready && position < task.rows_.count(); ++position) {
          const int64_t row = task.rows_.at(position);
          if (task.memoized_) {
            if (task.states_[row] == Task::CACHE_WAIT) {
              ready = task.cache_handles_[row] &&
                  task.cache_handles_[row]->state() != SemanticFunctionCache::Entry::PENDING;
            } else if (task.states_[row] == Task::QUEUED) {
              const int64_t config_bytes = nullptr == task.config_ ? 0 : task.config_->get_serialize_size();
              int64_t proxy_bytes = 0;
              if (task.join_proxy_) {
                proxy_bytes = 6 * (task.proxy_lefts_[row].length() +
                                  task.proxy_rights_[row].length()) + 8192;
              } else if (nullptr != task.proxy_config_) {
                const auto &parts = task.inputs_.at(position)->parts_;
                if (parts.count() != 2 || parts.at(1).value_.empty()) {
                  ready = true;
                  continue;
                }
                proxy_bytes = 6 * (parts.at(1).value_.length() +
                    task.proxy_config_->query_.length()) + 4096;
              }
              ready = operation.function_cache_ && operation.function_cache_->can_submit(
                  task.cache_binding_id_, task.cache_keys_[row], task.cache_prompts_[row],
                  config_bytes, proxy_bytes, task.task_group_, task.task_group_limit_);
            }
            continue;
          }
          if (task.states_[row] != Task::QUEUED) {
            continue;
          }
          bool media_ready = true;
          for (const auto &part : task.inputs_.at(position)->parts_) {
            if (part.kind_ == SemanticInputPart::FILE) {
              if (!part.media_) {
                ready = ready || !task.media_ || task.media_->can_demand();
                media_ready = false;
              } else if (!part.media_->ready()) {
                media_ready = false;
              } else if (part.media_->result() != OB_SUCCESS) {
                ready = true;
              }
            }
          }
          if (!ready && media_ready) {
            const auto &budget = operation.execution_->budget();
            const SemanticRequestMemory bytes = request_memory(*task.inputs_.at(position));
            const int64_t config_bytes = nullptr == task.config_ ? 0 : task.config_->get_serialize_size();
            const int64_t required = sizeof(Dispatch) + bytes.body_ + bytes.wire_ + 8 * config_bytes;
            const bool local_px = operation.semantic_spec().execution_scope_ ==
                SemanticExecutionScope::LOCAL_PX_WORKER;
            const bool process_memory_available = !local_px ||
                required <= ObAIFuncClient::pipeline_buffer_limit() -
                    ObAIFuncClient::pipeline_buffer_usage();
            ready = (task.in_flight_rows_ < task.max_parallel_ &&
                     (task.credit_ || budget.task_available(task.task_group_, task.task_group_limit_)) &&
                     required <= budget.memory_limit() - budget.memory_used() &&
                     process_memory_available) ||
                    (!operation.has_pending_requests() &&
                     (!task.media_ || !task.media_->has_pending()) &&
                     (!local_px || (budget.active_tasks() == 0 &&
                         required > budget.memory_limit() - budget.memory_used())));
          }
        }
      }
    }
  }
  return ready;
}

int SemanticOp::wait_for_work(bool &suspended)
{
  int ret = OB_SUCCESS;
  if (SemanticSuspendScope::allowed() &&
      lib::RequestAwait::suspend(&ctx_, execution_.get(), &SemanticExecutionState::can_resume)) {
    suspended = true;
    ret = OB_EAGAIN;
  } else if (SemanticSuspendScope::allowed() && nullptr != lib::RequestAwait::current() &&
             lib::RequestAwait::current()->owns(&ctx_) &&
             lib::RequestAwait::current()->is_pending()) {
    suspended = true;
    ret = OB_EAGAIN;
  } else {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return ret;
}

int SemanticOp::inner_get_next_batch(int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  bool suspended = false;
  if (OB_FAIL(child_frame_.restore())) {
  } else if (nullptr != lib::RequestAwait::current() &&
             OB_FAIL(lib::RequestAwait::current()->cancel_ret())) {
  }
  if (OB_SUCC(ret)) {
    // The parent has finished borrowing the previous output before pulling again.
    last_output_slot_ = nullptr;
  }
  if (OB_SUCC(ret) && !semantic_spec().unordered_output() &&
      count_ > 0 && slot_drained(*slots_[head_])) {
    reset_slot(*slots_[head_]);
    head_ = (head_ + 1) % slot_count_;
    --count_;
  }
  while (OB_SUCC(ret)) {
    bool progress = false;
    if (OB_FAIL(ctx_.check_status())) {
    } else if (OB_FAIL(execution_->budget().failure())) {
    } else if (OB_FAIL(execution_->poll_ready_tasks(progress))) {
    }
    for (int64_t index = 0; OB_SUCC(ret) && index < count_; ++index) {
      Slot &slot = *slots_[(head_ + index) % slot_count_];
      if (!slot.ready_ && slot.needs_prepare_ && OB_FAIL(prepare(slot, progress))) {
      } else if (OB_FAIL(submit_tasks(slot, progress))) {
      }
    }
    if (OB_SUCC(ret) && semantic_spec().unordered_output()) {
      reclaim_unordered_slots(progress);
    }
    if (OB_FAIL(ret)) {
    } else if (semantic_spec().unordered_output() && nullptr != ready_head_) {
      ret = output_ready_slot(max_row_cnt);
      if (nullptr != lib::RequestAwait::current()) {
        lib::RequestAwait::current()->reset_pending();
      }
      break;
    } else if (!semantic_spec().unordered_output() &&
               count_ > 0 && slot_drained(*slots_[head_]) &&
               semantic_spec().row_async_enabled_) {
      reset_slot(*slots_[head_]);
      head_ = (head_ + 1) % slot_count_;
      --count_;
    } else if (!semantic_spec().unordered_output() && count_ > 0 && (slots_[head_]->ready_ ||
               (semantic_spec().row_async_enabled_ &&
                slots_[head_]->offset_ < slots_[head_]->kept_count_))) {
      ret = output(*slots_[head_], max_row_cnt);
      if (nullptr != lib::RequestAwait::current()) {
        lib::RequestAwait::current()->reset_pending();
      }
      break;
    } else if (!input_end_ && count_ < slot_count_ && waiting_memory_ == 0) {
      Slot &slot = *slots_[(head_ + count_) % slot_count_];
      const int captured = capture(slot);
      if (captured == OB_EAGAIN) {
        ret = wait_for_work(suspended);
      } else if (captured != OB_SUCCESS) {
        ret = captured;
      } else {
        progress = true;
      }
    } else if (input_end_ && count_ == 0) {
      brs_.end_ = true;
      break;
    } else if (!progress) {
      ret = wait_for_work(suspended);
    }
    if (OB_SUCC(ret) && waiting_memory_ > 0 && can_resume(this)) {
      waiting_memory_ = 0;
    }
  }
  if (OB_FAIL(ret) && !suspended) {
    if (nullptr != lib::RequestAwait::current() &&
        lib::RequestAwait::current()->owns(&ctx_)) {
      lib::RequestAwait::current()->reset_pending();
    }
    if (execution_) {
      execution_->budget().fail(ret);
    }
    reset_pipeline();
    LOG_WARN("semantic operator execution failed", K(ret), "operator", spec_.type_);
  }
  return ret;
}
}
}
