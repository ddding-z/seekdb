#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_semantic_op.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
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
}

OB_SERIALIZE_MEMBER((SemanticSpec, ObOpSpec), expressions_, semantic_exprs_, owned_exprs_,
                    local_exprs_, shared_exprs_, carried_exprs_, query_task_count_, filter_expr_count_);
OB_SERIALIZE_MEMBER((SemanticMapSpec, SemanticSpec));
OB_SERIALIZE_MEMBER((SemanticFilterSpec, SemanticSpec));

struct SemanticOp::Task
{
  enum RowState : uint8_t { UNREQUESTED, QUEUED, IN_FLIGHT, COMPLETE };
  Task(const ObExpr *expr)
      : expression_(expr), request_arena_("SemanticReq"), request_allocator_(request_arena_),
        batch_(request_allocator_), states_(nullptr), results_(nullptr), config_(nullptr),
        input_bytes_(0), active_(false), credit_(false) {}
  const ObExpr *expression_;
  ObArenaAllocator request_arena_;
  MultimodeAlloctor request_allocator_;
  AIFuncBatch batch_;
  uint8_t *states_;
  ObString *results_;
  ObArray<int64_t> rows_;
  ObArray<ObString> prompts_;
  ObString model_;
  ObJsonObject *config_;
  int64_t input_bytes_;
  bool active_;
  bool credit_;
};

struct SemanticOp::Slot
{
  Slot() : allocator_("SemanticRows"), inputs_(nullptr), values_(nullptr), evaluated_(nullptr),
           kept_rows_(nullptr), tasks_(nullptr), task_count_(0), size_(0), kept_count_(0),
           offset_(0), retained_bytes_(0), ready_(false), needs_prepare_(false) {}
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
};

SemanticOp::SemanticOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(ctx, spec, input), slots_(nullptr), slot_count_(0), head_(0), count_(0),
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
  } else if (OB_FAIL(validate_shared_expressions())) {
  } else if (!spec_.is_vectorized()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI_MAP and AI_FILTER require a vectorized semantic operator");
  } else if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(
                         share::SYS_VAR_AI_PIPELINE_SLOTS, slot_count_))) {
  } else if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(
                         share::SYS_VAR_AI_PIPELINE_MEMORY_LIMIT, memory_limit))) {
  } else if (slot_count_ < 1 || slot_count_ > 1024 || memory_limit <= 0 ||
             spec.query_task_count_ > INT64_MAX / slot_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid semantic query limits", K(ret), K(slot_count_), K(memory_limit));
  } else if (OB_FAIL(SemanticExecutionState::acquire(ctx_, memory_limit,
                         slot_count_ * spec.query_task_count_, execution_))) {
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

void SemanticOp::reset_task(Slot &slot, Task &task)
{
  task.batch_.cancel();
  if (task.credit_) {
    execution_->budget().release_task();
    task.credit_ = false;
  }
  if (task.input_bytes_ > 0) {
    execution_->budget().release(task.input_bytes_);
    slot.retained_bytes_ -= task.input_bytes_;
  }
  task.request_allocator_.reset();
  task.rows_.reset();
  task.prompts_.reset();
  task.model_.reset();
  task.config_ = nullptr;
  task.input_bytes_ = 0;
  task.active_ = false;
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
}

void SemanticOp::reset_pipeline()
{
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
  execution_.reset();
  return ret;
}

int SemanticOp::inner_rescan()
{
  int ret = child_frame_.restore();
  child_frame_.reset();
  reset_pipeline();
  if (OB_SUCC(ret)) {
    ret = ObOperator::inner_rescan();
  }
  return ret;
}

void SemanticOp::destroy()
{
  reset_pipeline();
  if (registered_) {
    execution_->unregister_operator(this);
    registered_ = false;
  }
  execution_.reset();
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

int SemanticOp::capture(Slot &slot)
{
  int ret = OB_SUCCESS;
  const auto &columns = child_->get_spec().output_;
  const int64_t output_count = semantic_spec().expressions_.count();
  const int64_t task_count = semantic_spec().semantic_exprs_.count();
  if (OB_FAIL(child_frame_.restore())) {
  } else if (nullptr == pending_input_ &&
      OB_FAIL(child_->get_next_batch(spec_.max_batch_size_, pending_input_))) {
  }
  if (OB_SUCC(ret)) {
    const ObBatchRows &rows = *pending_input_;
    // Prefix sort and joins can retain a live input tail beyond the returned rows.
    const int64_t size = rows.size_ - rows.skip_->accumulate_bit_cnt(rows.size_);
    int64_t bytes = size * (columns.count() * sizeof(ObDatum) +
        output_count * (sizeof(ObDatum) + sizeof(uint8_t)) + sizeof(int64_t) +
        task_count * (sizeof(uint8_t) + sizeof(ObString))) +
        task_count * (sizeof(Task) + sizeof(Task *));
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
        auto &allocator = slot.allocator_;
        if (columns.count() > 0 && OB_ISNULL(slot.inputs_ = static_cast<ObDatum *>(
            allocator.alloc(size * columns.count() * sizeof(ObDatum))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_ISNULL(slot.values_ = static_cast<ObDatum *>(
            allocator.alloc(size * output_count * sizeof(ObDatum)))) ||
            OB_ISNULL(slot.evaluated_ = static_cast<uint8_t *>(
            allocator.alloc(size * output_count))) ||
            OB_ISNULL(slot.kept_rows_ = static_cast<int64_t *>(allocator.alloc(size * sizeof(int64_t)))) ||
            OB_ISNULL(slot.tasks_ = static_cast<Task **>(allocator.alloc(task_count * sizeof(Task *))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMSET(slot.evaluated_, 0, size * output_count);
          MEMSET(slot.tasks_, 0, task_count * sizeof(Task *));
          slot.task_count_ = task_count;
          for (int64_t index = 0; index < size * output_count; ++index) {
            new (&slot.values_[index]) ObDatum();
          }
          for (int64_t index = 0; OB_SUCC(ret) && index < task_count; ++index) {
            auto *task = OB_NEWx(Task, &allocator, semantic_spec().semantic_exprs_.at(index));
            if (OB_ISNULL(task)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              slot.tasks_[index] = task;
              task->batch_.set_shared_buffer_usage(*execution_->budget().memory_counter(),
                                                    execution_->budget().memory_limit());
              task->batch_.set_nonblocking_admission();
              task->states_ = static_cast<uint8_t *>(allocator.alloc(size));
              task->results_ = static_cast<ObString *>(allocator.alloc(size * sizeof(ObString)));
              if (OB_ISNULL(task->states_) || OB_ISNULL(task->results_)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                MEMSET(task->states_, Task::UNREQUESTED, size);
                for (int64_t row = 0; row < size; ++row) {
                  new (&task->results_[row]) ObString();
                }
              }
            }
          }
          int64_t target = 0;
          for (int64_t row = 0; OB_SUCC(ret) && row < rows.size_; ++row) {
            if (!rows.skip_->at(row)) {
              for (int64_t column = 0; OB_SUCC(ret) && column < columns.count(); ++column) {
                if (OB_FAIL(slot.inputs_[target * columns.count() + column].deep_copy(
                    *columns.at(column)->locate_expr_datumvector(eval_ctx_).at(row), allocator))) {
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
    } else if (task->states_[row] != Task::UNREQUESTED || task->active_) {
      ret = OB_EAGAIN;
    } else {
      ObEvalCtx::TempAllocGuard input_guard(ctx);
      MultimodeAlloctor input_allocator(input_guard.get_allocator());
      ObString model;
      ObString prompt;
      ObJsonObject *config = nullptr;
      const bool first_request = task->rows_.empty();
      int64_t bytes = 0;
      if (OB_FAIL(SemanticExprUtils::prepare_input(expr, ctx, input_allocator,
                                                  model, prompt, config, first_request))) {
      } else if (prompt.length() > ObAIFuncClient::MAX_REQUEST_BYTES) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        const uint64_t config_bytes = nullptr == config ? 0 : config->get_serialize_size();
        bytes = prompt.length() + (first_request ? model.length() : 0);
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
        ObString owned_prompt;
        if (OB_FAIL(ob_write_string(task->request_allocator_, prompt, owned_prompt))) {
        } else if (first_request &&
                   OB_FAIL(ob_write_string(task->request_allocator_, model, task->model_))) {
        } else if (first_request && nullptr != config &&
                   OB_ISNULL(task->config_ = static_cast<ObJsonObject *>(
                       config->clone(&task->request_allocator_, true)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(task->rows_.push_back(row))) {
        } else if (OB_FAIL(task->prompts_.push_back(owned_prompt))) {
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
    for (int64_t row = 0; OB_SUCC(ret) && row < slot.size_; ++row) {
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
        ++finished;
        if (accepted) {
          slot.kept_rows_[kept++] = row;
        }
      }
    }
    evaluating_slot_ = nullptr;
    slot.needs_prepare_ = false;
    if (OB_SUCC(ret) && finished == slot.size_) {
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

int SemanticOp::submit_tasks(Slot &slot, bool &progress)
{
  int ret = OB_SUCCESS;
  for (int64_t index = 0; OB_SUCC(ret) && index < slot.task_count_; ++index) {
    Task &task = *slot.tasks_[index];
    if (!task.active_ && !task.rows_.empty()) {
      const int admission = execution_->budget().acquire_task();
      if (admission == OB_EAGAIN) {
      } else if (admission != OB_SUCCESS) {
        ret = admission;
      } else {
        task.credit_ = true;
        ObAIFuncExprInfo *info = nullptr;
        share::ObAiModelEndpointInfo endpoint;
        auto *resolver = share::server_service<query::ObIAiEndpointResolver>();
        if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(task.request_allocator_, task.model_, info))) {
        } else if (OB_ISNULL(resolver)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(resolver->resolve_by_model_name(task.model_, task.request_allocator_, endpoint))) {
        } else {
          ObAIFuncModel model(task.request_allocator_, *info, endpoint);
          ret = model.start_completion_batch(task.prompts_, task.config_, task.batch_);
        }
        if (OB_SUCC(ret)) {
          task.active_ = true;
          for (int64_t row : task.rows_) {
            task.states_[row] = Task::IN_FLIGHT;
          }
          progress = true;
        }
      }
    }
  }
  return ret;
}

int SemanticOp::poll_tasks(Slot &slot, bool &progress)
{
  int ret = OB_SUCCESS;
  for (int64_t index = 0; OB_SUCC(ret) && index < slot.task_count_; ++index) {
    Task &task = *slot.tasks_[index];
    if (task.active_) {
      bool finished = false;
      if (OB_FAIL(task.batch_.poll(finished))) {
      } else if (finished) {
        ObArray<ObString> results;
        if (OB_FAIL(task.batch_.get_results(results))) {
        } else if (results.count() != task.rows_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("semantic result count differs from submitted rows", K(ret));
        } else {
          for (int64_t row = 0; OB_SUCC(ret) && row < results.count(); ++row) {
            const int64_t source_row = task.rows_.at(row);
            if (OB_FAIL(reserve(slot, results.at(row).length()))) {
            } else if (OB_FAIL(ob_write_string(slot.allocator_, results.at(row),
                                              task.results_[source_row]))) {
            } else {
              task.states_[source_row] = Task::COMPLETE;
            }
          }
        }
        if (OB_SUCC(ret)) {
          reset_task(slot, task);
          slot.needs_prepare_ = true;
          progress = true;
        }
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
    ret = operation.poll_tasks(slot, progress);
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
  }
  for (int64_t index = 0; !ready && index < operation.count_; ++index) {
    const Slot &slot = *operation.slots_[(operation.head_ + index) % operation.slot_count_];
    for (int64_t task_index = 0; !ready && task_index < slot.task_count_; ++task_index) {
      const Task &task = *slot.tasks_[task_index];
      ready = task.active_ ? task.batch_.is_ready()
                          : !task.rows_.empty() && operation.execution_->budget().task_available();
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
  if (OB_SUCC(ret) && count_ > 0 && slots_[head_]->ready_ &&
      slots_[head_]->offset_ == slots_[head_]->kept_count_) {
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
    if (OB_FAIL(ret)) {
    } else if (count_ > 0 && slots_[head_]->ready_) {
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
