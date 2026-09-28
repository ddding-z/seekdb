#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_ai_func_op.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_embed.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_complete.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

OB_SERIALIZE_MEMBER((AIFuncSpec, ObOpSpec), ai_expr_);

struct AIFuncOp::Slot
{
  Slot() : data_allocator_("AIFuncRows"), request_arena_("AIFuncReq"),
           request_allocator_(request_arena_), batch_(request_allocator_), datums_(nullptr),
           size_(0), offset_(0), retained_bytes_(0), ready_(false) {}
  ObArenaAllocator data_allocator_;
  ObArenaAllocator request_arena_;
  MultimodeAlloctor request_allocator_;
  AIFuncBatch batch_;
  ObArray<ObString> results_;
  ObDatum *datums_;
  int64_t size_;
  int64_t offset_;
  int64_t retained_bytes_;
  bool ready_;
};

int AIFuncOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_) || OB_ISNULL(ai_spec().ai_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ai_spec().ai_expr_->type_ != T_FUN_SYS_AI_EMBED &&
             ai_spec().ai_expr_->type_ != T_FUN_SYS_AI_COMPLETE) {
    ret = OB_NOT_SUPPORTED;
  } else if (spec_.is_vectorized() &&
             OB_FAIL(child_frame_.init(child_->get_spec().output_, eval_ctx_, &ctx_.get_allocator()))) {
  }
  for (int64_t index = 0; OB_SUCC(ret) && spec_.is_vectorized() && index < 2; ++index) {
    if (OB_ISNULL(slots_[index] = OB_NEWx(Slot, &ctx_.get_allocator()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      slots_[index]->batch_.set_shared_buffer_usage(buffered_bytes_);
    }
  }
  return ret;
}

int AIFuncOp::reserve(Slot &slot, int64_t bytes)
{
  int ret = OB_SUCCESS;
  int64_t used = ATOMIC_LOAD(&buffered_bytes_);
  while (OB_SUCC(ret)) {
    if (bytes < 0 || bytes > ObAIFuncClient::MAX_BATCH_BYTES - used) {
      ret = OB_SIZE_OVERFLOW;
    } else if (ATOMIC_BCAS(&buffered_bytes_, used, used + bytes)) {
      slot.retained_bytes_ += bytes;
      break;
    } else {
      used = ATOMIC_LOAD(&buffered_bytes_);
    }
  }
  return ret;
}

void AIFuncOp::reset_slot(Slot &slot)
{
  slot.batch_.cancel();
  slot.request_allocator_.reset();
  slot.results_.reset();
  slot.data_allocator_.reset();
  ATOMIC_FAA(&buffered_bytes_, -slot.retained_bytes_);
  slot.datums_ = nullptr;
  slot.size_ = slot.offset_ = slot.retained_bytes_ = 0;
  slot.ready_ = false;
}

void AIFuncOp::reset_pipeline()
{
  for (int64_t index = 0; index < 2; ++index) {
    if (nullptr != slots_[index]) {
      reset_slot(*slots_[index]);
    }
  }
  head_ = count_ = 0;
  input_end_ = false;
}

int AIFuncOp::inner_close()
{
  const int ret = child_frame_.restore();
  child_frame_.reset();
  reset_pipeline();
  return ret;
}

int AIFuncOp::inner_rescan()
{
  int ret = child_frame_.restore();
  child_frame_.reset();
  reset_pipeline();
  if (OB_SUCC(ret)) {
    ret = ObOperator::inner_rescan();
  }
  return ret;
}

void AIFuncOp::destroy()
{
  reset_pipeline();
  for (int64_t index = 0; index < 2; ++index) {
    OB_DELETEx(Slot, &ctx_.get_allocator(), slots_[index]);
    slots_[index] = nullptr;
  }
  child_frame_.destroy();
  child_frame_.~ObBatchResultHolder();
  ObOperator::destroy();
}

int AIFuncOp::inner_get_next_row()
{
  clear_evaluated_flag();
  return child_->get_next_row();
}

int AIFuncOp::submit(Slot &slot)
{
  int ret = OB_SUCCESS;
  const ObBatchRows *rows = nullptr;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_batch(ai_spec().max_batch_size_, rows))) {
  } else {
    input_end_ = rows->end_;
    slot.size_ = rows->size_ - rows->skip_->accumulate_bit_cnt(rows->size_);
  }
  const auto &columns = child_->get_spec().output_;
  if (OB_FAIL(ret) || slot.size_ == 0) {
  } else if (columns.count() > ObAIFuncClient::MAX_BATCH_BYTES / sizeof(ObDatum) / slot.size_) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(reserve(slot, slot.size_ *
                             (columns.count() * sizeof(ObDatum) + sizeof(ObString))))) {
  } else if (OB_ISNULL(slot.datums_ = static_cast<ObDatum *>(slot.data_allocator_.alloc(
                                      slot.size_ * columns.count() * sizeof(ObDatum))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_size(rows->size_);
    const ObExpr &expression = *ai_spec().ai_expr_;
    const bool is_completion = expression.type_ == T_FUN_SYS_AI_COMPLETE;
    ObArray<ObString> contents;
    ObString model_id;
    ObJsonObject *config = nullptr;
    int64_t target_row = 0;
    int64_t input_bytes = 0;
    for (int64_t row = 0; OB_SUCC(ret) && row < rows->size_; ++row) {
      if (rows->skip_->at(row)) {
        continue;
      }
      guard.set_batch_idx(row);
      ObString content;
      if (OB_FAIL(ctx_.check_status())) {
      } else if (OB_FAIL(is_completion
             ? ObExprAIComplete::prepare_input(expression, eval_ctx_, slot.request_allocator_,
                       model_id, content, config)
             : ObExprAIEmbed::prepare_input(expression, eval_ctx_, slot.request_allocator_,
                    model_id, content, config))) {
      } else if (content.length() > ObAIFuncClient::MAX_REQUEST_BYTES ||
                 input_bytes > ObAIFuncClient::MAX_BATCH_BYTES - content.length()) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_FAIL(contents.push_back(content))) {
      } else {
        input_bytes += content.length();
      }
      for (int64_t column = 0; OB_SUCC(ret) && column < columns.count(); ++column) {
        const ObDatum &source = *columns.at(column)->locate_expr_datumvector(eval_ctx_).at(row);
        if (OB_FAIL(reserve(slot, source.get_deep_copy_size()))) {
        } else if (OB_FAIL(slot.datums_[target_row * columns.count() + column].deep_copy(
                             source, slot.data_allocator_))) {
        }
      }
      ++target_row;
    }
    if (OB_SUCC(ret)) {
      ObAIFuncExprInfo *info = nullptr;
      share::ObAiModelEndpointInfo endpoint;
      auto *resolver = share::server_service<query::ObIAiEndpointResolver>();
      if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(slot.request_allocator_, model_id, info))) {
      } else if (OB_ISNULL(resolver)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(resolver->resolve_by_model_name(model_id, slot.request_allocator_, endpoint))) {
      } else {
        ObAIFuncModel model(slot.request_allocator_, *info, endpoint);
        ret = is_completion ? model.start_completion_batch(contents, config, slot.batch_)
                            : model.start_dense_embedding_batch(contents, config, slot.batch_);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(child_frame_.save(rows->size_))) {
      } else {
        ++count_;
      }
    }
  }
  return ret;
}

int AIFuncOp::poll()
{
  int ret = OB_SUCCESS;
  for (int64_t index = 0; OB_SUCC(ret) && index < count_; ++index) {
    Slot &slot = *slots_[(head_ + index) % 2];
    bool finished = false;
    if (slot.ready_) {
    } else if (OB_FAIL(slot.batch_.poll(finished))) {
    } else if (finished) {
      ObArray<ObString> results;
      if (OB_FAIL(slot.batch_.get_results(results))) {
      } else if (results.count() != slot.size_) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        for (int64_t row = 0; OB_SUCC(ret) && row < results.count(); ++row) {
          ObString result;
          if (OB_FAIL(reserve(slot, results.at(row).length()))) {
          } else if (OB_FAIL(ob_write_string(slot.data_allocator_, results.at(row), result))) {
          } else if (OB_FAIL(slot.results_.push_back(result))) {
          }
        }
      }
      if (OB_SUCC(ret)) {
        slot.batch_.cancel();
        slot.request_allocator_.reset();
        slot.ready_ = true;
      }
    }
  }
  return ret;
}

int AIFuncOp::output(Slot &slot, int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  brs_.size_ = std::min(max_row_cnt, slot.size_ - slot.offset_);
  brs_.skip_->reset(brs_.size_);
  brs_.all_rows_active_ = true;
  const auto &columns = child_->get_spec().output_;
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  guard.set_batch_size(brs_.size_);
  for (int64_t row = 0; OB_SUCC(ret) && row < brs_.size_; ++row) {
    guard.set_batch_idx(row);
    for (int64_t column = 0; column < columns.count(); ++column) {
      ObExpr &expr = *columns.at(column);
      *expr.locate_expr_datumvector(eval_ctx_).at(row) =
          slot.datums_[(slot.offset_ + row) * columns.count() + column];
      expr.get_eval_info(eval_ctx_).evaluated_ = true;
      if (expr.is_batch_result()) {
        expr.get_evaluated_flags(eval_ctx_).set(row);
      }
    }
    ObExpr &ai_expr = *ai_spec().ai_expr_;
    if (OB_FAIL(ObAIFuncUtils::set_string_result(ai_expr, eval_ctx_,
                    *ai_expr.locate_expr_datumvector(eval_ctx_).at(row), slot.results_.at(slot.offset_ + row)))) {
    } else {
      ai_expr.get_eval_info(eval_ctx_).evaluated_ = true;
      ai_expr.get_evaluated_flags(eval_ctx_).set(row);
    }
  }
  slot.offset_ += brs_.size_;
  return ret;
}

int AIFuncOp::inner_get_next_batch(int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(child_frame_.restore())) {
  } else if (ctx_.get_my_session()->is_diagnosis_enabled()) {
    const ObBatchRows *rows = nullptr;
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_batch(max_row_cnt, rows))) {
    } else {
      brs_ = *rows;
    }
  } else {
    if (count_ > 0 && slots_[head_]->offset_ == slots_[head_]->size_) {
      reset_slot(*slots_[head_]);
      head_ = (head_ + 1) % 2;
      --count_;
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ctx_.check_status())) {
      } else if (OB_FAIL(poll())) {
      } else if (count_ > 0 && slots_[head_]->ready_) {
        ret = output(*slots_[head_], max_row_cnt);
        break;
      } else if (!input_end_ && count_ < 2) {
        ret = submit(*slots_[(head_ + count_) % 2]);
      } else if (count_ == 0) {
        brs_.end_ = true;
        break;
      } else {
        bool finished = false;
        ret = slots_[head_]->batch_.poll(finished, 20);
      }
    }
  }
  if (OB_FAIL(ret)) {
    reset_pipeline();
  }
  return ret;
}

}
}