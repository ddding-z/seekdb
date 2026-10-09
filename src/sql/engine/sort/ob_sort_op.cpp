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

#include "sql/engine/sort/ob_sort_op.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/aggregate/ob_hash_groupby_op.h"
#include "sql/engine/expr/ob_expr_topn_filter.h"
#include "sql/engine/basic/ob_semantic_runtime.h"
#include "lib/worker.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace sql
{

ObSortSpec::ObSortSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type),
  topn_expr_(nullptr),
  topk_limit_expr_(nullptr),
  topk_offset_expr_(nullptr),
  all_exprs_(alloc),
  sort_collations_(alloc),
  sort_cmp_funs_(alloc),
  minimum_row_count_(0),
  topk_precision_(0),
  prefix_pos_(0),
  is_local_merge_sort_(false),
  is_fetch_with_ties_(false),
  prescan_enabled_(false),
  enable_encode_sortkey_opt_(false),
  part_cnt_(0),
  pd_topn_filter_info_(alloc)
{}

OB_SERIALIZE_MEMBER((ObSortSpec, ObOpSpec),
                    topn_expr_,
                    topk_limit_expr_,
                    topk_offset_expr_,
                    all_exprs_,
                    sort_collations_,
                    sort_cmp_funs_,
                    minimum_row_count_,
                    topk_precision_,
                    prefix_pos_,
                    is_local_merge_sort_,
                    is_fetch_with_ties_,
                    prescan_enabled_,
                    enable_encode_sortkey_opt_,
                    part_cnt_,
                    compress_type_,
                    pd_topn_filter_info_);

ObSortOp::ObSortOp(ObExecContext &ctx_, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(ctx_, spec, input),
  sort_impl_(op_monitor_info_),
  prefix_sort_impl_(op_monitor_info_),
  read_func_(&ObSortOp::sort_impl_next),
  read_batch_func_(&ObSortOp::sort_impl_next_batch),
  sort_row_count_(0),
  is_first_(true),
  batch_sort_phase_(BatchSortPhase::INITIAL),
  batch_sort_error_(OB_SUCCESS),
  ret_row_count_(0),
  iter_end_(false),
  prefix_frame_()
{}

bool ObSortOp::supports_semantic_suspend() const
{
  // Capability checks must not consume the force-dump tracepoint.
  const common::EventItem &force_dump_event = common::EventTable::EN_SORT_IMPL_FORCE_DO_DUMP.item_;
  return is_vectorized() && MY_SPEC.prefix_pos_ == 0 && !MY_SPEC.prescan_enabled_
      && !MY_SPEC.is_local_merge_sort_ && MY_SPEC.part_cnt_ == 0
      && nullptr == MY_SPEC.topk_limit_expr_ && !MY_SPEC.is_fetch_with_ties_
      && !MY_SPEC.enable_pd_topn_filter()
      && ATOMIC_LOAD(&force_dump_event.error_code_) == 0
      && op_monitor_info_.otherstat_4_id_ != ObSqlMonitorStatIds::SORT_DUMP_DATA_TIME;
}

int ObSortOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  }
  return OB_SUCCESS;
}

int ObSortOp::inner_rescan()
{
  if (MY_SPEC.enable_pd_topn_filter() && !MY_SPEC.pd_topn_filter_info_.is_shuffle_) {
    // for local topn runtime filter, rescan topn filter expression context
    reset_pd_topn_filter_expr_ctx();
  }
  reset();
  iter_end_ = false;
  return ObOperator::inner_rescan();
}

void ObSortOp::reset_pd_topn_filter_expr_ctx()
{
  uint32_t expr_ctx_id = MY_SPEC.pd_topn_filter_info_.expr_ctx_id_;
  ObExprTopNFilterContext *topn_filter_ctx =
      static_cast<ObExprTopNFilterContext *>(ctx_.get_expr_op_ctx(expr_ctx_id));
  if (nullptr != topn_filter_ctx) {
    topn_filter_ctx->reset_for_rescan();
  }
}

void ObSortOp::reset()
{
  prefix_frame_.reset();
  sort_impl_.reset();
  prefix_sort_impl_.reset();
  read_func_ = &ObSortOp::sort_impl_next;
  read_batch_func_ = &ObSortOp::sort_impl_next_batch;
  sort_row_count_ = 0;
  ret_row_count_ = 0;
  is_first_ = true;
  batch_sort_phase_ = BatchSortPhase::INITIAL;
  batch_sort_error_ = OB_SUCCESS;
}

void ObSortOp::destroy()
{
  prefix_frame_.destroy();
  prefix_frame_.~ObBatchResultHolder();
  sort_impl_.unregister_profile_if_necessary();
  sort_impl_.~ObSortOpImpl();
  prefix_sort_impl_.unregister_profile_if_necessary();
  prefix_sort_impl_.~ObPrefixSortImpl();
  read_func_ = nullptr;
  read_batch_func_ = nullptr;
  sort_row_count_ = 0;
  is_first_ = true;
  batch_sort_phase_ = BatchSortPhase::INITIAL;
  batch_sort_error_ = OB_SUCCESS;
  ret_row_count_ = 0;
  ObOperator::destroy();
}

int ObSortOp::inner_close()
{
  sort_impl_.collect_memory_dump_info(op_monitor_info_);
  sort_impl_.unregister_profile();
  prefix_sort_impl_.unregister_profile();
  reset();
  return OB_SUCCESS;
}

int ObSortOp::get_int_value(const ObExpr *in_val, int64_t &out_val)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = NULL;
  if (NULL != in_val) {
    if (OB_FAIL(in_val->eval(eval_ctx_, datum))) {
    } else if (OB_ISNULL(datum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: datum is null", K(ret));
    } else if (datum->is_null()) {
      out_val = 0;
    } else {
      out_val = *datum->int_;
    }
  }
  return ret;
}

int ObSortOp::get_topn_count(int64_t &topn_cnt)
{
  int ret = OB_SUCCESS;
  topn_cnt = INT64_MAX;
  if ((OB_ISNULL(MY_SPEC.topn_expr_) && OB_ISNULL(MY_SPEC.topk_limit_expr_))) {
    // do nothing
  } else if (((NULL != MY_SPEC.topn_expr_) && (NULL != MY_SPEC.topk_limit_expr_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid topn_expr or topk_limit_expr", K(MY_SPEC.topn_expr_),
      K(MY_SPEC.topk_limit_expr_), K(ret));
  } else if (NULL != MY_SPEC.topn_expr_) {
    if (OB_FAIL(get_int_value(MY_SPEC.topn_expr_, topn_cnt))) {
    } else {
      topn_cnt = std::max(MY_SPEC.minimum_row_count_, topn_cnt);
    }
  } else if (NULL != MY_SPEC.topk_limit_expr_) {
    int64_t limit = -1;
    int64_t offset = 0;
    if ((OB_FAIL(get_int_value(MY_SPEC.topk_limit_expr_, limit))
        || OB_FAIL(get_int_value(MY_SPEC.topk_offset_expr_, offset)))) {
      LOG_WARN("Get limit/offset value failed", K(ret));
    } else if (OB_UNLIKELY(limit < 0 || offset < 0)) {
      ret = OB_ERR_ILLEGAL_VALUE;
      LOG_WARN("Invalid limit/offset value", K(limit), K(offset), K(ret));
    } else {
      // TODO & FIXME by longzhong.wlz : Wait for groupby implementation to handle this logic
      topn_cnt = std::max(MY_SPEC.minimum_row_count_, limit + offset);
      int64_t row_count = 0;
      ObPhyOperatorType op_type = child_->get_spec().type_;
      if (PHY_HASH_GROUP_BY != op_type) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid child_op_", K(op_type), K(ret));
      } else {
        row_count = static_cast<ObHashGroupByOp *>(child_)->get_hash_groupby_row_count();
      }
      if (OB_SUCC(ret)) {
        topn_cnt = std::max(topn_cnt,
                            static_cast<int64_t>(row_count * MY_SPEC.topk_precision_ / 100));
        if (topn_cnt >= row_count) {
          ctx_.get_physical_plan_ctx()->set_is_result_accurate(true);
        } else {
          ctx_.get_physical_plan_ctx()->set_is_result_accurate(false);
        }
      }
    }
  }
  return ret;
}

int ObSortOp::process_sort()
{
  int ret = OB_SUCCESS;
  if (read_func_ == &ObSortOp::prefix_sort_impl_next) {
    // prefix sort get child row in it's own wrap, do nothing here
  } else if (read_func_ == &ObSortOp::sort_impl_next) {
    bool need_dump = false;
    while (OB_SUCC(ret)) {
      clear_evaluated_flag();
      if (OB_FAIL(try_check_status())) {
      } else if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else {
        sort_row_count_++;
        OZ(sort_impl_.add_row(MY_SPEC.all_exprs_, need_dump));
        sort_impl_.collect_memory_dump_info(op_monitor_info_);
        if (need_dump && MY_SPEC.prescan_enabled_) {
          break;
        }
      }
    }
    if (OB_SUCC(ret) && need_dump && MY_SPEC.prescan_enabled_
        && OB_FAIL(scan_all_then_sort())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to scan all rows before inmem sort", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    OZ(sort_impl_.sort());
    sort_impl_.collect_memory_dump_info(op_monitor_info_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid read function pointer",
        K(ret), K(*reinterpret_cast<int64_t *>(&read_func_)));
  }
  return ret;
}

int ObSortOp::process_sort_batch(bool &suspended)
{
  int ret = OB_SUCCESS;
  suspended = false;
  if (read_batch_func_ == &ObSortOp::prefix_sort_impl_next_batch) {
    // prefix sort get child row in it's own wrap, do nothing here
  } else if (read_batch_func_ == &ObSortOp::sort_impl_next_batch) {
    bool need_dump = false;
    while (OB_SUCC(ret)) {
      clear_evaluated_flag();
      const ObBatchRows *input_brs = NULL;
      lib::RequestAwait *await = lib::RequestAwait::current();
      if (nullptr != await && await->owns(&ctx_) && OB_FAIL(await->cancel_ret())) {
        LOG_WARN("sort input collection cancelled", K(ret), K(sort_row_count_));
      } else if (OB_FAIL(try_check_status())) {
      } else {
        const bool can_suspend = supports_semantic_suspend();
        {
          SemanticSuspendScope fetch_scope(MY_SPEC.type_, false, can_suspend);
          ret = child_->get_next_batch(MY_SPEC.max_batch_size_, input_brs);
        }
        if (OB_FAIL(ret)) {
          await = lib::RequestAwait::current();
          if (nullptr != await && await->owns(&ctx_) && OB_SUCCESS != await->cancel_ret()) {
            ret = await->cancel_ret();
          } else if (ret == OB_EAGAIN && can_suspend && nullptr != await
              && await->owns(&ctx_) && await->is_pending()) {
            suspended = true;
            brs_.size_ = 0;
            brs_.end_ = false;
            return ret;
          }
          LOG_WARN("failed to collect sort input batch", K(ret), K(sort_row_count_));
        }
      }
      if (OB_SUCC(ret)) {
        // Only fetching the child may unwind; materializing a batch must finish.
        SemanticSuspendScope materialize_scope(MY_SPEC.type_);
        if (input_brs->size_ > 0) {
          sort_row_count_ += input_brs->size_
              - input_brs->skip_->accumulate_bit_cnt(input_brs->size_);
          OZ(sort_impl_.add_batch(MY_SPEC.all_exprs_, *input_brs->skip_,
                                  input_brs->size_, need_dump));
          sort_impl_.collect_memory_dump_info(op_monitor_info_);
        }
        if (input_brs->end_ || (need_dump && MY_SPEC.prescan_enabled_)) {
          break;
        }
      }
    }
    SemanticSuspendScope finish_scope(MY_SPEC.type_);
    if (OB_SUCC(ret) && need_dump && MY_SPEC.prescan_enabled_
        && OB_FAIL(scan_all_then_sort_batch())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to scan all rows before inmem sort", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      op_monitor_info_.otherstat_7_id_ = ObSqlMonitorStatIds::ROW_COUNT;
      op_monitor_info_.otherstat_7_value_ = sort_row_count_;
      OZ(sort_impl_.sort());
      sort_impl_.collect_memory_dump_info(op_monitor_info_);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid read function pointer",
        K(ret), K(*reinterpret_cast<int64_t *>(&read_func_)));
  }
  return ret;
}

// if we need to do std sort, the thread will be blocked and cannot
// drive the table scan below, which will block other sort ops.
// To relieve this, we scan all rows into a cache store first then
// continue the sort part.
int ObSortOp::scan_all_then_sort()
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObCompactStore, cache_store) {
    if (OB_FAIL(cache_store.init(2 * 1024 * 1024,
        ObCtxIds::DEFAULT_CTX_ID, "SORT_CACHE_CTX", true/*enable dump*/, 0, true,
        MY_SPEC.compress_type_, &MY_SPEC.all_exprs_))) {
    } else if (OB_FAIL(cache_store.alloc_dir_id())) {
    }
    while (OB_SUCC(ret)) {
      clear_evaluated_flag();
      if (OB_FAIL(try_check_status())) {
      } else if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else {
        sort_row_count_++;
        if (OB_FAIL(cache_store.add_row(MY_SPEC.all_exprs_, eval_ctx_))) {
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(cache_store.finish_add_row(false))) {
      } else {
        const ObChunkDatumStore::StoredRow *store_row = NULL;
        bool has_next = false;
        while (OB_SUCC(ret) && OB_SUCC(cache_store.has_next(has_next)) && has_next) {
          if (OB_FAIL(cache_store.get_next_row(store_row))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next row", K(ret));
            }
          } else if (OB_ISNULL(store_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get next row", K(ret));
          } else {
            OZ(sort_impl_.add_stored_row(*store_row));
          }
        }
      }
    }
  }
  return ret;
}

int ObSortOp::scan_all_then_sort_batch()
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObCompactStore, cache_store) {
    if (OB_FAIL(cache_store.init(16 * 1024,
        ObCtxIds::DEFAULT_CTX_ID, "SORT_CACHE_CTX", true/*enable dump*/, 0, true,
        MY_SPEC.compress_type_, &MY_SPEC.all_exprs_))) {
    } else if (OB_FAIL(cache_store.alloc_dir_id())) {
    }
    while (OB_SUCC(ret)) {
      clear_evaluated_flag();
      const ObBatchRows *input_brs = NULL;
      if (OB_FAIL(try_check_status())) {
      } else if (OB_FAIL(child_->get_next_batch(MY_SPEC.max_batch_size_, input_brs))) {
      } else {
        if (input_brs->size_ > 0) {
          sort_row_count_ += input_brs->size_
              - input_brs->skip_->accumulate_bit_cnt(input_brs->size_);
          int64_t stored_row_count = -1;
          if (OB_FAIL(cache_store.add_batch(MY_SPEC.all_exprs_, eval_ctx_,
              *input_brs->skip_, input_brs->size_, stored_row_count))) {
          }
        }
        if (input_brs->end_) {
          break;
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    op_monitor_info_.otherstat_7_id_ = ObSqlMonitorStatIds::ROW_COUNT;
    op_monitor_info_.otherstat_7_value_ = sort_row_count_; 
    if (OB_SUCC(ret)) {
      if (OB_FAIL(cache_store.finish_add_row(false))) {
      } else {
        const ObChunkDatumStore::StoredRow *store_row = NULL;
        bool has_next = false;
        while (OB_SUCC(ret) && OB_SUCC(cache_store.has_next(has_next)) && has_next) {
          if (OB_FAIL(cache_store.get_next_row(store_row))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next row");
            }
          } else if (OB_ISNULL(store_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get next row");
          } else {
            OZ(sort_impl_.add_stored_row(*store_row));
          }
        }
      }
    }
  }
  return ret;
}

int ObSortOp::init_prefix_sort(int64_t row_count,
                               bool is_batch,
                               int64_t topn_cnt)
{
  int ret = OB_SUCCESS;
  if (is_batch && OB_FAIL(prefix_frame_.init(MY_SPEC.all_exprs_, eval_ctx_, &ctx_.get_allocator()))) {
    LOG_WARN("failed to initialize prefix sort frame snapshot", K(ret));
  }
  OZ(prefix_sort_impl_.init(MY_SPEC.prefix_pos_, MY_SPEC.all_exprs_,
      &MY_SPEC.sort_collations_, &MY_SPEC.sort_cmp_funs_, &eval_ctx_, child_,
      this, ctx_, MY_SPEC.enable_encode_sortkey_opt_, sort_row_count_, topn_cnt,
      MY_SPEC.is_fetch_with_ties_));
  if (is_batch) {
    read_batch_func_ = &ObSortOp::prefix_sort_impl_next_batch;
  } else {
    read_func_ = &ObSortOp::prefix_sort_impl_next;
  }
  int aqs_head = MY_SPEC.enable_encode_sortkey_opt_ ? sizeof(oceanbase::sql::ObSortOpImpl::AQSItem) : 0;
  prefix_sort_impl_.set_input_rows(row_count);
  prefix_sort_impl_.set_input_width(MY_SPEC.width_ + aqs_head);
  prefix_sort_impl_.set_operator_type(MY_SPEC.type_);
  prefix_sort_impl_.set_operator_id(MY_SPEC.id_);
  prefix_sort_impl_.set_io_event_observer(&io_event_observer_);
  return ret;
}

int ObSortOp::prefix_sort_impl_next_batch(const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prefix_frame_.restore())) {
    LOG_WARN("failed to restore prefix sort live frame", K(ret));
  } else if (OB_FAIL(sort_component_next_batch(prefix_sort_impl_, max_cnt))) {
  } else if (brs_.end_) {
    prefix_frame_.reset();
  } else if (OB_FAIL(prefix_frame_.save(eval_ctx_.max_batch_size_))) {
    // The next prefix remains live beyond the rows returned to the parent.
    LOG_WARN("failed to preserve prefix sort live frame", K(ret), K(brs_.size_));
  }
  return ret;
}

int ObSortOp::init_sort(int64_t row_count,
                        bool is_batch,
                        int64_t topn_cnt)
{
  int ret = OB_SUCCESS;
  int64_t est_rows = MY_SPEC.rows_;
  if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
      &ctx_, MY_SPEC.px_est_size_factor_, est_rows, est_rows))) {
  }
  OZ(sort_impl_.init(&MY_SPEC.sort_collations_, &MY_SPEC.sort_cmp_funs_, &eval_ctx_,
                     &ctx_, MY_SPEC.enable_encode_sortkey_opt_, MY_SPEC.is_local_merge_sort_,
                     false /* need_rewind */, MY_SPEC.part_cnt_, topn_cnt,
                     MY_SPEC.is_fetch_with_ties_, ObChunkDatumStore::BLOCK_SIZE,
                     MY_SPEC.compress_type_, &MY_SPEC.all_exprs_, est_rows,
                     MY_SPEC.prescan_enabled_, &MY_SPEC.pd_topn_filter_info_));
  if (is_batch) {
    read_batch_func_ = &ObSortOp::sort_impl_next_batch;
  } else {
    read_func_ = &ObSortOp::sort_impl_next;
  }
  int aqs_head = MY_SPEC.enable_encode_sortkey_opt_ ? sizeof(oceanbase::sql::ObSortOpImpl::AQSItem) : 0;
  sort_impl_.set_input_rows(row_count);
  sort_impl_.set_input_width(MY_SPEC.width_ + aqs_head);
  sort_impl_.set_operator_type(MY_SPEC.type_);
  sort_impl_.set_operator_id(MY_SPEC.id_);
  sort_impl_.set_io_event_observer(&io_event_observer_);
  return ret;
}

int ObSortOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else if (is_first_) {
    // Here what we want is to account
    // Charge memory to the runtime that owns the execution context.
    is_first_ = false;
    int64_t topn_cnt = INT64_MAX;
    int64_t row_count = MY_SPEC.rows_;
    
    if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
        &ctx_, MY_SPEC.px_est_size_factor_, MY_SPEC.rows_, row_count))) {
    } else if (OB_FAIL(get_topn_count(topn_cnt))) {
    } else if (topn_cnt <= 0) { 
      iter_end_ = true; 
      ret = OB_ITER_END;
    } else if (MY_SPEC.prefix_pos_ > 0) {
      if (OB_FAIL(init_prefix_sort(row_count, false, topn_cnt))) {
      }
    } else {
      if (OB_FAIL(init_sort(row_count, false, topn_cnt))) {
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(process_sort())) { // process sort
        if (OB_ITER_END != ret) {
          LOG_WARN("process sort failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (OB_FAIL((this->*read_func_)())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed");
      } else {
        if (ctx_.get_my_session()->get_ddl_info().is_ddl() && ret_row_count_ != sort_row_count_) {
          ret = OB_CHECKSUM_ERROR;
          LOG_WARN("output row count not match", K(ret), K(sort_row_count_), K(ret_row_count_));
        }
        iter_end_ = true;
        reset();
      }
    } else {
      ++ret_row_count_;
      LOG_DEBUG("finish ObSortOp::inner_get_next_row", K(ObToStringExprRow(eval_ctx_, MY_SPEC.output_)), K(ret_row_count_), K(MY_SPEC.output_));
    }
  }
  return ret;
}

int ObSortOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  bool suspended = false;
  if (BatchSortPhase::FAILED == batch_sort_phase_) {
    ret = batch_sort_error_;
  } else if (OB_UNLIKELY(iter_end_)) {
    brs_.end_ = true;
    brs_.size_ = 0;
  } else if (BatchSortPhase::INITIAL == batch_sort_phase_) {
    SemanticSuspendScope init_scope(MY_SPEC.type_);
    int64_t topn_cnt = INT64_MAX;
    int64_t row_count = MY_SPEC.rows_;
    
    if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
        &ctx_, MY_SPEC.px_est_size_factor_, MY_SPEC.rows_, row_count))) {
    } else if (OB_FAIL(get_topn_count(topn_cnt))) {
    } else if (topn_cnt <= 0) { 
      brs_.end_ = true;
      brs_.size_ = 0;
      iter_end_ = true;
    } else if (MY_SPEC.prefix_pos_ > 0) {
      if (OB_FAIL(init_prefix_sort(row_count, true, topn_cnt))) {
      }
    } else {
      if (OB_FAIL(init_sort(row_count, true, topn_cnt))) {
      }
    }
    if (OB_SUCC(ret)) {
      batch_sort_phase_ = brs_.end_ ? BatchSortPhase::OUTPUT : BatchSortPhase::COLLECTING;
      if (brs_.end_) {
        is_first_ = false;
      }
    }
  }

  if (OB_SUCC(ret) && BatchSortPhase::COLLECTING == batch_sort_phase_) {
    if (OB_FAIL(process_sort_batch(suspended))) {
    } else {
      batch_sort_phase_ = BatchSortPhase::OUTPUT;
      is_first_ = false;
    }
  }

  if (OB_SUCC(ret) && !brs_.end_) {
    SemanticSuspendScope output_scope(MY_SPEC.type_);
    clear_evaluated_flag();
    if (OB_FAIL((this->*read_batch_func_)(std::min(max_row_cnt, MY_SPEC.max_batch_size_)))) {
    } else {
      ret_row_count_ += brs_.size_;
      if (brs_.end_) {
        if (ctx_.get_my_session()->get_ddl_info().is_ddl() && ret_row_count_ != sort_row_count_) {
          ret = OB_CHECKSUM_ERROR;
          LOG_WARN("output row count not match", K(ret), K(sort_row_count_), K(ret_row_count_));
        }
      }
    }
  }
  if (OB_FAIL(ret) && !suspended && BatchSortPhase::FAILED != batch_sort_phase_) {
    batch_sort_phase_ = BatchSortPhase::FAILED;
    batch_sort_error_ = ret;
    LOG_WARN("sort batch execution failed", K(ret), K(sort_row_count_), K(ret_row_count_));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
