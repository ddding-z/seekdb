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

#include <algorithm>
#include <array>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <limits>
#include <map>
#include <thread>
#include <vector>
#include "lib/worker.h"
#include "share/config/ob_server_config.h"
#include "sql/engine/aggregate/ob_hash_groupby_op.h"
#include "sql/engine/basic/ob_semantic_runtime.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/sort/ob_sort_op.h"
#include "sql/session/ob_sql_session_info.h"

// Link with --wrap=_ZN9oceanbase10data_plane18tmp_file_alloc_dirERl.
// In-memory collection allocates directory IDs, but never performs temporary I/O.
extern "C" int __wrap__ZN9oceanbase10data_plane18tmp_file_alloc_dirERl(int64_t &dir_id)
{
  static int64_t next_id = 0;
  dir_id = ++next_id;
  return oceanbase::common::OB_SUCCESS;
}

namespace oceanbase
{
namespace sql
{
namespace
{
using namespace common;

constexpr int64_t BATCH_SIZE = 8;

void require(bool condition, const char *message)
{
  if (!condition) {
    std::cerr << message << std::endl;
    std::abort();
  }
}

void require_success(int ret, const char *message)
{
  if (OB_SUCCESS != ret) {
    std::cerr << message << ": " << ret << std::endl;
    std::abort();
  }
}

struct Frame
{
  ObDatum datums_[BATCH_SIZE];
  ObEvalInfo eval_info_;
  uint64_t evaluated_flags_ = 0;
  int64_t values_[BATCH_SIZE] = {};
};

enum class StepKind
{
  ROWS,
  WAIT,
  ERROR,
  CANCEL
};

struct Step
{
  StepKind kind_;
  std::vector<int64_t> values_;
  int64_t skip_idx_ = -1;
  bool end_ = false;
  int error_ = OB_SUCCESS;
  std::vector<int64_t> payload_;
};

Step rows(std::initializer_list<int64_t> values, bool end = false, int64_t skip_idx = -1)
{
  return {StepKind::ROWS, values, skip_idx, end, OB_SUCCESS};
}

Step wait()
{
  return {StepKind::WAIT, {}, -1, false, OB_SUCCESS};
}

Step error(int ret)
{
  return {StepKind::ERROR, {}, -1, false, ret};
}

Step cancel(int ret)
{
  return {StepKind::CANCEL, {}, -1, false, ret};
}

Step pairs(std::initializer_list<int64_t> keys, std::initializer_list<int64_t> values, bool end = false)
{
  Step result = rows(keys, end);
  result.payload_ = values;
  return result;
}

class ScriptedInput final : public ObOperator
{
public:
  ScriptedInput(ObExecContext &ctx, const ObOpSpec &spec, ObExpr &key, std::vector<Step> steps,
                ObExpr *payload = nullptr)
    : ObOperator(ctx, spec, nullptr), key_(key), payload_(payload), steps_(std::move(steps))
  {}

  bool supports_semantic_suspend() const override { return true; }

  int inner_open() override
  {
    require(!SemanticSuspendScope::allowed(), "open must remain nonsuspending");
    return OB_SUCCESS;
  }

  int inner_rescan() override
  {
    step_ = 0;
    ready_ = false;
    return ObOperator::inner_rescan();
  }

  int inner_get_next_row() override { return OB_NOT_SUPPORTED; }

  int inner_get_next_batch(int64_t max_row_cnt) override
  {
    ++fetches_;
    while (step_ < steps_.size()) {
      const Step &step = steps_[step_];
      if (StepKind::WAIT == step.kind_) {
        if (!SemanticSuspendScope::allowed() || nullptr == lib::RequestAwait::current()) {
          ++blocking_waits_;
          ++step_;
          continue;
        } else if (!ready_) {
          poison_frame();
          lib::RequestAwait *await = lib::RequestAwait::current();
          require(lib::RequestAwait::suspend(&ctx_, this, &ScriptedInput::ready)
                  || (await->owns(&ctx_) && await->is_pending()),
                  "scripted semantic wait must be owned and pending");
          if (enable_dump_on_wait_) {
            require(GCONF.enable_sql_operator_dump.set_value("true"), "change dump setting during fetch");
            enable_dump_on_wait_ = false;
          }
          if (force_sort_dump_on_wait_) {
            TP_SET_EVENT(common::EventTable::EN_SORT_IMPL_FORCE_DO_DUMP, -4, 1, 0);
            force_sort_dump_on_wait_ = false;
          }
          return OB_EAGAIN;
        } else {
          ready_ = false;
          ++step_;
          lib::RequestAwait::current()->reset_pending();
          continue;
        }
      } else if (StepKind::ERROR == step.kind_) {
        ++step_;
        return step.error_;
      } else if (StepKind::CANCEL == step.kind_) {
        lib::RequestAwait *await = lib::RequestAwait::current();
        require(nullptr != await, "cancellation requires an await context");
        require(lib::RequestAwait::suspend(&ctx_, this, &ScriptedInput::ready), "arm cancelled wait");
        await->cancel(step.error_);
        ++step_;
        return OB_EAGAIN;
      } else {
        require(step.values_.size() <= static_cast<size_t>(max_row_cnt), "input batch is too large");
        brs_.size_ = step.values_.size();
        brs_.end_ = step.end_;
        brs_.skip_->reset(BATCH_SIZE);
        if (step.skip_idx_ >= 0) {
          brs_.skip_->set(step.skip_idx_);
        }
        ObDatum *datums = key_.locate_datums_for_update(eval_ctx_, brs_.size_);
        for (size_t i = 0; i < step.values_.size(); ++i) {
          datums[i].set_int(step.values_[i]);
        }
        key_.set_evaluated_projected(eval_ctx_);
        if (nullptr != payload_) {
          require(step.payload_.size() == step.values_.size(), "pair batch dimensions");
          ObDatum *payloads = payload_->locate_datums_for_update(eval_ctx_, brs_.size_);
          for (size_t i = 0; i < step.payload_.size(); ++i) {
            payloads[i].set_int(step.payload_[i]);
          }
          payload_->set_evaluated_projected(eval_ctx_);
        }
        lib::RequestAwait *await = lib::RequestAwait::current();
        if (nullptr != await && await->owns(&ctx_)) {
          await->reset_pending();
        }
        if (after_rows_ && ++completed_row_batches_ == hook_batch_) {
          auto hook = std::move(after_rows_);
          hook();
        }
        ++step_;
        return OB_SUCCESS;
      }
    }
    brs_.size_ = 0;
    brs_.end_ = true;
    return OB_SUCCESS;
  }

  void destroy() override
  {
    steps_.~vector();
    after_rows_.~function();
    ObOperator::destroy();
  }

  void make_ready() { ready_ = true; }
  void replace_steps(std::vector<Step> steps)
  {
    steps_ = std::move(steps);
    step_ = 0;
    ready_ = false;
  }
  void enable_dump_on_wait() { enable_dump_on_wait_ = true; }
  void force_sort_dump_on_wait() { force_sort_dump_on_wait_ = true; }
  void after_rows(int64_t batch, std::function<void()> hook)
  {
    hook_batch_ = batch;
    after_rows_ = std::move(hook);
  }
  int64_t fetches() const { return fetches_; }
  int64_t blocking_waits() const { return blocking_waits_; }

  static bool ready(const void *input)
  {
    return static_cast<const ScriptedInput *>(input)->ready_;
  }

private:
  void poison_frame()
  {
    ObDatum *datums = key_.locate_datums_for_update(eval_ctx_, BATCH_SIZE);
    for (int64_t i = 0; i < BATCH_SIZE; ++i) {
      datums[i].set_int(-999);
    }
    key_.get_eval_info(eval_ctx_).clear_evaluated_flag();
  }

  ObExpr &key_;
  ObExpr *payload_;
  std::vector<Step> steps_;
  size_t step_ = 0;
  bool ready_ = false;
  bool enable_dump_on_wait_ = false;
  bool force_sort_dump_on_wait_ = false;
  int64_t fetches_ = 0;
  int64_t blocking_waits_ = 0;
  int64_t completed_row_batches_ = 0;
  int64_t hook_batch_ = 0;
  std::function<void()> after_rows_;
};

enum class Kind
{
  SORT,
  HASH_GROUP_BY
};

struct FixtureOptions
{
  int64_t topn_count_ = std::numeric_limits<int64_t>::max();
  ObExprOperatorType aggregate_type_ = T_FUN_COUNT;
  bool prefix_sort_ = false;
  bool hidden_suffix_key_ = false;
};

class Fixture
{
public:
  Fixture(Kind kind, std::vector<Step> steps, const FixtureOptions &options = FixtureOptions())
    : allocator_("CollectionTest"), ctx_(allocator_), plan_(), session_(),
      input_spec_(allocator_, PHY_SEMANTIC_MAP), sort_spec_(allocator_, PHY_SORT),
      group_spec_(allocator_, PHY_HASH_GROUP_BY), kind_(kind), options_(options)
  {
    plan_.set_batch_size(BATCH_SIZE);
    ctx_.set_my_session(&session_);
    require_success(ctx_.create_physical_plan_ctx(), "create plan context");
    ctx_.get_physical_plan_ctx()->set_phy_plan(&plan_);
    ctx_.get_physical_plan_ctx()->set_timeout_timestamp(std::numeric_limits<int64_t>::max());
    for (size_t i = 0; i < exprs_.size(); ++i) {
      void *buf = allocator_.alloc(sizeof(Frame));
      require(nullptr != buf, "allocate expression frame");
      frames_[i] = reinterpret_cast<char *>(new (buf) Frame());
      ObExpr &expr = exprs_[i];
      if (Kind::HASH_GROUP_BY == kind_ && i == 1 && options_.aggregate_type_ == T_FUN_SUM) {
        expr.datum_meta_.type_ = ObNumberType;
        expr.obj_meta_.set_number();
      } else {
        expr.datum_meta_.type_ = ObIntType;
        expr.obj_meta_.set_int();
      }
      expr.frame_idx_ = i;
      expr.datum_off_ = offsetof(Frame, datums_);
      expr.eval_info_off_ = offsetof(Frame, eval_info_);
      expr.eval_flags_off_ = offsetof(Frame, evaluated_flags_);
      expr.res_buf_off_ = offsetof(Frame, values_);
      expr.res_buf_len_ = sizeof(int64_t);
      expr.batch_result_ = true;
      expr.batch_idx_mask_ = std::numeric_limits<uint64_t>::max();
      expr.basic_funcs_ = ObDatumFuncs::get_basic_func(expr.datum_meta_.type_,
          expr.datum_meta_.cs_type_, expr.datum_meta_.scale_, expr.obj_meta_.has_lob_header(),
          expr.datum_meta_.precision_);
      require(nullptr != expr.basic_funcs_, "initialize integer expression");
    }
    exprs_[1].type_ = options_.aggregate_type_;
    ctx_.set_frames(frames_.data());
    ctx_.set_frame_cnt(frames_.size());
    init_spec(input_spec_, 1, options_.prefix_sort_ ? 2 : 1);
    require_success(input_spec_.output_.push_back(&exprs_[0]), "input output");
    require_success(input_spec_.calc_exprs_.init(options_.prefix_sort_ ? 2 : 1), "input calculated expression capacity");
    require_success(input_spec_.calc_exprs_.push_back(&exprs_[0]), "input calculated expression");
    if (options_.prefix_sort_) {
      require_success(input_spec_.output_.push_back(&exprs_[1]), "pair payload output");
      require_success(input_spec_.calc_exprs_.push_back(&exprs_[1]), "pair calculated payload");
    }
    void *input_buf = allocator_.alloc(sizeof(ScriptedInput));
    require(nullptr != input_buf, "allocate input");
    input_ = new (input_buf) ScriptedInput(ctx_, input_spec_, exprs_[0], std::move(steps),
                                         options_.prefix_sort_ ? &exprs_[1] : nullptr);
    children_[0] = input_;
    spec_children_[0] = &input_spec_;

    ObSortCmpFunc cmp;
    cmp.cmp_func_ = exprs_[0].basic_funcs_->null_last_cmp_;
    if (Kind::SORT == kind_) {
      init_spec(sort_spec_, 2, options_.prefix_sort_ && !options_.hidden_suffix_key_ ? 2 : 1);
      if (options_.topn_count_ != std::numeric_limits<int64_t>::max()) {
        ObExpr &topn = exprs_[1];
        topn.type_ = T_INT;
        topn.batch_result_ = false;
        topn.batch_idx_mask_ = 0;
        topn.is_static_const_ = true;
        Frame *frame = reinterpret_cast<Frame *>(frames_[1]);
        frame->datums_[0].ptr_ = reinterpret_cast<char *>(&frame->values_[0]);
        frame->datums_[0].set_int(options_.topn_count_);
        sort_spec_.topn_expr_ = &topn;
      }
      require_success(sort_spec_.output_.push_back(&exprs_[0]), "sort output");
      require_success(sort_spec_.all_exprs_.init(options_.prefix_sort_ ? 2 : 1), "sort stored expression capacity");
      require_success(sort_spec_.all_exprs_.push_back(&exprs_[0]), "sort stored expression");
      require_success(sort_spec_.sort_collations_.init(options_.prefix_sort_ ? 2 : 1), "sort collation capacity");
      require_success(sort_spec_.sort_collations_.push_back(
          ObSortFieldCollation(0, CS_TYPE_BINARY, true, NULL_LAST)), "sort collation");
      require_success(sort_spec_.sort_cmp_funs_.init(options_.prefix_sort_ ? 2 : 1), "sort comparison capacity");
      require_success(sort_spec_.sort_cmp_funs_.push_back(cmp), "sort comparison");
      if (options_.prefix_sort_) {
        sort_spec_.prefix_pos_ = 1;
        if (!options_.hidden_suffix_key_) {
          require_success(sort_spec_.output_.push_back(&exprs_[1]), "prefix payload output");
        }
        require_success(sort_spec_.all_exprs_.push_back(&exprs_[1]), "prefix stored payload");
        require_success(sort_spec_.sort_collations_.push_back(
            ObSortFieldCollation(1, CS_TYPE_BINARY, true, NULL_LAST)), "prefix suffix collation");
        require_success(sort_spec_.sort_cmp_funs_.push_back(cmp), "prefix suffix comparison");
      }
      require_success(sort_spec_.set_children_pointer(spec_children_.data(), spec_children_.size()),
                      "set sort spec child");
      void *buf = allocator_.alloc(sizeof(ObSortOp));
      require(nullptr != buf, "allocate sort");
      root_ = new (buf) ObSortOp(ctx_, sort_spec_, nullptr);
    } else {
      init_spec(group_spec_, 2, 2);
      group_spec_.est_group_cnt_ = 16;
      require_success(group_spec_.output_.push_back(&exprs_[0]), "group key output");
      require_success(group_spec_.output_.push_back(&exprs_[1]), "count output");
      require_success(group_spec_.init_group_exprs(1), "group expression capacity");
      require_success(group_spec_.group_exprs_.push_back(&exprs_[0]), "group expression");
      require_success(group_spec_.cmp_funcs_.init(1), "group comparison capacity");
      require_success(group_spec_.cmp_funcs_.push_back(cmp), "group comparison");
      ObAggrInfo count(allocator_);
      count.expr_ = &exprs_[1];
      if (options_.aggregate_type_ == T_FUN_SUM) {
        require_success(count.param_exprs_.init(1), "sum parameter capacity");
        require_success(count.param_exprs_.push_back(&exprs_[0]), "sum parameter");
      }
      require_success(group_spec_.aggr_infos_.init(1), "count aggregate capacity");
      require_success(group_spec_.aggr_infos_.push_back(count), "count aggregate");
      require_success(group_spec_.set_children_pointer(spec_children_.data(), spec_children_.size()),
                      "set group spec child");
      void *buf = allocator_.alloc(sizeof(ObHashGroupByOp));
      require(nullptr != buf, "allocate hash group by");
      root_ = new (buf) ObHashGroupByOp(ctx_, group_spec_, nullptr);
    }
    require_success(root_->set_children_pointer(children_.data(), children_.size()), "set child");
    require(root_->get_spec().has_semantic_operator(), "root spec must discover its semantic descendant");
    root_->get_monitor_info().op_id_ = root_->get_spec().id_;
    root_->get_monitor_info().op_type_ = root_->get_spec().type_;
    await_.enable(&ctx_);
    {
      lib::RequestAwaitGuard guard(await_);
      require_success(root_->open(), "open root");
    }
    require(0 == input_->fetches(), "open must initialize without collecting input");
    require(!await_.is_pending(), "open must not register a pending await");
  }

  ~Fixture()
  {
    await_.reset();
    require_success(root_->close(), "close root");
    root_->destroy();
    input_->destroy();
  }

  int next(const ObBatchRows *&brs, int64_t max_rows = 1, bool with_await = true)
  {
    if (with_await) {
      lib::RequestAwaitGuard guard(await_);
      return root_->get_next_batch(max_rows, brs);
    } else {
      return root_->get_next_batch(max_rows, brs);
    }
  }

  void rescan()
  {
    await_.reset();
    await_.enable(&ctx_);
    require_success(root_->rescan(), "rescan root");
  }

  int64_t collected() const
  {
    return Kind::SORT == kind_
        ? static_cast<ObSortOp *>(root_)->get_sort_row_count()
        : static_cast<ObHashGroupByOp *>(root_)->get_hash_groupby_row_count();
  }

  std::vector<int64_t> drain(const ObBatchRows *brs, bool with_await = true)
  {
    std::vector<int64_t> result;
    int64_t batches = 0;
    while (true) {
      if (nullptr != brs) {
        for (int64_t i = 0; i < brs->size_; ++i) {
          if (!brs->skip_->at(i)) {
            result.push_back(exprs_[0].locate_expr_datum(root_->get_eval_ctx(), i).get_int());
            if (Kind::HASH_GROUP_BY == kind_) {
              const ObDatum &datum = exprs_[1].locate_expr_datum(root_->get_eval_ctx(), i);
              int64_t aggregate = 0;
              if (options_.aggregate_type_ == T_FUN_SUM) {
                number::ObNumber value(datum.get_number());
                require(value.is_valid_int(), "SUM must produce an exact integer");
                require_success(value.extract_valid_int64_with_trunc(aggregate), "extract SUM result");
              } else {
                aggregate = datum.get_int();
              }
              aggregate_values_[result.back()] = aggregate;
            }
          }
        }
        if (brs->end_) {
          break;
        }
      }
      require(++batches <= 16, "output must reach EOF");
      require_success(next(brs, 1, with_await), "drain output");
    }
    return result;
  }

  ObArenaAllocator allocator_;
  ObExecContext ctx_;
  ObPhysicalPlan plan_;
  ObSQLSessionInfo session_;
  std::array<ObExpr, 2> exprs_;
  std::array<char *, 2> frames_;
  ObOpSpec input_spec_;
  ObSortSpec sort_spec_;
  ObHashGroupBySpec group_spec_;
  Kind kind_;
  FixtureOptions options_;
  ScriptedInput *input_ = nullptr;
  std::array<ObOperator *, 1> children_;
  std::array<ObOpSpec *, 1> spec_children_;
  ObOperator *root_ = nullptr;
  lib::RequestAwait await_;
  std::map<int64_t, int64_t> aggregate_values_;

private:
  void init_spec(ObOpSpec &spec, uint64_t id, int64_t outputs = 1)
  {
    spec.id_ = id;
    spec.plan_ = &plan_;
    spec.max_batch_size_ = BATCH_SIZE;
    spec.rows_ = 16;
    spec.width_ = sizeof(int64_t);
    require_success(spec.output_.init(outputs), "output expression capacity");
  }
};

void verify_result(Fixture &fixture, const ObBatchRows *brs, bool with_await = true)
{
  std::vector<int64_t> result = fixture.drain(brs, with_await);
  if (Kind::SORT == fixture.kind_) {
    std::vector<int64_t> expected({1, 1, 3, 4, 5, 7, 7});
    if (fixture.options_.topn_count_ < static_cast<int64_t>(expected.size())) {
      expected.resize(fixture.options_.topn_count_);
    }
    require(result == expected, "sort output must be exact");
  } else {
    std::sort(result.begin(), result.end());
    require(result == std::vector<int64_t>({1, 3, 4, 5, 7}), "group keys must be exact");
    require(fixture.aggregate_values_ == std::map<int64_t, int64_t>({{1, 2}, {3, 1}, {4, 1}, {5, 1}, {7, 2}}),
            "resuming must not replay aggregate input");
  }
}

std::vector<Step> input_steps()
{
  return {rows({7, 2, 5}, false, 1), wait(), rows({}), rows({3, 7, 1}), wait(),
          rows({4, 1}, true)};
}

void test_repeated_pending_and_nonempty_eof(Kind kind)
{
  Fixture fixture(kind, input_steps());
  require(fixture.root_->supports_semantic_suspend(), "plain in-memory collection must be supported");
  const ObBatchRows *brs = nullptr;
  require(OB_EAGAIN == fixture.next(brs, 3), "first collection must pause");
  require(fixture.await_.is_pending() && !fixture.await_.is_ready(), "pause must register readiness");
  require(0 == brs->size_ && !brs->end_, "a pause must not publish output or EOF");
  require(2 == fixture.collected(), "first batch must be consumed once and honor skips");
  require(0 == fixture.root_->get_monitor_info().output_row_count_, "no partial output");
  require(OB_EAGAIN == fixture.next(brs, 2), "unready collection must pause again");
  require(2 == fixture.collected(), "repeated pending must not replay or reinitialize");

  fixture.input_->make_ready();
  require(OB_EAGAIN == fixture.next(brs, 1), "collection must pause at the next boundary");
  require((Kind::SORT == kind ? 5 : 4) == fixture.collected(), "partial collection must survive");
  require(0 == brs->size_ && !brs->end_, "second pause must not finalize");
  fixture.input_->make_ready();
  require_success(fixture.next(brs, 1), "resume at EOF");
  require(!fixture.await_.is_pending(), "successful input must clear pending");
  verify_result(fixture, brs);
}

void test_pending_before_first_batch(Kind kind)
{
  Fixture fixture(kind, {wait(), rows({9}, true)});
  const ObBatchRows *brs = nullptr;
  require(OB_EAGAIN == fixture.next(brs), "initial input must pause");
  require(0 == fixture.collected(), "initial pause has no input");
  require(OB_EAGAIN == fixture.next(brs), "initial state must remain resumable");
  fixture.input_->make_ready();
  require_success(fixture.next(brs), "resume initial input");
  require(fixture.drain(brs) == std::vector<int64_t>({9}), "initial resume output");
  if (Kind::HASH_GROUP_BY == kind) {
    require(fixture.aggregate_values_.at(9) == 1, "initial resume count");
  }
}

void test_ordinary_eagain_is_terminal(Kind kind, bool foreign_pending)
{
  Fixture fixture(kind, {rows({7}), error(OB_EAGAIN), rows({1}, true)});
  int foreign_owner = 0;
  if (foreign_pending) {
    fixture.await_.enable(&foreign_owner);
    lib::RequestAwaitGuard guard(fixture.await_);
    require(lib::RequestAwait::suspend(&foreign_owner, fixture.input_, &ScriptedInput::ready),
            "register foreign wait");
  }
  const ObBatchRows *brs = nullptr;
  require(OB_EAGAIN == fixture.next(brs), "ordinary EAGAIN must propagate");
  const int64_t fetches = fixture.input_->fetches();
  require(OB_EAGAIN == fixture.next(brs), "ordinary EAGAIN must remain terminal");
  require(fetches == fixture.input_->fetches(), "ordinary EAGAIN must not resume collection");
  require(0 == fixture.root_->get_monitor_info().output_row_count_, "errors must not expose partial output");
  require(foreign_pending == fixture.await_.is_pending(), "do not claim or clear a foreign wait");
}

void test_cancel_and_rescan(Kind kind, bool cancel_first = true)
{
  Fixture fixture(kind, input_steps());
  const ObBatchRows *brs = nullptr;
  require(OB_EAGAIN == fixture.next(brs), "collection must pause before rescan");
  if (cancel_first) {
    const int64_t fetches = fixture.input_->fetches();
    fixture.await_.cancel(OB_TIMEOUT);
    require(OB_TIMEOUT == fixture.next(brs), "cancellation must propagate exactly");
    require(fetches == fixture.input_->fetches(), "cancellation must not fetch more input");
    require(OB_TIMEOUT == fixture.next(brs), "cancelled collection must not restart");
  }

  fixture.rescan();
  require(OB_EAGAIN == fixture.next(brs), "rescan starts fresh collection");
  require(2 == fixture.collected(), "rescan must discard old collection");
  fixture.input_->make_ready();
  require(OB_EAGAIN == fixture.next(brs), "rescan resumes only new input");
  fixture.input_->make_ready();
  require_success(fixture.next(brs), "rescan reaches EOF");
  verify_result(fixture, brs);
}

void test_empty_eof_after_pending(Kind kind)
{
  Fixture fixture(kind, {rows({7}, false, 0), wait(), rows({}, true)});
  const ObBatchRows *brs = nullptr;
  require(OB_EAGAIN == fixture.next(brs), "filtered empty input must remain resumable");
  require(0 == fixture.collected(), "all-skipped input must not be collected");
  fixture.input_->make_ready();
  require_success(fixture.next(brs), "resume empty input at EOF");
  require(brs->end_ && brs->size_ == 0, "empty input must produce exact EOF");
  require(fixture.drain(brs).empty(), "empty input must not expose uninitialized output");
}

void test_close_while_pending(Kind kind)
{
  Fixture fixture(kind, {rows({7}), wait(), rows({1}, true)});
  const ObBatchRows *brs = nullptr;
  require(OB_EAGAIN == fixture.next(brs), "collection must pause before close");
}

void test_cancel_during_fetch(Kind kind)
{
  Fixture fixture(kind, {rows({7}), cancel(OB_TIMEOUT), rows({1}, true)});
  const ObBatchRows *brs = nullptr;
  require(OB_TIMEOUT == fixture.next(brs), "cancellation during fetch must not become a pending EAGAIN");
  const int64_t fetches = fixture.input_->fetches();
  require(OB_TIMEOUT == fixture.next(brs), "cancellation during fetch must remain terminal");
  require(fetches == fixture.input_->fetches(), "do not replay cancelled input");
}

void test_serial_worker_handoff(Kind kind)
{
  Fixture fixture(kind, {rows({7, 5}), wait(), rows({1}, true)});
  const ObBatchRows *brs = nullptr;
  require(OB_EAGAIN == fixture.next(brs), "collection must pause before worker handoff");
  fixture.input_->make_ready();
  int ret = OB_ERR_UNEXPECTED;
  std::thread worker([&]() { ret = fixture.next(brs); });
  worker.join();
  require_success(ret, "resume collection on another worker");
  std::vector<int64_t> result = fixture.drain(brs);
  if (Kind::HASH_GROUP_BY == kind) {
    std::sort(result.begin(), result.end());
    require(fixture.aggregate_values_ == std::map<int64_t, int64_t>({{1, 1}, {5, 1}, {7, 1}}),
            "worker handoff must not replay aggregation");
  }
  require(result == std::vector<int64_t>({1, 5, 7}), "worker handoff must preserve owned rows");
}

void test_without_await(Kind kind)
{
  Fixture fixture(kind, input_steps());
  const ObBatchRows *brs = nullptr;
  require_success(fixture.next(brs, 1, false), "ordinary query must stay synchronous");
  require(2 == fixture.input_->blocking_waits(), "ordinary query must block at semantic input");
  verify_result(fixture, brs, false);
}

void test_conservative_capabilities()
{
  Fixture fixture(Kind::SORT, {rows({}, true)});
  fixture.sort_spec_.prefix_pos_ = 1;
  require(!fixture.root_->supports_semantic_suspend(), "prefix sort remains blocking");
  fixture.sort_spec_.prefix_pos_ = 0;
  fixture.sort_spec_.prescan_enabled_ = true;
  require(!fixture.root_->supports_semantic_suspend(), "prescan remains blocking");
  fixture.sort_spec_.prescan_enabled_ = false;
  fixture.sort_spec_.is_local_merge_sort_ = true;
  require(!fixture.root_->supports_semantic_suspend(), "local merge remains blocking");
  fixture.sort_spec_.is_local_merge_sort_ = false;
  fixture.sort_spec_.topk_limit_expr_ = &fixture.exprs_[0];
  require(!fixture.root_->supports_semantic_suspend(), "topk remains blocking");
  fixture.sort_spec_.topk_limit_expr_ = nullptr;
  fixture.sort_spec_.is_fetch_with_ties_ = true;
  require(!fixture.root_->supports_semantic_suspend(), "fetch with ties remains blocking");
  fixture.sort_spec_.is_fetch_with_ties_ = false;
  fixture.sort_spec_.part_cnt_ = 1;
  require(!fixture.root_->supports_semantic_suspend(), "partition sort remains blocking");
  fixture.sort_spec_.part_cnt_ = 0;
  TP_SET_EVENT(common::EventTable::EN_SORT_IMPL_FORCE_DO_DUMP, -2, 1, 0);
  require(!fixture.root_->supports_semantic_suspend(), "forced sort spill remains blocking");
  require(ATOMIC_LOAD(&common::EventTable::EN_SORT_IMPL_FORCE_DO_DUMP.item_.occur_) == 1,
          "capability checking must not consume a forced spill event");
  TP_SET_EVENT(common::EventTable::EN_SORT_IMPL_FORCE_DO_DUMP, 0, 0, 0);
  fixture.root_->get_monitor_info().otherstat_4_id_ = ObSqlMonitorStatIds::SORT_DUMP_DATA_TIME;
  require(!fixture.root_->supports_semantic_suspend(), "previously spilled sort remains blocking");

  Fixture group(Kind::HASH_GROUP_BY, {rows({}, true)});
  group.group_spec_.by_pass_enabled_ = true;
  require(!group.root_->supports_semantic_suspend(), "adaptive bypass remains blocking");
  group.group_spec_.by_pass_enabled_ = false;
  group.group_spec_.aggr_stage_ = ObThreeStageAggrStage::FIRST_STAGE;
  require(!group.root_->supports_semantic_suspend(), "three-stage aggregation remains blocking");
  group.group_spec_.aggr_stage_ = ObThreeStageAggrStage::NONE_STAGE;
}

void test_dump_setting_changes_during_in_memory_wait()
{
  const bool previous_dump = GCONF.enable_sql_operator_dump;
  Fixture fixture(Kind::SORT, {rows({7}), wait(), rows({1}, true)});
  const ObBatchRows *brs = nullptr;
  fixture.input_->enable_dump_on_wait();
  require(OB_EAGAIN == fixture.next(brs), "collection must pause before dump setting changes");
  require(fixture.root_->supports_semantic_suspend(), "configured dumping alone must not block in-memory waits");
  require(OB_EAGAIN == fixture.next(brs), "an unready in-memory collection remains pending");
  fixture.input_->make_ready();
  require_success(fixture.next(brs), "finish an in-memory collection after dump setting changes");
  require(0 == fixture.input_->blocking_waits(), "in-memory resume must still release the worker");
  require(fixture.drain(brs) == std::vector<int64_t>({1, 7}), "capability change must not lose input");
  require(GCONF.enable_sql_operator_dump.set_value(previous_dump ? "true" : "false"),
          "restore dump setting after resume");
}

void test_capability_changes_during_fetch()
{
  Fixture fixture(Kind::SORT, {rows({7}), wait(), rows({1}, true)});
  fixture.input_->force_sort_dump_on_wait();
  const ObBatchRows *brs = nullptr;
  require(OB_EAGAIN == fixture.next(brs), "authorized pending must survive a capability change during fetch");
  require(!fixture.root_->supports_semantic_suspend(), "forced spill must block future waits");
  require_success(fixture.next(brs), "finish a pending collection after capability narrows");
  require(1 == fixture.input_->blocking_waits(), "the next child boundary must be nonsuspending");
  require(fixture.drain(brs) == std::vector<int64_t>({1, 7}), "capability narrowing must preserve input");
  TP_SET_EVENT(common::EventTable::EN_SORT_IMPL_FORCE_DO_DUMP, 0, 0, 0);
}

void test_topn_resumes(int64_t count)
{
  FixtureOptions options;
  options.topn_count_ = count;
  Fixture fixture(Kind::SORT, input_steps(), options);
  require(fixture.root_->supports_semantic_suspend(), "ordinary in-memory TopN must be resumable");
  const ObBatchRows *brs = nullptr;
  require(OB_EAGAIN == fixture.next(brs), "TopN must pause after its first batch");
  require(2 == fixture.collected(), "TopN input count before pause");
  require(OB_EAGAIN == fixture.next(brs), "TopN must preserve a partial heap across repeated waits");
  fixture.input_->make_ready();
  require(OB_EAGAIN == fixture.next(brs), "TopN must resume collection rather than output a partial heap");
  fixture.input_->make_ready();
  require_success(fixture.next(brs), "TopN reaches EOF");
  verify_result(fixture, brs);
}

void test_zero_topn_does_not_fetch()
{
  FixtureOptions options;
  options.topn_count_ = 0;
  Fixture fixture(Kind::SORT, {wait(), rows({7}, true)}, options);
  const ObBatchRows *brs = nullptr;
  require_success(fixture.next(brs), "zero TopN must return EOF");
  require(brs->size_ == 0 && brs->end_, "zero TopN output shape");
  require(0 == fixture.input_->fetches() && !fixture.await_.is_pending(), "zero TopN must not consume semantic input");
}

void test_sum_resumes()
{
  FixtureOptions options;
  options.aggregate_type_ = T_FUN_SUM;
  Fixture fixture(Kind::HASH_GROUP_BY, input_steps(), options);
  const ObBatchRows *brs = nullptr;
  require(OB_EAGAIN == fixture.next(brs), "SUM must pause after partial aggregation");
  fixture.input_->make_ready();
  require(OB_EAGAIN == fixture.next(brs), "SUM must keep collecting after resume");
  fixture.input_->make_ready();
  require_success(fixture.next(brs), "SUM reaches EOF");
  std::vector<int64_t> result = fixture.drain(brs);
  std::sort(result.begin(), result.end());
  require(result == std::vector<int64_t>({1, 3, 4, 5, 7}), "SUM group keys must be exact");
  require(fixture.aggregate_values_ == std::map<int64_t, int64_t>({{1, 2}, {3, 3}, {4, 4}, {5, 5}, {7, 14}}),
          "SUM must neither lose nor replay partial input");
}

void test_sort_stops_suspending_after_spill()
{
  Fixture fixture(Kind::SORT, {rows({7}), wait(), rows({1}, true)});
  const ObBatchRows *brs = nullptr;
  require(OB_EAGAIN == fixture.next(brs), "sort must pause before spill");
  fixture.root_->get_monitor_info().otherstat_4_id_ = ObSqlMonitorStatIds::SORT_DUMP_DATA_TIME;
  require(!fixture.root_->supports_semantic_suspend(), "recorded spill must disable suspension");
  require_success(fixture.next(brs), "spilled sort must complete child collection without another suspension");
  require(1 == fixture.input_->blocking_waits(), "spilled sort must block at the next child boundary");
  require(fixture.drain(brs) == std::vector<int64_t>({1, 7}), "spill gate must not restart collection");
}

void test_hash_stops_suspending_after_dump_setup()
{
  Fixture fixture(Kind::HASH_GROUP_BY, {rows({7, 5}), wait(), rows({7}, true)});
  const ObBatchRows *brs = nullptr;
  require(OB_EAGAIN == fixture.next(brs), "hash group by must pause before dump setup");
  ObHashGroupByOp &op = *static_cast<ObHashGroupByOp *>(fixture.root_);
  ObHashGroupByOp::DatumStoreLinkPartition *parts[ObHashGroupByOp::MAX_PARTITION_CNT] = {};
  int64_t part_count = 0;
  ObGbyBloomFilter *bloom = nullptr;
  require_success(op.setup_dump_env(0, 16, parts, part_count, bloom), "prepare real hash dump environment");
  require(part_count > 0 && nullptr != bloom, "dump setup must allocate its partitions and bloom filter");
  require(!op.supports_semantic_suspend(), "dump setup must disable suspension");
  require_success(fixture.next(brs), "hash group by must block after dump setup");
  require(1 == fixture.input_->blocking_waits(), "hash spill gate must block child waits");
  std::vector<int64_t> result = fixture.drain(brs);
  std::sort(result.begin(), result.end());
  require(result == std::vector<int64_t>({5, 7}), "dump setup gate must preserve group keys");
  require(fixture.aggregate_values_ == std::map<int64_t, int64_t>({{5, 1}, {7, 2}}),
          "dump setup gate must not replay aggregation");
  require_success(op.cleanup_dump_env(false, 0, parts, part_count, bloom), "release unused native dump environment");
  require(nullptr == bloom, "dump environment cleanup must release its bloom filter");
}

void test_spill_gate_changes_inside_sort_collection()
{
  Fixture fixture(Kind::SORT, {rows({7}), wait(), rows({1}, true)});
  fixture.input_->after_rows(1, [&]() {
    fixture.root_->get_monitor_info().otherstat_4_id_ = ObSqlMonitorStatIds::SORT_DUMP_DATA_TIME;
  });
  const ObBatchRows *brs = nullptr;
  require_success(fixture.next(brs), "sort must recheck its spill gate inside an already resumable pull");
  require(1 == fixture.input_->blocking_waits(), "sort spill transition must block the next fetch in the same pull");
  require(fixture.drain(brs) == std::vector<int64_t>({1, 7}), "sort spill transition output");
}

void test_dump_gate_changes_inside_hash_collection()
{
  Fixture fixture(Kind::HASH_GROUP_BY, {rows({7, 5}), rows({7}), wait(), rows({5}, true)});
  ObHashGroupByOp &op = *static_cast<ObHashGroupByOp *>(fixture.root_);
  ObHashGroupByOp::DatumStoreLinkPartition *parts[ObHashGroupByOp::MAX_PARTITION_CNT] = {};
  int64_t part_count = 0;
  ObGbyBloomFilter *bloom = nullptr;
  fixture.input_->after_rows(2, [&]() {
    require_success(op.setup_dump_env(0, 16, parts, part_count, bloom), "prepare dump environment during collection");
  });
  const ObBatchRows *brs = nullptr;
  require_success(fixture.next(brs), "hash must recheck its dump gate inside an already resumable pull");
  require(1 == fixture.input_->blocking_waits(), "hash dump transition must block the next fetch in the same pull");
  std::vector<int64_t> result = fixture.drain(brs);
  std::sort(result.begin(), result.end());
  require(result == std::vector<int64_t>({5, 7}), "hash dump transition keys");
  require(fixture.aggregate_values_ == std::map<int64_t, int64_t>({{5, 2}, {7, 2}}), "hash dump transition counts");
  require_success(op.cleanup_dump_env(false, 0, parts, part_count, bloom), "release unused mid-pull dump environment");
}

std::vector<Step> prefix_input_steps(int64_t multiplier = 1)
{
  std::vector<Step> steps(
      {pairs({1, 1, 2, 2, 3, 3, 4, 4}, {7, 3, 8, 5, 9, 2, 8, 4}),
       wait(), pairs({4, 4, 5, 5}, {9, 1, 6, 2}, true)});
  for (Step &step : steps) {
    for (int64_t &value : step.values_) {
      value *= multiplier;
    }
    for (int64_t &value : step.payload_) {
      value *= multiplier;
    }
  }
  return steps;
}

std::vector<std::pair<int64_t, int64_t>> prefix_expected_pairs(int64_t multiplier = 1)
{
  std::vector<std::pair<int64_t, int64_t>> result(
      {{1, 3}, {1, 7}, {2, 5}, {2, 8}, {3, 2}, {3, 9}, {4, 1}, {4, 4}, {4, 8}, {4, 9}, {5, 2}, {5, 6}});
  for (auto &pair : result) {
    pair.first *= multiplier;
    pair.second *= multiplier;
  }
  return result;
}

std::vector<std::pair<int64_t, int64_t>> drain_prefix_with_slot_preparation(
    Fixture &fixture, bool full_parent_snapshot)
{
  ObBatchResultHolder parent_frame;
  require_success(parent_frame.init(fixture.sort_spec_.output_, fixture.root_->get_eval_ctx(),
                                   &fixture.allocator_), "initialize parent frame snapshot");
  std::vector<std::pair<int64_t, int64_t>> result;
  int64_t poison_key = 2;
  int64_t poison_payload = 7;
  ObDatum alien_key;
  ObDatum alien_payload;
  alien_key.ptr_ = reinterpret_cast<char *>(&poison_key);
  alien_payload.ptr_ = reinterpret_cast<char *>(&poison_payload);
  alien_key.set_int(poison_key);
  alien_payload.set_int(poison_payload);
  const ObBatchRows *brs = nullptr;
  for (int64_t batch = 0; batch < 16; ++batch) {
    require_success(parent_frame.restore(), "restore parent frame snapshot");
    require_success(fixture.next(brs, 2), "read prefix batch");
    for (int64_t row = 0; row < brs->size_; ++row) {
      result.emplace_back(fixture.exprs_[0].locate_expr_datum(fixture.root_->get_eval_ctx(), row).get_int(),
                          fixture.exprs_[1].locate_expr_datum(fixture.root_->get_eval_ctx(), row).get_int());
    }
    if (brs->end_) {
      break;
    }
    const int64_t saved_size = full_parent_snapshot
        ? fixture.root_->get_eval_ctx().max_batch_size_ : brs->size_;
    require_success(parent_frame.save(saved_size), "save parent frame before preparing another slot");
    ObDatum *keys = fixture.exprs_[0].locate_batch_datums(fixture.root_->get_eval_ctx());
    ObDatum *payloads = fixture.exprs_[1].locate_batch_datums(fixture.root_->get_eval_ctx());
    for (int64_t row = 0; row < BATCH_SIZE; ++row) {
      keys[row] = alien_key;
      payloads[row] = alien_payload;
    }
  }
  parent_frame.destroy();
  require(nullptr != brs && brs->end_, "prefix collection must reach EOF");
  return result;
}

void test_prefix_live_tail_survives_slot_preparation(bool full_parent_snapshot, bool hidden_suffix_key)
{
  FixtureOptions options;
  options.prefix_sort_ = true;
  options.hidden_suffix_key_ = hidden_suffix_key;
  Fixture fixture(Kind::SORT, prefix_input_steps(), options);
  require(!fixture.root_->supports_semantic_suspend(), "prefix waits remain blocking");
  require(fixture.sort_spec_.output_.count() == (hidden_suffix_key ? 1 : 2), "prefix public output shape");
  require(drain_prefix_with_slot_preparation(fixture, full_parent_snapshot) == prefix_expected_pairs(),
      "prefix live-tail pairs must survive another slot overwriting the full shared frame");
  require(1 == fixture.input_->blocking_waits() && !fixture.await_.is_pending(),
          "prefix child waits must complete synchronously");
}

void test_prefix_rescan_with_live_tail()
{
  FixtureOptions options;
  options.prefix_sort_ = true;
  options.hidden_suffix_key_ = true;
  Fixture fixture(Kind::SORT, prefix_input_steps(), options);
  const ObBatchRows *brs = nullptr;
  require_success(fixture.next(brs, 2), "read prefix output before rescan");
  require(2 == brs->size_ && !brs->end_, "rescan must interrupt live prefix collection");
  require(1 == fixture.input_->fetches(), "rescan must interrupt before the second child batch");
  fixture.rescan();
  fixture.input_->replace_steps(prefix_input_steps(10));
  require(0 == fixture.collected(), "rescan must reset prefix row count");
  require(drain_prefix_with_slot_preparation(fixture, false) == prefix_expected_pairs(10),
          "rescan must discard old prefix state and snapshots");
  require(1 == fixture.input_->blocking_waits(), "rescanned prefix waits remain blocking");
}

void test_prefix_close_with_live_tail()
{
  FixtureOptions options;
  options.prefix_sort_ = true;
  Fixture fixture(Kind::SORT, prefix_input_steps(), options);
  const ObBatchRows *brs = nullptr;
  require_success(fixture.next(brs, 2), "read prefix output before close");
  require(2 == brs->size_ && !brs->end_, "close must interrupt live prefix collection");
  require(1 == fixture.input_->fetches() && !fixture.await_.is_pending(),
          "close must not fetch or suspend on the remaining prefix");
}

} // namespace
} // namespace sql
} // namespace oceanbase

int run_collection_tests()
{
  using namespace oceanbase::sql;
  const bool previous_dump = GCONF.enable_sql_operator_dump;
  for (bool dump_enabled : {false, true}) {
    require(GCONF.enable_sql_operator_dump.set_value(dump_enabled ? "true" : "false"),
            "configure native dump capability");
    for (Kind kind : {Kind::SORT, Kind::HASH_GROUP_BY}) {
      test_repeated_pending_and_nonempty_eof(kind);
      test_pending_before_first_batch(kind);
      test_empty_eof_after_pending(kind);
      test_ordinary_eagain_is_terminal(kind, false);
      test_ordinary_eagain_is_terminal(kind, true);
      test_cancel_and_rescan(kind);
      test_cancel_and_rescan(kind, false);
      test_cancel_during_fetch(kind);
      test_close_while_pending(kind);
      test_serial_worker_handoff(kind);
      test_without_await(kind);
    }
    test_topn_resumes(1);
    test_topn_resumes(3);
    test_zero_topn_does_not_fetch();
    test_sum_resumes();
    test_sort_stops_suspending_after_spill();
    test_hash_stops_suspending_after_dump_setup();
    test_spill_gate_changes_inside_sort_collection();
    test_dump_gate_changes_inside_hash_collection();
    test_conservative_capabilities();
    test_dump_setting_changes_during_in_memory_wait();
    test_capability_changes_during_fetch();
    for (bool full_parent_snapshot : {false, true}) {
      for (bool hidden_suffix_key : {false, true}) {
        test_prefix_live_tail_survives_slot_preparation(full_parent_snapshot, hidden_suffix_key);
      }
    }
    test_prefix_rescan_with_live_tail();
    test_prefix_close_with_live_tail();
  }
  require(GCONF.enable_sql_operator_dump.set_value(previous_dump ? "true" : "false"), "restore dump setting");
  std::cout << "semantic collection native tests passed" << std::endl;
  return 0;
}

#ifdef SEMANTIC_COLLECTION_WRAP_MAIN
extern "C" int __wrap_main()
{
  return run_collection_tests();
}
#else
int main()
{
  return run_collection_tests();
}
#endif
