#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_ai_func_op.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_embed.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_complete.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "lib/hash/ob_hashmap.h"
#include <algorithm>

namespace oceanbase
{
namespace sql
{
using namespace common;

OB_SERIALIZE_MEMBER((AIFuncSpec, ObOpSpec), ai_expr_, solo_);

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

namespace
{
class SoloMemory : public ObIAllocator
{
  struct alignas(16) Block
  {
    Block *previous_;
    Block *next_;
    int64_t size_;
  };
public:
  SoloMemory(int64_t &shared_bytes, int64_t limit)
      : shared_bytes_(shared_bytes), limit_(limit), head_(nullptr), error_(OB_SUCCESS), peak_(0) {}
  ~SoloMemory() override
  {
    while (nullptr != head_) {
      free(head_ + 1);
    }
  }
  void *alloc(int64_t size) override
  {
    void *result = nullptr;
    if (size <= 0 || size > INT64_MAX - static_cast<int64_t>(sizeof(Block))) {
      error_ = OB_SIZE_OVERFLOW;
    } else {
      const int64_t bytes = size + sizeof(Block);
      error_ = ObAIFuncClient::reserve_pipeline_buffer(shared_bytes_, limit_, bytes);
      if (OB_SUCCESS == error_) {
        auto *block = static_cast<Block *>(ob_malloc(bytes, ObMemAttr("AISolo")));
        if (nullptr == block) {
          ObAIFuncClient::release_pipeline_buffer(shared_bytes_, bytes);
          error_ = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          block->previous_ = nullptr;
          block->next_ = head_;
          block->size_ = bytes;
          if (nullptr != head_) {
            head_->previous_ = block;
          }
          head_ = block;
          peak_ = std::max(peak_, ATOMIC_LOAD(&shared_bytes_));
          result = block + 1;
        }
      }
    }
    return result;
  }
  void *alloc(int64_t size, const ObMemAttr &) override { return alloc(size); }
  void free(void *pointer) override
  {
    if (nullptr != pointer) {
      auto *block = static_cast<Block *>(pointer) - 1;
      if (nullptr == block->previous_) {
        head_ = block->next_;
      } else {
        block->previous_->next_ = block->next_;
      }
      if (nullptr != block->next_) {
        block->next_->previous_ = block->previous_;
      }
      ObAIFuncClient::release_pipeline_buffer(shared_bytes_, block->size_);
      ob_free(block);
    }
  }
  int64_t &shared_bytes_;
  int64_t limit_;
  Block *head_;
  int error_;
  int64_t peak_;
};

template <typename Node>
struct SoloNodeAllocator
{
  SoloNodeAllocator() : allocator_(nullptr) {}
  void set_attr(const ObMemAttr &) {}
  Node *alloc()
  {
    void *memory = allocator_->alloc(sizeof(Node));
    return nullptr == memory ? nullptr : new (memory) Node();
  }
  void free(Node *node)
  {
    node->~Node();
    allocator_->free(node);
  }
  ObIAllocator *allocator_;
};

template <typename Key>
using SoloMap = hash::ObHashMap<Key, int64_t, hash::NoPthreadDefendMode,
    hash::hash_func<Key>, hash::equal_to<Key>,
    SoloNodeAllocator<typename hash::HashMapTypes<Key, int64_t>::AllocType>,
    hash::NormalPointer, ObWrapperAllocatorWithAttr>;

template <typename Value>
using SoloArray = ObArray<Value, ObWrapperAllocator, true>;

struct SoloPrefix
{
  int64_t prefix_;
  int64_t value_;
  int hash(uint64_t &value) const
  {
    value = murmurhash(this, sizeof(*this), 0);
    return OB_SUCCESS;
  }
  bool operator==(const SoloPrefix &other) const
  { return prefix_ == other.prefix_ && value_ == other.value_; }
  TO_STRING_KV(K_(prefix), K_(value));
};
}

struct AIFuncOp::SoloState
{
  struct Row
  {
    Row() : data_(nullptr), fields_(nullptr), prompt_bytes_(0), ready_(false) {}
    ObChunkDatumStore::StoredRow *data_;
    ObString *fields_;
    ObString result_;
    int64_t prompt_bytes_;
    bool ready_;
    TO_STRING_KV(K_(prompt_bytes), K_(ready));
  };
  SoloState(int64_t &shared_bytes, int64_t limit)
      : memory_(shared_bytes, limit / 2), store_("AISoloRows", &memory_),
        rows_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(memory_)),
        keys_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(memory_)),
        columns_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(memory_)),
        order_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(memory_)), submitted_(0), output_(0), planned_(false) {}

  int plan(ObExecContext &ctx)
  {
    int ret = OB_SUCCESS;
    const int64_t row_count = rows_.count();
    const int64_t column_count = keys_.count();
    SoloArray<int64_t> codes(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(memory_));
    SoloArray<int64_t> prefixes(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(memory_));
    SoloArray<int64_t> candidate(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(memory_));
    SoloArray<int64_t> best(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(memory_));
    SoloMap<ObString> dictionary;
    SoloMap<SoloPrefix> groups;
    dictionary.get_local_allocer().allocator_ = &memory_;
    dictionary.get_local_bucket_allocer().set_alloc(&memory_);
    groups.get_local_allocer().allocator_ = &memory_;
    groups.get_local_bucket_allocer().set_alloc(&memory_);
    if (row_count == 0) {
    } else if (column_count <= 0 || row_count > INT64_MAX / column_count / sizeof(int64_t)) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_FAIL(codes.prepare_allocate(row_count * column_count))) {
    } else if (OB_FAIL(prefixes.prepare_allocate(row_count))) {
    } else if (OB_FAIL(candidate.prepare_allocate(row_count))) {
    } else if (OB_FAIL(best.prepare_allocate(row_count))) {
    } else if (OB_FAIL(order_.prepare_allocate(row_count))) {
    } else if (OB_FAIL(dictionary.create(row_count, &dictionary.get_local_allocer(),
                                       &dictionary.get_local_bucket_allocer()))) {
    } else if (OB_FAIL(groups.create(row_count, &groups.get_local_allocer(),
                                    &groups.get_local_bucket_allocer()))) {
    } else {
      MEMSET(prefixes.get_data(), 0, row_count * sizeof(int64_t));
      for (int64_t row = 0; row < row_count; ++row) {
        order_.at(row) = row;
      }
    }
    for (int64_t column = 0; OB_SUCC(ret) && column < column_count; ++column) {
      if (OB_FAIL(dictionary.reuse())) {
      }
      for (int64_t row = 0; OB_SUCC(ret) && row < row_count; ++row) {
        int64_t code = 0;
        if ((row & 1023) == 0 && OB_FAIL(ctx.check_status())) {
        } else if (OB_HASH_NOT_EXIST == (ret = dictionary.get_refactored(rows_.at(row).fields_[column], code))) {
          code = dictionary.size();
          ret = dictionary.set_refactored(rows_.at(row).fields_[column], code);
        }
        if (OB_SUCC(ret)) {
          codes.at(row * column_count + column) = code;
        }
      }
    }
    dictionary.destroy();
    for (int64_t position = 0; OB_SUCC(ret) && row_count > 0 && position < column_count; ++position) {
      int64_t best_column = -1;
      int64_t best_count = INT64_MAX;
      for (int64_t column = 0; OB_SUCC(ret) && column < column_count; ++column) {
        bool selected = false;
        for (int64_t index = 0; index < columns_.count(); ++index) {
          selected |= columns_.at(index) == column;
        }
        if (selected) {
          continue;
        }
        if (OB_FAIL(groups.reuse())) {
        }
        for (int64_t row = 0; OB_SUCC(ret) && row < row_count; ++row) {
          SoloPrefix key{prefixes.at(row), codes.at(row * column_count + column)};
          int64_t group = 0;
          if ((row & 1023) == 0 && OB_FAIL(ctx.check_status())) {
          } else if (OB_HASH_NOT_EXIST == (ret = groups.get_refactored(key, group))) {
            group = groups.size();
            ret = groups.set_refactored(key, group);
          }
          if (OB_SUCC(ret)) {
            candidate.at(row) = group;
          }
        }
        if (OB_SUCC(ret) && groups.size() < best_count) {
          best_count = groups.size();
          best_column = column;
          MEMCPY(best.get_data(), candidate.get_data(), row_count * sizeof(int64_t));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(columns_.push_back(best_column))) {
      }
      if (OB_SUCC(ret)) {
        MEMCPY(prefixes.get_data(), best.get_data(), row_count * sizeof(int64_t));
      }
    }
    if (OB_SUCC(ret) && row_count > 1) {
      int64_t comparisons = 0;
      std::sort(order_.begin(), order_.end(), [&](int64_t left, int64_t right) {
        if ((++comparisons & 1023) == 0 && OB_SUCC(ret)) {
          ret = ctx.check_status();
        }
        for (int64_t position = 0; position < columns_.count(); ++position) {
          const int64_t column = columns_.at(position);
          const int comparison = rows_.at(left).fields_[column].compare(rows_.at(right).fields_[column]);
          if (comparison != 0) {
            return comparison < 0;
          }
        }
        return left < right;
      });
    }
    groups.destroy();
    if (OB_SUCC(ret)) {
      planned_ = true;
    } else if (ret == OB_ALLOCATE_MEMORY_FAILED && memory_.error_ != OB_SUCCESS) {
      ret = memory_.error_;
    }
    return ret;
  }

  SoloMemory memory_;
  ObChunkDatumStore store_;
  SoloArray<Row> rows_;
  SoloArray<ObString> keys_;
  SoloArray<int64_t> columns_;
  SoloArray<int64_t> order_;
  ObString model_id_;
  ObString instruction_;
  ObString config_;
  int64_t submitted_;
  int64_t output_;
  bool planned_;
};

int AIFuncOp::collect_solo()
{
  int ret = OB_SUCCESS;
  const int64_t start = ObTimeUtility::current_time();
  if (OB_ISNULL(solo_ = OB_NEWx(SoloState, &ctx_.get_allocator(), buffered_bytes_, buffer_limit_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(solo_->store_.init(0, ObCtxIds::DEFAULT_CTX_ID, "AISoloRows", false))) {
  }
  ObArenaAllocator scratch("AISoloParse");
  MultimodeAlloctor allocator(scratch);
  while (OB_SUCC(ret) && !input_end_) {
    clear_evaluated_flag();
    const ObBatchRows *batch = nullptr;
    if (OB_FAIL(ctx_.check_status())) {
    } else if (OB_FAIL(child_frame_.restore())) {
    } else if (OB_FAIL(child_->get_next_batch(ai_spec().max_batch_size_, batch))) {
    } else if (batch->size_ > 0 && OB_FAIL(child_frame_.save(batch->size_))) {
    } else {
      ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
      guard.set_batch_size(batch->size_);
      for (int64_t row_index = 0; OB_SUCC(ret) && row_index < batch->size_; ++row_index) {
        if (batch->skip_->at(row_index)) {
          continue;
        }
        guard.set_batch_idx(row_index);
        allocator.reset();
        ObString model_id;
        ObString instruction;
        ObString config_text;
        ObJsonObject *config = nullptr;
        ObJsonObject *fields = nullptr;
        SoloState::Row row;
        if (OB_FAIL(ctx_.check_status())) {
        } else if (OB_FAIL(ObExprAIComplete::prepare_input(*ai_spec().ai_expr_, eval_ctx_, allocator,
                                                          model_id, instruction, config, &fields))) {
        } else if (nullptr == fields) {
          ret = OB_INVALID_ARGUMENT;
        } else if (nullptr != config && OB_FAIL(ObAIFuncJsonUtils::print_json_to_str(allocator, config, config_text))) {
        } else if (solo_->rows_.empty()) {
          if (OB_FAIL(ob_write_string(solo_->memory_, model_id, solo_->model_id_))) {
          } else if (OB_FAIL(ob_write_string(solo_->memory_, instruction, solo_->instruction_))) {
          } else if (OB_FAIL(ob_write_string(solo_->memory_, config_text, solo_->config_))) {
          }
        } else if (model_id != solo_->model_id_ || instruction != solo_->instruction_ ||
                   config_text != solo_->config_ || fields->element_count() != solo_->keys_.count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "global SOLO requires fixed model, instruction, configuration and field names");
        }
        if (OB_SUCC(ret)) {
          const int64_t field_count = fields->element_count();
          if (field_count > buffer_limit_ / static_cast<int64_t>(sizeof(ObString))) {
            ret = OB_SIZE_OVERFLOW;
          } else if (OB_ISNULL(row.fields_ = static_cast<ObString *>(
                                  solo_->memory_.alloc(field_count * sizeof(ObString))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          row.prompt_bytes_ = STRLEN("Answer the below query:\n\nGiven the following data:\n{}") + instruction.length();
          for (int64_t column = 0; OB_SUCC(ret) && column < field_count; ++column) {
            ObString key;
            ObJsonBuffer name(&allocator);
            ObJsonBuffer value(&allocator);
            new (row.fields_ + column) ObString();
            if (OB_FAIL(fields->get_key(column, key))) {
            } else {
              ObJsonString json_key(key);
              if (OB_FAIL(json_key.print(name, true))) {
              } else if (OB_FAIL(fields->get_value(column)->print(value, true))) {
              } else if (OB_FAIL(ob_write_string(solo_->memory_, value.string(), row.fields_[column]))) {
              } else if (solo_->rows_.empty()) {
                ObString saved_key;
                if (OB_FAIL(ob_write_string(solo_->memory_, name.string(), saved_key))) {
                } else if (OB_FAIL(solo_->keys_.push_back(saved_key))) {
                }
              } else if (name.string() != solo_->keys_.at(column)) {
                ret = OB_INVALID_ARGUMENT;
                LOG_USER_ERROR(OB_INVALID_ARGUMENT, "global SOLO requires identical field names in every row");
              }
              row.prompt_bytes_ += name.length() + value.length() + 1 + (column > 0 ? 1 : 0);
              if (OB_SUCC(ret) && row.prompt_bytes_ > ObAIFuncClient::MAX_REQUEST_BYTES) {
                ret = OB_SIZE_OVERFLOW;
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(solo_->store_.add_row(child_->get_spec().output_, &eval_ctx_, &row.data_))) {
        } else if (OB_FAIL(solo_->rows_.push_back(row))) {
        }
      }
      input_end_ = batch->end_;
    }
  }
  const int64_t collected = ObTimeUtility::current_time();
  if (OB_SUCC(ret) && OB_FAIL(solo_->plan(ctx_))) {
  }
  if (OB_SUCC(ret)) {
    solo_->memory_.limit_ = buffer_limit_;
    const int64_t collect_us = collected - start;
    const int64_t plan_us = ObTimeUtility::current_time() - collected;
    LOG_INFO("AI SOLO global plan ready", K(collect_us), K(plan_us), K(solo_->rows_.count()),
             K(solo_->columns_), K(solo_->memory_.peak_), K_(buffered_bytes));
  } else if (nullptr != solo_ && ret == OB_ALLOCATE_MEMORY_FAILED && solo_->memory_.error_ != OB_SUCCESS) {
    ret = solo_->memory_.error_;
  }
  return ret;
}

int AIFuncOp::submit_solo(Slot &slot)
{
  int ret = OB_SUCCESS;
  SoloState &state = *solo_;
  slot.offset_ = state.submitted_;
  ObArray<ObString> contents;
  int64_t bytes = 0;
  for (int64_t position = state.submitted_; OB_SUCC(ret) && position < state.order_.count() &&
       slot.size_ < ai_spec().max_batch_size_; ++position) {
    const SoloState::Row &row = state.rows_.at(state.order_.at(position));
    if (slot.size_ > 0 && row.prompt_bytes_ > ObAIFuncClient::MAX_BATCH_BYTES - bytes) {
      break;
    }
    ObJsonBuffer prompt(&slot.request_allocator_);
    if (OB_FAIL(ctx_.check_status())) {
    } else if (OB_FAIL(reserve(slot, row.prompt_bytes_))) {
    } else if (OB_FAIL(prompt.append("Answer the below query:\n"))) {
    } else if (OB_FAIL(prompt.append(state.instruction_))) {
    } else if (OB_FAIL(prompt.append("\nGiven the following data:\n{"))) {
    }
    for (int64_t index = 0; OB_SUCC(ret) && index < state.columns_.count(); ++index) {
      const int64_t column = state.columns_.at(index);
      if (index > 0 && OB_FAIL(prompt.append(","))) {
      } else if (OB_FAIL(prompt.append(state.keys_.at(column)))) {
      } else if (OB_FAIL(prompt.append(":"))) {
      } else if (OB_FAIL(prompt.append(row.fields_[column]))) {
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(prompt.append("}"))) {
    } else if (OB_FAIL(contents.push_back(prompt.string()))) {
    } else {
      bytes += prompt.length();
      ++slot.size_;
    }
  }
  if (OB_SUCC(ret) && slot.size_ > 0) {
    ObJsonObject *config = nullptr;
    ObAIFuncExprInfo *info = nullptr;
    share::ObAiModelEndpointInfo endpoint;
    auto *resolver = share::server_service<query::ObIAiEndpointResolver>();
    if (!state.config_.empty() && OB_FAIL(ObAIFuncJsonUtils::get_json_object_form_str(
                                            slot.request_allocator_, state.config_, config))) {
    } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(slot.request_allocator_, state.model_id_, info))) {
    } else if (OB_ISNULL(resolver)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(resolver->resolve_by_model_name(state.model_id_, slot.request_allocator_, endpoint))) {
    } else {
      ObAIFuncModel model(slot.request_allocator_, *info, endpoint);
      ret = model.start_completion_batch(contents, config, slot.batch_);
    }
    if (OB_SUCC(ret)) {
      state.submitted_ += slot.size_;
      ++count_;
    }
  }
  return ret;
}

int AIFuncOp::poll_solo()
{
  int ret = OB_SUCCESS;
  for (int64_t index = 0; OB_SUCC(ret) && index < slot_count_; ++index) {
    Slot &slot = *slots_[index];
    bool finished = false;
    if (slot.size_ == 0) {
    } else if (OB_FAIL(slot.batch_.poll(finished))) {
    } else if (finished) {
      ObArray<ObString> results;
      if (OB_FAIL(slot.batch_.get_results(results))) {
      } else if (results.count() != slot.size_) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        for (int64_t offset = 0; OB_SUCC(ret) && offset < results.count(); ++offset) {
          SoloState::Row &row = solo_->rows_.at(solo_->order_.at(slot.offset_ + offset));
          if (OB_FAIL(ob_write_string(solo_->memory_, results.at(offset), row.result_))) {
          } else {
            row.ready_ = true;
          }
        }
      }
      if (OB_SUCC(ret)) {
        reset_slot(slot);
        --count_;
      }
    }
  }
  return ret;
}

int AIFuncOp::next_solo(int64_t max_row_cnt, bool &suspended)
{
  int ret = OB_SUCCESS;
  if (nullptr == solo_ && OB_FAIL(collect_solo())) {
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ctx_.check_status())) {
    } else if (OB_FAIL(poll_solo())) {
    } else if (solo_->output_ == solo_->rows_.count()) {
      brs_.size_ = 0;
      brs_.end_ = true;
      break;
    } else if (solo_->rows_.at(solo_->output_).ready_) {
      clear_evaluated_flag();
      brs_.size_ = 0;
      while (brs_.size_ < max_row_cnt && solo_->output_ + brs_.size_ < solo_->rows_.count() &&
             solo_->rows_.at(solo_->output_ + brs_.size_).ready_) {
        ++brs_.size_;
      }
      brs_.skip_->reset(brs_.size_);
      brs_.all_rows_active_ = true;
      ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
      guard.set_batch_size(brs_.size_);
      const auto &columns = child_->get_spec().output_;
      for (int64_t row_index = 0; OB_SUCC(ret) && row_index < brs_.size_; ++row_index) {
        guard.set_batch_idx(row_index);
        SoloState::Row &row = solo_->rows_.at(solo_->output_ + row_index);
        for (int64_t column = 0; column < columns.count(); ++column) {
          ObExpr &expr = *columns.at(column);
          *expr.locate_expr_datumvector(eval_ctx_).at(row_index) = row.data_->cells()[column];
          expr.get_eval_info(eval_ctx_).evaluated_ = true;
          if (expr.is_batch_result()) {
            expr.get_evaluated_flags(eval_ctx_).set(row_index);
          }
        }
        ObExpr &expr = *ai_spec().ai_expr_;
        if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, eval_ctx_,
                        *expr.locate_expr_datumvector(eval_ctx_).at(row_index), row.result_))) {
        } else {
          expr.get_eval_info(eval_ctx_).evaluated_ = true;
          expr.get_evaluated_flags(eval_ctx_).set(row_index);
        }
      }
      solo_->output_ += brs_.size_;
      break;
    } else {
      Slot *available = nullptr;
      Slot *waiting = nullptr;
      bool admitting = false;
      for (int64_t index = 0; index < slot_count_; ++index) {
        if (slots_[index]->size_ == 0) {
          if (nullptr == available) {
            available = slots_[index];
          }
        } else {
          waiting = slots_[index];
          admitting |= slots_[index]->batch_.is_waiting_for_admission();
        }
      }
      if (nullptr != available && !admitting && solo_->submitted_ < solo_->rows_.count() &&
          (count_ == 0 || ATOMIC_LOAD(&buffered_bytes_) < buffer_limit_ / 2)) {
        ret = submit_solo(*available);
      } else if (nullptr == waiting) {
        ret = OB_ERR_UNEXPECTED;
      } else if (lib::RequestAwait::suspend(&ctx_, this, &AIFuncOp::can_resume)) {
        suspended = true;
        ret = OB_EAGAIN;
        break;
      } else {
        bool finished = false;
        ret = waiting->batch_.poll(finished, 20);
      }
    }
  }
  if (nullptr != solo_ && ret == OB_ALLOCATE_MEMORY_FAILED && solo_->memory_.error_ != OB_SUCCESS) {
    ret = solo_->memory_.error_;
  }
  return ret;
}

int AIFuncOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_) || OB_ISNULL(ai_spec().ai_expr_) || OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ai_spec().ai_expr_->type_ != T_FUN_SYS_AI_EMBED &&
             ai_spec().ai_expr_->type_ != T_FUN_SYS_AI_COMPLETE) {
    ret = OB_NOT_SUPPORTED;
  } else if (ai_spec().solo_ && !spec_.is_vectorized()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "global SOLO requires vectorized execution");
  } else if (spec_.is_vectorized()) {
    if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(share::SYS_VAR_AI_PIPELINE_SLOTS, slot_count_))) {
    } else if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(
                         share::SYS_VAR_AI_PIPELINE_MEMORY_LIMIT, buffer_limit_))) {
    } else if (slot_count_ < 1 || slot_count_ > 1024 || buffer_limit_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(child_frame_.init(child_->get_spec().output_, eval_ctx_, &ctx_.get_allocator()))) {
    } else if (OB_ISNULL(slots_ = static_cast<Slot **>(
                            ctx_.get_allocator().alloc(sizeof(Slot *) * slot_count_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(slots_, 0, sizeof(Slot *) * slot_count_);
    }
  }
  for (int64_t index = 0; OB_SUCC(ret) && nullptr != slots_ && index < slot_count_; ++index) {
    if (OB_ISNULL(slots_[index] = OB_NEWx(Slot, &ctx_.get_allocator()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      slots_[index]->batch_.set_shared_buffer_usage(buffered_bytes_, buffer_limit_);
      slots_[index]->batch_.set_nonblocking_admission();
    }
  }
  return ret;
}

int AIFuncOp::reserve(Slot &slot, int64_t bytes)
{
  const int ret = ObAIFuncClient::reserve_pipeline_buffer(buffered_bytes_, buffer_limit_, bytes);
  if (OB_SUCCESS == ret) {
    slot.retained_bytes_ += bytes;
  }
  return ret;
}

void AIFuncOp::reset_slot(Slot &slot)
{
  slot.batch_.cancel();
  slot.request_allocator_.reset();
  slot.results_.reset();
  slot.data_allocator_.reset();
  ObAIFuncClient::release_pipeline_buffer(buffered_bytes_, slot.retained_bytes_);
  slot.datums_ = nullptr;
  slot.size_ = slot.offset_ = slot.retained_bytes_ = 0;
  slot.ready_ = false;
}

void AIFuncOp::reset_pipeline()
{
  for (int64_t index = 0; nullptr != slots_ && index < slot_count_; ++index) {
    if (nullptr != slots_[index]) {
      reset_slot(*slots_[index]);
    }
  }
  if (nullptr != solo_) {
    OB_DELETEx(SoloState, &ctx_.get_allocator(), solo_);
    solo_ = nullptr;
  }
  head_ = count_ = 0;
  input_end_ = false;
  pending_input_ = nullptr;
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
  for (int64_t index = 0; nullptr != slots_ && index < slot_count_; ++index) {
    OB_DELETEx(Slot, &ctx_.get_allocator(), slots_[index]);
    slots_[index] = nullptr;
  }
  ctx_.get_allocator().free(slots_);
  slots_ = nullptr;
  slot_count_ = 0;
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
  clear_evaluated_flag();
  if (nullptr == pending_input_ &&
      OB_FAIL(child_->get_next_batch(ai_spec().max_batch_size_, pending_input_))) {
  }
  const ObBatchRows *rows = pending_input_;
  if (OB_SUCC(ret)) {
    slot.size_ = rows->size_ - rows->skip_->accumulate_bit_cnt(rows->size_);
  }
  const auto &columns = child_->get_spec().output_;
  int64_t retained_bytes = 0;
  if (OB_FAIL(ret)) {
  } else if (slot.size_ == 0) {
    input_end_ = rows->end_;
    pending_input_ = nullptr;
  } else if (slot.size_ > buffer_limit_ / (columns.count() * sizeof(ObDatum) + sizeof(ObString))) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(child_frame_.save(rows->size_))) {
  } else {
    retained_bytes = slot.size_ * (columns.count() * sizeof(ObDatum) + sizeof(ObString));
    for (int64_t row = 0; OB_SUCC(ret) && row < rows->size_; ++row) {
      if (!rows->skip_->at(row)) {
        for (int64_t column = 0; OB_SUCC(ret) && column < columns.count(); ++column) {
          const int64_t bytes = columns.at(column)->locate_expr_datumvector(eval_ctx_).at(row)->get_deep_copy_size();
          if (bytes > buffer_limit_ - retained_bytes) {
            ret = OB_SIZE_OVERFLOW;
          } else {
            retained_bytes += bytes;
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(reserve(slot, retained_bytes)) && count_ > 0 && ret == OB_SIZE_OVERFLOW) {
      ret = OB_EAGAIN;
    }
  }
  if (OB_SUCC(ret) && slot.size_ > 0) {
    if (OB_ISNULL(slot.datums_ = static_cast<ObDatum *>(slot.data_allocator_.alloc(
                                       slot.size_ * columns.count() * sizeof(ObDatum))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
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
        if (OB_FAIL(slot.datums_[target_row * columns.count() + column].deep_copy(
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
      input_end_ = rows->end_;
      pending_input_ = nullptr;
      ++count_;
    }
  }
  return ret;
}

int AIFuncOp::poll()
{
  int ret = OB_SUCCESS;
  for (int64_t index = 0; OB_SUCC(ret) && index < count_; ++index) {
    Slot &slot = *slots_[(head_ + index) % slot_count_];
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

bool AIFuncOp::can_resume(const void *state)
{
  const auto &operation = *static_cast<const AIFuncOp *>(state);
  bool ready = false;
  if (nullptr != operation.solo_) {
    for (int64_t index = 0; !ready && index < operation.slot_count_; ++index) {
      const Slot &slot = *operation.slots_[index];
      ready = slot.size_ > 0 && slot.batch_.is_ready();
    }
  }
  for (int64_t index = 0; nullptr == operation.solo_ && !ready && index < operation.count_; ++index) {
    const Slot &slot = *operation.slots_[(operation.head_ + index) % operation.slot_count_];
    ready = !slot.ready_ && slot.batch_.is_ready();
  }
  return ready;
}

int AIFuncOp::inner_get_next_batch(int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  bool suspended = false;
  bool memory_backpressure = false;
  if (nullptr != lib::RequestAwait::current() &&
      OB_FAIL(lib::RequestAwait::current()->cancel_ret())) {
  } else if (OB_FAIL(child_frame_.restore())) {
  } else if (ctx_.get_my_session()->is_diagnosis_enabled()) {
    const ObBatchRows *rows = nullptr;
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_batch(max_row_cnt, rows))) {
    } else {
      brs_ = *rows;
    }
  } else if (ai_spec().solo_) {
    ret = next_solo(max_row_cnt, suspended);
  } else {
    if (count_ > 0 && slots_[head_]->offset_ == slots_[head_]->size_) {
      reset_slot(*slots_[head_]);
      head_ = (head_ + 1) % slot_count_;
      --count_;
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ctx_.check_status())) {
      } else if (OB_FAIL(poll())) {
      } else if (count_ > 0 && slots_[head_]->ready_) {
        ret = output(*slots_[head_], max_row_cnt);
        break;
      } else if (!input_end_ && count_ < slot_count_ && !memory_backpressure &&
                 (count_ == 0 || (ATOMIC_LOAD(&buffered_bytes_) < buffer_limit_ / 2 &&
                                 !slots_[(head_ + count_ - 1) % slot_count_]->batch_.is_waiting_for_admission()))) {
        ret = submit(*slots_[(head_ + count_) % slot_count_]);
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
          memory_backpressure = true;
        }
      } else if (count_ == 0) {
        brs_.end_ = true;
        break;
      } else if (lib::RequestAwait::suspend(&ctx_, this, &AIFuncOp::can_resume)) {
        suspended = true;
        ret = OB_EAGAIN;
        break;
      } else {
        bool finished = false;
        ret = slots_[head_]->batch_.poll(finished, 20);
      }
    }
  }
  if (OB_FAIL(ret) && !suspended) {
    reset_pipeline();
  }
  return ret;
}

}
}