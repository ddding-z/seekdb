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
#include "sql/engine/basic/ob_semantic_aggregation.h"
#include "lib/allocator/ob_malloc.h"
#include <limits>
#include <new>

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{
const char PROMPT_PREFIX[] = "{\"operation\":\"ai_agg\",\"phase\":\"";
const char INSTRUCTION_PREFIX[] = "\",\"instruction\":";
const char INPUTS_PREFIX[] = ",\"inputs\":[";
const char PROMPT_SUFFIX[] = "],\"source_count\":";

const char *phase_name(SemanticAggregationRequest::Phase phase)
{
  const char *name = nullptr;
  switch (phase) {
    case SemanticAggregationRequest::DIRECT: name = "direct"; break;
    case SemanticAggregationRequest::EXTRACT: name = "extract"; break;
    case SemanticAggregationRequest::COMBINE: name = "combine"; break;
    case SemanticAggregationRequest::FINALIZE: name = "finalize"; break;
  }
  return name;
}

int64_t decimal_bytes(int64_t value)
{
  int64_t bytes = 1;
  while (value >= 10) {
    value /= 10;
    ++bytes;
  }
  return bytes;
}

// Sizing and emission share one allocation-free JSON encoder, including control bytes.
int64_t json_character(unsigned char value, char *destination)
{
  int64_t bytes = 1;
  char escaped = 0;
  switch (value) {
    case '\b': escaped = 'b'; break;
    case '\t': escaped = 't'; break;
    case '\n': escaped = 'n'; break;
    case '\f': escaped = 'f'; break;
    case '\r': escaped = 'r'; break;
    case '"': escaped = '"'; break;
    case '\\': escaped = '\\'; break;
    default: break;
  }
  if (escaped != 0) {
    bytes = 2;
    if (destination != nullptr) {
      destination[0] = '\\';
      destination[1] = escaped;
    }
  } else if (value < 0x20) {
    bytes = 6;
    if (destination != nullptr) {
      static const char HEX[] = "0123456789abcdef";
      MEMCPY(destination, "\\u00", 4);
      destination[4] = HEX[value >> 4];
      destination[5] = HEX[value & 0xf];
    }
  } else if (destination != nullptr) {
    destination[0] = static_cast<char>(value);
  }
  return bytes;
}

int64_t quoted_bytes(const ObString &value)
{
  int64_t bytes = 2;
  for (int64_t index = 0; index < value.length(); ++index) {
    bytes += json_character(static_cast<unsigned char>(value.ptr()[index]), nullptr);
  }
  return bytes;
}

int64_t write_quoted(char *destination, const ObString &value)
{
  int64_t position = 0;
  destination[position++] = '"';
  for (int64_t index = 0; index < value.length(); ++index) {
    position += json_character(static_cast<unsigned char>(value.ptr()[index]),
                               destination + position);
  }
  destination[position++] = '"';
  return position;
}

void quote_in_place(char *buffer, int64_t source_bytes, int64_t destination_offset,
                    int64_t quoted_length)
{
  int64_t position = destination_offset + quoted_length;
  buffer[--position] = '"';
  for (int64_t index = source_bytes; index > 0; --index) {
    char encoded[6];
    const int64_t bytes = json_character(static_cast<unsigned char>(buffer[index - 1]), encoded);
    position -= bytes;
    MEMCPY(buffer + position, encoded, bytes);
  }
  buffer[destination_offset] = '"';
}

bool valid_text(const ObString &value)
{
  return value.length() >= 0 && (value.length() == 0 || value.ptr() != nullptr);
}
}

SemanticAggregation::SemanticAggregation()
    : budget_(), limits_(), instruction_(), raw_(), result_(), head_(nullptr), tail_(nullptr),
      raw_count_(0), source_count_(0), chunk_count_(0), call_count_(0), retained_bytes_(0),
      failure_(OB_SUCCESS), initialized_(false), large_(false), sealed_(false), ready_(false),
      waiting_memory_(false)
{
}

SemanticAggregation::~SemanticAggregation()
{
  reset();
}

int SemanticAggregation::init(const ObString &instruction,
                              const SemanticAggregationLimits &limits,
                              const std::shared_ptr<SemanticQueryBudget> &budget)
{
  int ret = OB_SUCCESS;
  if (initialized_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("semantic aggregation already initialized", K(ret));
  } else if (!budget || !valid_text(instruction) ||
             limits.request_input_bytes_ <= 0 ||
             limits.request_input_bytes_ > SemanticAggregationLimits::MAX_REQUEST_INPUT_BYTES ||
             limits.max_partial_bytes_ <= 0 ||
             limits.max_partial_bytes_ > SemanticAggregationLimits::MAX_PARTIAL_BYTES ||
             limits.max_calls_ <= 0 || limits.max_calls_ > SemanticAggregationLimits::MAX_CALLS ||
             limits.max_levels_ <= 0 || limits.max_levels_ > SemanticAggregationLimits::MAX_LEVELS ||
             budget->memory_limit() <= 0 || budget->task_limit() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid semantic aggregation limits or instruction", K(ret),
             "input_bytes", limits.request_input_bytes_, "partial_bytes", limits.max_partial_bytes_,
             "calls", limits.max_calls_, "levels", limits.max_levels_);
  } else if (instruction.length() > limits.request_input_bytes_ ||
             quoted_bytes(instruction) > limits.request_input_bytes_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("semantic aggregation instruction exceeds request budget", K(ret));
  } else {
    budget_ = budget;
    limits_ = limits;
    if (OB_FAIL(reserve(sizeof(*this), false))) {
    } else if (OB_FAIL(resize_buffer(instruction_, quoted_bytes(instruction), false))) {
    } else {
      write_quoted(instruction_.data_, instruction);
      if (prompt_bytes(SemanticAggregationRequest::DIRECT, 0, 0) >
          limits_.request_input_bytes_) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        initialized_ = true;
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("initialize semantic aggregation storage failed", K(ret));
      reset();
    }
  }
  return ret;
}

int SemanticAggregation::status() const
{
  int ret = OB_SUCCESS;
  if (!initialized_) {
    ret = OB_NOT_INIT;
    LOG_WARN("semantic aggregation not initialized", K(ret));
  } else if (failure_ != OB_SUCCESS) {
    ret = failure_;
  } else if (budget_->failure() != OB_SUCCESS) {
    ret = budget_->failure();
    LOG_WARN("semantic aggregation query budget has failed", K(ret));
  }
  return ret;
}

int SemanticAggregation::fail(int ret, const char *reason)
{
  if (ret != OB_SUCCESS && ret != OB_EAGAIN) {
    failure_ = ret;
    LOG_WARN("semantic aggregation failed", K(ret), K(reason), K(call_count_), K(chunk_count_),
             K(retained_bytes_));
  }
  return ret;
}

int SemanticAggregation::reserve(int64_t bytes, bool allow_retry)
{
  int ret = budget_->reserve(bytes);
  if (ret == OB_SUCCESS) {
    retained_bytes_ += bytes;
    waiting_memory_ = false;
  } else if (ret == OB_SIZE_OVERFLOW && allow_retry && has_pending() &&
             bytes <= budget_->memory_limit()) {
    waiting_memory_ = true;
    ret = OB_EAGAIN;
  }
  return ret;
}

void SemanticAggregation::release(int64_t bytes)
{
  if (bytes > 0) {
    budget_->release(bytes);
    retained_bytes_ -= bytes;
  }
}

int SemanticAggregation::resize_buffer(Buffer &buffer, int64_t length, bool allow_retry)
{
  int ret = OB_SUCCESS;
  if (length == 0) {
    free_buffer(buffer);
  } else {
    const int64_t old_bytes = buffer.data_ == nullptr ? 0 : buffer.length_ + 1;
    const int64_t new_bytes = length + 1;
    const int64_t extra = new_bytes > old_bytes ? new_bytes - old_bytes : 0;
    if (extra > 0 && OB_FAIL(reserve(extra, allow_retry))) {
    } else {
      char *data = buffer.data_ == nullptr
          ? static_cast<char *>(ob_malloc(new_bytes, ObMemAttr("SemanticAgg")))
          : static_cast<char *>(ob_realloc(buffer.data_, new_bytes, ObMemAttr("SemanticAgg")));
      if (data == nullptr) {
        release(extra);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        if (old_bytes > new_bytes) {
          release(old_bytes - new_bytes);
        }
        buffer.data_ = data;
        buffer.length_ = length;
        buffer.data_[length] = '\0';
      }
    }
  }
  return ret;
}

void SemanticAggregation::free_buffer(Buffer &buffer)
{
  if (buffer.data_ != nullptr) {
    ob_free(buffer.data_);
    release(buffer.length_ + 1);
    buffer.data_ = nullptr;
  }
  buffer.length_ = 0;
}

int SemanticAggregation::allocate_node(Node *&node, bool allow_retry)
{
  int ret = OB_SUCCESS;
  node = nullptr;
  if (OB_FAIL(reserve(sizeof(Node), allow_retry))) {
  } else {
    void *memory = ob_malloc(sizeof(Node), ObMemAttr("SemanticAgg"));
    if (memory == nullptr) {
      release(sizeof(Node));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      node = new (memory) Node();
    }
  }
  return ret;
}

void SemanticAggregation::free_node(Node *node)
{
  if (node != nullptr) {
    free_buffer(node->text_);
    node->~Node();
    ob_free(node);
    release(sizeof(Node));
  }
}

int64_t SemanticAggregation::prefix_bytes(SemanticAggregationRequest::Phase phase) const
{
  return sizeof(PROMPT_PREFIX) - 1 + STRLEN(phase_name(phase)) +
         sizeof(INSTRUCTION_PREFIX) - 1 + instruction_.length_ + sizeof(INPUTS_PREFIX) - 1;
}

int64_t SemanticAggregation::prompt_bytes(SemanticAggregationRequest::Phase phase,
                                        int64_t inputs_bytes, int64_t source_count) const
{
  return prefix_bytes(phase) + inputs_bytes + sizeof(PROMPT_SUFFIX) - 1 +
         decimal_bytes(source_count) + 1;
}

int64_t SemanticAggregation::write_prefix(char *destination,
                                        SemanticAggregationRequest::Phase phase) const
{
  int64_t position = 0;
  const char *name = phase_name(phase);
  const int64_t name_bytes = STRLEN(name);
  MEMCPY(destination + position, PROMPT_PREFIX, sizeof(PROMPT_PREFIX) - 1);
  position += sizeof(PROMPT_PREFIX) - 1;
  MEMCPY(destination + position, name, name_bytes);
  position += name_bytes;
  MEMCPY(destination + position, INSTRUCTION_PREFIX, sizeof(INSTRUCTION_PREFIX) - 1);
  position += sizeof(INSTRUCTION_PREFIX) - 1;
  MEMCPY(destination + position, instruction_.data_, instruction_.length_);
  position += instruction_.length_;
  MEMCPY(destination + position, INPUTS_PREFIX, sizeof(INPUTS_PREFIX) - 1);
  position += sizeof(INPUTS_PREFIX) - 1;
  return position;
}

int64_t SemanticAggregation::write_suffix(char *destination, int64_t source_count) const
{
  int64_t position = sizeof(PROMPT_SUFFIX) - 1;
  MEMCPY(destination, PROMPT_SUFFIX, position);
  const int64_t digits = decimal_bytes(source_count);
  for (int64_t index = digits; index > 0; --index) {
    destination[position + index - 1] = static_cast<char>('0' + source_count % 10);
    source_count /= 10;
  }
  position += digits;
  destination[position++] = '}';
  return position;
}

int SemanticAggregation::raw_prefix(SemanticAggregationRequest::Phase phase,
                                   int64_t &bytes, int64_t &source_count) const
{
  int ret = OB_SUCCESS;
  bytes = raw_.length_;
  source_count = raw_count_;
  if (prompt_bytes(phase, bytes, source_count) > limits_.request_input_bytes_) {
    bytes = 0;
    source_count = 0;
    int64_t position = 0;
    int64_t count = 0;
    while (position < raw_.length_) {
      if (raw_.data_[position++] != '"') {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      bool closed = false;
      while (position < raw_.length_ && !closed) {
        const char value = raw_.data_[position++];
        if (value == '\\') {
          ++position;
        } else if (value == '"') {
          closed = true;
        }
      }
      if (!closed || position > raw_.length_) {
        ret = OB_ERR_UNEXPECTED;
        break;
      } else if (prompt_bytes(phase, position, ++count) > limits_.request_input_bytes_) {
        break;
      } else {
        bytes = position;
        source_count = count;
        if (position < raw_.length_ && raw_.data_[position++] != ',') {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
      }
    }
    if (OB_SUCC(ret) && source_count == 0) {
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int SemanticAggregation::flush_raw(SemanticAggregationRequest::Phase phase)
{
  int ret = OB_SUCCESS;
  Node *node = nullptr;
  Buffer remainder;
  int64_t input_bytes = 0;
  int64_t count = 0;
  if (raw_count_ == 0) {
  } else if (call_count_ >= limits_.max_calls_) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(raw_prefix(phase, input_bytes, count))) {
  } else if (phase == SemanticAggregationRequest::DIRECT && count != raw_count_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(allocate_node(node, true))) {
  } else {
    const int64_t remainder_bytes = count == raw_count_ ? 0 : raw_.length_ - input_bytes - 1;
    const int64_t bytes = prompt_bytes(phase, input_bytes, count);
    if (remainder_bytes > 0 && OB_FAIL(resize_buffer(remainder, remainder_bytes, true))) {
    } else {
      if (remainder_bytes > 0) {
        MEMCPY(remainder.data_, raw_.data_ + input_bytes + 1, remainder_bytes);
      }
      if (OB_FAIL(resize_buffer(raw_, bytes, true))) {
      } else {
        const int64_t prefix = prefix_bytes(phase);
        MEMMOVE(raw_.data_ + prefix, raw_.data_, input_bytes);
        const int64_t actual = write_prefix(raw_.data_, phase) + input_bytes +
                               write_suffix(raw_.data_ + prefix + input_bytes, count);
        if (actual != bytes) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          node->first_chunk_ = chunk_count_;
          node->chunk_count_ = phase == SemanticAggregationRequest::EXTRACT ? 1 : 0;
          node->source_count_ = count;
          node->phase_ = phase;
          node->id_ = phase == SemanticAggregationRequest::DIRECT ? 1 : (chunk_count_ + 1) * 64;
          node->text_ = raw_;
          raw_ = remainder;
          remainder = Buffer();
          raw_count_ -= count;
          if (tail_ != nullptr) {
            tail_->next_ = node;
          } else {
            head_ = node;
          }
          tail_ = node;
          node = nullptr;
          ++call_count_;
          if (phase == SemanticAggregationRequest::EXTRACT) {
            ++chunk_count_;
          }
        }
      }
    }
  }
  free_buffer(remainder);
  free_node(node);
  return ret;
}

int SemanticAggregation::append(const ObString &text)
{
  int ret = status();
  if (OB_FAIL(ret)) {
  } else if (sealed_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("append to sealed semantic aggregation", K(ret));
  } else if (!valid_text(text)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid semantic aggregation input", K(ret));
  } else if (source_count_ == std::numeric_limits<int64_t>::max() ||
             text.length() > limits_.request_input_bytes_) {
    ret = fail(OB_SIZE_OVERFLOW, "input row or source count exceeds limit");
  } else {
    const int64_t bytes = quoted_bytes(text);
    const auto phase = large_ ? SemanticAggregationRequest::EXTRACT
                              : SemanticAggregationRequest::DIRECT;
    if (prompt_bytes(phase, bytes, 1) > limits_.request_input_bytes_) {
      ret = fail(OB_SIZE_OVERFLOW, "single input row cannot fit a complete prompt");
    } else {
      const int64_t combined_bytes = raw_.length_ + (raw_count_ == 0 ? 0 : 1) + bytes;
      if (!large_ && prompt_bytes(phase, combined_bytes, raw_count_ + 1) >
                     limits_.request_input_bytes_) {
        large_ = true;
      }
      if (large_ && prompt_bytes(SemanticAggregationRequest::EXTRACT, bytes, 1) >
                    limits_.request_input_bytes_) {
        ret = OB_SIZE_OVERFLOW;
      }
      while (OB_SUCC(ret) && raw_count_ > 0 && large_ &&
             prompt_bytes(SemanticAggregationRequest::EXTRACT,
                          raw_.length_ + 1 + bytes, raw_count_ + 1) >
             limits_.request_input_bytes_) {
        ret = flush_raw(SemanticAggregationRequest::EXTRACT);
      }
      if (OB_SUCC(ret)) {
        const int64_t previous = raw_.length_;
        const int64_t separator = raw_count_ == 0 ? 0 : 1;
        if (OB_FAIL(resize_buffer(raw_, previous + separator + bytes, true))) {
        } else {
          if (separator != 0) {
            raw_.data_[previous] = ',';
          }
          write_quoted(raw_.data_ + previous + separator, text);
          ++raw_count_;
          ++source_count_;
        }
      }
      if (ret != OB_SUCCESS && ret != OB_EAGAIN) {
        ret = fail(ret, "retain or extract aggregation input; no progress is possible");
      }
    }
  }
  return ret;
}

bool SemanticAggregation::siblings(const Node &left, const Node &right) const
{
  bool eligible = false;
  if (left.state_ == Node::COMPLETE && right.state_ == Node::COMPLETE &&
      left.level_ < SemanticAggregationLimits::MAX_LEVELS) {
    const int64_t span = int64_t(1) << left.level_;
    if (left.chunk_count_ == span && left.first_chunk_ % (2 * span) == 0 &&
        right.first_chunk_ == left.first_chunk_ + span) {
      eligible = right.level_ == left.level_ && right.chunk_count_ == span;
      // EOF folds the rightmost incomplete subtree into its fixed aligned left sibling.
      if (!eligible && sealed_ && raw_count_ == 0 && right.level_ <= left.level_ &&
          right.chunk_count_ <= span && right.first_chunk_ + right.chunk_count_ == chunk_count_) {
        eligible = true;
      }
    }
  }
  return eligible;
}

int SemanticAggregation::combine(Node &left, Node &right)
{
  int ret = OB_SUCCESS;
  const int64_t level = left.level_ + 1;
  const int64_t count = left.source_count_ + right.source_count_;
  const int64_t left_length = left.text_.length_;
  const int64_t left_bytes = quoted_bytes(left.text_.string());
  const int64_t right_bytes = quoted_bytes(right.text_.string());
  const int64_t bytes = prompt_bytes(SemanticAggregationRequest::COMBINE,
                                    left_bytes + 1 + right_bytes, count);
  if (level >= limits_.max_levels_ || call_count_ >= limits_.max_calls_ ||
      bytes > limits_.request_input_bytes_) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(resize_buffer(left.text_, bytes, true))) {
  } else {
    const int64_t prefix = prefix_bytes(SemanticAggregationRequest::COMBINE);
    quote_in_place(left.text_.data_, left_length, prefix, left_bytes);
    write_prefix(left.text_.data_, SemanticAggregationRequest::COMBINE);
    left.text_.data_[prefix + left_bytes] = ',';
    const int64_t actual = prefix + left_bytes + 1 +
        write_quoted(left.text_.data_ + prefix + left_bytes + 1, right.text_.string()) +
        write_suffix(left.text_.data_ + prefix + left_bytes + 1 + right_bytes, count);
    if (actual != bytes) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      left.chunk_count_ += right.chunk_count_;
      left.source_count_ = count;
      left.level_ = level;
      left.id_ = (left.first_chunk_ + 1) * 64 + level;
      left.phase_ = SemanticAggregationRequest::COMBINE;
      left.state_ = Node::QUEUED;
      left.next_ = right.next_;
      if (tail_ == &right) {
        tail_ = &left;
      }
      free_node(&right);
      ++call_count_;
    }
  }
  return ret;
}

int SemanticAggregation::finalize(Node &node)
{
  int ret = OB_SUCCESS;
  const int64_t source_bytes = node.text_.length_;
  const int64_t input_bytes = quoted_bytes(node.text_.string());
  const int64_t bytes = prompt_bytes(SemanticAggregationRequest::FINALIZE,
                                    input_bytes, node.source_count_);
  if (node.level_ + 1 >= limits_.max_levels_ || call_count_ >= limits_.max_calls_ ||
      bytes > limits_.request_input_bytes_) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(resize_buffer(node.text_, bytes, true))) {
  } else {
    const int64_t prefix = prefix_bytes(SemanticAggregationRequest::FINALIZE);
    quote_in_place(node.text_.data_, source_bytes, prefix, input_bytes);
    const int64_t actual = write_prefix(node.text_.data_, SemanticAggregationRequest::FINALIZE) +
        input_bytes + write_suffix(node.text_.data_ + prefix + input_bytes, node.source_count_);
    if (actual != bytes) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      node.id_ = 2;
      node.phase_ = SemanticAggregationRequest::FINALIZE;
      node.state_ = Node::QUEUED;
      ++node.level_;
      ++call_count_;
    }
  }
  return ret;
}

int SemanticAggregation::drive()
{
  int ret = OB_SUCCESS;
  if (sealed_ && raw_count_ != 0) {
    if (!large_) {
      ret = flush_raw(SemanticAggregationRequest::DIRECT);
    } else {
      while (OB_SUCC(ret) && raw_count_ != 0) {
        ret = flush_raw(SemanticAggregationRequest::EXTRACT);
      }
    }
  }
  bool merged = true;
  while (OB_SUCC(ret) && merged) {
    merged = false;
    for (Node *node = head_; OB_SUCC(ret) && node != nullptr && node->next_ != nullptr;
         node = node->next_) {
      if (siblings(*node, *node->next_)) {
        ret = combine(*node, *node->next_);
        merged = OB_SUCC(ret);
        break;
      }
    }
  }
  if (OB_SUCC(ret) && sealed_ && raw_count_ == 0 && head_ != nullptr &&
      head_->next_ == nullptr && head_->state_ == Node::COMPLETE &&
      head_->phase_ != SemanticAggregationRequest::DIRECT &&
      head_->phase_ != SemanticAggregationRequest::FINALIZE) {
    if (head_->first_chunk_ != 0 || head_->chunk_count_ != chunk_count_ ||
        head_->source_count_ != source_count_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = finalize(*head_);
    }
  }
  return ret;
}

int SemanticAggregation::seal()
{
  int ret = status();
  if (OB_SUCC(ret)) {
    sealed_ = true;
    if (source_count_ == 0) {
      ready_ = true;
    } else if (OB_FAIL(drive())) {
      if (ret == OB_EAGAIN) {
        ret = OB_SUCCESS;
      } else {
        ret = fail(ret, "seal aggregation or finalize its root");
      }
    }
  }
  return ret;
}

int SemanticAggregation::peek_request(SemanticAggregationRequest &request, bool &available)
{
  int ret = status();
  request = SemanticAggregationRequest();
  available = false;
  if (OB_SUCC(ret) && !ready_) {
    waiting_memory_ = false;
    if (OB_FAIL(drive())) {
      if (ret == OB_EAGAIN) {
        ret = OB_SUCCESS;
      } else {
        ret = fail(ret, "prepare aggregation dependency; no progress is possible");
      }
    }
    if (OB_SUCC(ret)) {
      for (Node *node = head_; node != nullptr && !available; node = node->next_) {
        if (node->state_ == Node::QUEUED) {
          request.id_ = node->id_;
          request.phase_ = node->phase_;
          request.prompt_ = node->text_.string();
          request.source_count_ = node->source_count_;
          request.level_ = node->level_;
          available = true;
        }
      }
    }
  }
  return ret;
}

SemanticAggregation::Node *SemanticAggregation::find(int64_t request_id) const
{
  Node *found = nullptr;
  for (Node *node = head_; node != nullptr && found == nullptr; node = node->next_) {
    if (node->id_ == request_id) {
      found = node;
    }
  }
  return found;
}

int SemanticAggregation::mark_submitted(int64_t request_id)
{
  int ret = status();
  if (OB_SUCC(ret)) {
    Node *node = find(request_id);
    if (node == nullptr) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (node->state_ != Node::QUEUED) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      node->state_ = Node::SUBMITTED;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("invalid semantic aggregation request submission", K(ret), K(request_id));
    }
  }
  return ret;
}

int SemanticAggregation::next_request(SemanticAggregationRequest &request, bool &available)
{
  int ret = peek_request(request, available);
  if (OB_SUCC(ret) && available && OB_FAIL(mark_submitted(request.id_))) {
    available = false;
    request = SemanticAggregationRequest();
  }
  return ret;
}

int SemanticAggregation::rollback_submission(int64_t request_id)
{
  int ret = status();
  if (OB_SUCC(ret)) {
    Node *node = find(request_id);
    if (node == nullptr) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (node->state_ != Node::SUBMITTED) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      node->state_ = Node::QUEUED;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("invalid semantic aggregation submission rollback", K(ret), K(request_id));
    }
  }
  return ret;
}

int SemanticAggregation::complete(int64_t request_id, const ObString &output)
{
  int ret = status();
  Node *node = nullptr;
  if (OB_FAIL(ret)) {
  } else if ((node = find(request_id)) == nullptr) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("unknown semantic aggregation completion", K(ret), K(request_id));
  } else if (node->state_ != Node::SUBMITTED) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("semantic aggregation completion without submission", K(ret), K(request_id));
  } else if (!valid_text(output)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid semantic aggregation output", K(ret), K(request_id));
  } else if (output.length() > limits_.max_partial_bytes_) {
    ret = fail(OB_SIZE_OVERFLOW, "aggregation output exceeds partial byte limit");
  } else {
    const uintptr_t source = reinterpret_cast<uintptr_t>(output.ptr());
    const uintptr_t owned = reinterpret_cast<uintptr_t>(node->text_.data_);
    const bool aliases = node->text_.data_ != nullptr && source >= owned &&
                         source - owned <= static_cast<uint64_t>(node->text_.length_) &&
                         output.length() <= node->text_.length_ - static_cast<int64_t>(source - owned);
    if (aliases && output.length() > 0) {
      MEMMOVE(node->text_.data_, output.ptr(), output.length());
    }
    if (OB_FAIL(resize_buffer(node->text_, output.length(), false))) {
      ret = fail(ret, "retain completed aggregation output");
    } else {
      if (!aliases && output.length() > 0) {
        MEMCPY(node->text_.data_, output.ptr(), output.length());
      }
      node->state_ = Node::COMPLETE;
      if (node->phase_ == SemanticAggregationRequest::DIRECT ||
          node->phase_ == SemanticAggregationRequest::FINALIZE) {
        if (!sealed_) {
          ret = fail(OB_ERR_UNEXPECTED, "aggregation finalized before seal");
        } else {
          result_ = node->text_.string();
          ready_ = true;
        }
      } else if (OB_FAIL(drive())) {
        if (ret == OB_EAGAIN) {
          ret = OB_SUCCESS;
        } else {
          ret = fail(ret, "combine aggregation outputs; no progress is possible");
        }
      }
    }
  }
  return ret;
}

bool SemanticAggregation::ready() const
{
  return initialized_ && sealed_ && ready_ && failure_ == OB_SUCCESS &&
         budget_->failure() == OB_SUCCESS;
}

bool SemanticAggregation::has_pending() const
{
  bool pending = false;
  for (Node *node = head_; node != nullptr && !pending; node = node->next_) {
    pending = node->state_ != Node::COMPLETE;
  }
  return pending;
}

bool SemanticAggregation::has_queued() const
{
  bool queued = false;
  for (Node *node = head_; node != nullptr && !queued; node = node->next_) {
    queued = node->state_ == Node::QUEUED;
  }
  return queued;
}

bool SemanticAggregation::needs_input_backpressure() const
{
  return initialized_ && !sealed_ && large_ && failure_ == OB_SUCCESS &&
      (has_queued() || waiting_memory_ ||
       (has_pending() && (!budget_->task_available() ||
          budget_->memory_limit() - budget_->memory_used() <
              limits_.request_input_bytes_ + static_cast<int64_t>(sizeof(Node)))));
}

void SemanticAggregation::reset()
{
  while (head_ != nullptr) {
    Node *node = head_;
    head_ = node->next_;
    free_node(node);
  }
  free_buffer(raw_);
  free_buffer(instruction_);
  if (budget_ && retained_bytes_ != 0) {
    release(retained_bytes_);
  }
  budget_.reset();
  limits_ = SemanticAggregationLimits();
  result_.reset();
  tail_ = nullptr;
  raw_count_ = source_count_ = chunk_count_ = call_count_ = retained_bytes_ = 0;
  failure_ = OB_SUCCESS;
  initialized_ = large_ = sealed_ = ready_ = waiting_memory_ = false;
}

}
}
