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

#ifndef OCEANBASE_SQL_ENGINE_SEMANTIC_AGGREGATION_H_
#define OCEANBASE_SQL_ENGINE_SEMANTIC_AGGREGATION_H_

#include "lib/string/ob_string.h"
#include "sql/engine/basic/ob_semantic_runtime.h"

namespace oceanbase
{
namespace sql
{

struct SemanticAggregationLimits
{
  static constexpr int64_t MAX_REQUEST_INPUT_BYTES = 4 * 1024 * 1024;
  static constexpr int64_t MAX_PARTIAL_BYTES = 8 * 1024 * 1024;
  static constexpr int64_t MAX_CALLS = 1024 * 1024;
  static constexpr int64_t MAX_LEVELS = 63;

  SemanticAggregationLimits(int64_t input_bytes = 16 * 1024,
                            int64_t partial_bytes = 1024,
                            int64_t calls = 4096,
                            int64_t levels = 32)
      : request_input_bytes_(input_bytes), max_partial_bytes_(partial_bytes),
        max_calls_(calls), max_levels_(levels) {}

  int64_t request_input_bytes_;
  int64_t max_partial_bytes_;
  int64_t max_calls_;
  // Request levels are zero-based; DIRECT and EXTRACT are at level zero.
  int64_t max_levels_;
};

struct SemanticAggregationRequest
{
  enum Phase { DIRECT, EXTRACT, COMBINE, FINALIZE };
  SemanticAggregationRequest()
      : id_(0), phase_(DIRECT), prompt_(), source_count_(0), level_(0) {}

  int64_t id_;
  Phase phase_;
  common::ObString prompt_;
  int64_t source_count_;
  int64_t level_;
};

// All mutations belong to the owning SQL worker. This class owns no task credits.
class SemanticAggregation
{
public:
  SemanticAggregation();
  ~SemanticAggregation();
  int init(const common::ObString &instruction, const SemanticAggregationLimits &limits,
           const std::shared_ptr<SemanticQueryBudget> &budget);
  // OB_EAGAIN means this row was not accepted; dispatch/poll pending work and retry it.
  int append(const common::ObString &text);
  int seal();
  // Prompt views remain valid until this request completes or reset() is called.
  int peek_request(SemanticAggregationRequest &request, bool &available);
  int mark_submitted(int64_t request_id);
  // Acquire a parent task credit before this consuming convenience call.
  int next_request(SemanticAggregationRequest &request, bool &available);
  int rollback_submission(int64_t request_id);
  int complete(int64_t request_id, const common::ObString &output);
  bool ready() const;
  const common::ObString &result() const { return result_; }
  bool has_input() const { return source_count_ != 0; }
  bool has_pending() const;
  bool request_available() const { return has_queued(); }
  bool has_deferred_work() const { return waiting_memory_; }
  bool needs_input_backpressure() const;
  void reset();

private:
  struct Buffer
  {
    Buffer() : data_(nullptr), length_(0) {}
    common::ObString string() const { return common::ObString(length_, data_); }
    char *data_;
    int64_t length_;
  };

  struct Node
  {
    enum State { QUEUED, SUBMITTED, COMPLETE };
    Node()
        : next_(nullptr), first_chunk_(0), chunk_count_(0), source_count_(0), level_(0),
          id_(0), phase_(SemanticAggregationRequest::EXTRACT), state_(QUEUED), text_() {}
    Node *next_;
    int64_t first_chunk_;
    int64_t chunk_count_;
    int64_t source_count_;
    int64_t level_;
    int64_t id_;
    SemanticAggregationRequest::Phase phase_;
    State state_;
    Buffer text_;
  };

  int status() const;
  int fail(int ret, const char *reason);
  int reserve(int64_t bytes, bool allow_retry);
  void release(int64_t bytes);
  int resize_buffer(Buffer &buffer, int64_t length, bool allow_retry);
  void free_buffer(Buffer &buffer);
  int allocate_node(Node *&node, bool allow_retry);
  void free_node(Node *node);
  int64_t prompt_bytes(SemanticAggregationRequest::Phase phase, int64_t inputs_bytes,
                       int64_t source_count) const;
  int64_t prefix_bytes(SemanticAggregationRequest::Phase phase) const;
  int64_t write_prefix(char *destination, SemanticAggregationRequest::Phase phase) const;
  int64_t write_suffix(char *destination, int64_t source_count) const;
  int raw_prefix(SemanticAggregationRequest::Phase phase,
                 int64_t &bytes, int64_t &source_count) const;
  int flush_raw(SemanticAggregationRequest::Phase phase);
  bool siblings(const Node &left, const Node &right) const;
  int combine(Node &left, Node &right);
  int finalize(Node &node);
  int drive();
  Node *find(int64_t request_id) const;
  bool has_queued() const;

  std::shared_ptr<SemanticQueryBudget> budget_;
  SemanticAggregationLimits limits_;
  Buffer instruction_;
  Buffer raw_;
  common::ObString result_;
  Node *head_;
  Node *tail_;
  int64_t raw_count_;
  int64_t source_count_;
  int64_t chunk_count_;
  int64_t call_count_;
  int64_t retained_bytes_;
  int failure_;
  bool initialized_;
  bool large_;
  bool sealed_;
  bool ready_;
  bool waiting_memory_;

  DISALLOW_COPY_AND_ASSIGN(SemanticAggregation);
};

}
}
#endif
