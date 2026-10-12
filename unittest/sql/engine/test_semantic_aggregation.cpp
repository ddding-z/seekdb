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
#include <cstdlib>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include "common/json_type/ob_json_tree.h"
#include "sql/engine/basic/ob_semantic_aggregation.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{
void require(bool condition, const std::string &message)
{
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void require_equal(int64_t actual, int64_t expected, const std::string &message)
{
  if (actual != expected) {
    std::ostringstream diagnostic;
    diagnostic << message << ": expected " << expected << ", got " << actual;
    throw std::runtime_error(diagnostic.str());
  }
}

ObString view(const std::string &text)
{
  return ObString(text.length(), text.data());
}

std::string owned(const ObString &text)
{
  return text.length() == 0 ? std::string() : std::string(text.ptr(), text.length());
}

std::shared_ptr<SemanticQueryBudget> budget(int64_t bytes = 1024 * 1024, int64_t tasks = 4)
{
  static uint64_t execution_id = 0x51550000;
  return std::make_shared<SemanticQueryBudget>(execution_id++, bytes, tasks);
}

ObJsonObject &object(ObIJsonBase *value)
{
  require(value != nullptr && value->json_type() == ObJsonNodeType::J_OBJECT,
          "expected JSON prompt object");
  return *static_cast<ObJsonObject *>(value);
}

std::string string(ObIJsonBase *value)
{
  require(value != nullptr && value->json_type() == ObJsonNodeType::J_STRING,
          "expected JSON prompt string");
  return std::string(value->get_data(), value->get_data_length());
}

const char *phase_name(SemanticAggregationRequest::Phase phase)
{
  switch (phase) {
    case SemanticAggregationRequest::DIRECT: return "direct";
    case SemanticAggregationRequest::EXTRACT: return "extract";
    case SemanticAggregationRequest::COMBINE: return "combine";
    case SemanticAggregationRequest::FINALIZE: return "finalize";
  }
  throw std::runtime_error("unknown aggregation request phase");
}

struct ParsedPrompt
{
  std::string instruction_;
  std::string phase_;
  std::vector<std::string> inputs_;
  int64_t source_count_;
};

ParsedPrompt parse(const std::string &prompt)
{
  ObArenaAllocator allocator("SemanticTest");
  ObString document;
  ObIJsonBase *json = nullptr;
  require_equal(ob_write_string(allocator, view(prompt), document), OB_SUCCESS, "own JSON prompt");
  require_equal(ObJsonBaseFactory::get_json_base(&allocator, document,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, json), OB_SUCCESS, "parse JSON prompt");
  ObJsonObject &body = object(json);
  require_equal(body.element_count(), 5, "exact compact prompt fields");
  require(string(body.get_value("operation")) == "ai_agg", "aggregation operation marker");
  ParsedPrompt parsed;
  parsed.instruction_ = string(body.get_value("instruction"));
  parsed.phase_ = string(body.get_value("phase"));
  ObIJsonBase *inputs = body.get_value("inputs");
  require(inputs != nullptr && inputs->json_type() == ObJsonNodeType::J_ARRAY,
          "JSON prompt inputs array");
  ObJsonArray &array = *static_cast<ObJsonArray *>(inputs);
  for (uint64_t index = 0; index < array.element_count(); ++index) {
    parsed.inputs_.push_back(string(array.get_value(index)));
  }
  ObIJsonBase *count = body.get_value("source_count");
  require(count != nullptr && (count->json_type() == ObJsonNodeType::J_INT ||
                              count->json_type() == ObJsonNodeType::J_UINT),
          "SQL source count is a JSON integer");
  if (count->json_type() == ObJsonNodeType::J_UINT) {
    require(count->get_uint() <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()),
            "source count is representable");
    parsed.source_count_ = static_cast<int64_t>(count->get_uint());
  } else {
    parsed.source_count_ = count->get_int();
  }
  return parsed;
}

struct RequestCopy
{
  explicit RequestCopy(const SemanticAggregationRequest &request)
      : id_(request.id_), phase_(request.phase_), prompt_(owned(request.prompt_)),
        source_count_(request.source_count_), level_(request.level_) {}
  bool operator==(const RequestCopy &other) const
  {
    return id_ == other.id_ && phase_ == other.phase_ && prompt_ == other.prompt_ &&
           source_count_ == other.source_count_ && level_ == other.level_;
  }
  int64_t id_;
  SemanticAggregationRequest::Phase phase_;
  std::string prompt_;
  int64_t source_count_;
  int64_t level_;
};

int64_t token_count(const std::string &tokens)
{
  return tokens.empty() ? 0 : 1 + std::count(tokens.begin(), tokens.end(), ',');
}

std::string join(const std::vector<std::string> &texts)
{
  std::string result;
  for (const std::string &text : texts) {
    if (!result.empty()) {
      result += ',';
    }
    result += text;
  }
  return result;
}

std::string mock_output(const RequestCopy &request)
{
  const ParsedPrompt prompt = parse(request.prompt_);
  std::vector<std::string> tokens;
  for (const std::string &input : prompt.inputs_) {
    if (request.phase_ == SemanticAggregationRequest::DIRECT ||
        request.phase_ == SemanticAggregationRequest::EXTRACT) {
      tokens.push_back(input.substr(0, input.find(':')));
    } else {
      tokens.push_back(input);
    }
  }
  return join(tokens);
}

void check_request(const RequestCopy &request, const SemanticAggregationLimits &limits)
{
  require(request.id_ > 0, "positive request identity");
  require(request.prompt_.length() <= static_cast<uint64_t>(limits.request_input_bytes_),
          "whole serialized prompt respects exact input byte budget");
  require(request.level_ >= 0 && request.level_ < limits.max_levels_, "bounded request level");
  const ParsedPrompt prompt = parse(request.prompt_);
  require(prompt.phase_ == phase_name(request.phase_), "phase matches JSON");
  require_equal(prompt.source_count_, request.source_count_, "SQL source metadata matches JSON");
  if (request.phase_ == SemanticAggregationRequest::COMBINE) {
    require_equal(prompt.inputs_.size(), 2, "combine owns exactly two adjacent siblings");
  } else if (request.phase_ == SemanticAggregationRequest::FINALIZE) {
    require_equal(prompt.inputs_.size(), 1, "finalize owns exactly one root");
  } else {
    require(!prompt.inputs_.empty(), "input request owns at least one row");
    require_equal(prompt.inputs_.size(), request.source_count_, "raw chunk covers each input once");
  }
  if (request.phase_ == SemanticAggregationRequest::COMBINE ||
      request.phase_ == SemanticAggregationRequest::FINALIZE) {
    int64_t sources = 0;
    for (const std::string &input : prompt.inputs_) {
      sources += token_count(input);
    }
    require_equal(sources, request.source_count_, "combine metadata covers original source rows");
  }
}

int collect(SemanticAggregation &aggregation, const SemanticAggregationLimits &limits,
            std::vector<RequestCopy> &pending, std::map<int64_t, RequestCopy> &requests)
{
  int ret = OB_SUCCESS;
  bool available = true;
  while (ret == OB_SUCCESS && available) {
    SemanticAggregationRequest request;
    ret = aggregation.next_request(request, available);
    if (ret == OB_SUCCESS && available) {
      RequestCopy copy(request);
      check_request(copy, limits);
      require(requests.emplace(copy.id_, copy).second, "request identities are unique within a group");
      pending.push_back(copy);
    }
  }
  return ret;
}

int finish_one(SemanticAggregation &aggregation, std::vector<RequestCopy> &pending, bool reverse)
{
  require(!pending.empty(), "completion has a pending request");
  const size_t index = reverse ? pending.size() - 1 : 0;
  const RequestCopy request = pending.at(index);
  pending.erase(pending.begin() + index);
  const std::string output = mock_output(request);
  return aggregation.complete(request.id_, view(output));
}

struct Outcome
{
  int ret_ = OB_SUCCESS;
  std::string result_;
  std::map<int64_t, RequestCopy> requests_;
};

Outcome run_group(const std::vector<std::string> &rows, const SemanticAggregationLimits &limits,
                  bool reverse, int64_t memory_bytes = 1024 * 1024)
{
  auto resources = budget(memory_bytes);
  SemanticAggregation aggregation;
  std::vector<RequestCopy> pending;
  Outcome outcome;
  outcome.ret_ = aggregation.init(ObString("summarize"), limits, resources);
  for (size_t index = 0; outcome.ret_ == OB_SUCCESS && index < rows.size(); ++index) {
    outcome.ret_ = aggregation.append(view(rows[index]));
    if (outcome.ret_ == OB_SUCCESS) {
      outcome.ret_ = collect(aggregation, limits, pending, outcome.requests_);
    }
  }
  if (outcome.ret_ == OB_SUCCESS) {
    outcome.ret_ = aggregation.seal();
  }
  int64_t iterations = 0;
  while (outcome.ret_ == OB_SUCCESS && !aggregation.ready()) {
    require(++iterations <= limits.max_calls_ + 1, "reduction always makes bounded progress");
    outcome.ret_ = collect(aggregation, limits, pending, outcome.requests_);
    if (outcome.ret_ == OB_SUCCESS) {
      require(!pending.empty(), "sealed aggregation cannot stall without dispatchable work");
      outcome.ret_ = finish_one(aggregation, pending, reverse);
    }
  }
  if (outcome.ret_ == OB_SUCCESS) {
    require_equal(aggregation.has_input(), !rows.empty(), "empty group result flag");
    require(!aggregation.has_pending(), "ready group has no pending calls");
    outcome.result_ = owned(aggregation.result());
  }
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "group reset releases every budget charge");
  return outcome;
}

std::vector<std::string> tokens(int64_t count)
{
  std::vector<std::string> values;
  for (int64_t index = 0; index < count; ++index) {
    values.push_back("r" + std::to_string(index));
  }
  return values;
}

std::vector<std::string> rows(const std::vector<std::string> &values)
{
  std::vector<std::string> texts;
  for (const std::string &value : values) {
    texts.push_back(value + ":" + std::string(300, 'x'));
  }
  return texts;
}

void test_tiny_direct()
{
  const SemanticAggregationLimits limits(512, 160, 128, 16);
  auto resources = budget();
  SemanticAggregation aggregation;
  require_equal(aggregation.init(ObString("summarize"), limits, resources), OB_SUCCESS, "init tiny group");
  require_equal(aggregation.append(ObString("a")), OB_SUCCESS, "append first tiny row");
  require_equal(aggregation.append(ObString("b")), OB_SUCCESS, "append second tiny row");
  SemanticAggregationRequest request;
  bool available = true;
  require_equal(aggregation.peek_request(request, available), OB_SUCCESS, "peek before seal");
  require(!available && !aggregation.ready() && !aggregation.has_pending(),
          "small group waits for seal without an extraction call");
  require_equal(aggregation.seal(), OB_SUCCESS, "seal tiny group");
  require_equal(aggregation.next_request(request, available), OB_SUCCESS, "dispatch direct request");
  require(available && request.phase_ == SemanticAggregationRequest::DIRECT, "one direct phase");
  require_equal(request.source_count_, 2, "direct SQL row count");
  const ParsedPrompt prompt = parse(owned(request.prompt_));
  require(prompt.inputs_ == std::vector<std::string>({"a", "b"}), "direct includes both ordered inputs");
  require_equal(aggregation.complete(request.id_, ObString("answer")), OB_SUCCESS, "finish direct request");
  require(aggregation.ready() && aggregation.has_input(), "tiny group is ready after one call");
  require(owned(aggregation.result()) == "answer", "direct result is not summarized a second time");
  require_equal(aggregation.peek_request(request, available), OB_SUCCESS, "peek finished tiny group");
  require(!available, "tiny group issues exactly one request");
  require_equal(aggregation.seal(), OB_SUCCESS, "seal is idempotent");
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "tiny group cleanup");
}

void test_empty_and_empty_text()
{
  const SemanticAggregationLimits limits(512, 160, 128, 16);
  auto resources = budget();
  SemanticAggregation aggregation;
  require_equal(aggregation.init(ObString(""), limits, resources), OB_SUCCESS, "init empty instruction");
  require(!aggregation.ready() && !aggregation.has_input(), "unsealed empty group is not ready");
  require_equal(aggregation.seal(), OB_SUCCESS, "seal empty group");
  require(aggregation.ready() && !aggregation.has_input() && aggregation.result().empty(),
          "empty sealed group has an explicit nullable result flag");
  SemanticAggregationRequest request;
  bool available = true;
  require_equal(aggregation.next_request(request, available), OB_SUCCESS, "peek empty group");
  require(!available && !aggregation.has_pending(), "empty group performs no model calls");
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "empty group cleanup");

  require_equal(aggregation.init(ObString("summarize"), limits, resources), OB_SUCCESS, "reinit group");
  require_equal(aggregation.append(ObString()), OB_SUCCESS, "non-NULL empty text is an input row");
  require_equal(aggregation.seal(), OB_SUCCESS, "seal empty text group");
  require_equal(aggregation.next_request(request, available), OB_SUCCESS, "dispatch empty text");
  require(available && request.source_count_ == 1, "empty text contributes once");
  require(parse(owned(request.prompt_)).inputs_ == std::vector<std::string>({""}),
          "empty text is encoded as an empty JSON string");
  require_equal(aggregation.complete(request.id_, ObString()), OB_SUCCESS, "empty output is valid");
  require(aggregation.ready() && aggregation.has_input() && aggregation.result().empty(),
          "empty output differs from an all-NULL group");
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "empty text cleanup");
}

void test_duplicates_and_ownership()
{
  const SemanticAggregationLimits limits(1024, 160, 128, 16);
  auto resources = budget();
  SemanticAggregation aggregation;
  std::string instruction = "quote \" and slash \\ and\ncontrol ";
  instruction.push_back('\0');
  const std::string expected_instruction = instruction;
  require_equal(aggregation.init(view(instruction), limits, resources), OB_SUCCESS, "own instruction");
  instruction.assign(instruction.length(), 'x');
  std::string row = "same";
  require_equal(aggregation.append(view(row)), OB_SUCCESS, "own first duplicate");
  require_equal(aggregation.append(view(row)), OB_SUCCESS, "keep second duplicate");
  row.assign("changed");
  const std::string control("\0\b\t\n\f\r\x1f\"\\", 9);
  require_equal(aggregation.append(view(control)), OB_SUCCESS, "retain JSON control bytes");
  require_equal(aggregation.seal(), OB_SUCCESS, "seal duplicate group");
  SemanticAggregationRequest request;
  bool available = false;
  require_equal(aggregation.next_request(request, available), OB_SUCCESS, "dispatch duplicates");
  require(available, "duplicate group has a request");
  const ParsedPrompt prompt = parse(owned(request.prompt_));
  require(prompt.instruction_ == expected_instruction, "instruction survives source mutation");
  require(prompt.inputs_ == std::vector<std::string>({"same", "same", control}),
          "duplicates, control bytes, and row order survive ownership and JSON serialization");
  require_equal(request.source_count_, 3, "SQL counts duplicate rows independently");
  const std::string output("{\"source_count\":999}");
  require_equal(aggregation.complete(request.id_, view(output)), OB_SUCCESS, "output remains opaque text");
  require(owned(aggregation.result()) == output, "controller never parses LLM source metadata");
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "duplicate cleanup");
}

void test_submission_protocol()
{
  const SemanticAggregationLimits limits(512, 160, 128, 16);
  auto resources = budget(1024 * 1024, 1);
  SemanticAggregation aggregation;
  require_equal(aggregation.init(ObString("summarize"), limits, resources), OB_SUCCESS, "init protocol group");
  require_equal(aggregation.append(ObString("x")), OB_SUCCESS, "append protocol input");
  require_equal(aggregation.seal(), OB_SUCCESS, "seal protocol group");
  require_equal(resources->acquire_task(), OB_SUCCESS, "parent occupies its only task credit");
  require_equal(resources->acquire_task(), OB_EAGAIN, "parent has no additional credit");
  SemanticAggregationRequest first;
  SemanticAggregationRequest second;
  bool available = false;
  require_equal(aggregation.peek_request(first, available), OB_SUCCESS, "peek without task credit");
  require(available, "peek cannot consume a request on credit exhaustion");
  require_equal(aggregation.peek_request(second, available), OB_SUCCESS, "repeat non-consuming peek");
  require(available && RequestCopy(first) == RequestCopy(second), "repeated peeks preserve exact work");
  require_equal(aggregation.complete(first.id_, ObString("early")), OB_STATE_NOT_MATCH,
                "completion requires an explicit submission");
  resources->release_task();
  require_equal(resources->acquire_task(), OB_SUCCESS, "parent reacquires credit");
  require_equal(aggregation.mark_submitted(first.id_), OB_SUCCESS, "mark submission after credit");
  require_equal(aggregation.mark_submitted(first.id_), OB_STATE_NOT_MATCH, "no double submission");
  require_equal(aggregation.rollback_submission(first.id_), OB_SUCCESS, "roll back unaccepted dispatch");
  resources->release_task();
  require_equal(aggregation.peek_request(second, available), OB_SUCCESS, "peek rolled back dispatch");
  require(available && RequestCopy(first) == RequestCopy(second), "rollback preserves ID and prompt");
  require_equal(aggregation.rollback_submission(first.id_), OB_STATE_NOT_MATCH, "no double rollback");
  require_equal(resources->acquire_task(), OB_SUCCESS, "parent acquires retry credit");
  require_equal(aggregation.next_request(second, available), OB_SUCCESS, "consuming request after credit");
  require(available, "rolled back work remains dispatchable");
  require_equal(aggregation.complete(second.id_, ObString("answer")), OB_SUCCESS, "complete retried request");
  resources->release_task();
  require_equal(aggregation.complete(second.id_, ObString("again")), OB_STATE_NOT_MATCH,
                "completed requests cannot complete twice");
  require_equal(aggregation.append(ObString("late")), OB_STATE_NOT_MATCH, "seal forbids more input");
  aggregation.reset();
  require_equal(resources->active_tasks(), 0, "controller does not own parent task credits");
  require_equal(resources->memory_used(), 0, "protocol cleanup");
}

void test_reverse_completions_and_tail()
{
  const SemanticAggregationLimits limits(512, 160, 128, 16);
  std::vector<std::string> values = tokens(17);
  values[5] = values[3];
  const auto input = rows(values);
  const Outcome forward = run_group(input, limits, false);
  const Outcome reverse = run_group(input, limits, true);
  require_equal(forward.ret_, OB_SUCCESS, "forward asynchronous reduction");
  require_equal(reverse.ret_, OB_SUCCESS, "reverse asynchronous reduction");
  require(forward.result_ == join(values) && reverse.result_ == join(values),
          "all original source rows occur once, including duplicates, in input order");
  require(forward.requests_ == reverse.requests_,
          "chunk-index pairing gives identical prompts, IDs, levels, and metadata under reverse completions");
  require_equal(forward.requests_.size(), 34, "17 extracts + 16 binary combines + one finalize");
  int64_t max_level = 0;
  for (const auto &item : forward.requests_) {
    max_level = std::max(max_level, item.second.level_);
    if (item.second.phase_ == SemanticAggregationRequest::FINALIZE) {
      require_equal(item.second.source_count_, 17, "EOF tail source count");
    }
  }
  require_equal(max_level, 6, "non-power-of-two EOF tree has five combine levels and a final level");
}

void test_online_merges_without_layer_barrier()
{
  const SemanticAggregationLimits limits(512, 160, 128, 16);
  const auto values = tokens(17);
  const auto input = rows(values);
  auto resources = budget();
  SemanticAggregation aggregation;
  std::vector<RequestCopy> pending;
  std::map<int64_t, RequestCopy> requests;
  require_equal(aggregation.init(ObString("summarize"), limits, resources), OB_SUCCESS, "init online tree");
  for (int64_t index = 0; index < 5; ++index) {
    require_equal(aggregation.append(view(input.at(index))), OB_SUCCESS, "append early online row");
    require_equal(collect(aggregation, limits, pending, requests), OB_SUCCESS, "submit online extract");
  }
  require_equal(pending.size(), 4, "extracts are dispatched before EOF");
  const RequestCopy left = pending.at(0);
  const RequestCopy right = pending.at(1);
  require_equal(aggregation.complete(right.id_, view(mock_output(right))), OB_SUCCESS,
                "right sibling completes first");
  require_equal(aggregation.complete(left.id_, view(mock_output(left))), OB_SUCCESS,
                "left sibling completes second");
  pending.erase(pending.begin(), pending.begin() + 2);
  SemanticAggregationRequest merge;
  bool available = false;
  require_equal(aggregation.peek_request(merge, available), OB_SUCCESS, "peek dependency-ready merge");
  require(available && merge.phase_ == SemanticAggregationRequest::COMBINE &&
          merge.level_ == 1 && merge.source_count_ == 2,
          "one sibling pair merges while other extracts in the same layer remain unfinished");
  require(!aggregation.ready(), "online merges cannot finalize an unsealed group");
  require_equal(collect(aggregation, limits, pending, requests), OB_SUCCESS, "submit first online merge");
  for (size_t index = 5; index < input.size(); ++index) {
    require_equal(aggregation.append(view(input[index])), OB_SUCCESS, "append later online row");
    require_equal(collect(aggregation, limits, pending, requests), OB_SUCCESS, "submit later online work");
    while (pending.size() >= 4) {
      require_equal(finish_one(aggregation, pending, true), OB_SUCCESS, "complete online work out of order");
      require_equal(collect(aggregation, limits, pending, requests), OB_SUCCESS, "submit ready online parent");
    }
  }
  while (!pending.empty()) {
    require_equal(finish_one(aggregation, pending, true), OB_SUCCESS, "drain online work before seal");
    require_equal(collect(aggregation, limits, pending, requests), OB_SUCCESS, "submit online dependency");
  }
  int64_t online_combine_level = 0;
  for (const auto &item : requests) {
    require(item.second.phase_ != SemanticAggregationRequest::FINALIZE &&
            item.second.phase_ != SemanticAggregationRequest::DIRECT, "no final request before seal");
    if (item.second.phase_ == SemanticAggregationRequest::COMBINE) {
      online_combine_level = std::max(online_combine_level, item.second.level_);
    }
  }
  require(online_combine_level >= 3, "at least three hierarchy levels run during input");
  require(!aggregation.ready() && !aggregation.has_pending(),
          "completed online roots wait for more input or EOF, not a fake model wait");
  require_equal(aggregation.seal(), OB_SUCCESS, "seal online tree");
  while (!aggregation.ready()) {
    require_equal(collect(aggregation, limits, pending, requests), OB_SUCCESS, "submit EOF tail work");
    require_equal(finish_one(aggregation, pending, false), OB_SUCCESS, "complete EOF tail work");
  }
  require(owned(aggregation.result()) == join(values), "online and EOF stages cover the entire group");
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "online tree cleanup");
}

void test_exact_prompt_bytes_and_phase_boundary()
{
  const std::string exact =
      "{\"operation\":\"ai_agg\",\"phase\":\"direct\",\"instruction\":\"i\","
      "\"inputs\":[\"x\"],\"source_count\":1}";
  auto resources = budget();
  SemanticAggregation aggregation;
  SemanticAggregationLimits limits(exact.length(), 160, 128, 16);
  require_equal(aggregation.init(ObString("i"), limits, resources), OB_SUCCESS, "init exact byte budget");
  require_equal(aggregation.append(ObString("x")), OB_SUCCESS, "exact whole-prompt byte limit");
  require_equal(aggregation.seal(), OB_SUCCESS, "seal exact prompt");
  SemanticAggregationRequest request;
  bool available = false;
  require_equal(aggregation.next_request(request, available), OB_SUCCESS, "dispatch exact prompt");
  require(available && owned(request.prompt_) == exact, "complete instruction, phase and JSON framing counted");
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "exact prompt cleanup");

  --limits.request_input_bytes_;
  require_equal(aggregation.init(ObString("i"), limits, resources), OB_SUCCESS, "init one-byte-short budget");
  require_equal(aggregation.append(ObString("x")), OB_SIZE_OVERFLOW,
                "one byte beyond complete prompt is rejected without truncation");
  require(!aggregation.has_input(), "oversized row is never accepted");
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "over-budget prompt cleanup");

  const std::string pair =
      "{\"operation\":\"ai_agg\",\"phase\":\"direct\",\"instruction\":\"i\","
      "\"inputs\":[\"aaa\",\"bbb\"],\"source_count\":2}";
  limits.request_input_bytes_ = pair.length();
  require_equal(aggregation.init(ObString("i"), limits, resources), OB_SUCCESS, "init phase boundary");
  require_equal(aggregation.append(ObString("aaa")), OB_SUCCESS, "append boundary first row");
  require_equal(aggregation.append(ObString("bbb")), OB_SUCCESS, "append boundary second row");
  require_equal(aggregation.peek_request(request, available), OB_SUCCESS, "exact direct group stays raw");
  require(!available, "direct fast path is not pessimistically limited by longer phase names");
  require_equal(aggregation.append(ObString("c")), OB_SUCCESS, "cross direct boundary into extraction");
  require_equal(aggregation.next_request(request, available), OB_SUCCESS, "first exact-boundary extraction");
  require(available && request.phase_ == SemanticAggregationRequest::EXTRACT,
          "crossing budget creates online extraction");
  require(parse(owned(request.prompt_)).inputs_ == std::vector<std::string>({"aaa"}),
          "promotion splits on whole row boundaries when extract framing needs one extra byte");
  require_equal(aggregation.complete(request.id_, ObString("a")), OB_SUCCESS,
                "finish first boundary chunk with compact evidence");
  require_equal(aggregation.seal(), OB_SUCCESS, "seal boundary tail");
  int64_t sources = 1;
  while (!aggregation.ready()) {
    require_equal(aggregation.next_request(request, available), OB_SUCCESS, "dispatch boundary tree");
    require(available, "boundary tree keeps making progress");
    const RequestCopy copy(request);
    if (request.phase_ == SemanticAggregationRequest::EXTRACT) {
      sources += request.source_count_;
      require(parse(copy.prompt_).inputs_ == std::vector<std::string>({"bbb", "c"}),
              "phase promotion retains the last raw row and new input");
    }
    require(request.prompt_.length() <= limits.request_input_bytes_, "boundary prompts fit exactly");
    const std::string output = request.phase_ == SemanticAggregationRequest::EXTRACT
        ? "b,c" : mock_output(copy);
    require_equal(aggregation.complete(request.id_, view(output)), OB_SUCCESS,
                  "complete boundary tree");
  }
  require_equal(sources, 3, "phase boundary preserves source coverage");
  require(owned(aggregation.result()) == "a,b,c", "compact boundary root preserves input order");
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "phase boundary cleanup");
}

void test_limit_validation_and_oversize()
{
  const SemanticAggregationLimits defaults(512, 160, 128, 16);
  auto resources = budget();
  SemanticAggregation aggregation;
  std::vector<SemanticAggregationLimits> invalid;
  for (int64_t value : {int64_t(0), SemanticAggregationLimits::MAX_REQUEST_INPUT_BYTES + 1}) {
    auto limits = defaults;
    limits.request_input_bytes_ = value;
    invalid.push_back(limits);
  }
  for (int64_t value : {int64_t(0), SemanticAggregationLimits::MAX_PARTIAL_BYTES + 1}) {
    auto limits = defaults;
    limits.max_partial_bytes_ = value;
    invalid.push_back(limits);
  }
  for (int64_t value : {int64_t(0), SemanticAggregationLimits::MAX_CALLS + 1}) {
    auto limits = defaults;
    limits.max_calls_ = value;
    invalid.push_back(limits);
  }
  for (int64_t value : {int64_t(0), SemanticAggregationLimits::MAX_LEVELS + 1}) {
    auto limits = defaults;
    limits.max_levels_ = value;
    invalid.push_back(limits);
  }
  for (const auto &limits : invalid) {
    require_equal(aggregation.init(ObString("i"), limits, resources), OB_INVALID_ARGUMENT,
                  "every limit is positive and bounded");
    require_equal(resources->memory_used(), 0, "invalid limits never reserve memory");
  }
  require_equal(aggregation.init(ObString("i"), defaults, nullptr), OB_INVALID_ARGUMENT,
                "query memory budget is mandatory");
  require_equal(aggregation.init(view(std::string(513, 'x')), defaults, resources), OB_SIZE_OVERFLOW,
                "instruction also participates in the request budget");
  require_equal(resources->memory_used(), 0, "oversized instruction cleanup");
  require_equal(aggregation.init(ObString("i"), defaults, resources), OB_SUCCESS, "init oversized row case");
  require_equal(aggregation.append(view(std::string(512, 'x'))), OB_SIZE_OVERFLOW,
                "oversized single row fails explicitly");
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "oversized row reset");
  require_equal(aggregation.init(ObString("i"), defaults, resources), OB_SUCCESS, "init output limit case");
  require_equal(aggregation.append(ObString("x")), OB_SUCCESS, "append output limit input");
  require_equal(aggregation.seal(), OB_SUCCESS, "seal output limit input");
  SemanticAggregationRequest request;
  bool available = false;
  require_equal(aggregation.next_request(request, available), OB_SUCCESS, "dispatch output limit request");
  require_equal(aggregation.complete(request.id_, view(std::string(161, 'x'))), OB_SIZE_OVERFLOW,
                "over-limit output is never silently truncated");
  require(!aggregation.ready(), "over-limit output is not a success-shaped result");
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "output limit reset");
}

void test_call_and_level_limits()
{
  const auto input = rows(tokens(5));
  SemanticAggregationLimits limits(512, 160, 10, 5);
  require_equal(run_group(input, limits, false).ret_, OB_SUCCESS, "exact 2N logical call limit");
  --limits.max_calls_;
  require_equal(run_group(input, limits, false).ret_, OB_SIZE_OVERFLOW, "final call exceeds logical call limit");
  limits.max_calls_ = 10;
  --limits.max_levels_;
  require_equal(run_group(input, limits, true).ret_, OB_SIZE_OVERFLOW,
                "final request exceeds hierarchy level limit");
  limits.max_levels_ = 5;
  limits.max_calls_ = 1;
  require_equal(run_group({"tiny"}, limits, false).ret_, OB_SUCCESS, "tiny direct needs only one call");
  require_equal(run_group(input, limits, false).ret_, OB_SIZE_OVERFLOW, "large group cannot exceed one call");
}

void test_non_shrinking_partials()
{
  const SemanticAggregationLimits limits(256, 160, 128, 16);
  auto resources = budget();
  SemanticAggregation aggregation;
  require_equal(aggregation.init(ObString("i"), limits, resources), OB_SUCCESS, "init non-shrinking case");
  const std::string row(130, 'r');
  require_equal(aggregation.append(view(row)), OB_SUCCESS, "append first large row");
  require_equal(aggregation.append(view(row)), OB_SUCCESS, "append second large row");
  require_equal(aggregation.seal(), OB_SUCCESS, "seal two extract chunks");
  SemanticAggregationRequest first;
  SemanticAggregationRequest second;
  bool available = false;
  require_equal(aggregation.next_request(first, available), OB_SUCCESS, "dispatch first non-shrinking chunk");
  require(available, "first chunk available");
  require_equal(aggregation.next_request(second, available), OB_SUCCESS, "dispatch second non-shrinking chunk");
  require(available, "second chunk available");
  const std::string partial(100, '\x01');
  require_equal(aggregation.complete(first.id_, view(partial)), OB_SUCCESS, "accept bounded first partial");
  require_equal(aggregation.complete(second.id_, view(partial)), OB_SIZE_OVERFLOW,
                "escaped combine inputs cannot fit: fail instead of unary re-extraction or truncation");
  require(!aggregation.ready(), "no-progress combine failure is explicit");
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "non-shrinking output cleanup");
}

void test_backpressure_retry_and_bounded_storage()
{
  const SemanticAggregationLimits limits(512, 512, 512, 16);
  const auto values = tokens(100);
  const auto input = rows(values);
  auto resources = budget(1600);
  SemanticAggregation aggregation;
  std::vector<RequestCopy> pending;
  std::map<int64_t, RequestCopy> requests;
  require_equal(aggregation.init(ObString("summarize"), limits, resources), OB_SUCCESS, "init bounded memory case");
  size_t accepted = 0;
  int64_t retries = 0;
  int64_t peak = resources->memory_used();
  bool saw_backpressure = false;
  while (accepted < input.size()) {
    const int ret = aggregation.append(view(input[accepted]));
    peak = std::max(peak, resources->memory_used());
    saw_backpressure = saw_backpressure || aggregation.needs_input_backpressure();
    if (ret == OB_SUCCESS) {
      ++accepted;
    } else {
      require_equal(ret, OB_EAGAIN, "pending work turns input resource pressure into retry");
      require(++retries <= limits.max_calls_, "backpressure cannot spin without bounded progress");
      require_equal(collect(aggregation, limits, pending, requests), OB_SUCCESS, "submit bounded-memory work");
      require(!pending.empty(), "backpressure has actual work that can release memory");
      require_equal(finish_one(aggregation, pending, false), OB_SUCCESS, "free bounded-memory dependency");
    }
  }
  require(saw_backpressure && retries > 0, "input backpressure and unconsumed-row retry are exercised");
  require_equal(aggregation.seal(), OB_SUCCESS, "seal bounded-memory group");
  while (!aggregation.ready()) {
    require_equal(collect(aggregation, limits, pending, requests), OB_SUCCESS, "submit bounded EOF work");
    require_equal(finish_one(aggregation, pending, false), OB_SUCCESS, "complete bounded EOF work");
    peak = std::max(peak, resources->memory_used());
  }
  require(owned(aggregation.result()) == join(values),
          "retrying one unaccepted row never duplicates or loses source input");
  require(peak <= resources->memory_limit(), "retained text, prompts and metadata stay under memory budget");
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "bounded tree releases retired child metadata and payloads");
}

void test_resource_exhaustion_and_reset()
{
  const int64_t baseline = ObAIFuncClient::pipeline_buffer_usage();
  const SemanticAggregationLimits limits(512, 160, 128, 16);
  auto resources = budget(1024);
  SemanticAggregation aggregation;
  require_equal(aggregation.init(ObString("i"), limits, resources), OB_SUCCESS, "init exhausted resource case");
  const int64_t blocked = resources->memory_limit() - resources->memory_used();
  require_equal(resources->reserve(blocked), OB_SUCCESS, "pin all remaining query memory");
  require_equal(aggregation.append(ObString("x")), OB_SIZE_OVERFLOW,
                "resource exhaustion without pending work is an explicit no-progress error");
  require(!aggregation.has_input() && !aggregation.has_pending(), "denied row creates no phantom work");
  resources->release(blocked);
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "no-progress error cleanup");
  require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline, "shared accounting restored after error");

  require_equal(aggregation.init(ObString("i"), limits, resources), OB_SUCCESS, "reuse controller after error reset");
  const auto input = rows(tokens(3));
  require_equal(aggregation.append(view(input.at(0))), OB_SUCCESS, "retain raw input for reset");
  require_equal(aggregation.append(view(input.at(1))), OB_SUCCESS, "retain queued prompt for reset");
  SemanticAggregationRequest request;
  bool available = false;
  require_equal(aggregation.next_request(request, available), OB_SUCCESS, "retain submitted request for reset");
  require(available && resources->memory_used() > 0, "reset fixture owns payload and metadata");
  aggregation.reset();
  require_equal(resources->memory_used(), 0, "reset frees raw, queued and in-flight controller state");
  require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline, "shared accounting restored after reset");
  require(!aggregation.ready() && !aggregation.has_pending() && !aggregation.has_input(),
          "reset clears the complete lifecycle");
  require_equal(aggregation.peek_request(request, available), OB_NOT_INIT, "reset requires reinitialization");
}

struct TestCase
{
  const char *name_;
  void (*run_)();
};

const TestCase TESTS[] = {
  {"tiny_direct", test_tiny_direct},
  {"empty_and_empty_text", test_empty_and_empty_text},
  {"duplicates_and_ownership", test_duplicates_and_ownership},
  {"submission_protocol", test_submission_protocol},
  {"reverse_completions_and_tail", test_reverse_completions_and_tail},
  {"online_merges_without_layer_barrier", test_online_merges_without_layer_barrier},
  {"exact_prompt_bytes_and_phase_boundary", test_exact_prompt_bytes_and_phase_boundary},
  {"limit_validation_and_oversize", test_limit_validation_and_oversize},
  {"call_and_level_limits", test_call_and_level_limits},
  {"non_shrinking_partials", test_non_shrinking_partials},
  {"backpressure_retry_and_bounded_storage", test_backpressure_retry_and_bounded_storage},
  {"resource_exhaustion_and_reset", test_resource_exhaustion_and_reset},
};
}

int main(int argc, char **argv)
{
  try {
    std::string selected;
    int64_t repeat = 1;
    for (int index = 1; index < argc; ++index) {
      const std::string argument(argv[index]);
      if (argument == "--case" && index + 1 < argc) {
        selected = argv[++index];
      } else if (argument == "--repeat" && index + 1 < argc) {
        repeat = std::stoll(argv[++index]);
      } else {
        throw std::runtime_error("expected --case NAME or --repeat COUNT");
      }
    }
    require(repeat >= 1 && repeat <= 100, "repeat count must be in [1, 100]");
    int64_t executed = 0;
    for (int64_t iteration = 0; iteration < repeat; ++iteration) {
      for (const TestCase &test : TESTS) {
        if (selected.empty() || selected == test.name_) {
          test.run_();
          ++executed;
          std::cout << "PASS " << test.name_ << std::endl;
        }
      }
    }
    require(executed > 0, "unknown semantic aggregation test case");
    std::cout << "semantic aggregation native tests passed: " << executed << std::endl;
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "FAIL semantic aggregation native tests: " << error.what() << std::endl;
    return 1;
  }
}
