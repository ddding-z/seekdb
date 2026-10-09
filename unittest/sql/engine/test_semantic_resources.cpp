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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "sql/engine/basic/ob_semantic_runtime.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"

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

uint64_t execution_id()
{
  static std::atomic<uint64_t> next{0x51510000};
  return next.fetch_add(1);
}

class Gate
{
public:
  void arrive_and_wait()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    ++arrived_;
    changed_.notify_all();
    changed_.wait(lock, [this] { return released_; });
  }

  bool wait_for_arrivals(int64_t count)
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, std::chrono::seconds(20),
                             [this, count] { return arrived_ == count; });
  }

  void release()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    released_ = true;
    changed_.notify_all();
  }
private:
  std::mutex mutex_;
  std::condition_variable changed_;
  int64_t arrived_ = 0;
  bool released_ = false;
};

class JoiningThreads
{
public:
  explicit JoiningThreads(Gate &gate) : gate_(gate) {}
  ~JoiningThreads()
  {
    gate_.release();
    join();
  }

  template <typename Function>
  void start(Function function)
  {
    threads_.emplace_back(std::move(function));
  }

  void join()
  {
    for (auto &thread : threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }
private:
  Gate &gate_;
  std::vector<std::thread> threads_;
};

class ExecutionFixture
{
public:
  explicit ExecutionFixture(uint64_t id)
    : allocator_("SemanticTest"), plan_(), session_(), ctx_(allocator_)
  {
    session_.set_current_execution_id(id);
    ctx_.set_execution_id(id);
    ctx_.set_my_session(&session_);
    require_equal(ctx_.create_physical_plan_ctx(), OB_SUCCESS, "create execution plan context");
    ctx_.get_physical_plan_ctx()->set_phy_plan(&plan_);
    ctx_.get_physical_plan_ctx()->set_timeout_timestamp(std::numeric_limits<int64_t>::max());
    require(ctx_.is_valid(), "execution state fixture has a valid context");
    require_equal(session_.get_current_execution_id(), id, "execution state fixture session identity");
    require_equal(ctx_.get_execution_id(), id, "execution state fixture identity");
    require_equal(ctx_.check_status(), OB_SUCCESS, "execution state fixture status");
  }

  ObExecContext &context() { return ctx_; }
private:
  ObArenaAllocator allocator_;
  ObPhysicalPlan plan_;
  ObSQLSessionInfo session_;
  ObExecContext ctx_;
};

void update_peak(std::atomic<int64_t> &peak, int64_t value)
{
  int64_t previous = peak.load();
  while (previous < value && !peak.compare_exchange_weak(previous, value)) {
  }
}

void test_memory_thresholds()
{
  const int64_t baseline = ObAIFuncClient::pipeline_buffer_usage();
  const uint64_t id = execution_id();
  {
    SemanticQueryBudget budget(id, 31, 6);
    require_equal(budget.execution_id(), id, "execution identity");
    require_equal(budget.memory_limit(), 31, "exact local byte limit");
    require_equal(budget.reserve(7), OB_SUCCESS, "first local reservation");
    require_equal(budget.reserve(24), OB_SUCCESS, "reservation exactly at local limit");
    require_equal(budget.memory_used(), 31, "local limit usage");
    require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline + 31, "shared local charges");

    budget.release(7);
    require_equal(budget.memory_used(), 24, "partial release");
    require_equal(budget.reserve(7), OB_SUCCESS, "released bytes are reusable");
    require_equal(budget.reserve(1), OB_SIZE_OVERFLOW, "one byte beyond local limit");
    require_equal(budget.memory_used(), 31, "local denial leaves its charge unchanged");
    require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline + 31,
                   "local denial must not charge shared accounting");
    budget.release(31);
    require_equal(budget.memory_used(), 0, "full explicit release");
    require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline, "explicit shared release");
  }
  require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline,
                 "destruction must not release explicit charges twice");

  {
    SemanticQueryBudget owner(execution_id(), 31, 1);
    SemanticQueryBudget rejected(execution_id(), 3, 1);
    require_equal(owner.reserve(7), OB_SUCCESS, "independent owner reservation");
    require_equal(rejected.reserve(4), OB_SIZE_OVERFLOW, "initial local overflow");
    require_equal(rejected.memory_used(), 0, "initial overflow rolls back to zero");
    require_equal(owner.memory_used(), 7, "denial must not change another owner");
    require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline + 7,
                   "initial overflow must not acquire global quota");
    owner.release(7);
    require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline, "release independent owner");
  }
  require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline, "independent owner cleanup");
}

void test_global_accounting_rollback()
{
  const int64_t baseline = ObAIFuncClient::pipeline_buffer_usage();
  const int64_t limit = ObAIFuncClient::pipeline_buffer_limit();
  require(limit - baseline > 2, "shared quota must have room for exact boundary checks");
  {
    SemanticQueryBudget blocker(execution_id(), limit, 1);
    // These are logical byte charges, not allocations of the shared quota size.
    require_equal(blocker.reserve(limit - baseline - 1), OB_SUCCESS, "leave one shared byte");
    {
      SemanticQueryBudget contender(execution_id(), 3, 1);
      require_equal(contender.reserve(2), OB_SIZE_OVERFLOW, "one byte beyond shared quota");
      require_equal(contender.memory_used(), 0, "shared denial rolls back local accounting");
      require_equal(ObAIFuncClient::pipeline_buffer_usage(), limit - 1,
                     "shared denial preserves other owners");
      require_equal(contender.reserve(1), OB_SUCCESS, "exact last shared byte");
      require_equal(ObAIFuncClient::pipeline_buffer_usage(), limit, "shared limit is exact");
      require_equal(contender.reserve(1), OB_SIZE_OVERFLOW, "shared quota exhausted");
      require_equal(contender.memory_used(), 1, "shared denial preserves prior local charge");
      require_equal(ObAIFuncClient::pipeline_buffer_usage(), limit, "no shared overcharge");

      blocker.release(1);
      require_equal(contender.reserve(1), OB_SUCCESS, "released shared credit is reusable");
      require_equal(contender.memory_used(), 2, "reused shared credit belongs to contender");
      require_equal(ObAIFuncClient::pipeline_buffer_usage(), limit, "shared reuse reaches limit");
      contender.release(2);
      require_equal(ObAIFuncClient::pipeline_buffer_usage(), limit - 2,
                     "contender explicitly releases only its two bytes");
    }
    require_equal(ObAIFuncClient::pipeline_buffer_usage(), limit - 2,
                   "contender destruction must not release twice");
    require_equal(blocker.memory_used(), limit - baseline - 2, "blocker owns remaining bytes");
    blocker.release(blocker.memory_used());
  }
  require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline, "all shared credits restored");
}

void test_client_shared_counter()
{
  const int64_t baseline = ObAIFuncClient::pipeline_buffer_usage();
  {
    SemanticQueryBudget budget(execution_id(), 31, 1);
    require_equal(budget.reserve(7), OB_SUCCESS, "query reservation");
    require_equal(ObAIFuncClient::reserve_pipeline_buffer(
        *budget.memory_counter(), budget.memory_limit(), 17), OB_SUCCESS,
        "client reservation uses query counter");
    require_equal(budget.memory_used(), 24, "client and query share local accounting");
    require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline + 24,
                   "client and query share global accounting");
    require_equal(ObAIFuncClient::reserve_pipeline_buffer(
        *budget.memory_counter(), budget.memory_limit(), 8), OB_SIZE_OVERFLOW,
        "client enforces the query byte limit");
    require_equal(budget.memory_used(), 24, "denied client reservation rolls back");
    require_equal(budget.reserve(7), OB_SUCCESS, "mixed reservations reach exact limit");
    ObAIFuncClient::release_pipeline_buffer(*budget.memory_counter(), 17);
    require_equal(budget.memory_used(), 14, "client releases only its own charge");
    budget.release(7);
    require_equal(budget.memory_used(), 7, "query releases only its own charge");
    budget.release(7);
    require_equal(budget.memory_used(), 0, "release remaining mixed-entry charge");
  }
  require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline,
                 "destructor must not release mixed-entry charges twice");
}

void test_task_admission_boundaries()
{
  for (const auto &shape : {std::pair<int64_t, int64_t>{1, 1}, {2, 3}, {4, 5}}) {
    const int64_t limit = shape.first * shape.second;
    SemanticQueryBudget budget(execution_id(), 64, limit);
    require_equal(budget.task_limit(), limit, "slots times expression count task limit");
    for (int64_t task = 0; task < limit; ++task) {
      require(budget.task_available(), "credit available before exact task limit");
      require_equal(budget.acquire_task(), OB_SUCCESS, "admit task within limit");
    }
    require(!budget.task_available(), "no task credit at exact limit");
    require_equal(budget.acquire_task(), OB_EAGAIN, "task limit is backpressure");
    budget.release_task();
    require(budget.task_available(), "one released task credit");
    require_equal(budget.acquire_task(), OB_SUCCESS, "reuse exactly one task credit");
    require_equal(budget.acquire_task(), OB_EAGAIN, "release must not manufacture extra credit");
    for (int64_t task = 0; task < limit; ++task) {
      budget.release_task();
    }
    for (int64_t task = 0; task < limit; ++task) {
      require_equal(budget.acquire_task(), OB_SUCCESS, "all released credits are reusable");
    }
    require_equal(budget.acquire_task(), OB_EAGAIN, "second admission wave has same exact limit");
    for (int64_t task = 0; task < limit; ++task) {
      budget.release_task();
    }
    require(budget.task_available(), "all task credits restored");
    require_equal(budget.memory_used(), 0, "task admission does not reserve memory implicitly");
  }
}

void test_task_capacity_independent_of_workers()
{
  constexpr int64_t SLOTS = 3;
  constexpr int64_t EXPRESSIONS = 4;
  constexpr int64_t LIMIT = SLOTS * EXPRESSIONS;
  for (int64_t workers : {1, 2, 8, 16}) {
    SemanticQueryBudget budget(execution_id(), 64, LIMIT);
    Gate held;
    JoiningThreads threads(held);
    std::atomic<int64_t> admitted{0};
    std::vector<int> errors(workers, OB_SUCCESS);
    for (int64_t worker = 0; worker < workers; ++worker) {
      threads.start([&, worker] {
        int64_t owned = 0;
        for (int64_t attempt = 0; attempt <= LIMIT; ++attempt) {
          const int ret = budget.acquire_task();
          if (ret == OB_SUCCESS) {
            ++owned;
            ++admitted;
          } else if (ret != OB_EAGAIN) {
            errors[worker] = ret;
          }
        }
        held.arrive_and_wait();
        for (int64_t task = 0; task < owned; ++task) {
          budget.release_task();
        }
      });
    }
    require(held.wait_for_arrivals(workers), "workers must finish admission before release");
    require_equal(admitted.load(), LIMIT, "worker count must not multiply task admission");
    require(!budget.task_available(), "all workers share one exhausted task pool");
    require_equal(budget.acquire_task(), OB_EAGAIN, "held worker pool remains bounded");
    held.release();
    threads.join();
    for (int ret : errors) {
      require_equal(ret, OB_SUCCESS, "worker admission return code");
    }
    for (int64_t task = 0; task < LIMIT; ++task) {
      require_equal(budget.acquire_task(), OB_SUCCESS, "worker releases restore all owned credits");
    }
    require_equal(budget.acquire_task(), OB_EAGAIN, "worker releases are exactly once");
    for (int64_t task = 0; task < LIMIT; ++task) {
      budget.release_task();
    }
  }
}

void test_concurrent_shared_budget()
{
  constexpr int64_t WORKERS = 16;
  constexpr int64_t ITERATIONS = 1500;
  constexpr int64_t TASK_LIMIT = 12;
  constexpr int64_t MAX_BYTES = 17;
  const int64_t baseline = ObAIFuncClient::pipeline_buffer_usage();
  SemanticQueryBudget budget(execution_id(), TASK_LIMIT * MAX_BYTES, TASK_LIMIT);
  Gate start;
  JoiningThreads threads(start);
  std::atomic<int64_t> active{0};
  std::atomic<int64_t> peak{0};
  std::atomic<int64_t> completed{0};
  std::vector<int> errors(WORKERS, OB_SUCCESS);
  for (int64_t worker = 0; worker < WORKERS; ++worker) {
    threads.start([&, worker] {
      start.arrive_and_wait();
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
      for (int64_t iteration = 0; iteration < ITERATIONS; ++iteration) {
        int ret = OB_EAGAIN;
        while (ret == OB_EAGAIN && std::chrono::steady_clock::now() < deadline) {
          ret = budget.acquire_task();
          if (ret == OB_EAGAIN) {
            std::this_thread::yield();
          }
        }
        if (ret != OB_SUCCESS) {
          errors[worker] = ret == OB_EAGAIN ? OB_TIMEOUT : ret;
          break;
        }
        update_peak(peak, active.fetch_add(1) + 1);
        const int64_t bytes = (worker + iteration) % MAX_BYTES + 1;
        ret = budget.reserve(bytes);
        if (ret == OB_SUCCESS) {
          if (budget.memory_used() < 0 || budget.memory_used() > budget.memory_limit()) {
            errors[worker] = OB_ERR_UNEXPECTED;
          }
          std::this_thread::yield();
          budget.release(bytes);
          ++completed;
        } else {
          errors[worker] = ret;
        }
        --active;
        budget.release_task();
        if (errors[worker] != OB_SUCCESS) {
          break;
        }
      }
    });
  }
  require(start.wait_for_arrivals(WORKERS), "stress workers must start together");
  start.release();
  threads.join();
  for (int64_t worker = 0; worker < WORKERS; ++worker) {
    require_equal(errors[worker], OB_SUCCESS, "concurrent memory/task operations worker " + std::to_string(worker));
  }
  require_equal(completed.load(), WORKERS * ITERATIONS, "every concurrent operation completes");
  require(peak.load() > 1 && peak.load() <= TASK_LIMIT, "concurrent task peak respects shared limit");
  require_equal(active.load(), 0, "no outstanding worker credit");
  require_equal(budget.memory_used(), 0, "no outstanding concurrent memory charge");
  require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline, "concurrent global accounting balances");
}

void test_fatal_failure_and_cleanup_once()
{
  const int64_t baseline = ObAIFuncClient::pipeline_buffer_usage();
  int memory_ret = OB_SUCCESS;
  int task_ret = OB_SUCCESS;
  {
    SemanticQueryBudget survivor(execution_id(), 64, 1);
    require_equal(survivor.reserve(17), OB_SUCCESS, "independent survivor charge");
    {
      SemanticQueryBudget failed(execution_id(), 64, 2);
      require_equal(failed.reserve(23), OB_SUCCESS, "pre-failure query charge");
      require_equal(ObAIFuncClient::reserve_pipeline_buffer(
          *failed.memory_counter(), failed.memory_limit(), 5), OB_SUCCESS, "pre-failure client charge");
      require_equal(failed.acquire_task(), OB_SUCCESS, "first pre-failure task");
      require_equal(failed.acquire_task(), OB_SUCCESS, "second pre-failure task");
      failed.fail(OB_TIMEOUT);
      failed.fail(OB_ERR_UNEXPECTED);
      failed.fail(OB_SUCCESS);
      require_equal(failed.failure(), OB_TIMEOUT, "first fatal error is sticky");
      failed.release(7);
      failed.release_task();
      memory_ret = failed.reserve(1);
      task_ret = failed.acquire_task();
      if (memory_ret == OB_SUCCESS) {
        failed.release(1);
      }
      if (task_ret == OB_SUCCESS) {
        failed.release_task();
      }
      require_equal(failed.memory_used(), 21, "fatal admission leaves existing charges unchanged");
      require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline + 17 + 21,
                     "fatal admission must not acquire shared quota");
      failed.release_task();
      failed.release(21);
      require_equal(failed.memory_used(), 0, "fatal cleanup releases all owned memory");
      require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline + 17,
                     "fatal explicit cleanup preserves another owner");
    }
    require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline + 17,
                   "failed destructor must not repeat explicit cleanup");
    require_equal(survivor.memory_used(), 17, "failed cleanup must preserve another query");
    survivor.release(17);
  }
  require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline, "fatal and normal owners balance");
  require_equal(memory_ret, OB_TIMEOUT, "fatal memory grant; task return=" + std::to_string(task_ret));
  require_equal(task_ret, OB_TIMEOUT, "fatal query must not receive new task credit");
}

void test_concurrent_failure_no_new_credit()
{
  constexpr int64_t WORKERS = 8;
  constexpr int64_t ATTEMPTS = 500;
  constexpr int64_t TASK_LIMIT = 12;
  const int64_t baseline = ObAIFuncClient::pipeline_buffer_usage();
  SemanticQueryBudget budget(execution_id(), 128, TASK_LIMIT);
  require_equal(budget.reserve(128), OB_SUCCESS, "fatal stress existing memory");
  for (int64_t task = 0; task < TASK_LIMIT; ++task) {
    require_equal(budget.acquire_task(), OB_SUCCESS, "fatal stress existing tasks");
  }
  budget.fail(OB_TIMEOUT);
  budget.release(1);
  budget.release_task();
  Gate start;
  JoiningThreads threads(start);
  std::vector<int> memory_errors(WORKERS, OB_TIMEOUT);
  std::vector<int> task_errors(WORKERS, OB_TIMEOUT);
  for (int64_t worker = 0; worker < WORKERS; ++worker) {
    threads.start([&, worker] {
      start.arrive_and_wait();
      for (int64_t attempt = 0; attempt < ATTEMPTS; ++attempt) {
        const int task_ret = budget.acquire_task();
        if (task_ret != OB_TIMEOUT && task_errors[worker] == OB_TIMEOUT) {
          task_errors[worker] = task_ret;
        }
        if (task_ret == OB_SUCCESS) {
          budget.release_task();
        }
        const int memory_ret = budget.reserve(1);
        if (memory_ret != OB_TIMEOUT && memory_errors[worker] == OB_TIMEOUT) {
          memory_errors[worker] = memory_ret;
        }
        if (memory_ret == OB_SUCCESS) {
          budget.release(1);
        }
      }
    });
  }
  require(start.wait_for_arrivals(WORKERS), "fatal stress workers must start after failure");
  start.release();
  threads.join();
  for (int64_t worker = 0; worker < WORKERS; ++worker) {
    require_equal(task_errors[worker], OB_TIMEOUT, "concurrent fatal task return");
    require_equal(memory_errors[worker], OB_TIMEOUT, "concurrent fatal memory return");
  }
  require_equal(budget.failure(), OB_TIMEOUT, "fatal status survives concurrent attempts");
  require_equal(budget.memory_used(), 127, "concurrent fatal attempts add no local credit");
  require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline + 127,
                 "concurrent fatal attempts add no global credit");
  for (int64_t task = 0; task < TASK_LIMIT - 1; ++task) {
    budget.release_task();
  }
  budget.release(127);
  require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline, "fatal stress cleanup");
}

void test_concurrent_final_execution_state_teardown()
{
  constexpr int64_t REPETITIONS = 20;
  const int64_t baseline = SemanticExecutionState::query_count();
  for (int64_t repetition = 0; repetition < REPETITIONS; ++repetition) {
    const std::string iteration = "final teardown iteration " + std::to_string(repetition + 1) + ": ";
    const uint64_t id = execution_id();
    ExecutionFixture first(id);
    ExecutionFixture second(id);
    std::shared_ptr<SemanticExecutionState> states[2];
    require_equal(SemanticExecutionState::acquire(first.context(), 64, 2, states[0]),
                  OB_SUCCESS, iteration + "acquire first execution state");
    require(states[0] != nullptr, iteration + "first acquired state is present");
    require_equal(SemanticExecutionState::query_count(), baseline + 1,
                  iteration + "first state registers one query budget");
    require_equal(SemanticExecutionState::acquire(second.context(), 64, 2, states[1]),
                  OB_SUCCESS, iteration + "acquire second execution state");
    require(states[1] != nullptr, iteration + "second acquired state is present");
    require(states[0].get() != states[1].get(), iteration + "contexts own distinct execution states");
    require(&states[0]->budget() == &states[1]->budget(), iteration + "states share one query budget");
    require_equal(states[0]->budget().execution_id(), id, iteration + "shared execution identity");
    require_equal(SemanticExecutionState::query_count(), baseline + 1,
                  iteration + "shared states retain exactly one registry entry");
    const std::weak_ptr<SemanticExecutionState> weak_states[2] = {states[0], states[1]};
    for (const auto &state : states) {
      require_equal(state.use_count(), 1, iteration + "only the final state owner remains");
    }
    Gate teardown;
    JoiningThreads threads(teardown);
    for (int64_t worker = 0; worker < 2; ++worker) {
      threads.start([&, worker] {
        teardown.arrive_and_wait();
        states[worker].reset();
      });
    }
    require(teardown.wait_for_arrivals(2), iteration + "both final owners wait before teardown");
    require_equal(SemanticExecutionState::query_count(), baseline + 1,
                  iteration + "registry entry remains live behind the teardown gate");
    for (const auto &state : weak_states) {
      require(!state.expired(), iteration + "gated final state remains live");
    }
    teardown.release();
    threads.join();
    for (const auto &state : states) {
      require(state == nullptr, iteration + "final owner was reset");
    }
    for (const auto &state : weak_states) {
      require(state.expired(), iteration + "final state was destroyed");
    }
    require_equal(SemanticExecutionState::query_count(), baseline,
                  iteration + "concurrent final teardown removes all query metadata");
  }
}

void test_completed_producers_release_credits_without_another_pull()
{
  ExecutionFixture fixture(execution_id());
  std::shared_ptr<SemanticExecutionState> state;
  require_equal(SemanticExecutionState::acquire(fixture.context(), 64, 2, state),
                OB_SUCCESS, "acquire completion polling state");
  struct Producer
  {
    SemanticQueryBudget &budget_;
    bool credit_ = true;
    int status_ = OB_SUCCESS;
    int polls_ = 0;
    static bool ready(const void *data)
    {
      return static_cast<const Producer *>(data)->credit_;
    }
    static int poll(void *data, bool &progress)
    {
      Producer &producer = *static_cast<Producer *>(data);
      ++producer.polls_;
      if (producer.status_ == OB_SUCCESS && producer.credit_) {
        producer.budget_.release_task();
        producer.credit_ = false;
        progress = true;
      }
      return producer.status_;
    }
  };
  Producer first{state->budget()};
  Producer second{state->budget()};
  require_equal(state->budget().acquire_task(), OB_SUCCESS, "first producer admission");
  require_equal(state->budget().acquire_task(), OB_SUCCESS, "second producer admission");
  require_equal(state->budget().acquire_task(), OB_EAGAIN, "full downstream has no credit");
  require_equal(state->register_operator(&first, Producer::ready, Producer::poll),
                OB_SUCCESS, "register first producer");
  require_equal(state->register_operator(&second, Producer::ready, Producer::poll),
                OB_SUCCESS, "register second producer");
  bool progress = false;
  require_equal(state->poll_ready_tasks(progress), OB_SUCCESS, "poll without another producer pull");
  require(progress, "completed upstream producers make credit progress");
  require_equal(first.polls_, 1, "first producer was polled");
  require_equal(second.polls_, 1, "second producer was polled");
  require_equal(state->budget().acquire_task(), OB_SUCCESS, "first downstream admission");
  require_equal(state->budget().acquire_task(), OB_SUCCESS, "second downstream admission");
  progress = false;
  require_equal(state->poll_ready_tasks(progress), OB_SUCCESS, "repeat completion polling");
  require(!progress, "completed producer cannot release the same credit twice");
  require_equal(state->budget().acquire_task(), OB_EAGAIN, "repeat polling preserves task ceiling");
  state->budget().release_task();
  state->budget().release_task();
  first.status_ = OB_ERR_UNEXPECTED;
  require_equal(state->poll_ready_tasks(progress), OB_ERR_UNEXPECTED, "poll failure propagates");
  require_equal(second.polls_, 2, "failed polling stops before the next producer");
  state->unregister_operator(&first);
  state->unregister_operator(&second);
}

void test_suspension_scope_defaults_and_override()
{
  require(SemanticSuspendScope::allowed(), "initial suspension scope");
  {
    SemanticSuspendScope scan(PHY_TABLE_SCAN);
    require(SemanticSuspendScope::allowed(), "stateless scan is allowed by default");
    {
      SemanticSuspendScope unverified(PHY_SORT);
      require(!SemanticSuspendScope::allowed(), "unverified collection blocks suspension");
      {
        SemanticSuspendScope verified_child(PHY_PX_REDUCE_TRANSMIT, false, true);
        require(!SemanticSuspendScope::allowed(), "verified child cannot bypass unsafe ancestor");
      }
      require(!SemanticSuspendScope::allowed(), "unsafe ancestor survives child teardown");
    }
    require(SemanticSuspendScope::allowed(), "ancestor teardown restores eligibility");
  }
  for (ObPhyOperatorType type : {PHY_SORT, PHY_HASH_GROUP_BY, PHY_PX_REDUCE_TRANSMIT}) {
    {
      SemanticSuspendScope unverified(type);
      require(!SemanticSuspendScope::allowed(), "resumable operators default to blocking");
    }
    {
      SemanticSuspendScope verified(type, false, true);
      require(SemanticSuspendScope::allowed(), "verified override permits suspension");
      {
        SemanticSuspendScope opening(type, true, true);
        require(!SemanticSuspendScope::allowed(), "opening cannot unwind even with verified override");
      }
      require(SemanticSuspendScope::allowed(), "opening teardown restores verified scope");
    }
    require(SemanticSuspendScope::allowed(), "verified scope teardown restores default");
  }
}

void test_suspension_scope_thread_isolation()
{
  require(SemanticSuspendScope::allowed(), "main thread initial eligibility");
  Gate blocked;
  JoiningThreads threads(blocked);
  std::atomic<bool> worker_blocked{false};
  std::atomic<bool> worker_restored{false};
  threads.start([&] {
    {
      SemanticSuspendScope unsafe(PHY_SORT);
      worker_blocked = !SemanticSuspendScope::allowed();
      blocked.arrive_and_wait();
    }
    worker_restored = SemanticSuspendScope::allowed();
  });
  require(blocked.wait_for_arrivals(1), "worker must hold its unsafe scope");
  require(worker_blocked.load(), "worker scope must block its own suspension");
  require(SemanticSuspendScope::allowed(), "worker must not block main thread eligibility");
  {
    SemanticSuspendScope verified(PHY_PX_REDUCE_TRANSMIT, false, true);
    require(SemanticSuspendScope::allowed(), "verified main scope is independent of worker");
  }
  blocked.release();
  threads.join();
  require(worker_restored.load(), "worker scope must restore its thread-local state");
  require(SemanticSuspendScope::allowed(), "main eligibility remains unchanged");
}

struct TestCase
{
  const char *name_;
  void (*run_)();
};

const TestCase TESTS[] = {
  {"memory_thresholds", test_memory_thresholds},
  {"global_accounting_rollback", test_global_accounting_rollback},
  {"client_shared_counter", test_client_shared_counter},
  {"task_admission_boundaries", test_task_admission_boundaries},
  {"task_capacity_independent_of_workers", test_task_capacity_independent_of_workers},
  {"concurrent_shared_budget", test_concurrent_shared_budget},
  {"fatal_failure_and_cleanup_once", test_fatal_failure_and_cleanup_once},
  {"concurrent_failure_no_new_credit", test_concurrent_failure_no_new_credit},
  {"concurrent_final_execution_state_teardown", test_concurrent_final_execution_state_teardown},
  {"completed_producers_release_credits_without_another_pull",
   test_completed_producers_release_credits_without_another_pull},
  {"suspension_scope_defaults_and_override", test_suspension_scope_defaults_and_override},
  {"suspension_scope_thread_isolation", test_suspension_scope_thread_isolation},
};
} // namespace

int main(int argc, char **argv)
{
  try {
    int64_t repeats = 1;
    std::string selected;
    for (int index = 1; index < argc; index += 2) {
      require(index + 1 < argc, "native test options require a value");
      if (std::string(argv[index]) == "--repeat") {
        char *end = nullptr;
        repeats = std::strtoll(argv[index + 1], &end, 10);
        require(end != argv[index + 1] && *end == '\0' && repeats > 0 && repeats <= 100,
                "--repeat must be an integer in [1, 100]");
      } else if (std::string(argv[index]) == "--case") {
        selected = argv[index + 1];
        require(!selected.empty(), "--case must not be empty");
      } else {
        require(false, "usage: semantic_resource_tests [--repeat COUNT] [--case NAME]");
      }
    }
    bool found = selected.empty();
    for (const auto &test : TESTS) {
      found = found || selected == test.name_;
    }
    require(found, "unknown native resource test case: " + selected);
    int64_t completed_cases = 0;
    for (int64_t repeat = 0; repeat < repeats; ++repeat) {
      for (const auto &test : TESTS) {
        if (!selected.empty() && selected != test.name_) {
          continue;
        }
        const int64_t baseline = ObAIFuncClient::pipeline_buffer_usage();
        test.run_();
        require_equal(ObAIFuncClient::pipeline_buffer_usage(), baseline,
                       std::string(test.name_) + " leaves shared accounting unchanged");
        std::cout << "PASS " << test.name_ << " repeat=" << repeat + 1 << std::endl;
        ++completed_cases;
      }
    }
    std::cout << "semantic resource native tests passed: "
              << completed_cases << " cases" << std::endl;
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "FAIL semantic resource native tests: " << error.what() << std::endl;
    return 1;
  }
}
