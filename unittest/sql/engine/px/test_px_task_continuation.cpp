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
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
#include "observer/omt/ob_px_task_continuation_store.h"

using oceanbase::omt::PxContinuationAdmission;
using oceanbase::omt::PxTaskContinuationStore;
using oceanbase::query::IPxTaskContinuation;
using oceanbase::query::PxTaskRunResult;

struct Counts
{
  std::atomic<int> runs{0};
  std::atomic<int> polls{0};
  std::atomic<int> cancelled{0};
  std::atomic<int> destroyed{0};
};

class TestTask final : public IPxTaskContinuation
{
public:
  TestTask(Counts &counts, int suspensions) : counts_(counts), remaining_(suspensions) {}

  bool is_ready() override
  {
    assert(0 == executing_.load());
    ++counts_.polls;
    if (nullptr != poll_started_) {
      poll_started_->store(true);
      while (!finish_poll_->load()) {
        std::this_thread::yield();
      }
    }
    return ready_.load();
  }

  PxTaskRunResult run(bool need_exec) override
  {
    assert(0 == executing_.fetch_add(1));
    ++counts_.runs;
    PxTaskRunResult result = PxTaskRunResult::FINISHED;
    if (!need_exec) {
      ++counts_.cancelled;
    } else if (remaining_ > 0) {
      --remaining_;
      result = PxTaskRunResult::SUSPENDED;
    }
    assert(1 == executing_.fetch_sub(1));
    return result;
  }

  void destroy() override
  {
    ++counts_.destroyed;
    delete this;
  }

  std::atomic<bool> ready_{false};
  std::atomic<bool> *poll_started_ = nullptr;
  std::atomic<bool> *finish_poll_ = nullptr;

private:
  Counts &counts_;
  int remaining_;
  std::atomic<int> executing_{0};
};

template <size_t CAPACITY>
void execute(PxTaskContinuationStore<CAPACITY> &store,
             const typename PxTaskContinuationStore<CAPACITY>::Lease &lease)
{
  const PxTaskRunResult result = lease.task_->run(lease.need_exec_);
  assert(store.finish(lease, result));
  if (PxTaskRunResult::FINISHED == result) {
    lease.task_->destroy();
  }
}

void test_pending_retains_capacity_and_ownership()
{
  PxTaskContinuationStore<1> store;
  Counts counts;
  auto *task = new TestTask(counts, 1);
  assert(PxContinuationAdmission::ACCEPTED == store.submit(task));
  assert(PxContinuationAdmission::DUPLICATE == store.submit(task));
  assert(1 == store.size());
  assert(1 == store.runnable_count());

  PxTaskContinuationStore<1>::Lease initial;
  assert(store.take_ready(initial));
  PxTaskContinuationStore<1>::Lease duplicate;
  assert(!store.take_ready(duplicate));
  execute(store, initial);
  assert(1 == counts.runs);
  assert(0 == counts.destroyed);
  assert(1 == store.size());
  assert(0 == store.runnable_count());
  assert(!store.take_ready(duplicate));

  Counts rejected_counts;
  auto *rejected = new TestTask(rejected_counts, 0);
  assert(PxContinuationAdmission::FULL == store.submit(rejected));
  assert(0 == rejected_counts.runs);
  assert(0 == rejected_counts.destroyed);
  rejected->destroy();

  task->ready_.store(true);
  PxTaskContinuationStore<1>::Lease resumed;
  assert(store.take_ready(resumed));
  assert(resumed.task_ == task);
  assert(initial.generation_ == resumed.generation_);
  assert(!store.take_ready(duplicate));
  execute(store, resumed);
  assert(2 == counts.runs);
  assert(1 == counts.destroyed);
  assert(0 == store.size());
  assert(0 == store.runnable_count());
  assert(!store.finish(resumed, PxTaskRunResult::FINISHED));
}

void test_stale_lease_and_invalid_submission()
{
  PxTaskContinuationStore<1> store;
  assert(PxContinuationAdmission::INVALID == store.submit(nullptr));
  Counts first_counts;
  auto *first = new TestTask(first_counts, 0);
  assert(PxContinuationAdmission::ACCEPTED == store.submit(first));
  PxTaskContinuationStore<1>::Lease old_lease;
  assert(store.take_ready(old_lease));
  execute(store, old_lease);

  Counts second_counts;
  auto *second = new TestTask(second_counts, 0);
  assert(PxContinuationAdmission::ACCEPTED == store.submit(second));
  PxTaskContinuationStore<1>::Lease new_lease;
  assert(store.take_ready(new_lease));
  assert(old_lease.generation_ != new_lease.generation_);
  assert(!store.finish(old_lease, PxTaskRunResult::FINISHED));
  assert(1 == store.size());
  execute(store, new_lease);
  assert(1 == first_counts.destroyed);
  assert(1 == second_counts.destroyed);
}

void test_one_worker_advances_independent_waiters()
{
  PxTaskContinuationStore<2> store;
  Counts first_counts;
  Counts second_counts;
  auto *first = new TestTask(first_counts, 1);
  auto *second = new TestTask(second_counts, 1);
  assert(PxContinuationAdmission::ACCEPTED == store.submit(first));
  assert(PxContinuationAdmission::ACCEPTED == store.submit(second));
  PxTaskContinuationStore<2>::Lease lease;
  assert(store.take_ready(lease));
  execute(store, lease);
  assert(store.take_ready(lease));
  execute(store, lease);
  assert(2 == store.size());
  assert(0 == store.runnable_count());
  assert(!store.take_ready(lease));
  assert(1 == first_counts.runs);
  assert(1 == second_counts.runs);

  second->ready_.store(true);
  assert(store.take_ready(lease));
  assert(second == lease.task_);
  execute(store, lease);
  assert(1 == first_counts.runs);
  assert(2 == second_counts.runs);
  assert(1 == second_counts.destroyed);
  assert(1 == store.size());

  store.stop();
  assert(store.take_ready(lease));
  assert(first == lease.task_);
  assert(!lease.need_exec_);
  execute(store, lease);
  assert(1 == first_counts.cancelled);
  assert(1 == first_counts.destroyed);
  assert(0 == store.size());
}

void test_shutdown_drains_ready_and_waiting_tasks()
{
  PxTaskContinuationStore<3> store;
  Counts counts[3];
  TestTask *tasks[3];
  for (size_t i = 0; i < 3; ++i) {
    tasks[i] = new TestTask(counts[i], 10);
    assert(PxContinuationAdmission::ACCEPTED == store.submit(tasks[i]));
  }
  PxTaskContinuationStore<3>::Lease lease;
  assert(store.take_ready(lease));
  execute(store, lease);
  assert(2 == store.runnable_count());

  store.stop();
  Counts rejected_counts;
  auto *rejected = new TestTask(rejected_counts, 0);
  assert(PxContinuationAdmission::STOPPED == store.submit(rejected));
  assert(0 == rejected_counts.runs);
  rejected->destroy();
  while (store.take_ready(lease)) {
    assert(!lease.need_exec_);
    execute(store, lease);
  }
  assert(0 == store.size());
  for (const Counts &count : counts) {
    assert(1 == count.cancelled);
    assert(1 == count.destroyed);
  }
}

void test_recycling_does_not_require_shutdown_drain()
{
  PxTaskContinuationStore<1> store;
  Counts counts;
  auto *task = new TestTask(counts, 10);
  assert(PxContinuationAdmission::ACCEPTED == store.submit(task));
  PxTaskContinuationStore<1>::Lease lease;
  assert(store.take_ready(lease));
  execute(store, lease);
  assert(1 == store.size());
  assert(!store.needs_shutdown_drain());
  store.stop();
  assert(store.needs_shutdown_drain());
  assert(store.take_ready(lease));
  execute(store, lease);
  assert(0 == store.size());
  assert(!store.needs_shutdown_drain());
  assert(1 == counts.cancelled);
  assert(1 == counts.destroyed);
}

void test_shutdown_during_readiness_poll()
{
  PxTaskContinuationStore<1> store;
  Counts counts;
  auto *task = new TestTask(counts, 10);
  assert(PxContinuationAdmission::ACCEPTED == store.submit(task));
  PxTaskContinuationStore<1>::Lease initial;
  assert(store.take_ready(initial));
  execute(store, initial);

  std::atomic<bool> poll_started{false};
  std::atomic<bool> finish_poll{false};
  task->poll_started_ = &poll_started;
  task->finish_poll_ = &finish_poll;
  std::thread polling([&]() {
    PxTaskContinuationStore<1>::Lease resumed;
    assert(store.take_ready(resumed));
    assert(!resumed.need_exec_);
    execute(store, resumed);
  });
  while (!poll_started.load()) {
    std::this_thread::yield();
  }
  store.stop();
  PxTaskContinuationStore<1>::Lease duplicate;
  assert(!store.take_ready(duplicate));
  finish_poll.store(true);
  polling.join();
  assert(1 == counts.cancelled);
  assert(1 == counts.destroyed);
  assert(0 == store.size());
}

void test_shutdown_during_running_slice()
{
  PxTaskContinuationStore<1> store;
  Counts counts;
  auto *task = new TestTask(counts, 10);
  assert(PxContinuationAdmission::ACCEPTED == store.submit(task));
  PxTaskContinuationStore<1>::Lease running;
  assert(store.take_ready(running));
  store.stop();
  PxTaskContinuationStore<1>::Lease duplicate;
  assert(!store.take_ready(duplicate));
  execute(store, running);
  assert(1 == store.size());
  assert(store.take_ready(duplicate));
  assert(!duplicate.need_exec_);
  execute(store, duplicate);
  assert(1 == counts.cancelled);
  assert(1 == counts.destroyed);
  assert(0 == store.size());
}

void test_capacity_bound()
{
  PxTaskContinuationStore<64> store;
  Counts counts[65];
  for (size_t i = 0; i < 64; ++i) {
    assert(PxContinuationAdmission::ACCEPTED == store.submit(new TestTask(counts[i], 1)));
  }
  auto *overflow = new TestTask(counts[64], 1);
  assert(PxContinuationAdmission::FULL == store.submit(overflow));
  assert(64 == store.size());
  overflow->destroy();
  store.stop();
  PxTaskContinuationStore<64>::Lease lease;
  while (store.take_ready(lease)) {
    execute(store, lease);
  }
  for (size_t i = 0; i < 64; ++i) {
    assert(1 == counts[i].runs);
    assert(1 == counts[i].cancelled);
    assert(1 == counts[i].destroyed);
  }
  assert(0 == store.size());
}

void test_multiworker_exactly_once()
{
  static constexpr size_t TASK_COUNT = 32;
  static constexpr int SUSPENSIONS = 50;
  PxTaskContinuationStore<TASK_COUNT> store;
  Counts counts[TASK_COUNT];
  for (size_t i = 0; i < TASK_COUNT; ++i) {
    auto *task = new TestTask(counts[i], SUSPENSIONS);
    task->ready_.store(true);
    assert(PxContinuationAdmission::ACCEPTED == store.submit(task));
  }
  std::vector<std::thread> workers;
  for (size_t i = 0; i < 8; ++i) {
    workers.emplace_back([&]() {
      while (store.size() > 0) {
        PxTaskContinuationStore<TASK_COUNT>::Lease lease;
        if (store.take_ready(lease)) {
          execute(store, lease);
        } else {
          std::this_thread::yield();
        }
      }
    });
  }
  for (std::thread &worker : workers) {
    worker.join();
  }
  for (const Counts &count : counts) {
    assert(SUSPENSIONS + 1 == count.runs);
    assert(0 == count.cancelled);
    assert(1 == count.destroyed);
  }
  assert(0 == store.size());
}

int main()
{
  test_pending_retains_capacity_and_ownership();
  test_stale_lease_and_invalid_submission();
  test_one_worker_advances_independent_waiters();
  test_shutdown_drains_ready_and_waiting_tasks();
  test_recycling_does_not_require_shutdown_drain();
  test_shutdown_during_readiness_poll();
  test_shutdown_during_running_slice();
  test_capacity_bound();
  test_multiworker_exactly_once();
  std::cout << "PX continuation lifecycle tests passed (9 cases)" << std::endl;
  return 0;
}
