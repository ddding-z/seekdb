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

#ifndef OCEANBASE_OBSERVER_OMT_OB_PX_TASK_CONTINUATION_STORE_H_
#define OCEANBASE_OBSERVER_OMT_OB_PX_TASK_CONTINUATION_STORE_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include "query/runtime/ob_px_task_continuation.h"

namespace oceanbase
{
namespace omt
{

enum class PxContinuationAdmission { ACCEPTED, FULL, STOPPED, DUPLICATE, INVALID };

template <size_t CAPACITY>
class PxTaskContinuationStore
{
  static_assert(CAPACITY > 0, "PX continuation capacity must be positive");

public:
  struct Lease
  {
    query::IPxTaskContinuation *task_ = nullptr;
    size_t index_ = CAPACITY;
    uint64_t generation_ = 0;
    bool need_exec_ = true;
  };

  PxTaskContinuationStore() = default;

  PxContinuationAdmission submit(query::IPxTaskContinuation *task)
  {
    std::lock_guard<std::mutex> guard(lock_);
    PxContinuationAdmission result = PxContinuationAdmission::FULL;
    if (nullptr == task) {
      result = PxContinuationAdmission::INVALID;
    } else if (stopped_) {
      result = PxContinuationAdmission::STOPPED;
    } else {
      size_t free_index = CAPACITY;
      for (size_t i = 0; i < CAPACITY; ++i) {
        if (slots_[i].task_ == task) {
          return PxContinuationAdmission::DUPLICATE;
        } else if (State::EMPTY == slots_[i].state_ && CAPACITY == free_index) {
          free_index = i;
        }
      }
      if (CAPACITY != free_index) {
        Slot &slot = slots_[free_index];
        slot.task_ = task;
        ++slot.generation_;
        slot.state_ = State::READY;
        ++count_;
        result = PxContinuationAdmission::ACCEPTED;
      }
    }
    return result;
  }

  bool take_ready(Lease &lease)
  {
    bool found = false;
    size_t start = 0;
    {
      std::lock_guard<std::mutex> guard(lock_);
      start = next_;
    }
    for (size_t offset = 0; !found && offset < CAPACITY; ++offset) {
      query::IPxTaskContinuation *poll_task = nullptr;
      size_t index = CAPACITY;
      uint64_t generation = 0;
      {
        std::lock_guard<std::mutex> guard(lock_);
        index = (start + offset) % CAPACITY;
        Slot &slot = slots_[index];
        if (State::READY == slot.state_ || (stopped_ && State::WAITING == slot.state_)) {
          acquire(index, lease);
          found = true;
        } else if (State::WAITING == slot.state_) {
          slot.state_ = State::POLLING;
          poll_task = slot.task_;
          generation = slot.generation_;
        }
      }
      if (nullptr != poll_task) {
        const bool ready = poll_task->is_ready();
        std::lock_guard<std::mutex> guard(lock_);
        Slot &slot = slots_[index];
        if (State::POLLING == slot.state_ && slot.generation_ == generation) {
          if (stopped_ || ready) {
            acquire(index, lease);
            found = true;
          } else {
            slot.state_ = State::WAITING;
          }
        }
      }
    }
    return found;
  }

  bool finish(const Lease &lease, query::PxTaskRunResult result)
  {
    std::lock_guard<std::mutex> guard(lock_);
    bool valid = lease.index_ < CAPACITY;
    if (valid) {
      Slot &slot = slots_[lease.index_];
      valid = State::RUNNING == slot.state_ && slot.task_ == lease.task_ &&
              slot.generation_ == lease.generation_;
      if (valid) {
        if (query::PxTaskRunResult::FINISHED == result) {
          slot.task_ = nullptr;
          slot.state_ = State::EMPTY;
          --count_;
        } else {
          slot.state_ = State::WAITING;
        }
      }
    }
    return valid;
  }

  void stop()
  {
    std::lock_guard<std::mutex> guard(lock_);
    stopped_ = true;
  }

  size_t size() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return count_;
  }

  bool needs_shutdown_drain() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return stopped_ && count_ > 0;
  }

  size_t runnable_count() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    size_t count = 0;
    for (const Slot &slot : slots_) {
      count += State::READY == slot.state_ || State::RUNNING == slot.state_;
    }
    return count;
  }

private:
  enum class State { EMPTY, READY, WAITING, POLLING, RUNNING };
  struct Slot
  {
    query::IPxTaskContinuation *task_ = nullptr;
    uint64_t generation_ = 0;
    State state_ = State::EMPTY;
  };

  void acquire(size_t index, Lease &lease)
  {
    Slot &slot = slots_[index];
    slot.state_ = State::RUNNING;
    lease.task_ = slot.task_;
    lease.index_ = index;
    lease.generation_ = slot.generation_;
    lease.need_exec_ = !stopped_;
    next_ = (index + 1) % CAPACITY;
  }

  mutable std::mutex lock_;
  std::array<Slot, CAPACITY> slots_;
  size_t count_ = 0;
  size_t next_ = 0;
  bool stopped_ = false;

  PxTaskContinuationStore(const PxTaskContinuationStore &) = delete;
  PxTaskContinuationStore &operator=(const PxTaskContinuationStore &) = delete;
};

} // namespace omt
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_OMT_OB_PX_TASK_CONTINUATION_STORE_H_
