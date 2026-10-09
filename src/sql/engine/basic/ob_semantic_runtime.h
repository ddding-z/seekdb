#ifndef OCEANBASE_SQL_ENGINE_SEMANTIC_RUNTIME_H_
#define OCEANBASE_SQL_ENGINE_SEMANTIC_RUNTIME_H_

#include "lib/ob_errno.h"
#include "query/engine/ob_phy_operator_type.h"
#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

namespace oceanbase
{
namespace sql
{
class ObExecContext;

class SemanticQueryBudget
{
public:
  SemanticQueryBudget(uint64_t execution_id, int64_t memory_limit, int64_t task_limit);
  ~SemanticQueryBudget();
  int reserve(int64_t bytes);
  void release(int64_t bytes);
  int acquire_task();
  void release_task();
  void fail(int ret);
  int failure() const { return failure_.load(); }
  bool task_available() const { return active_tasks_.load() < task_limit_; }
  int64_t memory_used() const;
  int64_t memory_limit() const { return memory_limit_; }
  int64_t task_limit() const { return task_limit_; }
  int64_t *memory_counter() { return &memory_used_; }
  uint64_t execution_id() const { return execution_id_; }
private:
  uint64_t execution_id_;
  int64_t memory_limit_;
  int64_t task_limit_;
  int64_t memory_used_;
  std::atomic<int64_t> active_tasks_;
  std::atomic<int> failure_;
};

class SemanticExecutionState
{
public:
  using ReadyCheck = bool (*)(const void *);
  using ReadyPoll = int (*)(void *, bool &);
  static int acquire(ObExecContext &ctx, int64_t memory_limit, int64_t task_limit,
                     std::shared_ptr<SemanticExecutionState> &state);
  static int64_t query_count();
  ~SemanticExecutionState();
  int register_operator(void *operation, ReadyCheck ready, ReadyPoll poll = nullptr);
  void unregister_operator(const void *operation);
  // The owning SQL worker collects completions without evaluating another operator's frame.
  int poll_ready_tasks(bool &progress);
  static bool can_resume(const void *state);
  SemanticQueryBudget &budget() { return *budget_; }
  const SemanticQueryBudget &budget() const { return *budget_; }
private:
  struct Waiter
  {
    void *operation_;
    ReadyCheck ready_;
    ReadyPoll poll_;
  };
  SemanticExecutionState(ObExecContext &ctx, std::shared_ptr<SemanticQueryBudget> budget,
                         int64_t deadline);
  ObExecContext *ctx_;
  std::shared_ptr<SemanticQueryBudget> budget_;
  int64_t deadline_;
  bool registered_;
  std::mutex mutex_;
  std::vector<Waiter> waiters_;
};

// A pull operator with stack-local partial state must not unwind on an AI wait.
class SemanticSuspendScope
{
public:
  explicit SemanticSuspendScope(ObPhyOperatorType type, bool opening = false,
                                 bool resumable = false);
  ~SemanticSuspendScope();
  static bool allowed();
private:
  bool blocked_;
  static thread_local int64_t blocked_depth_;
};

}
}
#endif
