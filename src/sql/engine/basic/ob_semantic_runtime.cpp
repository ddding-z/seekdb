#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_semantic_runtime.h"
#include "sql/engine/basic/ob_semantic_media.h"
#include "sql/engine/basic/ob_semantic_cache.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/time/ob_time_utility.h"
#include <algorithm>
#include <map>
#include <new>

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{
struct SemanticRuntimeRegistry
{
  std::mutex mutex_;
  std::map<uint64_t, std::weak_ptr<SemanticQueryBudget>> budgets_;
  std::map<ObExecContext *, std::weak_ptr<SemanticExecutionState>> states_;
};

SemanticRuntimeRegistry &runtime_registry()
{
  static SemanticRuntimeRegistry registry;
  return registry;
}
}

SemanticQueryBudget::SemanticQueryBudget(uint64_t execution_id, int64_t memory_limit,
                                         int64_t task_limit, bool row_async_enabled)
    : execution_id_(execution_id), memory_limit_(memory_limit), task_limit_(task_limit),
      row_async_enabled_(row_async_enabled), memory_used_(0), active_tasks_(0), failure_(OB_SUCCESS)
{
}

SemanticQueryBudget::~SemanticQueryBudget()
{
  function_cache_.reset();
  if (memory_used() != 0 || active_tasks_.load() != 0) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "semantic query resources remain at destruction",
              K(execution_id_), K(memory_used_), "active_tasks", active_tasks_.load());
  }
}

int SemanticQueryBudget::function_cache(int64_t deadline, SemanticFunctionCache *&cache)
{
  int ret = failure();
  std::lock_guard<std::mutex> guard(cache_mutex_);
  if (OB_FAIL(ret)) {
  } else if (deadline <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (function_cache_ && function_cache_->deadline() != deadline) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parallel semantic cache users disagree on query deadline", K(ret), K(deadline));
  } else if (!function_cache_) {
    try {
      function_cache_.reset(new SemanticFunctionCache(*this, deadline));
    } catch (const std::bad_alloc &) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate semantic function cache failed", K(ret));
    }
  }
  cache = OB_SUCC(ret) ? function_cache_.get() : nullptr;
  return ret;
}

int SemanticQueryBudget::poll_function_cache(bool &progress)
{
  SemanticFunctionCache *cache = nullptr;
  {
    std::lock_guard<std::mutex> guard(cache_mutex_);
    cache = function_cache_.get();
  }
  return nullptr == cache ? OB_SUCCESS : cache->poll(progress);
}

bool SemanticQueryBudget::function_cache_ready() const
{
  std::lock_guard<std::mutex> guard(cache_mutex_);
  return function_cache_ && function_cache_->is_ready();
}

bool SemanticQueryBudget::function_cache_pending() const
{
  std::lock_guard<std::mutex> guard(cache_mutex_);
  return function_cache_ && function_cache_->has_pending();
}

int SemanticQueryBudget::reserve(int64_t bytes)
{
  int ret = failure();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObAIFuncClient::reserve_pipeline_buffer(memory_used_, memory_limit_, bytes))) {
  } else if (OB_FAIL(failure())) {
    release(bytes);
  }
  return ret;
}

void SemanticQueryBudget::release(int64_t bytes)
{
  ObAIFuncClient::release_pipeline_buffer(memory_used_, bytes);
}

int64_t SemanticQueryBudget::memory_used() const
{
  return ATOMIC_LOAD(&memory_used_);
}

int SemanticQueryBudget::acquire_task()
{
  int ret = failure();
  int64_t active = active_tasks_.load();
  while (OB_SUCC(ret)) {
    if (OB_FAIL(failure())) {
    } else if (active >= task_limit_) {
      ret = OB_EAGAIN;
    } else if (active_tasks_.compare_exchange_weak(active, active + 1)) {
      if (OB_FAIL(failure())) {
        release_task();
      }
      break;
    }
  }
  return ret;
}

void SemanticQueryBudget::release_task()
{
  const int64_t previous = active_tasks_.fetch_sub(1);
  if (previous <= 0) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "semantic task credit released without ownership",
                  K(execution_id_), K(previous));
  }
}

void SemanticQueryBudget::fail(int ret)
{
  if (ret != OB_SUCCESS && ret != OB_EAGAIN) {
    int expected = OB_SUCCESS;
    failure_.compare_exchange_strong(expected, ret);
  }
}

SemanticExecutionState::SemanticExecutionState(ObExecContext &ctx,
    std::shared_ptr<SemanticQueryBudget> budget, int64_t deadline)
    : ctx_(&ctx), budget_(std::move(budget)), deadline_(deadline), registered_(false)
{
}

int SemanticExecutionState::acquire(ObExecContext &ctx, int64_t memory_limit,
                                    int64_t task_limit,
                                    std::shared_ptr<SemanticExecutionState> &state,
                                    bool row_async_enabled)
{
  int ret = OB_SUCCESS;
  uint64_t execution_id = OB_INVALID_ID;
  if (OB_ISNULL(ctx.get_my_session()) || OB_ISNULL(ctx.get_physical_plan_ctx()) ||
      memory_limit <= 0 || task_limit <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid semantic query runtime arguments", K(ret), K(memory_limit), K(task_limit));
  } else {
    execution_id = nullptr == ctx.get_sqc_handler()
        ? ctx.get_my_session()->get_current_execution_id()
        : ctx.get_sqc_handler()->get_sqc_init_arg().sqc_.get_execution_id();
    if (execution_id == OB_INVALID_ID) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semantic query has no execution identity", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    auto &registry = runtime_registry();
    std::lock_guard<std::mutex> guard(registry.mutex_);
    for (auto entry = registry.budgets_.begin(); entry != registry.budgets_.end();) {
      if (entry->second.expired()) {
        entry = registry.budgets_.erase(entry);
      } else {
        ++entry;
      }
    }
    try {
      auto local = registry.states_.find(&ctx);
      if (local != registry.states_.end()) {
        state = local->second.lock();
      }
      if (state) {
        if (state->budget().execution_id() != execution_id ||
            state->budget().memory_limit() != memory_limit ||
            state->budget().task_limit() != task_limit ||
            state->budget().row_async_enabled() != row_async_enabled) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inconsistent semantic execution context", K(ret), K(execution_id),
                   K(memory_limit), K(task_limit));
        }
      } else {
        std::shared_ptr<SemanticQueryBudget> budget;
        auto query = registry.budgets_.find(execution_id);
        if (query != registry.budgets_.end()) {
          budget = query->second.lock();
        }
        if (budget && (budget->memory_limit() != memory_limit ||
                       budget->task_limit() != task_limit ||
                       budget->row_async_enabled() != row_async_enabled)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parallel semantic workers disagree on query limits",
                   K(ret), K(execution_id), K(memory_limit), K(task_limit));
        } else {
          if (!budget) {
            budget = std::make_shared<SemanticQueryBudget>(
                execution_id, memory_limit, task_limit, row_async_enabled);
            registry.budgets_[execution_id] = budget;
          }
          state.reset(new SemanticExecutionState(ctx, std::move(budget),
              ctx.get_physical_plan_ctx()->get_timeout_timestamp()));
          registry.states_[&ctx] = state;
          state->registered_ = true;
        }
      }
    } catch (const std::bad_alloc &) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      auto query = registry.budgets_.find(execution_id);
      if (query != registry.budgets_.end() && query->second.expired()) {
        registry.budgets_.erase(query);
      }
      LOG_WARN("allocate semantic execution state failed", K(ret));
    }
  }
  return ret;
}

SemanticExecutionState::~SemanticExecutionState()
{
  if (!waiters_.empty()) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "semantic execution state still has registered operators",
                  "count", waiters_.size());
  }
  media_.reset();
  auto &registry = runtime_registry();
  std::lock_guard<std::mutex> guard(registry.mutex_);
  if (registered_) {
    registry.states_.erase(ctx_);
  }
  if (budget_) {
    const uint64_t execution_id = budget_->execution_id();
    budget_.reset();
    auto query = registry.budgets_.find(execution_id);
    if (query != registry.budgets_.end() && query->second.expired()) {
      registry.budgets_.erase(query);
    }
  }
}

int64_t SemanticExecutionState::query_count()
{
  auto &registry = runtime_registry();
  std::lock_guard<std::mutex> guard(registry.mutex_);
  for (auto entry = registry.budgets_.begin(); entry != registry.budgets_.end();) {
    if (entry->second.expired()) {
      entry = registry.budgets_.erase(entry);
    } else {
      ++entry;
    }
  }
  return registry.budgets_.size();
}

int SemanticExecutionState::function_cache(SemanticFunctionCache *&cache)
{
  ctx_->set_semantic_query_budget(budget_);
  return budget_->function_cache(deadline_, cache);
}

int SemanticExecutionState::register_operator(void *operation, ReadyCheck ready, ReadyPoll poll)
{
  int ret = OB_SUCCESS;
  if (nullptr == operation || nullptr == ready) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid semantic readiness registration", K(ret));
  } else {
    std::lock_guard<std::mutex> guard(mutex_);
    try {
      waiters_.push_back({operation, ready, poll});
    } catch (const std::bad_alloc &) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate semantic readiness registration failed", K(ret));
    }
  }
  return ret;
}

int SemanticExecutionState::media_runtime(std::shared_ptr<SemanticMediaRuntime> &runtime)
{
  int ret = OB_SUCCESS;
  if (!media_) {
    ObArenaAllocator policy_allocator("SemanticPolicy");
    ObObj global_root;
    ObObj global_remote_root;
    ObString root;
    ObString remote_root;
    int64_t max_bytes = 0;
    int64_t parallel = 0;
    ObSQLSessionInfo *session = ctx_->get_my_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ObBasicSessionInfo::get_global_sys_variable(
        policy_allocator, session->get_dtc_params(), share::SYS_VAR_AI_MEDIA_ROOT, global_root))) {
    } else if (OB_FAIL(global_root.get_varchar(root))) {
    } else if (OB_FAIL(ObBasicSessionInfo::get_global_sys_variable(
        policy_allocator, session->get_dtc_params(), share::SYS_VAR_AI_MEDIA_REMOTE_ROOT,
        global_remote_root))) {
    } else if (OB_FAIL(global_remote_root.get_varchar(remote_root))) {
    } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_AI_MEDIA_MAX_BYTES, max_bytes))) {
    } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_AI_MEDIA_PARALLEL, parallel))) {
    } else if (OB_FAIL(SemanticMediaRuntime::acquire(budget_, root, remote_root, max_bytes, parallel,
                                                   deadline_, media_))) {
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to initialize query media runtime", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    runtime = media_;
  }
  return ret;
}

void SemanticExecutionState::unregister_operator(const void *operation)
{
  std::lock_guard<std::mutex> guard(mutex_);
  waiters_.erase(std::remove_if(waiters_.begin(), waiters_.end(),
      [operation](const Waiter &waiter) { return waiter.operation_ == operation; }), waiters_.end());
}

int SemanticExecutionState::poll_ready_tasks(bool &progress)
{
  int ret = budget_->poll_function_cache(progress);
  std::lock_guard<std::mutex> guard(mutex_);
  for (const Waiter &waiter : waiters_) {
    if (OB_SUCC(ret) && nullptr != waiter.poll_ &&
        OB_FAIL(waiter.poll_(waiter.operation_, progress))) {
      LOG_WARN("poll semantic execution completion failed", K(ret));
    }
  }
  return ret;
}

bool SemanticExecutionState::can_resume(const void *data)
{
  auto &state = *const_cast<SemanticExecutionState *>(
      static_cast<const SemanticExecutionState *>(data));
  const ObSQLSessionInfo *session = state.ctx_->get_my_session();
  bool ready = state.budget().failure() != OB_SUCCESS ||
               ObTimeUtility::current_time() >= state.deadline_ ||
               (nullptr != session && (session->is_query_killed() || session->is_zombie()));
  ready = ready || state.budget().function_cache_ready();
  std::lock_guard<std::mutex> guard(state.mutex_);
  for (const Waiter &waiter : state.waiters_) {
    if (!ready && waiter.ready_(waiter.operation_)) {
      ready = true;
    }
  }
  return ready;
}

thread_local int64_t SemanticSuspendScope::blocked_depth_ = 0;

SemanticSuspendScope::SemanticSuspendScope(ObPhyOperatorType type, bool opening, bool resumable)
    : blocked_(opening || (!resumable && type != PHY_SEMANTIC_MAP && type != PHY_SEMANTIC_FILTER &&
                            type != PHY_SEMANTIC_JOIN &&
                            type != PHY_SEMANTIC_AGGREGATE &&
                            type != PHY_SEMANTIC_RANK &&
                            type != PHY_TABLE_SCAN))
{
  if (blocked_) {
    ++blocked_depth_;
  }
}

SemanticSuspendScope::~SemanticSuspendScope()
{
  if (blocked_) {
    --blocked_depth_;
  }
}

bool SemanticSuspendScope::allowed()
{
  return blocked_depth_ == 0;
}

}
}
