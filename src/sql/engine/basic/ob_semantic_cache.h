#ifndef OCEANBASE_SQL_ENGINE_SEMANTIC_CACHE_H_
#define OCEANBASE_SQL_ENGINE_SEMANTIC_CACHE_H_

#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "query/engine/expr/ob_expr_operator.h"
#include <atomic>
#include <memory>
#include <vector>

namespace oceanbase
{
namespace common
{
class ObJsonObject;
}
namespace sql
{
class SemanticQueryBudget;

class SemanticFunctionCache
{
  struct Impl;

public:
  class Entry
  {
    friend struct Impl;
    friend class SemanticFunctionCache;

  public:
    enum State
    {
      PENDING,
      COMPLETE,
      FAILED
    };
    Entry() : state_(PENDING), error_(common::OB_SUCCESS), binding_(0) {}
    State state() const { return state_.load(std::memory_order_acquire); }
    int error() const { return error_; }
    const common::ObString &result() const { return result_; }

  private:
    std::atomic<State> state_;
    int error_;
    uint64_t binding_;
    std::unique_ptr<char[]> key_buffer_;
    common::ObString key_;
    std::unique_ptr<char[]> result_buffer_;
    common::ObString result_;
  };
  using Handle = std::shared_ptr<Entry>;

  SemanticFunctionCache(SemanticQueryBudget &budget, int64_t deadline);
  ~SemanticFunctionCache();
  int submit(ObItemType type, const common::ObString &model,
             common::ObJsonObject *config,
             const common::ObIArray<common::ObString> &keys,
             const common::ObIArray<common::ObString> &prompts,
             int64_t max_parallel, uint64_t &binding_id,
             std::vector<Handle> &handles, bool &progress);
  bool can_submit(uint64_t binding_id, const common::ObString &key,
                  const common::ObString &prompt, int64_t config_bytes) const;
  int poll(bool &progress);
  bool is_ready() const;
  bool has_pending() const;
  int64_t deadline() const;

private:
  std::unique_ptr<Impl> impl_;
  DISALLOW_COPY_AND_ASSIGN(SemanticFunctionCache);
};
} // namespace sql
} // namespace oceanbase
#endif
