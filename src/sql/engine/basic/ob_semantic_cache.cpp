#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_semantic_cache.h"
#include "sql/engine/basic/ob_semantic_runtime.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_semantic.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/time/ob_time_utility.h"
#include <algorithm>
#include <cstring>
#include <map>
#include <mutex>
#include <new>
#include <unordered_map>

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{
int canonical_config(const ObIJsonBase &node, ObJsonBuffer &buffer)
{
  const uint8_t type = static_cast<uint8_t>(node.json_type());
  int ret = buffer.append(reinterpret_cast<const char *>(&type), sizeof(type));
  if (OB_FAIL(ret))
  {
    return ret;
  }
  if (node.json_type() == ObJsonNodeType::J_OBJECT)
  {
    struct Field
    {
      ObString key_;
      ObIJsonBase *value_;
      TO_STRING_KV(K_(key));
    };
    ObSEArray<Field, 16> fields;
    for (uint64_t index = 0; OB_SUCC(ret) && index < node.element_count();
         ++index)
    {
      Field field;
      if (OB_FAIL(node.get_object_value(index, field.key_, field.value_)))
      {
      }
      else
      {
        ret = fields.push_back(field);
      }
    }
    if (OB_SUCC(ret))
    {
      std::sort(fields.begin(), fields.end(),
                [](const Field &left, const Field &right)
                { return left.key_.compare(right.key_) < 0; });
      ret = buffer.append("{", 1);
    }
    for (int64_t index = 0; OB_SUCC(ret) && index < fields.count(); ++index)
    {
      ObJsonString key(fields.at(index).key_);
      if (index > 0 && fields.at(index - 1).key_ == fields.at(index).key_)
      {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(
            OB_INVALID_ARGUMENT,
            "semantic cache options must not contain duplicate JSON keys");
      }
      else if (index > 0 && OB_FAIL(buffer.append(",", 1)))
      {
      }
      else if (OB_FAIL(key.print(buffer, true)))
      {
      }
      else if (OB_FAIL(buffer.append(":", 1)))
      {
      }
      else
      {
        if (nullptr == fields.at(index).value_)
        {
          ret = OB_INVALID_DATA;
        }
        else
        {
          ret = SMART_CALL(canonical_config(*fields.at(index).value_, buffer));
        }
      }
    }
    if (OB_SUCC(ret))
    {
      ret = buffer.append("}", 1);
    }
  }
  else if (node.json_type() == ObJsonNodeType::J_ARRAY)
  {
    ret = buffer.append("[", 1);
    for (uint64_t index = 0; OB_SUCC(ret) && index < node.element_count();
         ++index)
    {
      ObIJsonBase *element = nullptr;
      if (index > 0 && OB_FAIL(buffer.append(",", 1)))
      {
      }
      else if (OB_FAIL(node.get_array_element(index, element)))
      {
      }
      else
      {
        if (nullptr == element)
        {
          ret = OB_INVALID_DATA;
        }
        else
        {
          ret = SMART_CALL(canonical_config(*element, buffer));
        }
      }
    }
    if (OB_SUCC(ret))
    {
      ret = buffer.append("]", 1);
    }
  }
  else
  {
    ret = node.print(buffer, true);
  }
  return ret;
}
} // namespace

struct SemanticFunctionCache::Impl
{
  struct Model
  {
    Model() : arena_("SemanticMemo"), allocator_(arena_), info_(nullptr) {}
    ObArenaAllocator arena_;
    MultimodeAlloctor allocator_;
    ObAIFuncExprInfo *info_;
    ObString requested_name_;
    share::ObAiModelEndpointInfo endpoint_;
  };

  struct Binding
  {
    Binding()
        : arena_("SemanticMemo"), allocator_(arena_), type_(T_INVALID), id_(0),
          config_(nullptr)
    {
    }
    ObArenaAllocator arena_;
    MultimodeAlloctor allocator_;
    std::shared_ptr<Model> model_;
    ObItemType type_;
    uint64_t id_;
    ObString canonical_;
    ObJsonObject *config_;
  };

  struct BindingKey
  {
    const Model *model_;
    ObItemType type_;
    ObString config_;
  };
  struct BindingLess
  {
    bool operator()(const BindingKey &left, const BindingKey &right) const
    {
      if (left.model_ != right.model_)
      {
        return std::less<const Model *>()(left.model_, right.model_);
      }
      else if (left.type_ != right.type_)
      {
        return left.type_ < right.type_;
      }
      else
      {
        return left.config_.compare(right.config_) < 0;
      }
    }
  };

  struct Key
  {
    uint64_t binding_;
    ObString input_;
  };
  struct KeyHash
  {
    size_t operator()(const Key &key) const
    {
      return murmurhash(key.input_.ptr(), key.input_.length(), key.binding_);
    }
  };
  struct KeyEqual
  {
    bool operator()(const Key &left, const Key &right) const
    {
      return left.binding_ == right.binding_ && left.input_ == right.input_;
    }
  };

  struct Flight
  {
    enum Phase
    {
      QUEUED,
      RUNNING,
      DONE
    };
    Flight(SemanticQueryBudget &budget, int64_t deadline)
        : budget_(budget), deadline_(deadline), arena_("SemanticMemo"),
          allocator_(arena_), batch_(allocator_), busy_(false), phase_(QUEUED),
          bytes_(0), credit_(false), max_parallel_(1)
    {
    }
    ~Flight()
    {
      batch_.cancel();
      if (credit_)
      {
        budget_.release_task();
      }
      if (bytes_ > 0)
      {
        budget_.release(bytes_);
      }
    }
    static int check_status(void *data)
    {
      const auto &flight = *static_cast<const Flight *>(data);
      int ret = flight.budget_.failure();
      if (OB_SUCC(ret) && ObTimeUtility::current_time() >= flight.deadline_)
      {
        ret = OB_TIMEOUT;
      }
      return ret;
    }
    SemanticQueryBudget &budget_;
    int64_t deadline_;
    ObArenaAllocator arena_;
    MultimodeAlloctor allocator_;
    AIFuncBatch batch_;
    std::atomic<bool> busy_;
    std::atomic<Phase> phase_;
    int64_t bytes_;
    bool credit_;
    int64_t max_parallel_;
    std::shared_ptr<Binding> binding_;
    std::vector<Handle> entries_;
    ObArray<ObString> prompts_;
  };

  Impl(SemanticQueryBudget &budget, int64_t deadline)
      : budget_(budget), deadline_(deadline),
        cache_limit_(budget.memory_limit() / 2), cache_bytes_(0),
        next_binding_(1), distinct_keys_(0), ready_hits_(0), coalesced_(0),
        submitted_(0)
  {
  }
  ~Impl()
  {
    LOG_INFO("semantic function cache execution summary", "execution_id",
             budget_.execution_id(), K_(distinct_keys), K_(ready_hits),
             K_(coalesced), K_(submitted), K_(cache_bytes));
    flights_.clear();
    entries_.clear();
    bindings_.clear();
    models_.clear();
    budget_.release(cache_bytes_);
  }

  int reserve_locked(int64_t bytes)
  {
    int ret = OB_SUCCESS;
    if (bytes < 0 || bytes > cache_limit_ - cache_bytes_)
    {
      ret = OB_SIZE_OVERFLOW;
    }
    else if (OB_FAIL(budget_.reserve(bytes)))
    {
    }
    else
    {
      cache_bytes_ += bytes;
    }
    if (ret == OB_SIZE_OVERFLOW)
    {
      LOG_WARN(
          "non-evicting semantic function cache exceeds its query memory quota",
          K(ret), K(bytes), K_(cache_bytes), K_(cache_limit));
      FORWARD_USER_ERROR(
          ret, "semantic function cache memory quota exceeded; increase "
               "ai_pipeline_memory_limit or disable enable_semantic_cache");
    }
    return ret;
  }

  int binding_locked(ObItemType type, const ObString &model_name,
                     ObJsonObject *config, const ObString &canonical,
                     std::shared_ptr<Binding> &binding)
  {
    int ret = OB_SUCCESS;
    std::shared_ptr<Model> model;
    auto found_model = models_.find(model_name);
    if (found_model != models_.end())
    {
      model = found_model->second;
    }
    else
    {
      model = std::make_shared<Model>();
      auto *resolver = share::server_service<query::ObIAiEndpointResolver>();
      if (OB_FAIL(ob_write_string(model->allocator_, model_name,
                                  model->requested_name_)))
      {
      }
      else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(
                   model->allocator_, model_name, model->info_)))
      {
      }
      else if (nullptr == resolver)
      {
        ret = OB_ERR_UNEXPECTED;
      }
      else if (OB_FAIL(resolver->resolve_by_model_name(
                   model_name, model->allocator_, model->endpoint_)))
      {
      }
      else if (model->info_->type_ != share::EndpointType::COMPLETION)
      {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                       "semantic functions require a completion model");
      }
      else if (OB_FAIL(
                   reserve_locked(sizeof(Model) + model->arena_.total() + 64)))
      {
      }
      else
      {
        models_.emplace(model->requested_name_, model);
      }
    }
    if (OB_SUCC(ret))
    {
      const BindingKey key{model.get(), type, canonical};
      auto found = bindings_.find(key);
      if (found != bindings_.end())
      {
        binding = found->second;
      }
      else
      {
        binding = std::make_shared<Binding>();
        binding->model_ = model;
        binding->type_ = type;
        binding->id_ = next_binding_++;
        if (OB_FAIL(ob_write_string(binding->allocator_, canonical,
                                    binding->canonical_)))
        {
        }
        else if (nullptr != config &&
                 nullptr == (binding->config_ = static_cast<ObJsonObject *>(
                                 config->clone(&binding->allocator_, true))))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (OB_FAIL(reserve_locked(sizeof(Binding) +
                                        binding->arena_.total() + 64)))
        {
        }
        else
        {
          bindings_.emplace(BindingKey{model.get(), type, binding->canonical_},
                            binding);
        }
      }
    }
    return ret;
  }

  int drive(const std::shared_ptr<Flight> &flight, bool &progress)
  {
    int ret = OB_SUCCESS;
    bool expected = false;
    if (!flight->busy_.compare_exchange_strong(expected, true))
    {
      return ret;
    }
    struct BusyGuard
    {
      explicit BusyGuard(std::atomic<bool> &busy) : busy_(busy) {}
      ~BusyGuard() { busy_.store(false); }
      std::atomic<bool> &busy_;
    } busy_guard(flight->busy_);
    try
    {
      if (flight->phase_.load() == Flight::DONE)
      {
      }
      else
      {
        ret = Flight::check_status(flight.get());
        if (OB_SUCC(ret) && flight->phase_.load() == Flight::QUEUED)
        {
          ObJsonObject *config = nullptr;
          if (nullptr != flight->binding_->config_ &&
              nullptr == (config = static_cast<ObJsonObject *>(
                              flight->binding_->config_->clone(
                                  &flight->allocator_, true))))
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            flight->batch_.set_nonblocking_admission();
            flight->batch_.set_max_parallel(flight->max_parallel_);
            flight->batch_.set_shared_buffer_usage(*budget_.memory_counter(),
                                                   budget_.memory_limit());
            flight->batch_.set_status_checker(&Flight::check_status,
                                              flight.get());
            ObAIFuncModel model(flight->allocator_,
                                *flight->binding_->model_->info_,
                                flight->binding_->model_->endpoint_);
            ret = model.start_completion_batch(flight->prompts_, config,
                                               flight->batch_);
          }
          if (OB_SUCC(ret))
          {
            flight->phase_.store(Flight::RUNNING);
            std::lock_guard<std::mutex> guard(mutex_);
            submitted_ += flight->entries_.size();
            progress = true;
          }
        }
        bool finished = false;
        ObArray<ObString> results;
        if (OB_SUCC(ret) && OB_FAIL(flight->batch_.poll(finished)))
        {
        }
        else if (OB_SUCC(ret) && finished)
        {
          if (OB_FAIL(flight->batch_.get_results(results)))
          {
          }
          else if (results.count() !=
                   static_cast<int64_t>(flight->entries_.size()))
          {
            ret = OB_INVALID_DATA;
            LOG_WARN(
                "cached semantic response count differs from unique inputs",
                K(ret));
          }
          for (int64_t index = 0; OB_SUCC(ret) && index < results.count();
               ++index)
          {
            const ObItemType type = flight->binding_->type_;
            bool value = false;
            if (type != T_FUN_SYS_AI_MAP &&
                OB_FAIL(SemanticExprUtils::parse_boolean_result(
                    type, flight->allocator_, results.at(index), value)))
            {
            }
            else
            {
              const int64_t bytes = results.at(index).length();
              std::lock_guard<std::mutex> guard(mutex_);
              if (OB_FAIL(reserve_locked(bytes)))
              {
              }
              else
              {
                Handle &entry = flight->entries_.at(index);
                entry->result_buffer_.reset(
                    new char[std::max<int64_t>(bytes, 1)]);
                if (bytes > 0)
                {
                  std::memcpy(entry->result_buffer_.get(),
                              results.at(index).ptr(), bytes);
                }
                entry->result_.assign_ptr(entry->result_buffer_.get(), bytes);
              }
            }
          }
        }
        if (ret != OB_SUCCESS || finished)
        {
          std::lock_guard<std::mutex> guard(mutex_);
          for (const Handle &entry : flight->entries_)
          {
            entry->error_ = ret;
            entry->state_.store(ret == OB_SUCCESS ? Entry::COMPLETE
                                                  : Entry::FAILED,
                                std::memory_order_release);
          }
          flight->phase_.store(Flight::DONE);
          progress = true;
          if (OB_FAIL(ret))
          {
            LOG_WARN("cached semantic request failed; subscribers receive the "
                     "original error",
                     K(ret), "execution_id", budget_.execution_id(),
                     "binding_id", flight->binding_->id_, "function_type",
                     flight->binding_->type_);
          }
        }
      }
    }
    catch (const std::bad_alloc &)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      std::lock_guard<std::mutex> guard(mutex_);
      for (const Handle &entry : flight->entries_)
      {
        entry->error_ = ret;
        entry->state_.store(Entry::FAILED, std::memory_order_release);
      }
      flight->phase_.store(Flight::DONE);
      progress = true;
      LOG_WARN("allocate memoized semantic result failed", K(ret));
    }
    return ret;
  }

  SemanticQueryBudget &budget_;
  int64_t deadline_;
  int64_t cache_limit_;
  int64_t cache_bytes_;
  uint64_t next_binding_;
  mutable std::mutex mutex_;
  std::map<ObString, std::shared_ptr<Model>> models_;
  std::map<BindingKey, std::shared_ptr<Binding>, BindingLess> bindings_;
  std::unordered_map<Key, Handle, KeyHash, KeyEqual> entries_;
  std::vector<std::shared_ptr<Flight>> flights_;
  int64_t distinct_keys_;
  int64_t ready_hits_;
  int64_t coalesced_;
  int64_t submitted_;
};

SemanticFunctionCache::SemanticFunctionCache(SemanticQueryBudget &budget,
                                             int64_t deadline)
    : impl_(new Impl(budget, deadline))
{
}

SemanticFunctionCache::~SemanticFunctionCache() = default;

int SemanticFunctionCache::submit(ObItemType type, const ObString &model,
                                  ObJsonObject *config,
                                  const ObIArray<ObString> &keys,
                                  const ObIArray<ObString> &prompts,
                                  int64_t max_parallel, uint64_t &binding_id,
                                  std::vector<Handle> &handles, bool &progress)
{
  int ret = impl_->budget_.failure();
  if (OB_FAIL(ret))
  {
  }
  else if (!SemanticExprUtils::is_semantic(type) ||
           keys.count() != prompts.count() || keys.empty() || max_parallel <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid semantic memo submission", K(ret), K(type),
             K(max_parallel));
  }
  else
  {
    try
    {
      handles.assign(keys.count(), Handle());
      ObArenaAllocator arena("SemanticMemo");
      ObJsonBuffer canonical(&arena);
      if (nullptr == config)
      {
        ret = canonical.append("null", 4);
      }
      else
      {
        ret = canonical_config(*config, canonical);
      }
      if (OB_SUCC(ret))
      {
        std::lock_guard<std::mutex> guard(impl_->mutex_);
        std::shared_ptr<Impl::Binding> binding;
        std::shared_ptr<Impl::Flight> flight;
        if (OB_FAIL(impl_->binding_locked(type, model, config,
                                          canonical.string(), binding)))
        {
        }
        else
        {
          binding_id = binding->id_;
        }
        for (int64_t index = 0; OB_SUCC(ret) && index < keys.count(); ++index)
        {
          const Impl::Key key{binding->id_, keys.at(index)};
          auto found = impl_->entries_.find(key);
          if (found != impl_->entries_.end())
          {
            handles.at(index) = found->second;
            if (found->second->state() == Entry::PENDING)
            {
              ++impl_->coalesced_;
            }
            else
            {
              ++impl_->ready_hits_;
            }
          }
          else
          {
            const int64_t retained = sizeof(Entry) + sizeof(Impl::Key) +
                                     sizeof(Handle) + 64 +
                                     keys.at(index).length();
            const int64_t config_bytes =
                nullptr == config ? 0 : config->get_serialize_size();
            const int64_t request_bytes =
                6 * prompts.at(index).length() + 8 * config_bytes + 2048;
            const int64_t overhead =
                nullptr == flight ? sizeof(Impl::Flight) : 0;
            if (request_bytes + overhead > impl_->budget_.memory_limit() ||
                request_bytes > ObAIFuncClient::MAX_BATCH_BYTES)
            {
              ret = OB_SIZE_OVERFLOW;
            }
            else if (request_bytes + overhead + retained >
                     impl_->budget_.memory_limit() -
                         impl_->budget_.memory_used())
            {
              if (impl_->budget_.active_tasks() == 0)
              {
                ret = OB_SIZE_OVERFLOW;
                LOG_WARN("semantic memo cannot construct a request with "
                         "retained input/cache memory",
                         K(ret), K(request_bytes), K(retained));
              }
            }
            else
            {
              if (!flight)
              {
                const int admission = impl_->budget_.acquire_task();
                if (admission == OB_EAGAIN)
                {
                  continue;
                }
                else if (admission != OB_SUCCESS)
                {
                  ret = admission;
                }
                else
                {
                  try
                  {
                    flight = std::make_shared<Impl::Flight>(impl_->budget_,
                                                            impl_->deadline_);
                    flight->credit_ = true;
                    flight->binding_ = binding;
                    flight->max_parallel_ = max_parallel;
                    impl_->flights_.push_back(flight);
                  }
                  catch (const std::bad_alloc &)
                  {
                    if (!flight)
                    {
                      impl_->budget_.release_task();
                    }
                    throw;
                  }
                }
              }
              if (OB_SUCC(ret) && OB_FAIL(impl_->reserve_locked(retained)))
              {
              }
              else if (OB_SUCC(ret) && OB_FAIL(impl_->budget_.reserve(
                                           request_bytes + overhead)))
              {
              }
              else if (OB_SUCC(ret))
              {
                flight->bytes_ += request_bytes + overhead;
                Handle entry = std::make_shared<Entry>();
                entry->binding_ = binding->id_;
                entry->key_buffer_.reset(
                    new char[std::max<int64_t>(keys.at(index).length(), 1)]);
                if (!keys.at(index).empty())
                {
                  std::memcpy(entry->key_buffer_.get(), keys.at(index).ptr(),
                              keys.at(index).length());
                }
                entry->key_.assign_ptr(entry->key_buffer_.get(),
                                       keys.at(index).length());
                ObString prompt;
                if (OB_FAIL(ob_write_string(flight->allocator_,
                                            prompts.at(index), prompt)))
                {
                }
                else if (OB_FAIL(flight->prompts_.push_back(prompt)))
                {
                }
                else
                {
                  flight->entries_.push_back(entry);
                  impl_->entries_.emplace(Impl::Key{binding->id_, entry->key_},
                                          entry);
                  handles.at(index) = entry;
                  ++impl_->distinct_keys_;
                  progress = true;
                }
              }
            }
          }
        }
        if (flight && flight->entries_.empty())
        {
          flight->phase_.store(Impl::Flight::DONE);
        }
      }
    }
    catch (const std::bad_alloc &)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  if (OB_FAIL(ret))
  {
    LOG_WARN("failed to submit memoized semantic inputs", K(ret), K(type));
  }
  return ret;
}

bool SemanticFunctionCache::can_submit(uint64_t binding_id, const ObString &key,
                                       const ObString &prompt,
                                       int64_t config_bytes) const
{
  std::lock_guard<std::mutex> guard(impl_->mutex_);
  const int64_t retained =
      sizeof(Entry) + sizeof(Impl::Key) + sizeof(Handle) + 64 + key.length();
  const int64_t request =
      sizeof(Impl::Flight) + 6 * prompt.length() + 8 * config_bytes + 2048;
  return impl_->entries_.find(Impl::Key{binding_id, key}) !=
             impl_->entries_.end() ||
         retained > impl_->cache_limit_ - impl_->cache_bytes_ ||
         request > impl_->budget_.memory_limit() ||
         (impl_->budget_.task_available() &&
          (request + retained <=
               impl_->budget_.memory_limit() - impl_->budget_.memory_used() ||
           impl_->budget_.active_tasks() == 0));
}

int SemanticFunctionCache::poll(bool &progress)
{
  int ret = OB_SUCCESS;
  try
  {
    std::vector<std::shared_ptr<Impl::Flight>> flights;
    {
      std::lock_guard<std::mutex> guard(impl_->mutex_);
      flights = impl_->flights_;
    }
    for (const auto &flight : flights)
    {
      // A failed prefetched call is delivered when a subscriber demands its
      // result.
      impl_->drive(flight, progress);
    }
    {
      std::lock_guard<std::mutex> guard(impl_->mutex_);
      impl_->flights_.erase(
          std::remove_if(impl_->flights_.begin(), impl_->flights_.end(),
                         [](const std::shared_ptr<Impl::Flight> &flight)
                         {
                           return flight->phase_.load() == Impl::Flight::DONE &&
                                  !flight->busy_.load();
                         }),
          impl_->flights_.end());
    }
  }
  catch (const std::bad_alloc &)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate semantic cache polling snapshot failed", K(ret));
  }
  return ret;
}

bool SemanticFunctionCache::is_ready() const
{
  bool ready = false;
  std::lock_guard<std::mutex> guard(impl_->mutex_);
  for (const auto &flight : impl_->flights_)
  {
    bool expected = false;
    if (!ready && flight->busy_.compare_exchange_strong(expected, true))
    {
      ready = flight->phase_.load() != Impl::Flight::RUNNING ||
              (flight->phase_.load() == Impl::Flight::RUNNING &&
               flight->batch_.is_ready());
      flight->busy_.store(false);
    }
  }
  return ready;
}

bool SemanticFunctionCache::has_pending() const
{
  std::lock_guard<std::mutex> guard(impl_->mutex_);
  return !impl_->flights_.empty();
}

int64_t SemanticFunctionCache::deadline() const
{
  return impl_->deadline_;
}
} // namespace sql
} // namespace oceanbase
