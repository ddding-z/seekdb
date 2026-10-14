#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_semantic_cache.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/time/ob_time_utility.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"
#include "sql/engine/basic/ob_semantic_runtime.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_semantic.h"
#include <algorithm>
#include <cmath>
#include <cstddef>
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

int append_proxy_identity(ObJsonBuffer &buffer,
                          const SemanticProxyConfig &proxy)
{
  int ret = buffer.append("\0embedding_proxy", 16);
  const int64_t model_size = proxy.model_.length();
  const int64_t query_size = proxy.query_.length();
  if (OB_FAIL(ret))
  {
  }
  else if (OB_FAIL(buffer.append(reinterpret_cast<const char *>(&model_size),
                                 sizeof(model_size))))
  {
  }
  else if (OB_FAIL(buffer.append(proxy.model_.ptr(), proxy.model_.length())))
  {
  }
  else if (OB_FAIL(buffer.append(reinterpret_cast<const char *>(&query_size),
                                 sizeof(query_size))))
  {
  }
  else if (!proxy.query_.empty() &&
           OB_FAIL(buffer.append(proxy.query_.ptr(), proxy.query_.length())))
  {
  }
  else
  {
    ret = buffer.append(reinterpret_cast<const char *>(&proxy.threshold_),
                        sizeof(proxy.threshold_));
  }
  return ret;
}

class ProxyVectorAllocator : public ObIAllocator
{
  struct alignas(std::max_align_t) Block
  {
    Block *previous_;
    Block *next_;
    int64_t bytes_;
  };

public:
  explicit ProxyVectorAllocator(SemanticQueryBudget &budget)
      : budget_(budget), head_(nullptr), bytes_(0), error_(OB_SUCCESS)
  {
  }
  ~ProxyVectorAllocator()
  {
    while (nullptr != head_)
    {
      free(head_ + 1);
    }
  }
  void *alloc(int64_t size) override
  {
    return alloc(size, ObMemAttr("SemanticProxy"));
  }
  void *alloc(int64_t size, const ObMemAttr &attr) override
  {
    void *result = nullptr;
    if (error_ != OB_SUCCESS)
    {
    }
    else if (size < 0 || size > INT64_MAX - static_cast<int64_t>(sizeof(Block)))
    {
      error_ = OB_SIZE_OVERFLOW;
    }
    else
    {
      const int64_t bytes = size + sizeof(Block);
      error_ = budget_.reserve(bytes);
      if (error_ == OB_SUCCESS)
      {
        Block *block = static_cast<Block *>(ob_malloc(bytes, attr));
        if (nullptr == block)
        {
          budget_.release(bytes);
          error_ = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          block->previous_ = nullptr;
          block->next_ = head_;
          block->bytes_ = bytes;
          if (nullptr != head_)
          {
            head_->previous_ = block;
          }
          head_ = block;
          bytes_ += bytes;
          result = block + 1;
        }
      }
    }
    return result;
  }
  void free(void *pointer) override
  {
    if (nullptr != pointer)
    {
      Block *block = static_cast<Block *>(pointer) - 1;
      if (nullptr != block->previous_)
      {
        block->previous_->next_ = block->next_;
      }
      else
      {
        head_ = block->next_;
      }
      if (nullptr != block->next_)
      {
        block->next_->previous_ = block->previous_;
      }
      bytes_ -= block->bytes_;
      budget_.release(block->bytes_);
      ob_free(block);
    }
  }
  int64_t total() const override { return bytes_; }
  int64_t used() const override { return bytes_; }
  int error() const { return error_; }

private:
  SemanticQueryBudget &budget_;
  Block *head_;
  int64_t bytes_;
  int error_;
};
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
    enum QueryState
    {
      EMPTY,
      PENDING,
      READY,
      FAILED
    };
    Binding()
        : arena_("SemanticMemo"), allocator_(arena_), type_(T_INVALID), id_(0),
          config_(nullptr), query_state_(EMPTY), query_error_(OB_SUCCESS),
          query_dimension_(0)
    {
    }
    ObArenaAllocator arena_;
    MultimodeAlloctor allocator_;
    std::shared_ptr<Model> model_;
    ObItemType type_;
    uint64_t id_;
    ObString canonical_;
    ObJsonObject *config_;
    SemanticProxyConfig proxy_;
    std::shared_ptr<Model> embedding_model_;
    std::atomic<QueryState> query_state_;
    int query_error_;
    std::unique_ptr<float[]> query_vector_;
    int64_t query_dimension_;
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

  struct VectorEntry
  {
    VectorEntry() : state_(Entry::PENDING), error_(OB_SUCCESS), dimension_(0) {}
    Entry::State state() const
    {
      return state_.load(std::memory_order_acquire);
    }
    std::atomic<Entry::State> state_;
    int error_;
    std::unique_ptr<char[]> text_buffer_;
    ObString text_;
    std::unique_ptr<float[]> values_;
    int64_t dimension_;
  };
  using VectorHandle = std::shared_ptr<VectorEntry>;

  struct VectorKey
  {
    const Model *model_;
    ObString text_;
  };
  struct VectorLess
  {
    bool operator()(const VectorKey &left, const VectorKey &right) const
    {
      return left.model_ != right.model_
                 ? std::less<const Model *>()(left.model_, right.model_)
                 : left.text_.compare(right.text_) < 0;
    }
  };

  struct Flight
  {
    enum Phase
    {
      QUEUED,
      RUNNING,
      WAIT_QUERY,
      WAIT_VECTORS,
      DONE
    };
    enum RequestKind
    {
      COMPLETION,
      EMBEDDING,
      JOIN_EMBEDDING
    };
    Flight(SemanticQueryBudget &budget, int64_t deadline)
        : budget_(budget), deadline_(deadline), arena_("SemanticMemo"),
          allocator_(arena_), batch_(allocator_), busy_(false), phase_(QUEUED),
          bytes_(0), credit_(false), max_parallel_(1), kind_(COMPLETION),
          includes_query_(false), completed_count_(0)
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
    RequestKind kind_;
    bool includes_query_;
    int64_t completed_count_;
    ObArray<ObString> documents_;
    ObArray<ObString> completion_prompts_;
    ObArray<int64_t> completion_positions_;
    std::vector<VectorHandle> embedding_entries_;
    std::vector<VectorHandle> left_vectors_;
    std::vector<VectorHandle> right_vectors_;
  };

  Impl(SemanticQueryBudget &budget, int64_t deadline)
      : budget_(budget), deadline_(deadline),
        cache_limit_(budget.memory_limit() / 2), cache_bytes_(0),
        next_binding_(1), distinct_keys_(0), ready_hits_(0), coalesced_(0),
        submitted_(0), proxy_query_embeddings_(0),
        proxy_document_embeddings_(0), proxy_rejected_(0), proxy_forwarded_(0),
        proxy_join_embeddings_(0), proxy_vector_ready_hits_(0), proxy_vector_coalesced_(0)
  {
  }
  ~Impl()
  {
    LOG_INFO("semantic function cache execution summary", "execution_id",
             budget_.execution_id(), K_(distinct_keys), K_(ready_hits),
             K_(coalesced), K_(submitted), K_(cache_bytes),
             K_(proxy_query_embeddings), K_(proxy_document_embeddings),
             K_(proxy_rejected), K_(proxy_forwarded), K_(proxy_join_embeddings),
             K_(proxy_vector_ready_hits), K_(proxy_vector_coalesced));
    flights_.clear();
    entries_.clear();
    join_vectors_.clear();
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

  int model_locked(const ObString &model_name, bool embedding,
                   std::shared_ptr<Model> &model)
  {
    int ret = OB_SUCCESS;
    const auto expected_type = embedding ? share::EndpointType::DENSE_EMBEDDING
                                         : share::EndpointType::COMPLETION;
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
      else if (model->info_->type_ != expected_type)
      {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(
            OB_INVALID_ARGUMENT,
            embedding ? "semantic_proxy_model must be a dense embedding model"
                      : "semantic functions require a completion model");
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
    if (OB_SUCC(ret) && model->info_->type_ != expected_type)
    {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(
          OB_INVALID_ARGUMENT,
          embedding ? "semantic_proxy_model must be a dense embedding model"
                    : "semantic functions require a completion model");
    }
    return ret;
  }

  int binding_locked(ObItemType type, const ObString &model_name,
                     ObJsonObject *config, const ObString &canonical,
                     const SemanticProxyConfig *proxy,
                     std::shared_ptr<Binding> &binding)
  {
    int ret = OB_SUCCESS;
    std::shared_ptr<Model> model;
    if (OB_FAIL(model_locked(model_name, false, model)))
    {
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
        if (OB_SUCC(ret) && nullptr != proxy && proxy->enabled_)
        {
          binding->proxy_ = *proxy;
          if (OB_FAIL(
                  model_locked(proxy->model_, true, binding->embedding_model_)))
          {
          }
          else if (OB_FAIL(ob_write_string(binding->allocator_, proxy->query_,
                                           binding->proxy_.query_)))
          {
          }
          else
          {
            binding->proxy_.model_ = binding->embedding_model_->requested_name_;
          }
        }
        if (OB_SUCC(ret) &&
            OB_FAIL(
                reserve_locked(sizeof(Binding) + binding->arena_.total() + 64)))
        {
        }
        else if (OB_SUCC(ret))
        {
          bindings_.emplace(BindingKey{model.get(), type, binding->canonical_},
                            binding);
        }
      }
    }
    return ret;
  }

  int store_result_locked(Entry &entry, const ObString &result)
  {
    int ret = OB_SUCCESS;
    const int64_t bytes = result.length();
    if (OB_FAIL(reserve_locked(bytes)))
    {
    }
    else
    {
      entry.result_buffer_.reset(new char[std::max<int64_t>(bytes, 1)]);
      if (bytes > 0)
      {
        std::memcpy(entry.result_buffer_.get(), result.ptr(), bytes);
      }
      entry.result_.assign_ptr(entry.result_buffer_.get(), bytes);
    }
    return ret;
  }

  int vector_locked(Flight &flight, const ObString &text, VectorHandle &vector)
  {
    int ret = OB_SUCCESS;
    const VectorKey key{flight.binding_->embedding_model_.get(), text};
    const auto found = join_vectors_.find(key);
    if (text.empty())
    {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                     "AI_JOIN embedding proxy inputs must not be empty");
      LOG_WARN("empty AI_JOIN proxy embedding input", K(ret));
    }
    else if (found != join_vectors_.end())
    {
      vector = found->second;
      if (vector->state() == Entry::PENDING)
      {
        ++proxy_vector_coalesced_;
      }
      else
      {
        ++proxy_vector_ready_hits_;
      }
    }
    else if (OB_FAIL(reserve_locked(sizeof(VectorEntry) + sizeof(VectorKey) +
                                    sizeof(VectorHandle) + 64 + text.length())))
    {
    }
    else
    {
      vector = std::make_shared<VectorEntry>();
      vector->text_buffer_.reset(new char[text.length()]);
      std::memcpy(vector->text_buffer_.get(), text.ptr(), text.length());
      vector->text_.assign_ptr(vector->text_buffer_.get(), text.length());
      flight.embedding_entries_.push_back(vector);
      join_vectors_.emplace(VectorKey{key.model_, vector->text_}, vector);
    }
    return ret;
  }

  int join_vectors_status(const Flight &flight) const
  {
    bool pending = false;
    for (const auto *vectors : {&flight.left_vectors_, &flight.right_vectors_})
    {
      for (const VectorHandle &vector : *vectors)
      {
        if (!vector)
        {
          return OB_ERR_UNEXPECTED;
        }
        else
        {
          const Entry::State state = vector->state();
          if (state == Entry::FAILED)
          {
            return vector->error_ == OB_SUCCESS ? OB_ERR_UNEXPECTED
                                                : vector->error_;
          }
          pending |= state == Entry::PENDING;
        }
      }
    }
    return pending ? OB_EAGAIN : OB_SUCCESS;
  }

  int start_join_embedding(Flight &flight, bool &started)
  {
    int ret = OB_SUCCESS;
    started = false;
    ObArray<ObString> inputs;
    for (const VectorHandle &vector : flight.embedding_entries_)
    {
      if (OB_SUCC(ret))
      {
        ret = inputs.push_back(vector->text_);
      }
    }
    if (OB_FAIL(ret))
    {
    }
    else if (inputs.empty())
    {
      flight.phase_.store(Flight::WAIT_VECTORS);
    }
    else
    {
      const Model &snapshot = *flight.binding_->embedding_model_;
      ObAIFuncModel model(flight.allocator_, *snapshot.info_,
                          snapshot.endpoint_);
      if (OB_FAIL(model.start_dense_embedding_batch(inputs, nullptr,
                                                    flight.batch_)))
      {
      }
      else
      {
        std::lock_guard<std::mutex> guard(mutex_);
        proxy_join_embeddings_ += inputs.count();
        started = true;
      }
    }
    return ret;
  }

  int collect_join_vectors(Flight &flight, const ObIArray<ObString> &results)
  {
    int ret = OB_SUCCESS;
    if (results.count() !=
        static_cast<int64_t>(flight.embedding_entries_.size()))
    {
      ret = OB_INVALID_DATA;
      LOG_WARN("AI_JOIN proxy response count differs from unique side inputs",
               K(ret));
      FORWARD_USER_ERROR(
          ret,
          "AI_JOIN embedding proxy response count differs from its inputs");
    }
    for (int64_t index = 0; OB_SUCC(ret) && index < results.count(); ++index)
    {
      ProxyVectorAllocator allocator(budget_);
      float *values = nullptr;
      int64_t dimension = 0;
      ret = SemanticEmbeddingProxy::parse_vector(allocator, results.at(index),
                                                 values, dimension);
      if (allocator.error() != OB_SUCCESS)
      {
        ret = allocator.error();
      }
      if (OB_SUCC(ret))
      {
        std::lock_guard<std::mutex> guard(mutex_);
        if (OB_FAIL(reserve_locked(dimension * sizeof(float) + 64)))
        {
        }
        else
        {
          VectorEntry &vector = *flight.embedding_entries_.at(index);
          vector.values_.reset(new float[dimension]);
          std::memcpy(vector.values_.get(), values, dimension * sizeof(float));
          vector.dimension_ = dimension;
        }
      }
    }
    if (OB_SUCC(ret))
    {
      for (const VectorHandle &vector : flight.embedding_entries_)
      {
        vector->state_.store(Entry::COMPLETE, std::memory_order_release);
      }
      flight.phase_.store(Flight::WAIT_VECTORS);
    }
    return ret;
  }

  int publish_routing(Flight &flight, bool &terminal)
  {
    int ret = OB_SUCCESS;
    std::lock_guard<std::mutex> guard(mutex_);
    int64_t kept = 0;
    for (int64_t index = 0;
         OB_SUCC(ret) && index < static_cast<int64_t>(flight.entries_.size());
         ++index)
    {
      if (kept < flight.completion_positions_.count() &&
          flight.completion_positions_.at(kept) == index)
      {
        ++kept;
      }
      else
      {
        Entry &entry = *flight.entries_.at(index);
        if (OB_FAIL(store_result_locked(entry, ObString("{\"value\":false}"))))
        {
        }
        else
        {
          entry.state_.store(Entry::COMPLETE, std::memory_order_release);
          ++proxy_rejected_;
        }
      }
    }
    if (OB_SUCC(ret))
    {
      proxy_forwarded_ += flight.completion_positions_.count();
      terminal = flight.completion_positions_.empty();
      flight.kind_ = Flight::COMPLETION;
      if (!terminal)
      {
        flight.phase_.store(Flight::QUEUED);
      }
    }
    return ret;
  }

  int route_join(Flight &flight, bool &terminal)
  {
    int ret = join_vectors_status(flight);
    if (OB_SUCC(ret) &&
        (flight.left_vectors_.size() != flight.entries_.size() ||
         flight.right_vectors_.size() != flight.entries_.size()))
    {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("AI_JOIN proxy lost candidate vector bindings", K(ret));
    }
    for (int64_t index = 0;
         OB_SUCC(ret) && index < static_cast<int64_t>(flight.entries_.size());
         ++index)
    {
      const VectorEntry &left = *flight.left_vectors_.at(index);
      const VectorEntry &right = *flight.right_vectors_.at(index);
      double score = 0;
      if (OB_FAIL(SemanticEmbeddingProxy::cosine(
              left.values_.get(), left.dimension_, right.values_.get(),
              right.dimension_, score)))
      {
      }
      else if (score >= flight.binding_->proxy_.threshold_)
      {
        if (OB_FAIL(flight.completion_positions_.push_back(index)))
        {
        }
        else
        {
          ret = flight.completion_prompts_.push_back(flight.prompts_.at(index));
        }
      }
    }
    if (OB_SUCC(ret))
    {
      ret = publish_routing(flight, terminal);
    }
    return ret;
  }

  int start_embedding(Flight &flight, bool &started)
  {
    int ret = OB_SUCCESS;
    started = false;
    Binding &binding = *flight.binding_;
    ObArray<ObString> inputs;
    {
      std::lock_guard<std::mutex> guard(mutex_);
      const auto state = binding.query_state_.load();
      if (state == Binding::EMPTY)
      {
        binding.query_state_.store(Binding::PENDING);
        flight.includes_query_ = true;
      }
      else if (state == Binding::PENDING)
      {
        flight.phase_.store(Flight::WAIT_QUERY);
      }
      else if (state == Binding::FAILED)
      {
        ret = binding.query_error_;
      }
    }
    if (OB_SUCC(ret) && flight.phase_.load() != Flight::WAIT_QUERY)
    {
      if (flight.includes_query_)
      {
        ret = inputs.push_back(binding.proxy_.query_);
      }
      for (int64_t index = 0; OB_SUCC(ret) && index < flight.documents_.count();
           ++index)
      {
        ret = inputs.push_back(flight.documents_.at(index));
      }
      if (OB_SUCC(ret))
      {
        ObAIFuncModel model(flight.allocator_, *binding.embedding_model_->info_,
                            binding.embedding_model_->endpoint_);
        ret = model.start_dense_embedding_batch(inputs, nullptr, flight.batch_);
      }
      if (OB_SUCC(ret))
      {
        std::lock_guard<std::mutex> guard(mutex_);
        proxy_query_embeddings_ += flight.includes_query_;
        proxy_document_embeddings_ += flight.documents_.count();
        started = true;
      }
    }
    return ret;
  }

  int route_embedding(Flight &flight, const ObIArray<ObString> &results,
                      bool &terminal)
  {
    int ret = OB_SUCCESS;
    Binding &binding = *flight.binding_;
    const int64_t offset = flight.includes_query_ ? 1 : 0;
    if (results.count() !=
        static_cast<int64_t>(flight.entries_.size()) + offset)
    {
      ret = OB_INVALID_DATA;
      LOG_WARN("embedding proxy response count differs from unique inputs",
               K(ret));
      FORWARD_USER_ERROR(
          ret, "embedding proxy response count differs from its inputs");
    }
    if (OB_SUCC(ret) && flight.includes_query_)
    {
      ProxyVectorAllocator allocator(budget_);
      float *vector = nullptr;
      int64_t dimension = 0;
      ret = SemanticEmbeddingProxy::parse_vector(allocator, results.at(0),
                                                 vector, dimension);
      if (allocator.error() != OB_SUCCESS)
      {
        ret = allocator.error();
      }
      if (OB_SUCC(ret))
      {
        std::lock_guard<std::mutex> guard(mutex_);
        if (OB_FAIL(reserve_locked(dimension * sizeof(float) + 64)))
        {
        }
        else
        {
          binding.query_vector_.reset(new float[dimension]);
          std::memcpy(binding.query_vector_.get(), vector,
                      dimension * sizeof(float));
          binding.query_dimension_ = dimension;
          binding.query_state_.store(Binding::READY, std::memory_order_release);
        }
      }
    }
    for (int64_t index = 0; OB_SUCC(ret) && index < flight.documents_.count();
         ++index)
    {
      ProxyVectorAllocator allocator(budget_);
      float *vector = nullptr;
      int64_t dimension = 0;
      double score = 0;
      ret = SemanticEmbeddingProxy::parse_vector(
          allocator, results.at(index + offset), vector, dimension);
      if (allocator.error() != OB_SUCCESS)
      {
        ret = allocator.error();
      }
      if (OB_FAIL(ret))
      {
      }
      else if (OB_FAIL(SemanticEmbeddingProxy::cosine(
                   binding.query_vector_.get(), binding.query_dimension_,
                   vector, dimension, score)))
      {
      }
      else if (score >= binding.proxy_.threshold_)
      {
        if (OB_FAIL(flight.completion_positions_.push_back(index)))
        {
        }
        else
        {
          ret = flight.completion_prompts_.push_back(flight.prompts_.at(index));
        }
      }
    }
    if (OB_SUCC(ret))
    {
      ret = publish_routing(flight, terminal);
    }
    return ret;
  }

  void fail_pending_locked(Flight &flight, int error)
  {
    for (const VectorHandle &vector : flight.embedding_entries_)
    {
      if (vector->state() == Entry::PENDING)
      {
        vector->error_ = error;
        vector->state_.store(Entry::FAILED, std::memory_order_release);
      }
    }
    for (const Handle &entry : flight.entries_)
    {
      if (entry->state() == Entry::PENDING)
      {
        entry->error_ = error;
        entry->state_.store(Entry::FAILED, std::memory_order_release);
      }
    }
  }

  void finish(Flight &flight, int error, bool &progress)
  {
    const int ret = error;
    std::lock_guard<std::mutex> guard(mutex_);
    if (flight.includes_query_ &&
        flight.binding_->query_state_.load() == Binding::PENDING)
    {
      flight.binding_->query_error_ =
          error == OB_SUCCESS ? OB_ERR_UNEXPECTED : error;
      flight.binding_->query_state_.store(Binding::FAILED,
                                          std::memory_order_release);
    }
    if (error != OB_SUCCESS)
    {
      fail_pending_locked(flight, error);
    }
    else
    {
      for (const Handle &entry : flight.entries_)
      {
        if (entry->state() == Entry::PENDING)
        {
          entry->state_.store(Entry::COMPLETE, std::memory_order_release);
        }
      }
    }
    flight.phase_.store(Flight::DONE);
    progress = true;
    if (error != OB_SUCCESS)
    {
      LOG_WARN("cached semantic request failed; subscribers receive the "
               "original error",
               K(error), "execution_id", budget_.execution_id(), "binding_id",
               flight.binding_->id_, "function_type", flight.binding_->type_,
               "proxy", flight.binding_->proxy_.enabled_);
    }
  }

  int publish_completions(Flight &flight, const ObIArray<ObString> &results,
                           const ObIArray<int64_t> *positions,
                           ObIAllocator &allocator, bool &progress)
  {
    int ret = OB_SUCCESS;
    const bool proxy = flight.binding_->proxy_.enabled_;
    const int64_t expected_count = proxy ? flight.completion_positions_.count()
                                        : static_cast<int64_t>(flight.entries_.size());
    if ((nullptr == positions && results.count() != expected_count) ||
        (nullptr != positions && positions->count() != results.count()))
    {
      ret = OB_INVALID_DATA;
      LOG_WARN("cached semantic response count differs from submitted inputs", K(ret));
    }
    for (int64_t index = 0; OB_SUCC(ret) && index < results.count(); ++index)
    {
      const int64_t request = nullptr == positions ? index : positions->at(index);
      if (request < 0 || request >= expected_count)
      {
        ret = OB_INVALID_DATA;
        LOG_WARN("cached semantic completion has invalid position", K(ret), K(request));
      }
      else
      {
        const ObItemType type = flight.binding_->type_;
        bool value = false;
        if (type != T_FUN_SYS_AI_MAP &&
            OB_FAIL(SemanticExprUtils::parse_boolean_result(
                type, allocator, results.at(index), value)))
        {
        }
        else
        {
          const int64_t position = proxy ? flight.completion_positions_.at(request) : request;
          std::lock_guard<std::mutex> guard(mutex_);
          Entry &entry = *flight.entries_.at(position);
          if (entry.state() != Entry::PENDING)
          {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cached semantic completion was published more than once", K(ret), K(request));
          }
          else if (OB_FAIL(store_result_locked(entry, results.at(index))))
          {
          }
          else
          {
            ++flight.completed_count_;
            if (nullptr != positions)
            {
              entry.state_.store(Entry::COMPLETE, std::memory_order_release);
              progress = true;
            }
          }
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
        bool terminal = false;
        ret = Flight::check_status(flight.get());
        if (OB_SUCC(ret) && flight->phase_.load() == Flight::WAIT_QUERY)
        {
          const auto state =
              flight->binding_->query_state_.load(std::memory_order_acquire);
          if (state == Binding::FAILED)
          {
            ret = flight->binding_->query_error_;
          }
          else if (state == Binding::READY)
          {
            flight->phase_.store(Flight::QUEUED);
          }
        }
        if (OB_SUCC(ret) && flight->phase_.load() == Flight::WAIT_VECTORS &&
            join_vectors_status(*flight) != OB_EAGAIN)
        {
          ret = route_join(*flight, terminal);
          progress = true;
        }
        if (OB_SUCC(ret) && !terminal &&
            flight->phase_.load() == Flight::QUEUED)
        {
          bool started = false;
          flight->batch_.set_nonblocking_admission();
          flight->batch_.set_max_parallel(flight->max_parallel_);
          flight->batch_.set_shared_buffer_usage(*budget_.memory_counter(),
                                                 budget_.memory_limit());
          flight->batch_.set_status_checker(&Flight::check_status,
                                            flight.get());
          if (flight->kind_ == Flight::EMBEDDING)
          {
            ret = start_embedding(*flight, started);
          }
          else if (flight->kind_ == Flight::JOIN_EMBEDDING)
          {
            ret = start_join_embedding(*flight, started);
          }
          else
          {
            if (budget_.row_async_enabled())
            {
              flight->batch_.set_incremental_results();
            }
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
              ObAIFuncModel model(flight->allocator_,
                                  *flight->binding_->model_->info_,
                                  flight->binding_->model_->endpoint_);
              ObArray<ObString> &prompts = flight->binding_->proxy_.enabled_
                                               ? flight->completion_prompts_
                                               : flight->prompts_;
              ret =
                  model.start_completion_batch(prompts, config, flight->batch_);
              if (OB_SUCC(ret))
              {
                started = true;
                std::lock_guard<std::mutex> guard(mutex_);
                submitted_ += prompts.count();
              }
            }
          }
          if (OB_SUCC(ret) && started)
          {
            flight->phase_.store(Flight::RUNNING);
            progress = true;
          }
          else if (OB_SUCC(ret) &&
                   (flight->phase_.load() == Flight::WAIT_QUERY ||
                    flight->phase_.load() == Flight::WAIT_VECTORS))
          {
            progress = true;
          }
        }
        bool finished = false;
        ObArray<ObString> results;
        if (OB_SUCC(ret) && flight->phase_.load() == Flight::RUNNING &&
            flight->kind_ == Flight::COMPLETION && budget_.row_async_enabled())
        {
          ObArenaAllocator ready_allocator("SemanticReady");
          ObArray<int64_t> positions;
          if (OB_FAIL(flight->batch_.get_ready_results(
              ready_allocator, positions, results, finished)))
          {
          }
          else if (OB_FAIL(publish_completions(
              *flight, results, &positions, ready_allocator, progress)))
          {
          }
          else if (finished)
          {
            const int64_t expected_count = flight->binding_->proxy_.enabled_
                ? flight->completion_positions_.count()
                : static_cast<int64_t>(flight->entries_.size());
            if (flight->completed_count_ != expected_count)
            {
              ret = OB_INVALID_DATA;
              LOG_WARN("cached semantic flight finished before publishing all results", K(ret));
            }
            terminal = true;
          }
        }
        else if (OB_SUCC(ret) && flight->phase_.load() == Flight::RUNNING &&
            OB_FAIL(flight->batch_.poll(finished)))
        {
        }
        else if (OB_SUCC(ret) && finished)
        {
          if (OB_FAIL(flight->batch_.get_results(results)))
          {
          }
          else if (flight->kind_ == Flight::EMBEDDING)
          {
            ret = route_embedding(*flight, results, terminal);
            progress = true;
          }
          else if (flight->kind_ == Flight::JOIN_EMBEDDING)
          {
            ret = collect_join_vectors(*flight, results);
            flight->batch_.cancel();
            progress = true;
          }
          else
          {
            ret = publish_completions(*flight, results, nullptr, flight->allocator_, progress);
            terminal = true;
          }
        }
        if (ret != OB_SUCCESS || terminal)
        {
          finish(*flight, ret, progress);
        }
      }
    }
    catch (const std::bad_alloc &)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      finish(*flight, ret, progress);
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
  int64_t proxy_query_embeddings_;
  int64_t proxy_document_embeddings_;
  int64_t proxy_rejected_;
  int64_t proxy_forwarded_;
  int64_t proxy_join_embeddings_;
  int64_t proxy_vector_ready_hits_;
  int64_t proxy_vector_coalesced_;
  std::map<VectorKey, VectorHandle, VectorLess> join_vectors_;
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
                                  std::vector<Handle> &handles, bool &progress,
                                  const SemanticProxyConfig *proxy,
                                  const ObIArray<ObString> *documents,
                                  const ObIArray<ObString> *right_documents)
{
  int ret = impl_->budget_.failure();
  const bool use_proxy = nullptr != proxy && proxy->enabled_;
  const bool join_proxy = use_proxy && type == T_FUN_SYS_AI_JOIN;
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
  else if (use_proxy &&
           ((type != T_FUN_SYS_AI_FILTER && !join_proxy) || nullptr == documents ||
            documents->count() != keys.count() || proxy->model_.empty() ||
            (join_proxy ? (!proxy->query_.empty() || nullptr == right_documents ||
                           right_documents->count() != keys.count())
                        : (proxy->query_.empty() || nullptr != right_documents)) ||
            !std::isfinite(proxy->threshold_) ||
            proxy->threshold_ < -1 || proxy->threshold_ > 1))
  {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid embedding proxy submission", K(ret), K(type));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                   "invalid embedding proxy inputs or options");
  }
  else if (use_proxy &&
           (proxy->query_.length() > ObAIFuncClient::MAX_REQUEST_BYTES ||
            proxy->model_.length() > ObAIFuncClient::MAX_REQUEST_BYTES))
  {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("embedding proxy options exceed request size limit", K(ret));
    FORWARD_USER_ERROR(ret,
                       "embedding proxy options exceed the request size limit");
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
      if (OB_SUCC(ret) && use_proxy)
      {
        ret = append_proxy_identity(canonical, *proxy);
      }
      if (OB_SUCC(ret))
      {
        std::lock_guard<std::mutex> guard(impl_->mutex_);
        std::shared_ptr<Impl::Binding> binding;
        std::shared_ptr<Impl::Flight> flight;
        if (OB_FAIL(impl_->binding_locked(type, model, config,
                                          canonical.string(), proxy, binding)))
        {
        }
        else
        {
          binding_id = binding->id_;
        }
        try
        {
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
              const int64_t proxy_bytes =
                  join_proxy ? 6 * (documents->at(index).length() +
                                    right_documents->at(index).length()) +
                                   8192
                  : use_proxy ? 6 * (documents->at(index).length() +
                                     proxy->query_.length()) +
                                    4096
                              : 0;
              const int64_t request_bytes = 6 * prompts.at(index).length() +
                                            8 * config_bytes + 2048 +
                                            proxy_bytes;
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
                      if (use_proxy)
                      {
                        flight->kind_ = join_proxy
                                            ? Impl::Flight::JOIN_EMBEDDING
                                            : Impl::Flight::EMBEDDING;
                      }
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
                  if (OB_SUCC(ret) && join_proxy)
                  {
                    Impl::VectorHandle left;
                    Impl::VectorHandle right;
                    if (OB_FAIL(impl_->vector_locked(
                            *flight, documents->at(index), left)))
                    {
                    }
                    else if (OB_FAIL(impl_->vector_locked(
                                 *flight, right_documents->at(index), right)))
                    {
                    }
                    else
                    {
                      flight->left_vectors_.push_back(left);
                      flight->right_vectors_.push_back(right);
                    }
                  }
                  else if (OB_SUCC(ret) && use_proxy)
                  {
                    ObString document;
                    if (OB_FAIL(ob_write_string(flight->allocator_,
                                                documents->at(index),
                                                document)))
                    {
                    }
                    else
                    {
                      ret = flight->documents_.push_back(document);
                    }
                  }
                  if (OB_SUCC(ret))
                  {
                    flight->entries_.push_back(entry);
                    impl_->entries_.emplace(
                        Impl::Key{binding->id_, entry->key_}, entry);
                    handles.at(index) = entry;
                    ++impl_->distinct_keys_;
                    progress = true;
                  }
                }
              }
            }
          }
        }
        catch (const std::bad_alloc &)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        if (flight && join_proxy && ret != OB_SUCCESS)
        {
          impl_->fail_pending_locked(*flight, ret);
          flight->phase_.store(Impl::Flight::DONE);
          progress = true;
        }
        else if (flight && flight->entries_.empty())
        {
          flight->phase_.store(Impl::Flight::DONE);
        }
        else if (flight && join_proxy && flight->embedding_entries_.empty())
        {
          flight->phase_.store(Impl::Flight::WAIT_VECTORS);
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
                                       int64_t config_bytes,
                                       int64_t proxy_bytes) const
{
  std::lock_guard<std::mutex> guard(impl_->mutex_);
  const int64_t retained =
      sizeof(Entry) + sizeof(Impl::Key) + sizeof(Handle) + 64 + key.length();
  const int64_t request = sizeof(Impl::Flight) + 6 * prompt.length() +
                          8 * config_bytes + 2048 + proxy_bytes;
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
      const auto phase = flight->phase_.load();
      ready = phase == Impl::Flight::QUEUED || phase == Impl::Flight::DONE ||
              (phase == Impl::Flight::RUNNING && flight->batch_.is_ready()) ||
              (phase == Impl::Flight::WAIT_QUERY &&
               flight->binding_->query_state_.load(std::memory_order_acquire) !=
                   Impl::Binding::PENDING) ||
              (phase == Impl::Flight::WAIT_VECTORS &&
               impl_->join_vectors_status(*flight) != OB_EAGAIN) ||
              impl_->budget_.failure() != OB_SUCCESS ||
              ObTimeUtility::current_time() >= impl_->deadline_;
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

int64_t SemanticFunctionCache::deadline() const { return impl_->deadline_; }
} // namespace sql
} // namespace oceanbase
