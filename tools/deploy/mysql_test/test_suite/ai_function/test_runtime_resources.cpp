#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "lib/worker.h"
#include "observer/omt/ob_th_worker.h"
#include "rpc/frame/ob_req_processor.h"
#include "rpc/frame/ob_req_translator.h"
#include "sql/engine/basic/ob_ai_func_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{
void require(bool condition, const char *message)
{
  if (!condition) {
    throw std::runtime_error(message);
  }
}

enum class Fault { NONE, HEADER, MULTI, EASY, ADD, PERFORM, POLL };

struct CurlResources
{
  bool fail(Fault operation)
  {
    if (operation == fault && ++attempts == fail_at) {
      injected = true;
      return true;
    }
    return false;
  }

  void inject(Fault operation, int64_t attempt)
  {
    std::lock_guard<std::recursive_mutex> guard(mutex);
    fault = operation;
    fail_at = attempt;
    attempts = 0;
    injected = false;
  }

  void check_empty() const
  {
    std::lock_guard<std::recursive_mutex> guard(mutex);
    require(attached.empty(), "curl requests remained attached");
    require(easy.empty(), "curl easy handles were not released");
    require(multi.empty(), "curl multi handles were not released");
    require(headers.empty(), "curl header nodes were not released");
  }

  mutable std::recursive_mutex mutex;
  std::unordered_set<CURL *> easy;
  std::unordered_set<CURLM *> multi;
  std::unordered_set<curl_slist *> headers;
  std::unordered_map<CURL *, CURLM *> attached;
  Fault fault = Fault::NONE;
  int64_t fail_at = 0;
  int64_t attempts = 0;
  bool injected = false;
  bool omit_cleanup = false;
  bool creating_multi = false;
};

CurlResources &resources()
{
  static CurlResources state;
  return state;
}

bool omit_allocation_release = false;
std::mutex completion_mutex;
std::condition_variable completion_changed;
int64_t completed_transfers = 0;

class TrackingAllocator final : public ObIAllocator
{
public:
  ~TrackingAllocator() override
  {
    for (const auto &allocation : allocations_) {
      std::free(allocation.first);
    }
  }

  void *alloc(const int64_t size) override
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (++attempts_ == fail_at_) {
      injected_ = true;
      return nullptr;
    }
    void *pointer = std::malloc(size);
    if (pointer != nullptr) {
      allocations_.emplace(pointer, size);
    }
    return pointer;
  }

  void *alloc(const int64_t size, const ObMemAttr &) override { return alloc(size); }

  void free(void *pointer) override
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (pointer != nullptr) {
      if (omit_allocation_release) {
        omit_allocation_release = false;
        return;
      }
      require(allocations_.erase(pointer) == 1, "unknown or duplicate allocation release");
      std::free(pointer);
    }
  }

  void fail_at(int64_t attempt)
  {
    std::lock_guard<std::mutex> guard(mutex_);
    attempts_ = 0;
    fail_at_ = attempt;
    injected_ = false;
  }
  bool injected() const
  {
    std::lock_guard<std::mutex> guard(mutex_);
    return injected_;
  }
  size_t outstanding() const
  {
    std::lock_guard<std::mutex> guard(mutex_);
    return allocations_.size();
  }

private:
  mutable std::mutex mutex_;
  std::unordered_map<void *, int64_t> allocations_;
  int64_t attempts_ = 0;
  int64_t fail_at_ = 0;
  bool injected_ = false;
};

struct Inputs
{
  explicit Inputs(int64_t count = 3,
                  const char *body = "{\"messages\":[{\"content\":\"native-resource\"}]}")
  {
    require(headers.push_back(ObString::make_string("Content-Type: application/json")) == OB_SUCCESS,
            "prepare content header");
    require(headers.push_back(ObString::make_string("Authorization: Bearer test-only")) == OB_SUCCESS,
            "prepare authorization header");
    ObIJsonBase *json = nullptr;
    require(ObJsonBaseFactory::get_json_base(&arena,
                ObString::make_string(body),
                ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, json) == OB_SUCCESS,
            "prepare request JSON");
    for (int64_t index = 0; index < count; ++index) {
      require(data.push_back(static_cast<ObJsonObject *>(json)) == OB_SUCCESS, "prepare input row");
    }
  }

  ObArenaAllocator arena;
  ObArray<ObString> headers;
  ObArray<ObJsonObject *> data;
};

void initialize(ObAIFuncClient &client, TrackingAllocator &allocator, Inputs &inputs, const char *url)
{
  client.set_timeout_sec(10);
  require(client.init(allocator, ObString::make_string(url), inputs.headers) == OB_SUCCESS,
          "initialize client");
}

int finish(ObAIFuncClient &client, ObArray<ObJsonObject *> &responses)
{
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
  while (!client.check_batch_finished()) {
    require(std::chrono::steady_clock::now() < deadline, "native test exceeded its deadline");
    require(resources().multi.size() == 1, "expected one owned curl multi handle");
    int descriptors = 0;
    require(curl_multi_poll(*resources().multi.begin(), nullptr, 0, 20, &descriptors) == CURLM_OK,
            "drive pending requests");
  }
  return client.get_batch_result(responses);
}

void test_initialization_allocation_failures(int64_t first_failure = 1)
{
  ObArray<ObString> headers;
  require(headers.push_back(ObString::make_string("Content-Type: application/json")) == OB_SUCCESS,
          "prepare content header");
  require(headers.push_back(ObString::make_string("Authorization: Bearer test-only")) == OB_SUCCESS,
          "prepare authorization header");
  for (int64_t failure = first_failure; failure <= 3; ++failure) {
    TrackingAllocator allocator;
    {
      ObAIFuncClient client;
      allocator.fail_at(failure);
      require(client.init(allocator, ObString::make_string("http://127.0.0.1:1/"), headers)
                  == OB_ALLOCATE_MEMORY_FAILED,
              "initialization must report injected allocation failure");
      client.reset();
      client.reset();
      require(allocator.outstanding() == 0, "initialization rollback leaked an allocation");
      allocator.fail_at(0);
      require(client.init(allocator, ObString::make_string("http://127.0.0.1:1/"), headers) == OB_SUCCESS,
              "client must recover after initialization failure");
    }
    require(allocator.outstanding() == 0, "client destruction leaked an allocation");
  }
  resources().check_empty();
}

void test_preparation_allocation_failures(const char *url)
{
  Inputs inputs;
  for (int64_t failure = 1; failure <= 64; ++failure) {
    TrackingAllocator allocator;
    bool injected = false;
    {
      ObAIFuncClient client;
      initialize(client, allocator, inputs, url);
      const size_t initialized = allocator.outstanding();
      allocator.fail_at(failure);
      const int result = client.send_post_batch_no_wait(inputs.data);
      injected = allocator.injected();
      require(result == (injected ? OB_ALLOCATE_MEMORY_FAILED : OB_SUCCESS),
              "preparation allocation failure must be reported");
      client.clean_up();
      client.clean_up();
      require(allocator.outstanding() == initialized, "preparation rollback leaked request memory");
      require(resources().easy.empty() && resources().attached.empty(), "preparation leaked handles");
    }
    require(allocator.outstanding() == 0, "preparation destruction leaked memory");
    resources().check_empty();
    if (!injected) {
      require(failure > inputs.data.count(), "allocation sweep did not reach each request");
      std::printf("PASS preparation allocation sweep: %ld failure sites\n", failure - 1);
      return;
    }
  }
  require(false, "allocation sweep did not reach a successful preparation");
}

void test_curl_failure(Fault fault, int64_t attempt, int expected, const char *url)
{
  Inputs inputs;
  TrackingAllocator allocator;
  {
    ObAIFuncClient client;
    resources().inject(fault, attempt);
    int result = client.init(allocator, ObString::make_string(url), inputs.headers);
    if (result == OB_SUCCESS) {
      ObArray<ObJsonObject *> responses;
      result = client.send_post_batch(allocator, ObString::make_string(url), inputs.headers,
                                      inputs.data, responses);
      require(responses.empty(), "failed batch exposed partial results");
    }
    require(resources().injected, "curl fault was not reached");
    require(result == expected, "curl fault returned the wrong error");
    client.reset();
    client.reset();
    require(allocator.outstanding() == 0, "curl failure leaked request allocations");
    resources().inject(Fault::NONE, 0);
  }
  resources().check_empty();
}

void test_response_allocation_failure(const char *url)
{
  Inputs inputs;
  TrackingAllocator allocator;
  int64_t shared_bytes = 0;
  {
    ObAIFuncClient client;
    client.set_shared_buffer_usage(shared_bytes);
    initialize(client, allocator, inputs, url);
    const size_t initialized = allocator.outstanding();
    require(client.send_post_batch_no_wait(inputs.data) == OB_SUCCESS, "submit response allocation test");
    allocator.fail_at(1);
    ObArray<ObJsonObject *> responses;
    require(finish(client, responses) == OB_ALLOCATE_MEMORY_FAILED, "response OOM must propagate");
    require(allocator.injected(), "response allocation fault was not reached");
    require(responses.empty(), "response OOM exposed partial results");
    client.clean_up();
    require(allocator.outstanding() == initialized, "response OOM leaked request memory");
    require(shared_bytes == 0, "response OOM leaked shared buffer quota");
  }
  require(allocator.outstanding() == 0, "response OOM destruction leaked memory");
  resources().check_empty();
}

int query_status(void *context)
{
  return *static_cast<int *>(context);
}

void test_pending_lifecycle(const char *url)
{
  Inputs inputs;
  TrackingAllocator first;
  TrackingAllocator second;
  int status = OB_SUCCESS;
  {
    ObAIFuncClient client;
    client.set_max_parallel(2);
    client.set_status_checker(query_status, &status);
    for (int64_t iteration = 0; iteration < 32; ++iteration) {
      status = OB_SUCCESS;
      initialize(client, first, inputs, url);
      const size_t initialized = first.outstanding();
      require(client.send_post_batch_no_wait(inputs.data) == OB_SUCCESS, "submit pending batch");
      require(resources().attached.size() == 2, "explicit window must leave one request queued");
      ObArray<ObJsonObject *> responses;
      require(client.get_batch_result(responses) == OB_EAGAIN && responses.empty(),
              "pending batch must not expose results");
      status = OB_CANCELED;
      require(client.check_batch_finished() && client.check_batch_finished(), "cancel must finish batch");
      require(client.get_batch_result(responses) == OB_CANCELED && responses.empty(), "cancel must propagate");
      require(resources().attached.empty() && resources().easy.empty(), "cancel retained active handles");
      client.clean_up();
      client.clean_up();
      require(first.outstanding() == initialized, "repeated cancellation leaked allocations");
    }
    status = OB_SUCCESS;
    require(client.send_post_batch_no_wait(inputs.data) == OB_SUCCESS, "resubmit after cancellation");
    initialize(client, second, inputs, url);
    require(first.outstanding() == 0, "reinitialization kept the previous allocator alive");
    require(client.send_post_batch_no_wait(inputs.data) == OB_SUCCESS, "submit before destruction");
  }
  require(first.outstanding() == 0 && second.outstanding() == 0, "pending destruction leaked memory");
  resources().check_empty();
}

void test_request_await_protocol()
{
  using oceanbase::lib::RequestAwait;
  using oceanbase::lib::RequestAwaitGuard;
  RequestAwait context;
  bool ready = false;
  const auto check = [](const void *state) { return *static_cast<const bool *>(state); };
  require(nullptr == RequestAwait::current(), "request await context must default to disabled");
  require(!RequestAwait::suspend(&context, &ready, check), "unmanaged callers must not suspend");
  {
    RequestAwaitGuard guard(context);
    require(!RequestAwait::suspend(&context, &ready, check), "request must authorize its execution context");
    context.enable(&context);
    require(!RequestAwait::suspend(&ready, &ready, check), "nested execution must not suspend the outer request");
    require(RequestAwait::suspend(&context, &ready, check), "authorized execution must register its wait");
    require(context.is_pending() && !context.is_ready(), "pending is distinct from ready");
    require(!RequestAwait::suspend(&context, &ready, check), "pending wait must not be overwritten");
    {
      RequestAwait nested;
      RequestAwaitGuard nested_guard(nested);
      require(!RequestAwait::suspend(&context, &ready, check), "nested scope must isolate authorization");
    }
    require(RequestAwait::current() == &context, "nested scope must restore the original wait context");
    ready = true;
    require(context.is_ready(), "completion must make the request resumable");
    context.reset_pending();
    require(!context.is_pending() && !context.is_ready(), "resume must clear the previous wait");
    context.cancel(OB_CANCELED);
    require(OB_CANCELED == context.cancel_ret(), "shutdown cancellation must be preserved");
    require(!RequestAwait::suspend(&context, &ready, check), "cancelled requests must not suspend again");
    context.reset();
    require(OB_SUCCESS == context.cancel_ret(), "new request must not inherit cancellation");
    require(!RequestAwait::suspend(&context, &ready, check), "new request must not inherit authorization");
  }
  require(nullptr == RequestAwait::current(), "request scope must restore the previous thread context");
}

void test_resumed_processor_exception()
{
  using oceanbase::lib::RequestAwait;
  using oceanbase::lib::RequestAwaitGuard;
  using oceanbase::rpc::ObRequest;
  using oceanbase::rpc::frame::ObReqProcessor;
  class ProbeProcessor final : public ObReqProcessor {
  public:
    int run() override
    {
      if (runs_++ == 0) {
        require(RequestAwait::suspend(this, this, [](const void *) { return true; }),
                "probe request must suspend before throwing on resume");
        return OB_EAGAIN;
      }
      throw OB_EXCEPTION<OB_ALLOCATE_MEMORY_FAILED>();
    }
    int runs_ = 0;
  } probe;
  class ProbeTranslator final : public oceanbase::rpc::frame::ObReqTranslator {
  public:
    explicit ProbeTranslator(ObReqProcessor &processor) : processor_(processor) {}
    int release(ObReqProcessor *processor) override
    {
      released_ = processor;
      ++releases_;
      return OB_SUCCESS;
    }
    ObReqProcessor *get_processor(ObRequest &) override
    {
      ++translations_;
      return &processor_;
    }
    ObReqProcessor &processor_;
    ObReqProcessor *released_ = nullptr;
    int translations_ = 0;
    int releases_ = 0;
  } translator(probe);
  RequestAwait await;
  RequestAwaitGuard await_guard(await);
  await.enable(&probe);
  ObAddr address;
  oceanbase::omt::ObWorkerProcessor processor(translator, address);
  oceanbase::omt::ObThWorker worker;
  oceanbase::lib::Worker *previous = &THIS_WORKER;
  oceanbase::lib::Worker::set_worker_to_thread_local(&worker);
  ObRequest request(ObRequest::OB_TASK);
  ObReqProcessor *retained = nullptr;
  const int first_ret = processor.process(request, retained);
  const bool suspended = await.is_pending() && retained == &probe && translator.releases_ == 0;
  await.reset_pending();
  const int resume_ret = processor.process(request, retained);
  oceanbase::lib::Worker::set_worker_to_thread_local(previous);
  require(OB_EAGAIN == first_ret && suspended, "pending processor must be retained without release");
  require(OB_ALLOCATE_MEMORY_FAILED == resume_ret, "resumed exception must retain its error code");
  require(nullptr == retained && 1 == translator.releases_ && &probe == translator.released_,
          "exception must release the retained processor exactly once");
  require(1 == translator.translations_ && 2 == probe.runs_, "resume must not translate a new processor");
  std::puts("PASS resumed processor exceptions preserve errors and release ownership exactly once");
}

void test_background_progress(const char *url, bool schedule = true)
{
  Inputs inputs;
  TrackingAllocator allocator;
  {
    ObAIFuncClient client;
    initialize(client, allocator, inputs, url);
    int64_t expected = 0;
    {
      std::lock_guard<std::mutex> guard(completion_mutex);
      expected = completed_transfers + inputs.data.count();
    }
    require(client.send_post_batch_no_wait(inputs.data) == OB_SUCCESS, "prepare background batch");
    if (schedule) {
      require(client.start_async() == OB_SUCCESS, "schedule background batch");
    }
    {
      std::unique_lock<std::mutex> guard(completion_mutex);
      require(completion_changed.wait_for(guard, std::chrono::seconds(3),
                  [&] { return completed_transfers >= expected; }),
              "requests must complete while the SQL thread does not poll");
    }
    bool finished = false;
    require(client.poll_batch(finished, 20) == OB_SUCCESS && finished,
            "background completion must be observable by the SQL thread");
    ObArray<ObJsonObject *> responses;
    require(client.get_batch_result(responses) == OB_SUCCESS && responses.count() == inputs.data.count(),
            "background batch must retain every result");
    client.reset();
  }
  resources().check_empty();
}

void test_background_cancellation(const char *url)
{
  Inputs held(1, "{\"messages\":[{\"content\":\"native-hold\"}]}");
  Inputs ready(1);
  TrackingAllocator allocator;
  {
    ObAIFuncClient client;
    for (int64_t iteration = 0; iteration < 16; ++iteration) {
      initialize(client, allocator, held, url);
      require(client.send_post_batch_no_wait(held.data) == OB_SUCCESS, "prepare cancel race");
      require(client.start_async() == OB_SUCCESS, "submit cancel race");
      require(client.start_async() == OB_INIT_TWICE, "duplicate scheduling must be rejected");
      client.reset();
      client.reset();
      require(allocator.outstanding() == 0, "background cancel retained caller memory");
    }
    initialize(client, allocator, ready, url);
    require(client.send_post_batch_no_wait(ready.data) == OB_SUCCESS, "prepare recovery after cancel");
    require(client.start_async() == OB_SUCCESS, "schedule recovery after cancel");
    bool finished = false;
    while (!finished) {
      require(client.poll_batch(finished, 20) == OB_SUCCESS, "recovery after cancel failed");
    }
    ObArray<ObJsonObject *> responses;
    require(client.get_batch_result(responses) == OB_SUCCESS && responses.count() == 1,
            "recovery exposed stale or missing results");
  }
  resources().check_empty();
}

void test_background_deadline_and_peer_progress(const char *url)
{
  Inputs held(1, "{\"messages\":[{\"content\":\"native-hold\"}]}");
  Inputs ready;
  int64_t expected = 0;
  {
    std::lock_guard<std::mutex> guard(completion_mutex);
    expected = completed_transfers + held.data.count() + ready.data.count();
  }
  TrackingAllocator allocator;
  {
    ObAIFuncClient client;
    client.set_timeout_sec(1);
    require(client.init(allocator, ObString::make_string(url), held.headers) == OB_SUCCESS,
            "initialize background deadline");
    require(client.send_post_batch_no_wait(held.data) == OB_SUCCESS, "prepare background deadline");
    require(client.start_async() == OB_SUCCESS, "schedule background deadline");
    TrackingAllocator ready_allocator;
    ObAIFuncClient peer;
    initialize(peer, ready_allocator, ready, url);
    require(peer.send_post_batch_no_wait(ready.data) == OB_SUCCESS, "prepare independent peer");
    require(peer.start_async() == OB_SUCCESS, "schedule independent peer");
    bool finished = false;
    while (!finished) {
      require(peer.poll_batch(finished, 20) == OB_SUCCESS, "held batch blocked an independent peer");
    }
    require(!client.check_batch_finished(), "independent peer must finish before held batch deadline");
    {
      std::unique_lock<std::mutex> guard(completion_mutex);
      require(completion_changed.wait_for(guard, std::chrono::seconds(3),
                  [&] { return completed_transfers >= expected; }),
              "background must enforce deadline without SQL-thread polling");
    }
    require(client.poll_batch(finished, 20) == OB_TIMEOUT && finished, "background deadline must propagate");
    ObArray<ObJsonObject *> responses;
    require(client.get_batch_result(responses) == OB_TIMEOUT && responses.empty(),
            "timed-out background request must not expose results");
  }
  resources().check_empty();
}

void test_background_admission(const char *url)
{
  Inputs held(1, "{\"messages\":[{\"content\":\"native-admission\"}]}");
  TrackingAllocator allocator;
  {
    ObAIFuncClient clients[65];
    for (int64_t index = 0; index < 64; ++index) {
      initialize(clients[index], allocator, held, url);
      require(clients[index].send_post_batch_no_wait(held.data) == OB_SUCCESS, "prepare bounded admission");
      require(clients[index].start_async() == OB_SUCCESS, "fill bounded scheduler");
    }
    clients[64].set_timeout_sec(1);
    require(clients[64].init(allocator, ObString::make_string(url), held.headers) == OB_SUCCESS,
            "initialize admission deadline");
    require(clients[64].send_post_batch_no_wait(held.data) == OB_SUCCESS, "prepare queued admission");
    require(clients[64].start_async() == OB_TIMEOUT, "full scheduler must honor admission deadline");
    for (ObAIFuncClient &client : clients) {
      client.reset();
    }
    require(allocator.outstanding() == 0, "bounded admission or destruction retained request memory");
    Inputs ready(1);
    initialize(clients[0], allocator, ready, url);
    require(clients[0].send_post_batch_no_wait(ready.data) == OB_SUCCESS, "prepare after full scheduler");
    require(clients[0].start_async() == OB_SUCCESS, "released scheduler slots must admit new work");
  }
  resources().check_empty();
}

void test_nonblocking_admission(const char *url)
{
  Inputs held(1, "{\"messages\":[{\"content\":\"native-admission\"}]}");
  Inputs ready(1);
  TrackingAllocator allocator;
  TrackingAllocator ready_allocator;
  {
    ObAIFuncClient clients[65];
    for (int64_t index = 0; index < 64; ++index) {
      initialize(clients[index], allocator, held, url);
      require(clients[index].send_post_batch_no_wait(held.data) == OB_SUCCESS, "prepare full admission gate");
      require(clients[index].start_async() == OB_SUCCESS, "fill admission gate");
    }
    initialize(clients[64], ready_allocator, ready, url);
    require(clients[64].send_post_batch_no_wait(ready.data) == OB_SUCCESS, "prepare nonblocking admission");
    const auto started = std::chrono::steady_clock::now();
    require(clients[64].start_async(false) == OB_SUCCESS, "register nonblocking admission");
    require(std::chrono::steady_clock::now() - started < std::chrono::milliseconds(250),
            "full admission must return without waiting for a network slot");
    require(clients[64].is_waiting_for_admission(), "full scheduler must retain pending admission");
    require(clients[64].start_async(false) == OB_INIT_TWICE, "pending admission must reject duplicate submission");
    bool finished = true;
    require(clients[64].poll_batch(finished) == OB_SUCCESS && !finished, "full scheduler must remain pending");
    clients[0].reset();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!finished) {
      require(std::chrono::steady_clock::now() < deadline, "pending admission did not recover its slot");
      require(clients[64].poll_batch(finished, 20) == OB_SUCCESS, "pending admission failed after slot release");
    }
    ObArray<ObJsonObject *> responses;
    require(clients[64].get_batch_result(responses) == OB_SUCCESS && responses.count() == 1,
            "pending admission lost its prepared request");
    clients[64].reset();
    const auto retained_results = ready_allocator.outstanding();
    initialize(clients[0], allocator, held, url);
    require(clients[0].send_post_batch_no_wait(held.data) == OB_SUCCESS, "refill admission gate");
    require(clients[0].start_async() == OB_SUCCESS, "restore full scheduler");
    initialize(clients[64], ready_allocator, ready, url);
    require(clients[64].send_post_batch_no_wait(ready.data) == OB_SUCCESS, "prepare pending cancellation");
    require(clients[64].start_async(false) == OB_SUCCESS && clients[64].is_waiting_for_admission(),
            "register pending cancellation");
    clients[64].reset();
        require(ready_allocator.outstanding() == retained_results, "pending cancellation retained request memory");
        clients[64].set_timeout_sec(1);
        require(clients[64].init(ready_allocator, ObString::make_string(url), ready.headers) == OB_SUCCESS,
          "initialize pending admission deadline");
        require(clients[64].send_post_batch_no_wait(ready.data) == OB_SUCCESS, "prepare pending admission deadline");
        require(clients[64].start_async(false) == OB_SUCCESS && clients[64].is_waiting_for_admission(),
          "deadline test must wait for network capacity");
        finished = false;
        int result = OB_SUCCESS;
        const auto timeout_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (!finished && OB_SUCCESS == result) {
          require(std::chrono::steady_clock::now() < timeout_deadline, "pending admission exceeded its deadline");
          result = clients[64].poll_batch(finished, 20);
        }
        require(finished && result == OB_TIMEOUT, "pending admission must retain the original timeout");
        clients[64].reset();
        require(ready_allocator.outstanding() == retained_results, "pending deadline retained request memory");
    for (ObAIFuncClient &client : clients) {
      client.reset();
    }
    require(allocator.outstanding() == 0, "pending admission or cancellation retained memory");
  }
  resources().check_empty();
}

void test_background_shutdown(const char *url)
{
  Inputs held(1, "{\"messages\":[{\"content\":\"native-hold\"}]}");
  TrackingAllocator allocator;
  {
    ObAIFuncClient clients[2];
    for (ObAIFuncClient &client : clients) {
      initialize(client, allocator, held, url);
      require(client.send_post_batch_no_wait(held.data) == OB_SUCCESS, "prepare scheduler shutdown");
      require(client.start_async() == OB_SUCCESS, "submit before scheduler shutdown");
    }
    ObAIFuncClient::stop_async_scheduler();
    ObAIFuncClient::stop_async_scheduler();
    for (ObAIFuncClient &client : clients) {
      bool finished = false;
      require(client.poll_batch(finished) == OB_CANCELED && finished, "shutdown must cancel in-flight work");
      client.reset();
    }
    require(allocator.outstanding() == 0, "shutdown retained caller request memory");
    initialize(clients[0], allocator, held, url);
    require(clients[0].send_post_batch_no_wait(held.data) == OB_SUCCESS, "prepare after shutdown");
    require(clients[0].start_async() == OB_CANCELED, "shutdown must reject new work");
  }
  resources().check_empty();
}

void test_global_pipeline_budget()
{
  const int64_t limit = ObAIFuncClient::pipeline_buffer_limit();
  int64_t first = 0;
  int64_t second = 0;
  require(limit > 1 && ObAIFuncClient::pipeline_buffer_usage() == 0, "initial global pipeline budget");
  require(ObAIFuncClient::reserve_pipeline_buffer(first, limit, limit - 1) == OB_SUCCESS,
    "first pipeline reserves global quota");
  require(ObAIFuncClient::reserve_pipeline_buffer(second, limit, 2) == OB_SIZE_OVERFLOW,
    "independent pipelines must share the global limit");
  require(second == 0 && first == limit - 1 && ObAIFuncClient::pipeline_buffer_usage() == limit - 1,
    "global admission failure must roll back only its own local charge");
  require(ObAIFuncClient::reserve_pipeline_buffer(second, 1, 2) == OB_SIZE_OVERFLOW,
    "local admission failure must not acquire global quota");
  ObAIFuncClient::release_pipeline_buffer(first, first);
  require(ObAIFuncClient::reserve_pipeline_buffer(second, limit, limit) == OB_SUCCESS,
    "released global quota must be reusable");
  ObAIFuncClient::release_pipeline_buffer(second, second);
  require(first == 0 && second == 0 && ObAIFuncClient::pipeline_buffer_usage() == 0,
    "global quota must return to zero after both pipelines finish");
}

void test_shared_pipeline_budget(const char *url, bool background = false,
                                int64_t buffer_limit = ObAIFuncClient::MAX_BATCH_BYTES)
{
  Inputs inputs(1);
  TrackingAllocator first_allocator;
  TrackingAllocator second_allocator;
  int64_t shared_bytes = 0;
  {
    ObAIFuncClient first;
    ObAIFuncClient second;
    first.set_shared_buffer_usage(shared_bytes, buffer_limit);
    second.set_shared_buffer_usage(shared_bytes, buffer_limit);
    initialize(first, first_allocator, inputs, url);
    initialize(second, second_allocator, inputs, url);
    require(first.send_post_batch_no_wait(inputs.data) == OB_SUCCESS, "submit first shared-budget batch");
    const int64_t first_bytes = shared_bytes;
    require(first_bytes > 0, "requests must charge shared quota");
    int64_t retained_bytes = buffer_limit - first_bytes - 1;
    shared_bytes += retained_bytes;
    require(second.send_post_batch_no_wait(inputs.data) == OB_SIZE_OVERFLOW,
            "request preparation must honor quota already retained by another batch");
    require(shared_bytes == retained_bytes + first_bytes, "failed preparation released another batch's quota");
    require(resources().attached.size() == 1, "failed admission affected the first batch or submitted another request");
    second.clean_up();
    shared_bytes -= retained_bytes;
    require(second.send_post_batch_no_wait(inputs.data) == OB_SUCCESS, "released quota must allow resubmission");
    require(resources().attached.size() == 2, "two batches must own independent active requests");
    if (buffer_limit > ObAIFuncClient::MAX_BATCH_BYTES) {
      shared_bytes += ObAIFuncClient::MAX_BATCH_BYTES;
      second.clean_up();
      require(second.send_post_batch_no_wait(inputs.data) == OB_SUCCESS,
              "larger shared budget must admit batches beyond the original 64MiB total");
      shared_bytes -= ObAIFuncClient::MAX_BATCH_BYTES;
    }
    retained_bytes = buffer_limit - shared_bytes;
    shared_bytes += retained_bytes;
    if (background) {
      require(first.start_async() == OB_SUCCESS, "schedule first shared-budget batch");
      require(second.start_async() == OB_SUCCESS, "schedule second shared-budget batch");
    }
    bool finished = false;
    int result = OB_SUCCESS;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (!finished && result == OB_SUCCESS) {
      require(std::chrono::steady_clock::now() < deadline, "shared response quota test exceeded deadline");
      result = first.poll_batch(finished, 20);
    }
    require(finished && result == OB_SIZE_OVERFLOW, "response bytes must honor the same shared quota");
    if (background) {
      finished = false;
      result = OB_SUCCESS;
      while (!finished && result == OB_SUCCESS) {
        require(std::chrono::steady_clock::now() < deadline, "second shared-budget batch exceeded deadline");
        result = second.poll_batch(finished, 20);
      }
      require(finished && result == OB_SIZE_OVERFLOW, "background peers must share the response quota");
    }
    first.reset();
    second.reset();
    require(shared_bytes == retained_bytes, "cancellation did not return exactly the request/response quota");
    shared_bytes -= retained_bytes;
  }
  require(shared_bytes == 0, "shared quota leaked after both batches were destroyed");
  require(first_allocator.outstanding() == 0 && second_allocator.outstanding() == 0,
          "shared-budget failure leaked client memory");
  resources().check_empty();
}

void test_recovery_and_result_lifetime(const char *url)
{
  Inputs inputs;
  for (int64_t iteration = 0; iteration < 16; ++iteration) {
    TrackingAllocator allocator;
    ObArray<ObJsonObject *> responses;
    {
      ObAIFuncClient client;
      initialize(client, allocator, inputs, url);
      allocator.fail_at(2);
      require(client.send_post_batch_no_wait(inputs.data) == OB_ALLOCATE_MEMORY_FAILED,
              "inject preparation failure before recovery");
      allocator.fail_at(0);
      require(client.send_post_batch(allocator, ObString::make_string(url), inputs.headers,
                                     inputs.data, responses) == OB_SUCCESS,
              "same client must recover and complete a real loopback request");
    }
    require(responses.count() == inputs.data.count(), "recovered batch lost results");
    for (int64_t index = 0; index < responses.count(); ++index) {
      require(responses.at(index) != nullptr && responses.at(index)->get_value("choices") != nullptr,
              "caller-owned results must survive client destruction");
    }
    resources().check_empty();
  }
}

class PipelineInput final : public ObOperator
{
public:
  PipelineInput(ObExecContext &context, const ObOpSpec &spec)
      : ObOperator(context, spec, nullptr), skip_storage_(0), pulls_(0), emit_rows_(false) {
    brs_.skip_ = to_bit_vector(&skip_storage_);
  }
  int inner_get_next_row() override { return OB_ITER_END; }
  void destroy() override { ObOperator::destroy(); }
  int inner_get_next_batch(int64_t) override
  {
    ++pulls_;
    brs_.size_ = emit_rows_ ? 2 : 0;
    brs_.skip_->reset(2);
    brs_.all_rows_active_ = true;
    brs_.end_ = true;
    return OB_SUCCESS;
  }
  uint64_t skip_storage_;
  int64_t pulls_;
  bool emit_rows_;
};

void test_pipeline_diagnosis_and_empty_rescan()
{
  for (int64_t mode = 0; mode < 3; ++mode) {
    ObArenaAllocator allocator;
    ObSQLSessionInfo session;
    require(session.load_sys_variable(allocator, ObString::make_string("ai_pipeline_slots"), ObIntType,
          ObString::make_string("2"), ObString::make_string("1"), ObString::make_string("1024"),
                oceanbase::share::ObSysVarFlag::SESSION_SCOPE, false) == OB_SUCCESS, "load lifecycle slot count");
    require(session.load_sys_variable(allocator, ObString::make_string("ai_pipeline_memory_limit"), ObIntType,
          ObString::make_string("67108864"), ObString::make_string("1048576"),
                ObString::make_string("1099511627776"), oceanbase::share::ObSysVarFlag::SESSION_SCOPE, false) == OB_SUCCESS,
        "load lifecycle memory budget");
    ObExecContext context(allocator);
    context.set_my_session(&session);
    require(context.create_physical_plan_ctx() == OB_SUCCESS, "create lifecycle plan context");
    ObPhysicalPlan plan;
    ObOpSpec input_spec(allocator, PHY_TABLE_SCAN);
    input_spec.plan_ = &plan;
    input_spec.max_batch_size_ = 16;
    PipelineInput input(context, input_spec);
    require(input.open() == OB_SUCCESS, "open lifecycle input");
    ObExpr expression;
    expression.type_ = mode == 0 ? T_FUN_SYS_AI_EMBED : T_FUN_SYS_AI_COMPLETE;
    AIFuncSpec spec(allocator, PHY_AI_FUNC);
    spec.plan_ = &plan;
    spec.max_batch_size_ = 16;
    spec.ai_expr_ = &expression;
    spec.solo_ = mode == 2;
    auto *pipeline = OB_NEWx(AIFuncOp, &allocator, context, spec, nullptr);
    require(pipeline != nullptr, "allocate lifecycle operator");
    ObOperator *children[] = {&input};
    require(pipeline->set_children_pointer(children, 1) == OB_SUCCESS, "attach lifecycle input");
    uint64_t skip_storage = 0;
    pipeline->get_brs().skip_ = to_bit_vector(&skip_storage);
    require(pipeline->inner_open() == OB_SUCCESS, "open lifecycle operator");
    for (int64_t iteration = 0; iteration < 3; ++iteration) {
      require(pipeline->inner_get_next_batch(16) == OB_SUCCESS && pipeline->get_brs().end_,
              "empty input must reach end");
      require(input.pulls_ == iteration + 1, "rescan must pull the child again instead of retaining end-of-input");
      require(pipeline->rescan() == OB_SUCCESS && !pipeline->get_brs().end_, "rescan must reset end state");
    }
    session.set_diagnosis_enabled(true);
    input.emit_rows_ = true;
    require(pipeline->inner_get_next_batch(16) == OB_SUCCESS && pipeline->get_brs().size_ == 2,
            "diagnosis mode must pass child rows without preparing an AI request");
    require(pipeline->rescan() == OB_SUCCESS, "rescan after diagnosis rows");
    require(pipeline->inner_get_next_batch(16) == OB_SUCCESS && pipeline->get_brs().size_ == 2,
            "diagnosis rows must remain available after rescan");
    require(pipeline->inner_close() == OB_SUCCESS, "close lifecycle operator");
    pipeline->destroy();
    resources().check_empty();
  }
}
}

extern "C"
{
CURL *__real_curl_easy_init();
void __real_curl_easy_cleanup(CURL *handle);
CURLM *__real_curl_multi_init();
CURLMcode __real_curl_multi_cleanup(CURLM *handle);
CURLMcode __real_curl_multi_add_handle(CURLM *multi, CURL *easy);
CURLMcode __real_curl_multi_remove_handle(CURLM *multi, CURL *easy);
CURLMcode __real_curl_multi_perform(CURLM *multi, int *running);
CURLMcode __real_curl_multi_poll(CURLM *multi, curl_waitfd *extra, unsigned int count, int timeout, int *ready);
curl_slist *__real_curl_slist_append(curl_slist *list, const char *value);
void __real_curl_slist_free_all(curl_slist *list);

CURL *__wrap_curl_easy_init()
{
  std::lock_guard<std::recursive_mutex> guard(resources().mutex);
  if (resources().creating_multi) {
    return __real_curl_easy_init();
  }
  CURL *handle = resources().fail(Fault::EASY) ? nullptr : __real_curl_easy_init();
  if (handle != nullptr) {
    require(resources().easy.insert(handle).second, "duplicate curl easy allocation");
  }
  return handle;
}

void __wrap_curl_easy_cleanup(CURL *handle)
{
  std::lock_guard<std::recursive_mutex> guard(resources().mutex);
  if (handle == nullptr || resources().easy.count(handle) == 0) {
    __real_curl_easy_cleanup(handle);
    return;
  }
  require(resources().attached.count(handle) == 0, "easy cleanup before multi removal");
  if (resources().omit_cleanup) {
    resources().omit_cleanup = false;
    return;
  }
  require(resources().easy.erase(handle) == 1, "duplicate curl easy cleanup");
  __real_curl_easy_cleanup(handle);
}

CURLM *__wrap_curl_multi_init()
{
  std::lock_guard<std::recursive_mutex> guard(resources().mutex);
  resources().creating_multi = true;
  CURLM *handle = resources().fail(Fault::MULTI) ? nullptr : __real_curl_multi_init();
  resources().creating_multi = false;
  if (handle != nullptr) {
    require(resources().multi.insert(handle).second, "duplicate curl multi allocation");
  }
  return handle;
}

CURLMcode __wrap_curl_multi_cleanup(CURLM *handle)
{
  std::lock_guard<std::recursive_mutex> guard(resources().mutex);
  for (const auto &entry : resources().attached) {
    require(entry.second != handle, "multi cleanup before request removal");
  }
  require(resources().multi.erase(handle) == 1, "duplicate curl multi cleanup");
  return __real_curl_multi_cleanup(handle);
}

CURLMcode __wrap_curl_multi_add_handle(CURLM *multi, CURL *easy)
{
  std::lock_guard<std::recursive_mutex> guard(resources().mutex);
  const CURLMcode result = resources().fail(Fault::ADD) ? CURLM_OUT_OF_MEMORY
                                                      : __real_curl_multi_add_handle(multi, easy);
  if (result == CURLM_OK) {
    require(resources().attached.emplace(easy, multi).second, "duplicate request attachment");
  }
  return result;
}

CURLMcode __wrap_curl_multi_remove_handle(CURLM *multi, CURL *easy)
{
  std::lock_guard<std::recursive_mutex> guard(resources().mutex);
  const CURLMcode result = __real_curl_multi_remove_handle(multi, easy);
  if (result == CURLM_OK) {
    require(resources().attached.erase(easy) == 1, "duplicate request removal");
    {
      std::lock_guard<std::mutex> guard(completion_mutex);
      ++completed_transfers;
    }
    completion_changed.notify_all();
  }
  return result;
}

CURLMcode __wrap_curl_multi_perform(CURLM *multi, int *running)
{
  std::lock_guard<std::recursive_mutex> guard(resources().mutex);
  return resources().fail(Fault::PERFORM) ? CURLM_INTERNAL_ERROR : __real_curl_multi_perform(multi, running);
}

CURLMcode __wrap_curl_multi_poll(CURLM *multi, curl_waitfd *extra, unsigned int count, int timeout, int *ready)
{
  std::lock_guard<std::recursive_mutex> guard(resources().mutex);
  return resources().fail(Fault::POLL) ? CURLM_INTERNAL_ERROR
                                      : __real_curl_multi_poll(multi, extra, count, timeout, ready);
}

curl_slist *__wrap_curl_slist_append(curl_slist *list, const char *value)
{
  std::lock_guard<std::recursive_mutex> guard(resources().mutex);
  curl_slist *result = resources().fail(Fault::HEADER) ? nullptr : __real_curl_slist_append(list, value);
  for (curl_slist *node = result; node != nullptr; node = node->next) {
    resources().headers.insert(node);
  }
  return result;
}

void __wrap_curl_slist_free_all(curl_slist *list)
{
  std::lock_guard<std::recursive_mutex> guard(resources().mutex);
  for (curl_slist *node = list; node != nullptr; node = node->next) {
    resources().headers.erase(node);
  }
  __real_curl_slist_free_all(list);
}
}

int main(int argc, char **argv)
{
  try {
    require(argc >= 2, "a loopback mock URL is required");
    require(curl_global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK, "initialize curl globally");
    const char *url = argv[1];
    if (argc == 3 && std::strcmp(argv[2], "--negative-control=allocation") == 0) {
      omit_allocation_release = true;
      test_initialization_allocation_failures(2);
      require(false, "allocation negative control unexpectedly passed");
    }
    if (argc == 3 && std::strcmp(argv[2], "--negative-control=curl") == 0) {
      resources().omit_cleanup = true;
      test_curl_failure(Fault::EASY, 2, OB_ALLOCATE_MEMORY_FAILED, url);
      require(false, "negative control unexpectedly passed");
    }
    if (argc == 3 && std::strcmp(argv[2], "--negative-control=progress") == 0) {
      test_background_progress(url, false);
      require(false, "progress negative control unexpectedly passed");
    }
    test_initialization_allocation_failures();
    std::puts("PASS initialization allocation failures and repeated cleanup");
    test_preparation_allocation_failures(url);
    test_curl_failure(Fault::HEADER, 1, OB_ALLOCATE_MEMORY_FAILED, url);
    test_curl_failure(Fault::HEADER, 2, OB_ALLOCATE_MEMORY_FAILED, url);
    test_curl_failure(Fault::MULTI, 1, OB_ALLOCATE_MEMORY_FAILED, url);
    test_curl_failure(Fault::EASY, 1, OB_ALLOCATE_MEMORY_FAILED, url);
    test_curl_failure(Fault::EASY, 2, OB_ALLOCATE_MEMORY_FAILED, url);
    test_curl_failure(Fault::ADD, 2, OB_CURL_ERROR, url);
    test_curl_failure(Fault::PERFORM, 1, OB_CURL_ERROR, url);
    test_curl_failure(Fault::POLL, 1, OB_CURL_ERROR, url);
    std::puts("PASS eight curl initialization/attachment/driver faults");
    test_response_allocation_failure(url);
    std::puts("PASS response allocation failure and peer cleanup");
    test_pending_lifecycle(url);
    std::puts("PASS 32 no_wait cancellations, queued work, reinitialization and pending destruction");
    test_request_await_protocol();
    test_resumed_processor_exception();
    std::puts("PASS opt-in request wait ownership, readiness and nested-scope isolation");
    test_background_progress(url);
    std::puts("PASS network completion without SQL-thread polling");
    test_background_cancellation(url);
    std::puts("PASS 16 background cancel/reuse races and duplicate scheduling rejection");
    test_background_deadline_and_peer_progress(url);
    std::puts("PASS independent background peer progress and autonomous deadline");
    test_background_admission(url);
    test_nonblocking_admission(url);
    std::puts("PASS nonblocking network admission, duplicate rejection, cancellation and slot recovery");
    std::puts("PASS bounded background admission, deadline and slot recovery");
    test_shared_pipeline_budget(url);
    test_global_pipeline_budget();
    std::puts("PASS cross-query pipeline memory limit, rollback and exact recovery");
    std::puts("PASS shared pipeline admission/response quota and exact rollback");
    test_shared_pipeline_budget(url, true);
    test_shared_pipeline_budget(url, false, 1024 * 1024);
    test_shared_pipeline_budget(url, true, 128 * 1024 * 1024);
    std::puts("PASS background batches share admission/response quota and exact rollback");
    test_recovery_and_result_lifetime(url);
    std::puts("PASS 16 failure/recovery cycles and caller-owned result lifetime");
    test_pipeline_diagnosis_and_empty_rescan();
    std::puts("PASS both AI functions and global SOLO: diagnosis passthrough and empty-input physical rescan");
    test_background_shutdown(url);
    std::puts("PASS scheduler shutdown cancels in-flight work and rejects new submissions");
    resources().check_empty();
    curl_global_cleanup();
    return 0;
  } catch (const std::exception &error) {
    std::fprintf(stderr, "FAIL %s\n", error.what());
    return 1;
  }
}