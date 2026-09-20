#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

using namespace oceanbase::common;

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
    fault = operation;
    fail_at = attempt;
    attempts = 0;
    injected = false;
  }

  void check_empty() const
  {
    require(attached.empty(), "curl requests remained attached");
    require(easy.empty(), "curl easy handles were not released");
    require(multi.empty(), "curl multi handles were not released");
    require(headers.empty(), "curl header nodes were not released");
  }

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
    if (pointer != nullptr) {
      if (omit_allocation_release) {
        omit_allocation_release = false;
        return;
      }
      require(allocations_.erase(pointer) == 1, "unknown or duplicate allocation release");
      std::free(pointer);
    }
  }

  void fail_at(int64_t attempt) { attempts_ = 0; fail_at_ = attempt; injected_ = false; }
  bool injected() const { return injected_; }
  size_t outstanding() const { return allocations_.size(); }

private:
  std::unordered_map<void *, int64_t> allocations_;
  int64_t attempts_ = 0;
  int64_t fail_at_ = 0;
  bool injected_ = false;
};

struct Inputs
{
  explicit Inputs(int64_t count = 3)
  {
    require(headers.push_back(ObString::make_string("Content-Type: application/json")) == OB_SUCCESS,
            "prepare content header");
    require(headers.push_back(ObString::make_string("Authorization: Bearer test-only")) == OB_SUCCESS,
            "prepare authorization header");
    ObIJsonBase *json = nullptr;
    require(ObJsonBaseFactory::get_json_base(&arena,
                ObString::make_string("{\"messages\":[{\"content\":\"native-resource\"}]}"),
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
  {
    ObAIFuncClient client;
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
  for (const auto &entry : resources().attached) {
    require(entry.second != handle, "multi cleanup before request removal");
  }
  require(resources().multi.erase(handle) == 1, "duplicate curl multi cleanup");
  return __real_curl_multi_cleanup(handle);
}

CURLMcode __wrap_curl_multi_add_handle(CURLM *multi, CURL *easy)
{
  const CURLMcode result = resources().fail(Fault::ADD) ? CURLM_OUT_OF_MEMORY
                                                      : __real_curl_multi_add_handle(multi, easy);
  if (result == CURLM_OK) {
    require(resources().attached.emplace(easy, multi).second, "duplicate request attachment");
  }
  return result;
}

CURLMcode __wrap_curl_multi_remove_handle(CURLM *multi, CURL *easy)
{
  const CURLMcode result = __real_curl_multi_remove_handle(multi, easy);
  if (result == CURLM_OK) {
    require(resources().attached.erase(easy) == 1, "duplicate request removal");
  }
  return result;
}

CURLMcode __wrap_curl_multi_perform(CURLM *multi, int *running)
{
  return resources().fail(Fault::PERFORM) ? CURLM_INTERNAL_ERROR : __real_curl_multi_perform(multi, running);
}

CURLMcode __wrap_curl_multi_poll(CURLM *multi, curl_waitfd *extra, unsigned int count, int timeout, int *ready)
{
  return resources().fail(Fault::POLL) ? CURLM_INTERNAL_ERROR
                                      : __real_curl_multi_poll(multi, extra, count, timeout, ready);
}

curl_slist *__wrap_curl_slist_append(curl_slist *list, const char *value)
{
  curl_slist *result = resources().fail(Fault::HEADER) ? nullptr : __real_curl_slist_append(list, value);
  for (curl_slist *node = result; node != nullptr; node = node->next) {
    resources().headers.insert(node);
  }
  return result;
}

void __wrap_curl_slist_free_all(curl_slist *list)
{
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
    test_recovery_and_result_lifetime(url);
    std::puts("PASS 16 failure/recovery cycles and caller-owned result lifetime");
    resources().check_empty();
    curl_global_cleanup();
    return 0;
  } catch (const std::exception &error) {
    std::fprintf(stderr, "FAIL %s\n", error.what());
    return 1;
  }
}