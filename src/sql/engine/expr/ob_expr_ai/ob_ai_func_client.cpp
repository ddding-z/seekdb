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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_ai_func_client.h"
#include "sql/session/ob_sql_session_info.h"
#include <algorithm>
#include <limits>
#include <cstdlib>

namespace oceanbase
{
namespace common
{

const int64_t ObAIFuncClient::CURL_MAX_TIMEOUT_SEC = INT_MAX / 1000;

struct ObAIFuncClient::Request
{
  Request(ObAIFuncClient &owner, ObIAllocator &allocator, int64_t index)
    : owner_(owner), index_(index), body_(&allocator), response_(&allocator),
      handle_(nullptr), result_(nullptr), attempts_(0), retry_at_(0),
      retry_after_us_(0), callback_ret_(OB_SUCCESS), active_(false), done_(false),
      queued_at_(ObTimeUtility::current_time()), started_at_(0) {}
  TO_STRING_KV(K_(index), K_(attempts), K_(active), K_(done));
  ObAIFuncClient &owner_;
  int64_t index_;
  ObJsonBuffer body_;
  ObStringBuffer response_;
  CURL *handle_;
  ObJsonObject *result_;
  int64_t attempts_;
  int64_t retry_at_;
  int64_t retry_after_us_;
  int callback_ret_;
  bool active_;
  bool done_;
  int64_t queued_at_;
  int64_t started_at_;
};

ObAIFuncClient::ObAIFuncClient()
  : allocator_(nullptr), url_(nullptr), header_list_(nullptr), curlm_(nullptr),
    requests_(), is_finished_(false), max_retry_times_(3), abs_timeout_ts_(0),
    timeout_sec_(60), max_parallel_(50), active_count_(0), completed_count_(0),
    batch_ret_(OB_SUCCESS), status_checker_(nullptr), status_context_(nullptr),
    batch_start_ts_(0), attempts_(0), retries_(0), peak_active_(0),
    buffered_bytes_(0), received_bytes_(0), submitted_bytes_(0)
{}

ObAIFuncClient::~ObAIFuncClient()
{
  reset();
  if (nullptr != curlm_) {
    curl_multi_cleanup(curlm_);
  }
}

void ObAIFuncClient::clean_up()
{
  for (int64_t index = 0; index < requests_.count(); ++index) {
    Request *request = requests_.at(index);
    if (nullptr != request->handle_) {
      if (request->active_) {
        curl_multi_remove_handle(curlm_, request->handle_);
      }
      curl_easy_cleanup(request->handle_);
    }
    OB_DELETEx(Request, allocator_, request);
  }
  requests_.reset();
  active_count_ = 0;
  completed_count_ = 0;
  buffered_bytes_ = 0;
  is_finished_.store(false);
}

void ObAIFuncClient::reset()
{
  clean_up();
  if (nullptr != header_list_) {
    curl_slist_free_all(header_list_);
    header_list_ = nullptr;
  }
  if (nullptr != url_ && nullptr != allocator_) {
    allocator_->free(url_);
  }
  url_ = nullptr;
  allocator_ = nullptr;
  batch_ret_ = OB_SUCCESS;
}

int ObAIFuncClient::init(ObIAllocator &allocator, const ObString &url, ObArray<ObString> &headers)
{
  int ret = OB_SUCCESS;
  reset();
  if (url.empty() || headers.empty() || max_parallel_ < 1 || max_parallel_ > 64 || timeout_sec_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    allocator_ = &allocator;
    const int64_t configured_us = std::min(timeout_sec_, CURL_MAX_TIMEOUT_SEC) * 1000000;
    const int64_t remaining_us = THIS_WORKER.is_timeout_ts_valid()
        ? THIS_WORKER.get_timeout_remain() : configured_us;
    abs_timeout_ts_ = ObTimeUtility::current_time() + remaining_us;
    ObString owned_url;
    if (OB_FAIL(check_status())) {
    } else if (OB_FAIL(ob_write_string(allocator, url, owned_url, true))) {
    } else {
      url_ = owned_url.ptr();
      for (int64_t index = 0; OB_SUCC(ret) && index < headers.count(); ++index) {
        ObString header;
        if (OB_FAIL(ob_write_string(allocator, headers.at(index), header, true))) {
        } else {
          curl_slist *new_list = curl_slist_append(header_list_, header.ptr());
          if (nullptr == new_list) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            header_list_ = new_list;
          }
          allocator.free(header.ptr());
        }
      }
    }
  }
  return ret;
}

int ObAIFuncClient::check_status()
{
  int ret = OB_SUCCESS;
  if (ObTimeUtility::current_time() >= abs_timeout_ts_) {
    ret = OB_TIMEOUT;
  } else if (nullptr != status_checker_) {
    ret = status_checker_(status_context_);
  } else if (nullptr != THIS_WORKER.get_session()) {
    THIS_WORKER.get_session()->is_terminate(ret);
  }
  return ret;
}

int ObAIFuncClient::error_handle(CURLcode result)
{
  int ret = result == CURLE_URL_MALFORMAT ? OB_INVALID_ARGUMENT : OB_CURL_ERROR;
  LOG_WARN("AI HTTP transport failed", K(ret), K(result));
  return ret;
}

int ObAIFuncClient::send_post(ObIAllocator &allocator, const ObString &url,
                            ObArray<ObString> &headers, ObJsonObject *data, ObJsonObject *&response)
{
  int ret = OB_SUCCESS;
  response = nullptr;
  if (OB_FAIL(init(allocator, url, headers))) {
  } else {
    ret = send_post(data, response);
  }
  return ret;
}

int ObAIFuncClient::send_post(ObJsonObject *data, ObJsonObject *&response)
{
  int ret = OB_SUCCESS;
  ObArray<ObJsonObject *> inputs;
  ObArray<ObJsonObject *> outputs;
  if (OB_FAIL(inputs.push_back(data))) {
  } else if (OB_FAIL(send_post_batch(inputs, outputs))) {
  } else {
    response = outputs.at(0);
  }
  return ret;
}

int ObAIFuncClient::send_post_batch(ObIAllocator &allocator, const ObString &url,
                                  ObArray<ObString> &headers, ObArray<ObJsonObject *> &data_array,
                                  ObArray<ObJsonObject *> &responses)
{
  int ret = OB_SUCCESS;
  responses.reset();
  if (OB_FAIL(init(allocator, url, headers))) {
  } else {
    ret = send_post_batch(data_array, responses);
  }
  return ret;
}

int ObAIFuncClient::send_post_batch(ObArray<ObJsonObject *> &data_array, ObArray<ObJsonObject *> &responses)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(send_post_batch_no_wait(data_array))) {
  } else {
    while (!check_batch_finished()) {
      int numfds = 0;
      if (CURLM_OK != curl_multi_poll(curlm_, nullptr, 0, 20, &numfds)) {
        batch_ret_ = OB_CURL_ERROR;
        break;
      }
    }
    ret = get_batch_result(responses);
  }
  const int64_t wall_us = ObTimeUtility::current_time() - batch_start_ts_;
  const int64_t unfinished = data_array.count() - completed_count_;
  LOG_TRACE("AI HTTP batch execution", K(ret), "requests", data_array.count(),
            K(attempts_), K(retries_), K(peak_active_), K(unfinished),
            K(submitted_bytes_), K(received_bytes_), K(wall_us));
  clean_up();
  return ret;
}

int ObAIFuncClient::send_post_batch_no_wait(ObArray<ObJsonObject *> &data_array)
{
  int ret = OB_SUCCESS;
  clean_up();
  batch_ret_ = OB_SUCCESS;
  attempts_ = retries_ = peak_active_ = received_bytes_ = submitted_bytes_ = 0;
  batch_start_ts_ = ObTimeUtility::current_time();
  if (data_array.empty() || nullptr == allocator_ || nullptr == url_ || nullptr == header_list_) {
    ret = OB_INVALID_ARGUMENT;
  } else if (data_array.count() > MAX_BATCH_BYTES / static_cast<int64_t>(sizeof(Request))) {
    ret = OB_SIZE_OVERFLOW;
  } else if (nullptr == curlm_ && nullptr == (curlm_ = curl_multi_init())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    buffered_bytes_ = data_array.count() * static_cast<int64_t>(sizeof(Request));
  }
  for (int64_t index = 0; OB_SUCC(ret) && index < data_array.count(); ++index) {
    Request *request = nullptr;
    if (OB_FAIL(check_status())) {
    } else if (nullptr == data_array.at(index)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (nullptr == (request = OB_NEWx(Request, allocator_, *this, *allocator_, index))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(requests_.push_back(request))) {
      OB_DELETEx(Request, allocator_, request);
    } else if (OB_FAIL(data_array.at(index)->print(request->body_, false))) {
    } else if (request->body_.length() > MAX_REQUEST_BYTES ||
               buffered_bytes_ + request->body_.length() > MAX_BATCH_BYTES) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      buffered_bytes_ += request->body_.length();
    }
  }
  if (OB_SUCC(ret)) {
    ret = advance_batch();
  }
  if (OB_FAIL(ret)) {
    clean_up();
    batch_ret_ = ret;
    is_finished_.store(true);
  }
  return ret;
}

int ObAIFuncClient::start_request(Request &request)
{
  int ret = OB_SUCCESS;
  buffered_bytes_ -= request.response_.length();
  request.response_.reset();
  request.callback_ret_ = OB_SUCCESS;
  request.retry_after_us_ = 0;
  if (OB_FAIL(check_status())) {
  } else if (nullptr == request.handle_ && nullptr == (request.handle_ = curl_easy_init())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    const long remaining_ms = std::max<int64_t>(1,
        std::min<int64_t>(INT_MAX, (abs_timeout_ts_ - ObTimeUtility::current_time()) / 1000));
    CURL *handle = request.handle_;
    if (CURLE_OK != curl_easy_setopt(handle, CURLOPT_URL, url_) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_HTTPHEADER, header_list_) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_POST, 1L) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_POSTFIELDSIZE, static_cast<long>(request.body_.length())) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_POSTFIELDS, request.body_.ptr()) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1L) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_TCP_NODELAY, 1L) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_CONNECTTIMEOUT_MS, std::min(10000L, remaining_ms)) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_TIMEOUT_MS, remaining_ms) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, write_callback) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_WRITEDATA, &request) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_HEADERFUNCTION, header_callback) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_HEADERDATA, &request) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_NOPROGRESS, 0L) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_XFERINFOFUNCTION, progress_callback) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_XFERINFODATA, &request) ||
        CURLE_OK != curl_easy_setopt(handle, CURLOPT_PRIVATE, &request) ||
        CURLM_OK != curl_multi_add_handle(curlm_, handle)) {
      ret = OB_CURL_ERROR;
    } else {
      request.active_ = true;
      request.started_at_ = ObTimeUtility::current_time();
      ++request.attempts_;
      ++attempts_;
      ++active_count_;
      peak_active_ = std::max(peak_active_, active_count_);
      submitted_bytes_ += request.body_.length();
    }
  }
  return ret;
}

int ObAIFuncClient::finish_request(Request &request, CURLcode result)
{
  int ret = OB_SUCCESS;
  long http_code = 0;
  curl_off_t connect_us = 0;
  curl_off_t first_byte_us = 0;
  curl_off_t transfer_us = 0;
  curl_easy_getinfo(request.handle_, CURLINFO_CONNECT_TIME_T, &connect_us);
  curl_easy_getinfo(request.handle_, CURLINFO_STARTTRANSFER_TIME_T, &first_byte_us);
  curl_easy_getinfo(request.handle_, CURLINFO_TOTAL_TIME_T, &transfer_us);
  const int64_t queue_us = request.started_at_ - request.queued_at_;
  const int64_t parse_start = ObTimeUtility::current_time();
  int64_t total_tokens = -1;
  curl_multi_remove_handle(curlm_, request.handle_);
  request.active_ = false;
  --active_count_;
  if (OB_FAIL(check_status())) {
  } else if (OB_SUCCESS != request.callback_ret_) {
    ret = request.callback_ret_;
  } else if (CURLE_OK != curl_easy_getinfo(request.handle_, CURLINFO_RESPONSE_CODE, &http_code)) {
    ret = OB_CURL_ERROR;
  } else if (request.attempts_ <= max_retry_times_ &&
             ((result == CURLE_OK && is_retryable_status_code(http_code)) ||
              result == CURLE_COULDNT_CONNECT || result == CURLE_COULDNT_RESOLVE_HOST)) {
    const int64_t delay_us = std::max<int64_t>(request.retry_after_us_,
        (1000000LL << (request.attempts_ - 1)) + static_cast<int64_t>(rand() % 1000000));
    request.retry_at_ = ObTimeUtility::current_time() + delay_us;
    request.queued_at_ = ObTimeUtility::current_time();
    ++retries_;
  } else if (result != CURLE_OK) {
    ret = result == CURLE_OPERATION_TIMEDOUT ? OB_TIMEOUT : error_handle(result);
  } else if (http_code / 100 != 2) {
    ret = OB_CURL_ERROR;
    LOG_WARN("AI HTTP request failed", K(ret), K(http_code), K(request.index_));
  } else {
    ObIJsonBase *json = nullptr;
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator_, request.response_.string(),
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, json))) {
    } else if (nullptr == json || json->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_INVALID_JSON_TEXT;
    } else {
      request.result_ = static_cast<ObJsonObject *>(json);
      request.done_ = true;
      ++completed_count_;
      ObIJsonBase *usage = request.result_->get_value("usage");
      if (nullptr != usage && usage->json_type() == ObJsonNodeType::J_OBJECT) {
        ObIJsonBase *tokens = static_cast<ObJsonObject *>(usage)->get_value("total_tokens");
        if (nullptr != tokens && tokens->json_type() == ObJsonNodeType::J_INT) {
          total_tokens = tokens->get_int();
        }
      }
    }
  }
  const int64_t parse_us = ObTimeUtility::current_time() - parse_start;
  LOG_TRACE("AI HTTP request attempt", K(ret), K(request.index_), K(request.attempts_),
            K(result), K(http_code), K(queue_us), K(connect_us), K(first_byte_us),
            K(transfer_us), K(parse_us), K(total_tokens));
  if (request.done_ || OB_FAIL(ret)) {
    curl_easy_cleanup(request.handle_);
    request.handle_ = nullptr;
  }
  return ret;
}

int ObAIFuncClient::advance_batch()
{
  int ret = check_status();
  int running = 0;
  if (nullptr == curlm_) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCC(ret) && CURLM_OK != curl_multi_perform(curlm_, &running)) {
    ret = OB_CURL_ERROR;
  }
  CURLMsg *message = nullptr;
  int messages_left = 0;
  while (OB_SUCC(ret) && nullptr != (message = curl_multi_info_read(curlm_, &messages_left))) {
    if (message->msg == CURLMSG_DONE) {
      Request *request = nullptr;
      if (CURLE_OK != curl_easy_getinfo(message->easy_handle, CURLINFO_PRIVATE, &request) || nullptr == request) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ret = finish_request(*request, message->data.result);
      }
    }
  }
  for (int64_t index = 0; OB_SUCC(ret) && active_count_ < max_parallel_ && index < requests_.count(); ++index) {
    Request &request = *requests_.at(index);
    if (!request.active_ && !request.done_ && request.retry_at_ <= ObTimeUtility::current_time()) {
      ret = start_request(request);
    }
  }
  return ret;
}

bool ObAIFuncClient::check_batch_finished()
{
  if (!is_finished_.load()) {
    if (OB_SUCCESS == batch_ret_) {
      batch_ret_ = advance_batch();
    }
    if (OB_SUCCESS != batch_ret_ || completed_count_ == requests_.count()) {
      if (OB_SUCCESS != batch_ret_) {
        for (int64_t index = 0; index < requests_.count(); ++index) {
          Request &request = *requests_.at(index);
          if (request.active_) {
            curl_multi_remove_handle(curlm_, request.handle_);
            curl_easy_cleanup(request.handle_);
            request.handle_ = nullptr;
            request.active_ = false;
          }
        }
        active_count_ = 0;
      }
      is_finished_.store(true);
    }
  }
  return is_finished_.load();
}

int ObAIFuncClient::get_batch_result(ObArray<ObJsonObject *> &responses)
{
  responses.reset();
  int ret = batch_ret_;
  if (OB_SUCC(ret) && !is_finished_.load()) {
    ret = OB_EAGAIN;
  }
  for (int64_t index = 0; OB_SUCC(ret) && index < requests_.count(); ++index) {
    if (OB_FAIL(responses.push_back(requests_.at(index)->result_))) {
      responses.reset();
    }
  }
  return ret;
}

size_t ObAIFuncClient::write_callback(void *contents, size_t size, size_t nmemb, void *userp)
{
  Request &request = *static_cast<Request *>(userp);
  if (size != 0 && nmemb > std::numeric_limits<size_t>::max() / size) {
    request.callback_ret_ = OB_SIZE_OVERFLOW;
    return 0;
  }
  const size_t bytes = size * nmemb;
  if (bytes > static_cast<size_t>(MAX_RESPONSE_BYTES - request.response_.length()) ||
      bytes > static_cast<size_t>(MAX_BATCH_BYTES - request.owner_.buffered_bytes_)) {
    request.callback_ret_ = OB_SIZE_OVERFLOW;
    return 0;
  }
  request.callback_ret_ = request.response_.append(static_cast<const char *>(contents), bytes, 0);
  if (OB_SUCCESS != request.callback_ret_) {
    return 0;
  }
  request.owner_.buffered_bytes_ += bytes;
  request.owner_.received_bytes_ += bytes;
  return bytes;
}

size_t ObAIFuncClient::header_callback(char *contents, size_t size, size_t nmemb, void *userp)
{
  Request &request = *static_cast<Request *>(userp);
  if (size != 0 && nmemb > std::numeric_limits<size_t>::max() / size) {
    request.callback_ret_ = OB_SIZE_OVERFLOW;
    return 0;
  }
  const size_t bytes = size * nmemb;
  const char prefix[] = "Retry-After:";
  if (bytes >= 5 && curl_strnequal(contents, "HTTP/", 5)) {
    request.retry_after_us_ = 0;
  } else if (bytes > sizeof(prefix) - 1 && bytes < 128 &&
             curl_strnequal(contents, prefix, sizeof(prefix) - 1)) {
    char value[128] = {};
    MEMCPY(value, contents + sizeof(prefix) - 1, bytes - (sizeof(prefix) - 1));
    char *end = nullptr;
    const long seconds = strtol(value, &end, 10);
    const bool has_digits = end != value;
    while (*end == ' ' || *end == '\t' || *end == '\r' || *end == '\n') {
      ++end;
    }
    int64_t delay_sec = 0;
    if (has_digits && *end == '\0' && seconds >= 0) {
      delay_sec = std::min<long>(seconds, CURL_MAX_TIMEOUT_SEC);
    } else {
      const time_t date = curl_getdate(value, nullptr);
      if (date >= 0) {
        delay_sec = std::max<int64_t>(0, date - time(nullptr));
      }
    }
    request.retry_after_us_ = std::min(delay_sec, CURL_MAX_TIMEOUT_SEC) * 1000000;
  }
  return bytes;
}

int ObAIFuncClient::progress_callback(void *userp, curl_off_t, curl_off_t, curl_off_t, curl_off_t)
{
  Request &request = *static_cast<Request *>(userp);
  const int ret = request.owner_.check_status();
  if (OB_SUCCESS != ret) {
    request.callback_ret_ = ret;
  }
  return OB_SUCCESS == ret ? 0 : 1;
}

bool ObAIFuncClient::is_retryable_status_code(int64_t http_code)
{
  return http_code == 429 || http_code == 500 || http_code == 502 || http_code == 503 || http_code == 504;
}

} // namespace common
} // namespace oceanbase