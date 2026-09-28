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

#ifndef OB_AI_FUNC_CLIENT_H_
#define OB_AI_FUNC_CLIENT_H_

#include <atomic>
#include <curl/curl.h>
#include "ob_ai_func.h"

namespace oceanbase 
{
namespace sql { class ObSQLSessionInfo; }
namespace common 
{
class AIFuncScheduler;
class ObAIFuncClient: public ObAIFuncHandle
{
public:
  ObAIFuncClient();
  virtual ~ObAIFuncClient();
  int init(common::ObIAllocator &allocator, const ObString &url, ObArray<ObString> &headers);
  void clean_up();
  void reset();
  void set_timeout_sec(int64_t timeout_sec) { timeout_sec_ = timeout_sec; }
  void set_max_parallel(int64_t max_parallel) { max_parallel_ = max_parallel; }
  void set_shared_buffer_usage(int64_t &bytes) { shared_buffered_bytes_ = &bytes; }
  void set_response_validator(ObAIFuncBase *validator) { response_validator_ = validator; }
  void set_status_checker(int (*checker)(void *), void *context)
  {
    status_checker_ = checker;
    status_context_ = context;
  }
  static constexpr int64_t MAX_REQUEST_BYTES = 4 * 1024 * 1024;
  static constexpr int64_t MAX_RESPONSE_BYTES = 8 * 1024 * 1024;
  static constexpr int64_t MAX_BATCH_BYTES = 64 * 1024 * 1024;
  // ai function interface
  virtual int send_post(common::ObIAllocator &allocator, 
                        const ObString &url,
                        ObArray<ObString> &headers, 
                        ObJsonObject *data,
                        ObJsonObject *&response) override;
  virtual int send_post_batch(common::ObIAllocator &allocator,
                              const ObString &url, 
                              ObArray<ObString> &headers,
                              ObArray<ObJsonObject *> &data_array,
                              ObArray<ObJsonObject *> &responses) override;
  // embedding service interface
  int send_post_batch_no_wait(ObArray<ObJsonObject *> &data_array);
  int start_async();
  static void stop_async_scheduler();
  bool check_batch_finished();
  int poll_batch(bool &finished, int64_t wait_ms = 0);
  int get_batch_result(ObArray<ObJsonObject *> &responses);
private:
  friend class AIFuncScheduler;
  struct Request;
  bool drive_batch();
  void cancel_async();
  int error_handle(CURLcode res);
  int send_post(ObJsonObject *data, ObJsonObject *&response);
  int send_post_batch(ObArray<ObJsonObject *> &data_array, ObArray<ObJsonObject *> &responses);
  int start_request(Request &request);
  int advance_batch();
  int check_status();
  int reserve_buffer(int64_t bytes);
  void release_buffer(int64_t bytes);
  int finish_request(Request &request, CURLcode result);
  static size_t write_callback(void *contents, size_t size, size_t nmemb, void *userp);
  static size_t header_callback(char *contents, size_t size, size_t nmemb, void *userp);
  static int progress_callback(void *userp, curl_off_t, curl_off_t, curl_off_t, curl_off_t);
  bool is_retryable_status_code(int64_t http_code);
private:
  static const int64_t CURL_MAX_TIMEOUT_SEC;
  common::ObIAllocator *allocator_;
  char *url_;
  struct curl_slist *header_list_;
  CURLM *curlm_;
  ObArray<Request *> requests_;
  // atomic boolean value, used to check if the batch task is finished
  std::atomic<bool> is_finished_;
  bool async_mode_;
  std::atomic<bool> async_done_;
  std::atomic<int> cancel_ret_;
  sql::ObSQLSessionInfo *request_session_;
  int64_t max_retry_times_;
  int64_t abs_timeout_ts_;
  int64_t timeout_sec_;
  int64_t max_parallel_;
  int64_t active_count_;
  int64_t completed_count_;
  int batch_ret_;
  int (*status_checker_)(void *);
  void *status_context_;
  ObAIFuncBase *response_validator_;
  int64_t batch_start_ts_;
  int64_t attempts_;
  int64_t retries_;
  int64_t peak_active_;
  int64_t buffered_bytes_;
  int64_t *shared_buffered_bytes_;
  int64_t received_bytes_;
  int64_t submitted_bytes_;
  DISALLOW_COPY_AND_ASSIGN(ObAIFuncClient);
};

class AIFuncBatch
{
public:
  explicit AIFuncBatch(ObIAllocator &allocator)
      : allocator_(allocator), provider_(nullptr), batched_response_(false) {}
  int poll(bool &finished, int64_t wait_ms = 0) { return client_.poll_batch(finished, wait_ms); }
  int get_results(ObArray<ObString> &results);
  void cancel()
  {
    client_.reset();
    provider_ = nullptr;
    input_counts_.reset();
    batched_response_ = false;
  }
  void set_shared_buffer_usage(int64_t &bytes) { client_.set_shared_buffer_usage(bytes); }
  void set_max_parallel(int64_t count) { client_.set_max_parallel(count); }
private:
  friend class ObAIFuncModel;
  ObIAllocator &allocator_;
  ObAIFuncBase *provider_;
  bool batched_response_;
  ObArray<int64_t> input_counts_;
  ObAIFuncClient client_;
  DISALLOW_COPY_AND_ASSIGN(AIFuncBatch);
};

} // namespace common
} // namespace oceanbase

#endif /* OB_AI_FUNC_CLIENT_H_ */
