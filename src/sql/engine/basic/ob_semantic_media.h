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

#ifndef OCEANBASE_SQL_ENGINE_SEMANTIC_MEDIA_H_
#define OCEANBASE_SQL_ENGINE_SEMANTIC_MEDIA_H_

#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include <array>
#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>

namespace oceanbase
{
namespace sql
{
class SemanticQueryBudget;
class SemanticMediaRuntime;
class SemanticMediaScheduler;
class SemanticRemoteMediaScheduler;

class SemanticMediaTask final
{
public:
  ~SemanticMediaTask();
  bool ready() const;
  int result() const;
  const common::ObString &base64() const;
  const common::ObString &mime_type() const;
  const common::ObString &audio_format() const;
  bool is_audio() const;
  bool is_image() const;

private:
  friend class SemanticMediaRuntime;
  friend class SemanticRemoteMediaScheduler;
  SemanticMediaTask(std::shared_ptr<SemanticQueryBudget> budget, int64_t charged_bytes);
  SemanticMediaTask(const SemanticMediaTask &) = delete;
  SemanticMediaTask &operator=(const SemanticMediaTask &) = delete;

  std::shared_ptr<SemanticQueryBudget> budget_;
  std::unique_ptr<char[]> descriptor_buffer_;
  std::unique_ptr<char[]> base64_buffer_;
  std::unique_ptr<uint8_t[]> source_buffer_;
  int64_t source_size_;
  int64_t source_capacity_;
  int64_t source_charge_;
  bool remote_;
  common::ObString uri_;
  common::ObString declared_mime_type_;
  common::ObString object_version_;
  int64_t declared_size_;
  int64_t charged_bytes_;
  common::ObString base64_;
  common::ObString mime_type_;
  common::ObString audio_format_;
  bool is_audio_;
  std::atomic<int> result_;
};

class SemanticMediaRuntime final
{
public:
  static constexpr int64_t WORKER_COUNT = 4;
  static constexpr int64_t QUEUE_CAPACITY = 128;
  static constexpr int64_t CACHE_CAPACITY = 64;

  // root is the global admin's explicit absolute directory policy; an empty policy denies access.
  // deadline uses ObTimeUtility::current_time()'s absolute microsecond clock.
  // Validation checks bounded container structure, not decoded pixels or audio.
  // Linux /proc/self/fd is required to reopen a verified, O_PATH-pinned regular inode.
  static int acquire(std::shared_ptr<SemanticQueryBudget> budget, const common::ObString &root,
                     int64_t max_bytes, int64_t parallel, int64_t deadline,
                     std::shared_ptr<SemanticMediaRuntime> &out);
  // remote_root is an admin-controlled, query-free URL prefix ending in '/'.
  // Both roots default to deny; one may be empty when only the other is enabled.
  static int acquire(std::shared_ptr<SemanticQueryBudget> budget, const common::ObString &root,
                     const common::ObString &remote_root, int64_t max_bytes, int64_t parallel,
                     int64_t deadline, std::shared_ptr<SemanticMediaRuntime> &out);
  ~SemanticMediaRuntime();

  // OB_EAGAIN leaves out empty. Resume on can_demand(), then retry demand().
  // Successful admission returns a handle whose result is OB_EAGAIN until ready().
  // URI/version reuse requires matching normalized supplied MIME and declared size.
  // Live/cached handles pin verified bytes; evicted unversioned files may be reloaded.
  // This is not filesystem MVCC; use sha256:<hex> to bind reloads to content.
  int demand(const common::ObString &encoded_file, std::shared_ptr<SemanticMediaTask> &out);
  // Media capacity is independent of HTTP task credits. Errors wake callers to collect status().
  bool can_demand() const;
  // Includes query-shared queued/running loads, not completed cached or externally held results.
  bool has_pending() const;
  bool admission_ready() const;
  int status() const;
  int cancel(int ret = common::OB_CANCELED);
  static void stop_scheduler();

  int64_t get_loaded_count() const { return loaded_count_.load(); }
  int64_t get_started_count() const { return started_count_.load(); }
  int64_t get_evicted_count() const { return evicted_count_.load(); }
  int64_t get_cached_count() const;
  int64_t get_active_count() const;

  enum class TestStage
  {
    BEFORE_READ,
    AFTER_READ_CHUNK,
    BEFORE_ENCODE,
    AFTER_ENCODE_CHUNK
  };
  using TestHook = void (*)(void *, TestStage);
  // Install only while idle. The hook must not throw; its context must outlive drain.
  int set_test_hook_for_testing(TestHook hook, void *context);

private:
  friend class SemanticMediaScheduler;
  friend class SemanticRemoteMediaScheduler;
  struct CacheEntry
  {
    std::shared_ptr<SemanticMediaTask> task_;
    uint64_t last_use_ = 0;
  };
  struct MediaKey
  {
    common::ObString uri_;
    common::ObString version_;
  };
  struct MediaKeyLess
  {
    bool operator()(const MediaKey &left, const MediaKey &right) const;
  };
  struct LiveEntry
  {
    std::weak_ptr<SemanticMediaTask> task_;
    std::unique_ptr<char[]> key_buffer_;
    int64_t charged_bytes_ = 0;
  };

  SemanticMediaRuntime(std::shared_ptr<SemanticQueryBudget> budget, int64_t max_bytes,
                       int64_t parallel, int64_t deadline, int64_t charged_bytes);
  SemanticMediaRuntime(const SemanticMediaRuntime &) = delete;
  SemanticMediaRuntime &operator=(const SemanticMediaRuntime &) = delete;

  int matches_policy(const std::shared_ptr<SemanticQueryBudget> &budget,
                     const common::ObString &root, const common::ObString &remote_root,
                     int64_t max_bytes, int64_t parallel, int64_t deadline) const;
  int reserve_with_eviction(int64_t bytes);
  int reserve_locked(int64_t bytes);
  bool evict_one_locked();
  void purge_expired_locked();
  void erase_live_locked(const MediaKey &key);
  void run_task(std::shared_ptr<SemanticMediaTask> task);
  void complete_task(std::shared_ptr<SemanticMediaTask> task, int ret);
  int load(SemanticMediaTask &task);
  int encode(SemanticMediaTask &task, const uint8_t *raw, int64_t size);
  int append_remote_data(SemanticMediaTask &task, const char *data, int64_t size);
  int remote_allowed(const common::ObString &uri) const;
  void release_source(SemanticMediaTask &task);
  void invoke_test_hook(TestStage stage);

  std::shared_ptr<SemanticQueryBudget> budget_;
  std::unique_ptr<char[]> root_buffer_;
  common::ObString root_;
  std::unique_ptr<char[]> remote_root_buffer_;
  common::ObString remote_root_;
  int root_fd_;
  int64_t max_bytes_;
  int64_t parallel_;
  int64_t deadline_;
  int64_t charged_bytes_;
  bool registered_;
  std::atomic<int> cancel_ret_;
  mutable std::mutex mutex_;
  std::condition_variable drained_;
  std::array<CacheEntry, CACHE_CAPACITY> cache_;
  // Weak entries coalesce externally held handles without retaining an entire dataset.
  std::map<MediaKey, LiveEntry, MediaKeyLess> live_;
  int64_t active_count_;
  uint8_t waiting_sources_;
  uint64_t cache_clock_;
  std::atomic<int64_t> started_count_;
  std::atomic<int64_t> loaded_count_;
  std::atomic<int64_t> evicted_count_;
  TestHook test_hook_;
  void *test_hook_context_;
};

}
}
#endif
