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

#ifndef OCEANBASE_QUERY_RUNTIME_OB_PX_TASK_CONTINUATION_H_
#define OCEANBASE_QUERY_RUNTIME_OB_PX_TASK_CONTINUATION_H_

namespace oceanbase
{
namespace query
{

enum class PxTaskRunResult { FINISHED, SUSPENDED };

class IPxTaskContinuation
{
public:
  // Readiness must be nonblocking and must not depend on execution-worker TLS.
  virtual bool is_ready() = 0;
  // A shutdown invocation (need_exec == false) must finish, not suspend again.
  virtual PxTaskRunResult run(bool need_exec) = 0;
  virtual void destroy() = 0;

protected:
  virtual ~IPxTaskContinuation() = default;
};

} // namespace query
} // namespace oceanbase

#endif // OCEANBASE_QUERY_RUNTIME_OB_PX_TASK_CONTINUATION_H_
