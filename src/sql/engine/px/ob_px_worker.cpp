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
#include "ob_px_worker.h"
#include "share/rc/ob_server_runtime.h"
#include "query/runtime/ob_query_runtime_environment.h"
#include "query/runtime/ob_px_task_continuation.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/ob_px_admission.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;
using namespace oceanbase::lib;
using namespace oceanbase::share;


//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

ObPxCoroWorker::ObPxCoroWorker(const share::ObGlobalContext &gctx,
                               common::ObIAllocator &alloc)
  : gctx_(gctx),
    alloc_(alloc),
    exec_ctx_(alloc, share::server_service<ObSQLSessionMgr>()),
    phy_plan_(),
    task_arg_(),
    task_proc_(gctx, task_arg_),
    task_co_id_(0)
{
}

int ObPxCoroWorker::run(ObPxInitTaskArgs &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_assign(arg, task_arg_))) {
  } else {
  }
  return ret;
}

int ObPxCoroWorker::exit()
{
  int ret = OB_SUCCESS;
  ret = OB_NOT_INIT;
  return ret;
}

int ObPxCoroWorker::deep_copy_assign(const ObPxInitTaskArgs &src,
                                     ObPxInitTaskArgs &dest)
{
  int ret = OB_SUCCESS;
  dest.set_deserialize_param(exec_ctx_, phy_plan_, &alloc_);
  // Deep copy all elements in arg, into session, op tree, etc.
  // Temporarily complete through serialization+deserialization
  int64_t ser_pos = 0;
  int64_t des_pos = 0;
  void *ser_ptr = NULL;
  int64_t ser_arg_len = src.get_serialize_size();

  if (OB_ISNULL(ser_ptr = alloc_.alloc(ser_arg_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail alloc memory", K(ser_arg_len), KP(ser_ptr), K(ret));
  } else if (OB_FAIL(src.serialize(static_cast<char *>(ser_ptr), ser_arg_len, ser_pos))) {
  } else if (OB_FAIL(dest.deserialize(static_cast<const char *>(ser_ptr), ser_pos, des_pos))) {
  } else if (ser_pos != des_pos) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("data_len and pos mismatch", K(ser_arg_len), K(ser_pos), K(des_pos), K(ret));
  } else {
    dest.exec_ctx_->set_runtime_services(
        src.exec_ctx_->get_runtime_services());
    // PLACE_HOLDER: if want to shared trans_desc
    // dest.exec_ctx_->get_my_session()->set_effective_trans_desc(src.exec_ctx_->get_my_session()->get_effective_trans_desc());
  }
  return ret;
}



//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
class SQCHandlerGuard
{
public:
  SQCHandlerGuard(ObPxSqcHandler *h) : sqc_handler_(h)
  {
    if (OB_LIKELY(sqc_handler_)) {
      sqc_handler_->get_notifier().worker_start(GETTID());
    }
  }
  ~SQCHandlerGuard()
  {
    if (OB_LIKELY(sqc_handler_)) {
      sqc_handler_->worker_end_hook();
      int report_ret = OB_SUCCESS;
      ObPxSqcHandler::release_handler(sqc_handler_, report_ret);
      sqc_handler_ = nullptr;
    }
  }
private:
  ObPxSqcHandler *sqc_handler_;
};

namespace
{
class PxSliceWorkerGuard
{
public:
  PxSliceWorkerGuard(const ObPxWorkerEnvArgs &env, ObPxSqcHandler *handler)
      : worker_(THIS_WORKER),
        session_(worker_.get_session()),
        timeout_(worker_.get_timeout_ts()),
        ntp_offset_(worker_.get_ntp_offset()),
        level_(worker_.get_worker_level()),
        request_level_(worker_.get_curr_request_level()),
        log_level_(ObThreadLogLevelUtils::get_level())
  {
    worker_.set_ntp_offset(0);
    if (nullptr != handler) {
      worker_.set_worker_level(handler->get_request_level());
      worker_.set_curr_request_level(handler->get_request_level());
    }
    if (OB_LOGGER.is_info_as_wdiag()) {
      ObThreadLogLevelUtils::clear();
    } else if (OB_LOG_LEVEL_NONE != env.get_log_level()) {
      ObThreadLogLevelUtils::init(env.get_log_level());
    }
  }

  ~PxSliceWorkerGuard()
  {
    worker_.set_session(session_);
    worker_.set_timeout_ts(timeout_);
    worker_.set_ntp_offset(ntp_offset_);
    worker_.set_worker_level(level_);
    worker_.set_curr_request_level(request_level_);
    if (OB_LOG_LEVEL_NONE == log_level_) {
      ObThreadLogLevelUtils::clear();
    } else {
      ObThreadLogLevelUtils::init(log_level_);
    }
  }

private:
  Worker &worker_;
  ObSQLSessionInfo *session_;
  int64_t timeout_;
  int64_t ntp_offset_;
  int32_t level_;
  int32_t request_level_;
  int log_level_;
  DISALLOW_COPY_AND_ASSIGN(PxSliceWorkerGuard);
};

class PxTaskContinuation final : public query::IPxTaskContinuation
{
public:
  PxTaskContinuation(const ObPxWorkerEnvArgs &env, const ObPxInitTaskArgs &arg)
      : env_(env), memory_(nullptr), process_(nullptr),
        interrupt_registered_(false), worker_started_(false), finished_(false)
  {
    source_arg_ = arg;
  }

  ~PxTaskContinuation() override
  {
    OB_ASSERT(nullptr == memory_ && nullptr == process_ && !interrupt_registered_);
  }

  bool is_ready() override
  {
    const int ret = check_cancellation();
    if (OB_SUCCESS != ret) {
      await_.cancel(ret);
      return true;
    } else if (!await_.is_pending()) {
      await_.cancel(OB_ERR_UNEXPECTED);
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "PX continuation has no pending await",
                    K(source_arg_.task_));
      return true;
    } else {
      return await_.is_ready();
    }
  }

  query::PxTaskRunResult run(bool need_exec) override
  {
    int ret = OB_SUCCESS;
    bool suspended = false;
    ObPxSqcHandler *handler = source_arg_.sqc_handler_;
    ObTraceIdGuard trace_guard(env_.get_trace_id());
    PxSliceWorkerGuard worker_guard(env_, handler);
    ObPxInterruptGuard interrupt_guard(source_arg_.task_.get_interrupt_id().px_interrupt_id_);
    if (finished_) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "finished PX continuation executed again");
      return query::PxTaskRunResult::FINISHED;
    }
    if (need_exec && !interrupt_registered_ &&
        OB_SUCCESS == interrupt_guard.get_interrupt_reg_ret()) {
      const int register_ret = interrupt_checker_.register_checker(
          source_arg_.task_.get_interrupt_id().px_interrupt_id_);
      if (OB_SUCCESS != register_ret) {
        await_.cancel(register_ret);
        LOG_WARN("register durable PX interrupt checker failed", K(register_ret));
      } else {
        interrupt_registered_ = true;
      }
    }
    if (nullptr != handler && !worker_started_) {
      handler->get_notifier().worker_start(GETTID());
      worker_started_ = true;
    }
    if (OB_ISNULL(handler) || OB_ISNULL(env_.get_gctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid resumable PX worker environment", K(ret), KP(handler));
      report_unprocessed_result(ret);
    } else {
      await_.reset_pending();
      if (!need_exec) {
        await_.cancel(OB_CANCELED);
      } else if (OB_SUCCESS != interrupt_guard.get_interrupt_reg_ret()) {
        await_.cancel(interrupt_guard.get_interrupt_reg_ret());
      } else if (IS_INTERRUPTED()) {
        await_.cancel(GET_INTERRUPT_CODE().code_);
      } else if (OB_SUCCESS != (ret = check_cancellation())) {
        await_.cancel(ret);
      }
      ret = OB_SUCCESS;
      static const uint64_t PROCESS_OWNER_ID = 1;
      SERVER_MODULE_SCOPE {
        CREATE_WITH_TEMP_ENTITY(RESOURCE_OWNER, PROCESS_OWNER_ID) {
          if (nullptr == memory_ && OB_SUCCESS == await_.cancel_ret()) {
            if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(memory_,
                    lib::ContextParam().set_mem_attr(ObModIds::OB_SQL_PX)))) {
              LOG_WARN("create resumable PX memory context failed", K(ret));
            }
          }
          if (OB_SUCC(ret) && nullptr != memory_) {
            WITH_CONTEXT(memory_) {
              // Retained arenas can migrate to another physical PX worker.
              lib::ContextTLOptGuard context_guard(false);
              lib::RequestAwaitGuard await_guard(await_);
              if (nullptr == process_) {
                if (OB_FAIL(runtime_arg_.init_deserialize_param(
                        source_arg_, memory_, *env_.get_gctx()))) {
                } else if (OB_FAIL(runtime_arg_.deep_copy_assign(
                        source_arg_, memory_->get_arena_allocator()))) {
                } else {
                  runtime_arg_.sqc_handler_ = handler;
                  void *buffer = memory_->get_arena_allocator().alloc(sizeof(ObPxTaskProcess));
                  if (OB_ISNULL(buffer)) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("allocate resumable PX task process failed", K(ret));
                  } else {
                    process_ = new (buffer) ObPxTaskProcess(*env_.get_gctx(), runtime_arg_);
                    process_->enable_resumable(await_);
                    await_.enable(runtime_arg_.exec_ctx_);
                  }
                }
              }
              if (OB_SUCC(ret)) {
                ret = process_->run();
                suspended = OB_EAGAIN == ret && await_.is_pending();
              }
              if (!suspended) {
                report_unprocessed_result(ret);
                await_.reset();
                if (nullptr != process_) {
                  process_->~ObPxTaskProcess();
                  process_ = nullptr;
                }
                runtime_arg_.destroy();
              }
            }
          } else {
            if (OB_SUCC(ret)) {
              ret = await_.cancel_ret();
              if (OB_SUCCESS == ret) {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("resumable PX memory context is null", K(ret));
              }
            }
            report_unprocessed_result(ret);
          }
          if (!suspended && nullptr != memory_) {
            DESTROY_CONTEXT(memory_);
            memory_ = nullptr;
          }
        }
      }
    }
    if (!suspended) {
      finished_ = true;
      if (OB_SUCCESS != ret) {
        LOG_WARN("resumable PX task finished with error", K(ret), K(source_arg_.task_));
      }
      if (worker_started_) {
        const int end_ret = handler->worker_end_hook();
        if (OB_SUCCESS != end_ret) {
          LOG_WARN("resumable PX worker end hook failed", K(end_ret));
        }
        int report_ret = OB_SUCCESS;
        ObPxSqcHandler::release_handler(handler, report_ret);
        if (OB_SUCCESS != report_ret) {
          LOG_WARN("release resumable PX SQC handler failed", K(report_ret));
        }
        source_arg_.sqc_handler_ = nullptr;
      }
      PxWorkerFinishFunctor finish;
      finish();
      if (interrupt_registered_) {
        interrupt_checker_.unregister_checker(source_arg_.task_.get_interrupt_id().px_interrupt_id_);
        interrupt_registered_ = false;
      }
    }
    return suspended ? query::PxTaskRunResult::SUSPENDED : query::PxTaskRunResult::FINISHED;
  }

  void destroy() override
  {
    PxTaskContinuation *task = this;
    OB_DELETE(PxTaskContinuation, "PxTask", task);
  }

private:
  int check_cancellation()
  {
    int ret = await_.cancel_ret();
    ObExecContext *ctx = nullptr == runtime_arg_.exec_ctx_
        ? source_arg_.exec_ctx_ : runtime_arg_.exec_ctx_;
    if (OB_SUCCESS != ret) {
    } else if (interrupt_registered_ && interrupt_checker_.is_interrupted()) {
      ret = interrupt_checker_.get_interrupt_code().code_;
      if (OB_SUCCESS == ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("durable PX checker received an invalid interrupt code", K(ret));
      }
    } else if (nullptr == source_arg_.sqc_handler_ || nullptr == ctx) {
      ret = OB_ERR_UNEXPECTED;
    } else if (source_arg_.sqc_handler_->has_interrupted()) {
      ret = OB_CANCELED;
    } else if (nullptr != ctx->get_my_session() &&
               ctx->get_my_session()->is_terminate(ret)) {
    } else if (nullptr != ctx->get_physical_plan_ctx() &&
               ctx->get_physical_plan_ctx()->get_timeout_timestamp() <= ObTimeUtility::current_time()) {
      ret = OB_TIMEOUT;
    } else if (nullptr != ctx->get_query_runtime_environment() &&
               ctx->get_query_runtime_environment()->server_stopped()) {
      ret = OB_CANCELED;
    }
    return ret;
  }

  void report_unprocessed_result(int ret)
  {
    if (nullptr == process_ || !process_->has_reported_result() ||
        (nullptr != source_arg_.sqc_task_ptr_ && source_arg_.sqc_task_ptr_->get_result() != ret)) {
      if (nullptr != source_arg_.sqc_task_ptr_) {
        source_arg_.sqc_task_ptr_->set_result(ret);
        source_arg_.sqc_task_ptr_->get_err_msg().rcode_ = ret;
        if (nullptr == process_) {
          source_arg_.sqc_task_ptr_->set_task_state(SQC_TASK_EXIT);
        }
      }
      if (OB_SUCCESS != ret) {
        const int interrupt_ret = ObInterruptUtil::interrupt_qc(source_arg_.task_, ret);
        if (OB_SUCCESS != interrupt_ret) {
          LOG_WARN("interrupt QC after resumable PX startup failure failed",
                   K(ret), K(interrupt_ret));
        }
      }
    }
  }

  ObPxWorkerEnvArgs env_;
  ObPxInitTaskArgs source_arg_;
  ObPxInitTaskArgs runtime_arg_;
  lib::MemoryContext memory_;
  ObPxTaskProcess *process_;
  lib::RequestAwait await_;
  // A TLS checker alone loses interrupts when every physical worker has yielded.
  ObInterruptChecker interrupt_checker_;
  bool interrupt_registered_;
  bool worker_started_;
  bool finished_;
  DISALLOW_COPY_AND_ASSIGN(PxTaskContinuation);
};
}

void PxWorkerFunctor::operator ()(bool need_exec)
{
  int ret = OB_SUCCESS;
  const char *px_parallel_rule_str = nullptr;
  if (task_arg_.op_spec_root_ != nullptr && task_arg_.op_spec_root_->plan_ != nullptr) {
    PXParallelRule px_parallel_rule = task_arg_.op_spec_root_->plan_->get_px_parallel_rule();
    px_parallel_rule_str = ob_px_parallel_rule_str(px_parallel_rule);
  }
  ObCurTraceId::set(env_arg_.get_trace_id());
  /**
   * The interrupt must cover the release handler, because its process involves sqc sending messages to qc,
   * requiring an interrupt check. The interrupt itself is thread-local and runtime-independent.
   */
  ObPxInterruptGuard px_int_guard(task_arg_.task_.get_interrupt_id().px_interrupt_id_);
  ObPxSqcHandler *sqc_handler = task_arg_.get_sqc_handler();
  SQCHandlerGuard sqc_handler_guard(sqc_handler);
  lib::MemoryContext mem_context = nullptr;
  //ensure PX worker skip updating timeout_ts_ by ntp offset
  THIS_WORKER.set_ntp_offset(0);
  if (!need_exec) {
    LOG_INFO("px pool already stopped, do not execute the task.");
  } else if (OB_FAIL(px_int_guard.get_interrupt_reg_ret())) {
  } else if (OB_NOT_NULL(sqc_handler) && OB_LIKELY(!sqc_handler->has_interrupted())) {
    THIS_WORKER.set_worker_level(sqc_handler->get_request_level());
    THIS_WORKER.set_curr_request_level(sqc_handler->get_request_level());
    // Do not set thread local log level while log level upgrading (OB_LOGGER.is_info_as_wdiag)
    if (OB_LOGGER.is_info_as_wdiag()) {
      ObThreadLogLevelUtils::clear();
    } else {
      if (OB_LOG_LEVEL_NONE != env_arg_.get_log_level()) {
        ObThreadLogLevelUtils::init(env_arg_.get_log_level());
      }
    }
    // Single-process resource owner.
    static const uint64_t PROCESS_OWNER_ID = 1;
    SERVER_MODULE_SCOPE {
      CREATE_WITH_TEMP_ENTITY(RESOURCE_OWNER, PROCESS_OWNER_ID) {
        if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context,
            lib::ContextParam().set_mem_attr(ObModIds::OB_SQL_PX)))) {
        } else {
          WITH_CONTEXT(mem_context) {
            lib::ContextTLOptGuard guard(true);
            // In the worker thread, perform a deep copy of args to alleviate the burden on the sqc thread.
            ObPxInitTaskArgs runtime_arg;
            if (OB_FAIL(runtime_arg.init_deserialize_param(
                    task_arg_, mem_context, *env_arg_.get_gctx()))) {
            } else if (OB_FAIL(runtime_arg.deep_copy_assign(task_arg_, mem_context->get_arena_allocator()))) {
            } else {
              // Bind sqc_handler, convenient for the operator to get sqc_handle anywhere
              runtime_arg.sqc_handler_ = sqc_handler;
            }
            // Execute
            ObPxTaskProcess worker(*env_arg_.get_gctx(), runtime_arg);
            if (OB_SUCC(ret)) {
              worker.run();
            }
            runtime_arg.destroy();
          }
        }
      }
      if (nullptr != mem_context) {
        DESTROY_CONTEXT(mem_context);
        mem_context = NULL;
      }
      auto *pm = common::ObPageManager::thread_local_instance();
      if (OB_LIKELY(nullptr != pm)) {
        if (pm->get_used() != 0) {
          LOG_ERROR("page manager's used should be 0, unexpected!!!", KP(pm));
        }
      }
    }
    ObThreadLogLevelUtils::clear();
  } else if (OB_ISNULL(sqc_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected null sqc handler", K(sqc_handler));
  } else {
    LOG_WARN("already interrupted");
  }

  //if start worker failed, still need set task state, interrupt qc
  if (OB_FAIL(ret)) {
    if (task_arg_.sqc_task_ptr_ != NULL) {
      task_arg_.sqc_task_ptr_->set_task_state(SQC_TASK_EXIT);
    }
    (void) ObInterruptUtil::interrupt_qc(task_arg_.task_, ret);
  }

  PxWorkerFinishFunctor on_func_finish;
  on_func_finish();
  ObCurTraceId::reset();
}

void PxWorkerFinishFunctor::operator ()()
{
  // Each worker ends, a slot is released
  ObPxSubAdmission::release(1);
}


ObPxThreadWorker::ObPxThreadWorker(const share::ObGlobalContext &gctx)
  : gctx_(gctx),
    task_co_id_(0)
{
}

ObPxThreadWorker::~ObPxThreadWorker()
{
}
// Execute in the px_pool corresponding to the group
int ObPxThreadWorker::run(ObPxInitTaskArgs &task_arg)
{
  int ret = OB_SUCCESS;
  static constexpr int64_t DEFAULT_PX_GROUP_ID = 0;
  query::ObIQueryRuntimeEnvironment *runtime = OB_ISNULL(task_arg.exec_ctx_)
      ? nullptr : task_arg.exec_ctx_->get_query_runtime_environment();
  if (OB_ISNULL(runtime)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("query runtime environment is unavailable", K(ret));
  } else {
    ObPxWorkerEnvArgs env_args;
    env_args.set_enqueue_timestamp(ObTimeUtility::current_time());
    env_args.set_trace_id(*ObCurTraceId::get_trace_id());
    env_args.set_gctx(&gctx_);
    if (OB_LOG_LEVEL_NONE != common::ObThreadLogLevelUtils::get_level()) {
      env_args.set_log_level(common::ObThreadLogLevelUtils::get_level());
    }
    if (ObPxTaskProcess::supports_resumable(task_arg)) {
      auto *task = OB_NEW(PxTaskContinuation, ObMemAttr("PxTask"), env_args, task_arg);
      if (OB_ISNULL(task)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate resumable PX worker failed", K(ret));
      } else if (OB_FAIL(runtime->submit_resumable_px_task(DEFAULT_PX_GROUP_ID, task))) {
        task->destroy();
        LOG_WARN("submit resumable PX worker failed", K(ret));
      }
    } else {
      PxWorkerFunctor task(env_args, task_arg);
      if (OB_FAIL(runtime->submit_px_task(DEFAULT_PX_GROUP_ID, task))) {
      }
    }
  }
  return ret;
}

int ObPxThreadWorker::exit()
{
  // SQC will wait all PxWorker finish.
  // Just return success.
  return OB_SUCCESS;
}

int ObPxLocalWorker::run(ObPxInitTaskArgs &task_arg)
{
  int ret = OB_SUCCESS;

  {
    ObPxTaskProcess task_proc(gctx_, task_arg);
    ret = task_proc.process();
  }

  return ret;
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////


//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

ObPxThreadWorker * ObPxThreadWorkerFactory::create_worker()
{
  ObPxThreadWorker *worker = NULL;
  int ret = OB_SUCCESS;
  void *ptr = alloc_.alloc(sizeof(ObPxThreadWorker));
  if (OB_NOT_NULL(ptr)) {
    worker = new(ptr)ObPxThreadWorker(gctx_);
    if (OB_FAIL(workers_.push_back(worker))) {
    }
    if (OB_SUCCESS != ret) {
      worker->~ObPxThreadWorker();
      worker = NULL;
    }
  }
  return worker;
}

int ObPxThreadWorkerFactory::join()
{
  int ret = OB_SUCCESS;
  int eret = OB_SUCCESS;
  for (int64_t i = 0; i < workers_.count(); ++i) {
    if (OB_SUCCESS != (eret = workers_.at(i)->exit())) {
      ret = eret; // try join as many workers as possible, return last error
      LOG_ERROR("fail join px thread workers", K(ret));
    }
  }
  return ret;
}

void ObPxThreadWorkerFactory::destroy()
{
  for (int64_t i = 0; i < workers_.count(); ++i) {
    workers_.at(i)->~ObPxThreadWorker();
  }
  workers_.reset();
}

ObPxThreadWorkerFactory::~ObPxThreadWorkerFactory()
{
  destroy();
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////




void ObPxCoroWorkerFactory::destroy()
{
  for (int64_t i = 0; i < workers_.count(); ++i) {
    workers_.at(i)->~ObPxCoroWorker();
  }
}

ObPxCoroWorkerFactory::~ObPxCoroWorkerFactory()
{
  destroy();
}


//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////


ObPxWorkerRunnable *ObPxLocalWorkerFactory::create_worker()
{
  return &worker_;
}

void ObPxLocalWorkerFactory::destroy()
{
}

ObPxLocalWorkerFactory::~ObPxLocalWorkerFactory()
{
  destroy();
}



//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
int ObPxWorker::check_status()
{
  int ret = OB_SUCCESS;
  if (nullptr != session_) {
    session_->is_terminate(ret);
  }

  if (OB_SUCC(ret)) {
    if (is_timeout()) {
      ret = OB_TIMEOUT;
    } else if (IS_INTERRUPTED()) {
      ObInterruptCode &ic = GET_INTERRUPT_CODE();
      ret = ic.code_;
      LOG_WARN("px execution was interrupted", K(ic), K(ret));
    }
  }
  return ret;
}
