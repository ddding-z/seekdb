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

#include "ob_px_reduce_transmit_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObPxReduceTransmitOpInput, ObPxTransmitOpInput));

OB_SERIALIZE_MEMBER((ObPxReduceTransmitSpec, ObPxTransmitSpec));

int ObPxReduceTransmitOp::inner_open()
{
  int ret = OB_SUCCESS;

  if (!MY_SPEC.is_no_repart_exchange()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect no repartition", K(ret));
  } else if (OB_FAIL(ObPxTransmitOp::inner_open())) {
  }
  return ret;
}

int ObPxReduceTransmitOp::do_transmit()
{
  int ret = OB_SUCCESS;
  if (!resumable_) {
    ObAllToOneSliceIdxCalc fixed_slice_calc(ctx_.get_allocator());
    ret = send_rows<ObSliceIdxCalc::ALL_TO_ONE>(fixed_slice_calc);
  } else {
    if (nullptr == resumable_slice_calc_) {
      void *buffer = ctx_.get_allocator().alloc(sizeof(ObAllToOneSliceIdxCalc));
      if (OB_ISNULL(buffer)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate resumable PX slice calculator failed", K(ret));
      } else {
        resumable_slice_calc_ = new (buffer) ObAllToOneSliceIdxCalc(ctx_.get_allocator());
      }
    }
    if (OB_SUCC(ret)) {
      ret = send_rows<ObSliceIdxCalc::ALL_TO_ONE>(*resumable_slice_calc_);
    }
  }
  return ret;
}

void ObPxReduceTransmitOp::destroy()
{
  if (nullptr != resumable_slice_calc_) {
    resumable_slice_calc_->~ObAllToOneSliceIdxCalc();
    resumable_slice_calc_ = nullptr;
  }
  ObPxTransmitOp::destroy();
}

int ObPxReduceTransmitOp::inner_close()
{
  return ObPxTransmitOp::inner_close();
}

} // end namespace sql
} // end namespace oceanbase
