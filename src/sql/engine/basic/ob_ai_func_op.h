#ifndef OCEANBASE_SQL_ENGINE_AI_FUNC_OP_H_
#define OCEANBASE_SQL_ENGINE_AI_FUNC_OP_H_

#include "query/engine/ob_operator.h"
#include "query/engine/basic/ob_batch_result_holder.h"

namespace oceanbase
{
namespace sql
{

class AIFuncSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  AIFuncSpec(common::ObIAllocator &allocator, ObPhyOperatorType type)
      : ObOpSpec(allocator, type), ai_expr_(nullptr) {}
  ObExpr *ai_expr_;
};

class AIFuncOp : public ObOperator
{
public:
  AIFuncOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
      : ObOperator(ctx, spec, input), buffered_bytes_(0), slots_{nullptr, nullptr},
        head_(0), count_(0), input_end_(false) {}
  int inner_open() override;
  int inner_rescan() override;
  int inner_get_next_row() override;
  int inner_get_next_batch(int64_t max_row_cnt) override;
  int inner_close() override;
  void destroy() override;
private:
  struct Slot;
  const AIFuncSpec &ai_spec() const { return static_cast<const AIFuncSpec &>(spec_); }
  int reserve(Slot &slot, int64_t bytes);
  int submit(Slot &slot);
  int poll();
  int output(Slot &slot, int64_t max_row_cnt);
  void reset_slot(Slot &slot);
  void reset_pipeline();
  ObBatchResultHolder child_frame_;
  int64_t buffered_bytes_;
  Slot *slots_[2];
  int64_t head_;
  int64_t count_;
  bool input_end_;
  DISALLOW_COPY_AND_ASSIGN(AIFuncOp);
};

}
}

#endif