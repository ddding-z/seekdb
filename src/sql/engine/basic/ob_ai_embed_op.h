#ifndef OCEANBASE_SQL_ENGINE_AI_EMBED_OP_H_
#define OCEANBASE_SQL_ENGINE_AI_EMBED_OP_H_

#include "query/engine/ob_operator.h"
#include "query/engine/basic/ob_batch_result_holder.h"

namespace oceanbase
{
namespace sql
{

class AIEmbedSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  AIEmbedSpec(common::ObIAllocator &allocator, ObPhyOperatorType type)
      : ObOpSpec(allocator, type), embedding_(nullptr) {}
  ObExpr *embedding_;
};

class AIEmbedOp : public ObOperator
{
public:
  AIEmbedOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
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
  const AIEmbedSpec &ai_spec() const { return static_cast<const AIEmbedSpec &>(spec_); }
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
  DISALLOW_COPY_AND_ASSIGN(AIEmbedOp);
};

}
}

#endif