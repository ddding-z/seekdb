#ifndef OCEANBASE_SQL_ENGINE_SEMANTIC_PROXY_H_
#define OCEANBASE_SQL_ENGINE_SEMANTIC_PROXY_H_

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObJsonObject;
}
namespace sql
{
struct SemanticProxyConfig
{
  OB_UNIS_VERSION(1);

public:
  SemanticProxyConfig() : enabled_(false), threshold_(0) {}
  bool enabled_;
  common::ObString model_;
  common::ObString query_;
  double threshold_;
  TO_STRING_KV(K_(enabled), K_(threshold));
};

class SemanticEmbeddingProxy
{
public:
  static int parse_filter_options(common::ObIAllocator &allocator,
                                  const common::ObJsonObject &options,
                                  SemanticProxyConfig &config,
                                  bool &has_threshold);
  static int parse_vector(common::ObIAllocator &allocator,
                          const common::ObString &result, float *&values,
                          int64_t &dimension);
  static int cosine(const float *query, int64_t query_dimension,
                    const float *document, int64_t document_dimension,
                    double &score);
};
} // namespace sql
} // namespace oceanbase
#endif
