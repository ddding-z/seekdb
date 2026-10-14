#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_semantic_proxy.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "data_plane/vector/ob_vector_cosine_distance.h"
#include <algorithm>
#include <cmath>

namespace oceanbase
{
namespace sql
{
using namespace common;

OB_SERIALIZE_MEMBER(SemanticProxyConfig, enabled_, model_, query_, threshold_);

int SemanticEmbeddingProxy::parse_filter_options(ObIAllocator &allocator,
                                                 const ObJsonObject &options,
                                                 SemanticProxyConfig &config,
                                                 bool &has_threshold)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *node = options.get_value("semantic_proxy");
  if (nullptr == node)
  {
  }
  else if (node->json_type() != ObJsonNodeType::J_OBJECT)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    const auto &proxy = *static_cast<const ObJsonObject *>(node);
    for (uint64_t index = 0; OB_SUCC(ret) && index < proxy.element_count(); ++index)
    {
      ObString key;
      ObJsonNode *value = nullptr;
      if (OB_FAIL(proxy.get_value_by_idx(index, key, value)))
      {
      }
      else if (nullptr == value)
      {
        ret = OB_ERR_UNEXPECTED;
      }
      else if (key == "model" || key == "query")
      {
        if (value->json_type() != ObJsonNodeType::J_STRING ||
            value->get_data_length() == 0)
        {
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          ObString &target = key == "model" ? config.model_ : config.query_;
          ret = ob_write_string(
              allocator, ObString(value->get_data_length(), value->get_data()),
              target);
        }
      }
      else if (key == "threshold")
      {
        const ObJsonNodeType type = value->json_type();
        if (type != ObJsonNodeType::J_INT && type != ObJsonNodeType::J_UINT &&
            type != ObJsonNodeType::J_DOUBLE && type != ObJsonNodeType::J_DECIMAL)
        {
          ret = OB_INVALID_ARGUMENT;
        }
        else if (OB_FAIL(value->to_double(config.threshold_)))
        {
        }
        else if (!std::isfinite(config.threshold_) || config.threshold_ < -1 ||
                 config.threshold_ > 1)
        {
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          has_threshold = true;
        }
      }
      else
      {
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  if (OB_FAIL(ret))
  {
    LOG_WARN("invalid per-FILTER embedding proxy options", K(ret));
    if (ret == OB_INVALID_ARGUMENT)
    {
      LOG_USER_ERROR(
          OB_INVALID_ARGUMENT,
          "AI_FILTER semantic_proxy must be an object containing only nonempty "
          "model/query strings and a finite numeric threshold in [-1,1]");
    }
  }
  return ret;
}

int SemanticEmbeddingProxy::parse_vector(ObIAllocator &allocator,
                                         const ObString &result, float *&values,
                                         int64_t &dimension)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *document = nullptr;
  values = nullptr;
  dimension = 0;
  share::ObJsonReaderHelper reader(allocator);
  if (result.empty())
  {
    ret = OB_INVALID_DATA;
  }
  else if (OB_FAIL(ObJsonBaseFactory::get_json_base(
               &allocator, result, ObJsonInType::JSON_TREE,
               ObJsonInType::JSON_TREE, document,
               ObJsonParser::JSN_STRICT_FLAG)))
  {
    if (ret == OB_ERR_INVALID_JSON_TEXT)
    {
      ret = OB_INVALID_DATA;
    }
  }
  else if (nullptr == document ||
           document->json_type() != ObJsonNodeType::J_ARRAY ||
           document->element_count() == 0 ||
           document->element_count() >
               static_cast<uint64_t>(INT64_MAX / sizeof(float)))
  {
    ret = OB_INVALID_DATA;
  }
  else
  {
    dimension = document->element_count();
    ret = ObAIFuncUtils::decode_float_embedding_array(
        *document, allocator, reader, dimension, values);
    if (ret == OB_INVALID_ARGUMENT || ret == OB_ERR_UNEXPECTED)
    {
      ret = OB_INVALID_DATA;
    }
    double norm = 0;
    for (int64_t index = 0; OB_SUCC(ret) && index < dimension; ++index)
    {
      if (!std::isfinite(values[index]))
      {
        ret = OB_INVALID_DATA;
      }
      else
      {
        norm += static_cast<double>(values[index]) * values[index];
      }
    }
    if (OB_SUCC(ret) && (norm == 0 || !std::isfinite(norm)))
    {
      ret = OB_INVALID_DATA;
    }
  }
  if (OB_FAIL(ret))
  {
    LOG_WARN("invalid embedding proxy vector", K(ret), K(dimension));
    if (ret == OB_INVALID_DATA)
    {
      FORWARD_USER_ERROR(
          ret,
          "embedding proxy requires non-empty finite nonzero numeric vectors");
    }
  }
  return ret;
}

int SemanticEmbeddingProxy::cosine(const float *query, int64_t query_dimension,
                                   const float *document,
                                   int64_t document_dimension, double &score)
{
  int ret = OB_SUCCESS;
  bool query_nonzero = false;
  bool document_nonzero = false;
  if (nullptr == query || nullptr == document || query_dimension <= 0 ||
      query_dimension != document_dimension)
  {
    ret = OB_INVALID_DATA;
  }
  for (int64_t index = 0; OB_SUCC(ret) && index < query_dimension; ++index)
  {
    const float q = query[index];
    const float d = document[index];
    if (!std::isfinite(q) || !std::isfinite(d))
    {
      ret = OB_INVALID_DATA;
    }
    else
    {
      query_nonzero |= q != 0;
      document_nonzero |= d != 0;
    }
  }
  if (OB_SUCC(ret))
  {
    double similarity = 0;
    if (!query_nonzero || !document_nonzero)
    {
      ret = OB_INVALID_DATA;
    }
    else if (OB_FAIL(ObVectorCosineDistance<float>::cosine_similarity_func(
                 query, document, query_dimension, similarity)))
    {
    }
    else if (!std::isfinite(similarity))
    {
      ret = OB_INVALID_DATA;
    }
    else
    {
      score = std::max(-1.0, std::min(1.0, similarity));
    }
  }
  if (OB_FAIL(ret))
  {
    LOG_WARN("cannot score embedding proxy vectors", K(ret), K(query_dimension),
             K(document_dimension));
    FORWARD_USER_ERROR(
        ret, "embedding proxy vector dimensions or values are invalid");
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase
