#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include "sql/engine/basic/ob_semantic_proxy.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{
int score_record(const std::string &line, std::string &output)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObIJsonBase *document = nullptr;
  ObString input(line.length(), line.data());
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(
          &allocator, input, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE,
          document, ObJsonParser::JSN_STRICT_FLAG))) {
  } else if (nullptr == document || document->json_type() != ObJsonNodeType::J_OBJECT ||
             document->element_count() != 3) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    auto &record = *static_cast<ObJsonObject *>(document);
    ObJsonNode *identity = record.get_value("id");
    ObJsonNode *left = record.get_value("left");
    ObJsonNode *right = record.get_value("right");
    if (nullptr == identity || identity->json_type() == ObJsonNodeType::J_NULL ||
        nullptr == left || nullptr == right) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ObJsonBuffer left_json(&allocator);
      ObJsonBuffer right_json(&allocator);
      ObJsonBuffer identity_json(&allocator);
      float *left_vector = nullptr;
      float *right_vector = nullptr;
      int64_t left_dimension = 0;
      int64_t right_dimension = 0;
      double score = 0;
      if (OB_FAIL(left->print(left_json, true))) {
      } else if (OB_FAIL(right->print(right_json, true))) {
      } else if (OB_FAIL(identity->print(identity_json, true))) {
      } else if (OB_FAIL(SemanticEmbeddingProxy::parse_vector(
                     allocator, left_json.string(), left_vector, left_dimension))) {
      } else if (OB_FAIL(SemanticEmbeddingProxy::parse_vector(
                     allocator, right_json.string(), right_vector, right_dimension))) {
      } else if (OB_FAIL(SemanticEmbeddingProxy::cosine(
                     left_vector, left_dimension, right_vector, right_dimension, score))) {
      } else {
        std::ostringstream result;
        result << "{\"id\":";
        result.write(identity_json.string().ptr(), identity_json.string().length());
        result << ",\"score\":" << std::setprecision(std::numeric_limits<double>::max_digits10)
               << score << "}";
        output = result.str();
      }
    }
  }
  return ret;
}
}

int main(int argc, char **argv)
{
  if (argc == 2 && std::string(argv[1]) == "--contract") {
    std::cout << R"({"version":1,"score_kind":"seekdb_native_raw_cosine_float32",)"
                 R"("range":[-1,1],"reject_comparison":"score<threshold","automatic_accept":false})"
              << std::endl;
    return 0;
  }
  if (argc != 1) {
    std::cerr << "Usage: semantic_proxy_score [--contract]; otherwise JSONL on stdin"
              << std::endl;
    return 2;
  }
  std::string line;
  int64_t row = 0;
  while (std::getline(std::cin, line)) {
    ++row;
    std::string output;
    const int ret = score_record(line, output);
    if (OB_SUCCESS != ret) {
      std::cerr << "Invalid native proxy score input at line " << row << ": " << ret
                << std::endl;
      return 1;
    }
    std::cout << output << std::endl;
    if (!std::cout) {
      std::cerr << "Cannot write native proxy score output" << std::endl;
      return 1;
    }
  }
  if (!std::cin.eof()) {
    std::cerr << "Cannot read native proxy score input" << std::endl;
    return 1;
  }
  return 0;
}
