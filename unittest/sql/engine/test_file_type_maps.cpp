#include "sql/engine/expr/ob_expr_type_map.h"
#include "query/parser/ob_item_type.h"
#include <cstdio>

namespace oceanbase
{
using namespace common;

#include "sql/engine/expr/ob_expr_merge_result_type.map"
#include "sql/engine/expr/ob_expr_relational_result_type.map"
#include "sql/engine/expr/ob_expr_relational_cmp_type.map"
#include "sql/engine/expr/ob_expr_relational_equal_type.map"
#include "sql/engine/expr/ob_expr_arithmetic_result_type.map"
#include "sql/engine/expr/ob_expr_div_result_type.map"
#include "sql/engine/expr/ob_expr_int_div_result_type.map"
#include "sql/engine/expr/ob_expr_mod_result_type.map"
#include "sql/engine/expr/ob_expr_abs_result_type.map"
#include "sql/engine/expr/ob_expr_neg_result_type.map"
#include "sql/engine/expr/ob_expr_round_result_type.map"
#include "sql/engine/expr/ob_expr_sum_result_type.map"

static_assert(static_cast<int>(T_FILE) == static_cast<int>(ObFileType),
              "FILE DDL and native values must use the same type tag");

template <size_t N>
bool check_matrix(const ObObjType (&extended)[ObMaxType][ObMaxType],
                  const ObObjType (&legacy)[N][N], const char *name, const bool merge = false)
{
  for (size_t i = 0; i < ObMaxType; ++i) {
    for (size_t j = 0; j < ObMaxType; ++j) {
      const ObObjType expected = i < N && j < N ? legacy[i][j]
          : merge && (i == ObFileType || i == ObNullType)
                  && (j == ObFileType || j == ObNullType) ? ObFileType : ObMaxType;
      if (extended[i][j] != expected) {
        std::fprintf(stderr, "FAIL %s[%zu][%zu]: got %d, expected %d\n",
                     name, i, j, extended[i][j], expected);
        return false;
      }
    }
  }
  return true;
}

template <size_t N>
bool check_vector(const ObObjType (&extended)[ObMaxType], const ObObjType (&legacy)[N],
                  const char *name)
{
  for (size_t i = 0; i < ObMaxType; ++i) {
    const ObObjType expected = i < N ? legacy[i] : ObMaxType;
    if (extended[i] != expected) {
      std::fprintf(stderr, "FAIL %s[%zu]: got %d, expected %d\n",
                   name, i, extended[i], expected);
      return false;
    }
  }
  return true;
}

} // namespace oceanbase

int main()
{
  using namespace oceanbase;
  const bool passed = check_matrix(MERGE_RESULT_TYPE, LEGACY_MERGE_RESULT_TYPE, "MERGE", true)
      && check_matrix(RELATIONAL_RESULT_TYPE, LEGACY_RELATIONAL_RESULT_TYPE, "RELATIONAL")
      && check_matrix(RELATIONAL_CMP_TYPE, LEGACY_RELATIONAL_CMP_TYPE, "CMP")
      && check_vector(RELATIONAL_EQUAL_TYPE, LEGACY_RELATIONAL_EQUAL_TYPE, "EQUAL")
      && check_matrix(ARITH_RESULT_TYPE, LEGACY_ARITH_RESULT_TYPE, "ARITHMETIC")
      && check_matrix(DIV_RESULT_TYPE, LEGACY_DIV_RESULT_TYPE, "DIV")
      && check_matrix(INT_DIV_RESULT_TYPE, LEGACY_INT_DIV_RESULT_TYPE, "INT_DIV")
      && check_matrix(MOD_RESULT_TYPE, LEGACY_MOD_RESULT_TYPE, "MOD")
      && check_vector(ABS_RESULT_TYPE, LEGACY_ABS_RESULT_TYPE, "ABS")
      && check_vector(NEG_RESULT_TYPE, LEGACY_NEG_RESULT_TYPE, "NEG")
      && check_vector(ROUND_RESULT_TYPE, LEGACY_ROUND_RESULT_TYPE, "ROUND")
      && check_vector(SUM_RESULT_TYPE, LEGACY_SUM_RESULT_TYPE, "SUM");
  if (passed) {
    std::puts("PASS: all twelve FILE type maps and every pre-FILE type entry");
  }
  return passed ? 0 : 1;
}
