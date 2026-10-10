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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TYPE_MAP_H_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TYPE_MAP_H_

#include "common/object/ob_obj_type.h"
#include <cstddef>

namespace oceanbase
{

template <typename T, std::size_t row, std::size_t col>
constexpr bool is_array_fully_initialized(const T (&arr)[row][col], int row_depth)
{
  return row_depth == 0 ? true : arr[row_depth - 1][col - 1] != T()
      && is_array_fully_initialized(arr, row_depth - 1);
}

template <typename T, std::size_t col>
constexpr bool is_array_fully_initialized(const T (&arr)[col])
{
  return arr[col - 1] != T();
}

namespace sql
{

struct ExprTypeMatrix
{
  common::ObObjType values_[common::ObMaxType][common::ObMaxType];
};

struct ExprTypeVector
{
  common::ObObjType values_[common::ObMaxType];
};

template <size_t N>
constexpr ExprTypeMatrix extend_file_type_map(const common::ObObjType (&legacy)[N][N],
                                             const bool allow_file_merge = false)
{
  static_assert(N == common::ObFileType, "the legacy SQL type map must cover every pre-FILE type");
  ExprTypeMatrix result{};
  for (size_t i = 0; i < common::ObMaxType; ++i) {
    for (size_t j = 0; j < common::ObMaxType; ++j) {
      result.values_[i][j] = i < N && j < N ? legacy[i][j] : common::ObMaxType;
    }
  }
  if (allow_file_merge) {
    result.values_[common::ObNullType][common::ObFileType] = common::ObFileType;
    result.values_[common::ObFileType][common::ObNullType] = common::ObFileType;
    result.values_[common::ObFileType][common::ObFileType] = common::ObFileType;
  }
  return result;
}

template <size_t N>
constexpr ExprTypeVector extend_file_type_map(const common::ObObjType (&legacy)[N])
{
  static_assert(N == common::ObFileType, "the legacy SQL type map must cover every pre-FILE type");
  ExprTypeVector result{};
  for (size_t i = 0; i < common::ObMaxType; ++i) {
    result.values_[i] = i < N ? legacy[i] : common::ObMaxType;
  }
  return result;
}

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TYPE_MAP_H_
