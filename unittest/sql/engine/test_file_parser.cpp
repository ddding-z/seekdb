#include "query/parser/parse_node.h"
#include "lib/alloc/ob_iallocator.h"
#include "lib/charset/ob_ctype.h"
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

using namespace oceanbase::common;

namespace
{

class ParserTestAllocator final : public ObIAllocator
{
public:
  ~ParserTestAllocator() override
  {
    for (void *pointer : pointers_) {
      std::free(pointer);
    }
  }

  void *alloc(const int64_t size) override
  {
    void *pointer = std::malloc(size);
    if (pointer != nullptr) {
      pointers_.push_back(pointer);
    }
    return pointer;
  }
  void *alloc(const int64_t size, const ObMemAttr &) override { return alloc(size); }

  void free(void *pointer) override
  {
    if (pointer == nullptr) {
      return;
    }
    const auto found = std::find(pointers_.begin(), pointers_.end(), pointer);
    if (found == pointers_.end()) {
      std::fputs("FAIL: freeing an unknown FILE parser allocation\n", stderr);
      std::abort();
    }
    std::free(pointer);
    pointers_.erase(found);
  }

private:
  std::vector<void *> pointers_;
};

int count_file_types(const ParseNode *node)
{
  int count = 0;
  if (node != nullptr) {
    count = node->type_ == T_FILE ? 1 : 0;
    for (int32_t i = 0; i < node->num_child_; ++i) {
      count += count_file_types(node->children_[i]);
    }
  }
  return count;
}

struct ParserCase
{
  const char *sql_;
  int file_types_;
  bool succeeds_;
};

} // namespace

extern "C" int lookup_pl_symbol(const void *, const char *, size_t, int64_t *)
{
  std::fputs("FAIL: ordinary FILE SQL entered the PL symbol lookup hook\n", stderr);
  std::abort();
}

int main()
{
  const ParserCase cases[] = {
      {"CREATE TABLE file (file FILE, id BIGINT PRIMARY KEY)", 1, true},
      {"CREATE TABLE FILE (FILE FiLe DEFAULT NULL)", 1, true},
      {"CREATE TABLE file (file INT)", 0, true},
      {"ALTER TABLE file ADD COLUMN file FILE", 1, true},
      {"ALTER TABLE file MODIFY COLUMN file FILE NULL", 1, true},
      {"SELECT file FROM file", 0, true},
      {"SELECT file.file AS file FROM file AS file", 0, true},
      {"SELECT 1 AS file", 0, true},
      {"SELECT FILE_URI(file), FILE_META(file), TO_FILE('/media/a.png') FROM file", 0, true},
      {"SELECT COALESCE(file, TO_FILE('/media/a.png')) FROM file", 0, true},
      {"CREATE TABLE file (file FILE(16384))", 0, false},
      {"CREATE TABLE file (file FILE UNSIGNED)", 0, false},
  };
  for (const auto &test : cases) {
    ParserTestAllocator allocator;
    ParseResult result{};
    result.malloc_pool_ = &allocator;
    result.charset_info_ = &ob_charset_utf8mb4_bin;
    result.charset_info_nls_db_ = &ob_charset_utf8mb4_bin;
    result.connection_collation_ = ob_charset_utf8mb4_bin.number;
    int ret = parse_init(&result);
    if (ret != 0) {
      std::fprintf(stderr, "FAIL initializing FILE parser test: %d\n", ret);
      return 1;
    }
    ret = parse_sql(&result, test.sql_, std::strlen(test.sql_));
    const bool valid = (ret == 0) == test.succeeds_
        && (!test.succeeds_ || count_file_types(result.result_tree_) == test.file_types_);
    const int terminate_ret = parse_terminate(&result);
    if (!valid || terminate_ret != 0) {
      std::fprintf(stderr, "FAIL FILE parser: %s; ret=%d, terminate=%d, message=%s\n",
                   test.sql_, ret, terminate_ret, result.error_msg_);
      return 1;
    }
  }
  std::puts("PASS: twelve FILE DDL and nonreserved identifier parser contracts");
  return 0;
}
