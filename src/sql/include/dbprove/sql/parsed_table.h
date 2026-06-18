#pragma once

#include "sql_type.h"

#include <string>
#include <string_view>
#include <vector>

namespace sql {
struct ColumnDdl {
  std::string name;
  SqlTypeMeta type;
  bool is_null = false;
};

class ParsedTable {
public:
  explicit ParsedTable(std::string_view ddl);

  [[nodiscard]] const std::string& tableName() const {
    return table_name_;
  }

  [[nodiscard]] const std::vector<ColumnDdl>& columns() const {
    return columns_;
  }

private:
  std::string table_name_;
  std::vector<ColumnDdl> columns_;
};
}
