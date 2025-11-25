#pragma once
#include "connection_base.h"
#include "explain/column.h"
#include "expression.h"

namespace sql {
struct ForeignKey {
  std::string_view fk_table_name;
  std::vector<std::string_view> fk_columns;
  std::string_view pk_table_name;
  std::vector<std::string_view> pk_columns;
};

struct Table {
  std::string schema_name;
  std::string table_name;
};


Table splitTable(std::string_view table_name);
}

