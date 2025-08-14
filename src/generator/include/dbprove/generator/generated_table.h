#pragma once
#include "generator_state.h"
#include <dbprove/sql/sql.h>
#include <filesystem>
#include <string_view>

namespace generator {
class GeneratorState;
class GeneratedTable {
public:
  GeneratedTable(const std::string_view name,
                 const std::string_view ddl,
                 const GeneratorFunc& generator,
                 const sql::RowCount row_count)
    : name(name)
    , ddl(ddl)
    , generator(generator)
    , row_count(row_count)
  {
  }
  bool is_generated = false;
  const std::string_view name;
  const std::string_view ddl;
  const GeneratorFunc generator;
  sql::RowCount row_count;
  std::filesystem::path path; ///< Where the CSV input of the generated files are stored

};
};