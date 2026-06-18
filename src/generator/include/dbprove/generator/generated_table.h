#pragma once
#include "generator_state.h"
#include <dbprove/sql/sql.h>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace generator {
class GeneratedTable {
public:
  GeneratedTable(const std::string_view name,
                 const std::string_view dataset,
                 const std::string_view ddl,
                 const sql::RowCount row_count,
                 const size_t expected_file_count,
                 TableMetadata metadata = {})
    : name(name)
    , dataset(dataset)
    , ddl(ddl)
    , row_count(row_count)
    , expected_file_count(expected_file_count)
    , metadata(std::move(metadata))
  {
  }
  bool is_generated = false;
  const std::string name;
  const std::string dataset;
  const std::string_view ddl;
  sql::RowCount row_count;
  const size_t expected_file_count;
  const TableMetadata metadata;
  std::vector<std::filesystem::path> csv_paths; ///< Where the CSV input files are stored
  std::vector<std::filesystem::path> parquet_paths; ///< Where the parquet version of the input files are stored
};
};
