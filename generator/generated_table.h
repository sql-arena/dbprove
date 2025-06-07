#pragma once
#include <filesystem>
#include <string_view>
#include <vector>
#include <atomic>

namespace generator {
class GeneratedTable {
  const std::string_view name_;
  std::atomic<size_t> row_count_; ///< How many rows have been generated so far
  const std::vector<std::filesystem::path> paths_; ///< Where the CSV input of the generated files are stored

public:
  GeneratedTable(const std::string_view name,
                 const size_t row_count,
                 const std::filesystem::path& path)
    : name_(name)
    , row_count_(row_count)
    , paths_({path}) {
  }

  std::string_view name() const { return name_; }
  size_t rowCount() const { return row_count_; }
  const std::vector<std::filesystem::path>& paths() const { return paths_; }
};
};