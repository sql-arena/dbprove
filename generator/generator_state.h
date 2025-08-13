#pragma once
#include <assert.h>
#include <filesystem>
#include <map>
#include <functional>
#include <set>

#include "sql/connection_base.h"
#include "sql/sql_type.h"

namespace sql {
class ConnectionFactory;
}

namespace generator {
class GeneratorState;
class GeneratedTable;
using GeneratorFunc = std::function<void(GeneratorState&)>;


class GeneratorState {
  friend struct Registrar;
  const std::filesystem::path basePath_;
  static constexpr std::string_view colSeparator_ = "|";
  static constexpr std::string_view rowSeparator_ = "\n";
  std::set<std::string_view> ready_tables_ = {};
public:
  explicit GeneratorState(const std::filesystem::path& basePath);
  ~GeneratorState();

  /**
   * Makes sure that a table is availabe on the given connection
   * @param table_name To guarantee exists and is ready
   * @param conn Connection to generate the table at
   * @return Rowcount of the generated table
   */
  void ensure(std::string_view table_name, sql::ConnectionFactory& conn);

  /**
   * Makes sure that a table is availabe on the given connection
   * @param table_names To guarantee exists and is ready
   * @param conn Connection to generate the table at
   * @return Rowcount of the generated table
   */
  void ensure(const std::span<std::string_view>& table_names, sql::ConnectionFactory& conn);

  /**
   * Generate a table input (if not already made) and return the row count
   */
  sql::RowCount generate(std::string_view table_name);
  /**
   * Load a table
   * @param table_name Table to load
   * @return Number of rows in the table
   */
  sql::RowCount load(std::string_view table_name, sql::ConnectionBase& conn);
  /**
   * Lets the state know that we have made the input files necessary for the generation
   * @param table_name The table we have generated
   * @param path The path where the input file can be found
   */
  void registerGeneration(const std::string_view table_name, const std::filesystem::path& path);
  GeneratedTable& table(std::string_view table_name) const;
  bool contains(std::string_view table_name) const;
  [[nodiscard]] const std::filesystem::path& basePath() const { return basePath_; }
  [[nodiscard]] std::filesystem::path csvPath(const std::string_view table_name) const {
    const std::string file_name = std::string(table_name) + ".csv";
    return basePath_ / file_name;
  }
  static constexpr std::string_view columnSeparator() { return colSeparator_; }
  static constexpr std::string_view rowSeparator() { return rowSeparator_; }
};


struct Registrar {
  Registrar(std::string_view table_name, const std::string_view ddl, const GeneratorFunc& f, sql::RowCount rows);
};
}

/**
 * Macros to register generator functions.
 *
 *  Usage:
 *      REGISTER_GENERATOR("<name>", <funcPtr>);
 *
 */

#define CONCATENATE_DETAIL(x, y) x##y
#define CONCATENATE(x, y) CONCATENATE_DETAIL(x, y)

#define REGISTER_GENERATOR(NAME, DDL, FUNC, ROWS) \
    extern void FUNC(generator::GeneratorState&); \
    static inline generator::Registrar CONCATENATE(_registrar_, __COUNTER__)(NAME, DDL, FUNC, ROWS);
