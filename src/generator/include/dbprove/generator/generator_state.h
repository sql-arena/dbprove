#pragma once
#include <dbprove/sql/sql.h>
#include <filesystem>
#include <functional>
#include <set>
#include <dbprove/common/cloud_provider.h>


namespace sql {
class ConnectionFactory;
}

namespace generator {
class GeneratorState;
class GeneratedTable;
using GeneratorFunc = std::function<void(GeneratorState&, sql::ConnectionBase*)>;


class GeneratorState {
  friend struct Registrar;
  const sql::Engine engine_;
  const std::filesystem::path basePath_;
  const CloudProvider dataProvider_;
  const std::string dataPath_;
  static constexpr std::string_view colSeparator_ = "|";
  static constexpr std::string_view rowSeparator_ = "\n";

  struct TransparentLess {
    using is_transparent = void;

    bool operator()(const std::string_view lhs, const std::string_view rhs) const {
      return lhs < rhs;
    }
  };


  std::set<std::string, TransparentLess> ready_tables_ = {};

public:
  explicit GeneratorState(const sql::Engine& engine, const std::filesystem::path& basePath, CloudProvider dataProvider = CloudProvider::NONE, std::string dataPath = "");
  ~GeneratorState();

  void prepareFileInput(std::string_view schemaName, std::string_view tableName, std::string_view relativePath);

  [[nodiscard]] const sql::Engine& engine() const { return engine_; }
  [[nodiscard]] CloudProvider cloudProvider() const { return dataProvider_; }
  [[nodiscard]] const std::string& dataPath() const { return dataPath_; }

  /**
   * Makes sure that a table is availabe on the given connection
   * @param table_name To guarantee exists and is ready
   * @param conn Connection to generate the table at
   * @return Rowcount of the generated table
   */
  void ensure(std::string_view table_name, sql::ConnectionFactory& conn);

  /**
   * Makes sure that a table is available on the given connection
   * @param table_names To guarantee exists and is ready
   * @param conn Connection to generate the table at
   * @return Rowcount of the generated table
   */
  void ensure(std::span<const std::string_view> table_names, sql::ConnectionFactory& conn);

  /**
   * Makes sure an entire dataset is available on the given connection.
   * @param dataset_name Dataset registered through REGISTER_GENERATOR
   * @param conn Connection factory used for ensure/load operations
   */
  void ensureDataset(std::string_view dataset_name, sql::ConnectionFactory& conn);

  /**
   * Generate a table input (if not already made) and return the row count
   */
  sql::RowCount generate(std::string_view table_name, sql::ConnectionBase* conn = nullptr);
  /**
   * Load a table
   * @param table_name Table to load
   * @return Amount rows in the table
   */
  sql::RowCount load(std::string_view table_name, sql::ConnectionBase& conn);
  /**
   * Lets the state know that we've made the input files necessary for the generation
   * @param table_name The table we've generated
   * @param path The path where the input file can be found
   */
  void registerGeneration(const std::string_view table_name, const std::filesystem::path& path) const;

  /**
   * @brief Prints a summary of all tables that were ensured/generated during this run.
   * @param out The output stream to print to
   */
  void printSummary(std::ostream& out) const;

  GeneratedTable& table(std::string_view table_name) const;
  static bool contains(std::string_view table_name);
  static bool containsDataset(std::string_view dataset_name);
  [[nodiscard]] const std::filesystem::path& basePath() const { return basePath_; }

  [[nodiscard]] std::filesystem::path csvPath(const std::string_view table_name) const {
    const std::string file_name = std::string(table_name) + ".csv";
    return basePath_ / file_name;
  }

  static constexpr std::string_view columnSeparator() { return colSeparator_; }
  static constexpr std::string_view rowSeparator() { return rowSeparator_; }
};


struct Registrar {
  Registrar(std::string_view table_name, std::string_view dataset_name, std::string_view ddl, const GeneratorFunc& f, sql::RowCount rows);
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
#define LIST(...) { __VA_ARGS__ }
#define EXPAND(...) __VA_ARGS__
#define UNPARENTHESIS(x) EXPAND x            // x must be of the form ( ... )
#define BRACED(...) { __VA_ARGS__ }


#define REGISTER_GENERATOR(NAME, DATASET, DDL, FUNC, ROWS) \
    extern void FUNC(generator::GeneratorState&, sql::ConnectionBase*); \
    static inline generator::Registrar CONCATENATE(_registrar_, __COUNTER__)(NAME, DATASET, DDL, FUNC, ROWS);

