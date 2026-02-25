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
  explicit GeneratorState(const std::filesystem::path& basePath, CloudProvider dataProvider, std::string dataPath);
  ~GeneratorState();

  void downloadFromCloud(std::string_view schemaName, std::string_view tableName, std::string_view relativePath);

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
  void ensure(const std::span<std::string_view>& table_names, sql::ConnectionFactory& conn);

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
   * Create the keys needed on this table.
   *
   * For database drives that don't support keys, this is a NOOP
   * @param table_name Table to declare keys on. Will also create keys on any tables (that already exist) which reference it
   * @param conn Connection to use
   */
  void declareKeys(const std::string_view table_name, sql::ConnectionBase& conn) const;
  GeneratedTable& table(std::string_view table_name) const;
  static bool contains(std::string_view table_name);
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

struct KeyRegistrar {
  KeyRegistrar(std::string_view fk_table_name,
               const std::vector<std::string_view>& fk_column_names,
               std::string_view pk_table_name,
               const std::vector<std::string_view>& pk_column_names
      );
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


#define REGISTER_GENERATOR(NAME, DDL, FUNC, ROWS) \
    extern void FUNC(generator::GeneratorState&, sql::ConnectionBase*); \
    static inline generator::Registrar CONCATENATE(_registrar_, __COUNTER__)(NAME, DDL, FUNC, ROWS);


#define REGISTER_FK(FK_TABLE, FK_COLUMNS, PK_TABLE, PK_COLUMNS) \
  static inline generator::KeyRegistrar CONCATENATE(_key_registrar_, __COUNTER__)( \
    FK_TABLE, BRACED(UNPARENTHESIS(FK_COLUMNS)), \
    PK_TABLE, BRACED(UNPARENTHESIS(PK_COLUMNS))  \
    );
