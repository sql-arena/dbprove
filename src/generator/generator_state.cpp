#include "generator_state.h"
#include <plog/Log.h>
#include "generated_table.h"
#include "sql/connection_factory.h"

namespace generator {
std::map<std::string_view, GeneratedTable*>&
available_tables() {
  static std::map<std::string_view, GeneratedTable*> registry;
  return registry;
}

GeneratorState::GeneratorState(const std::filesystem::path& basePath)
  : basePath_(basePath) {
}

GeneratorState::~GeneratorState() {
}

void GeneratorState::ensure(const std::string_view table_name, sql::ConnectionFactory& conn) {
  std::vector table_names{table_name};
  ensure(std::span(table_names), conn);
}

void GeneratorState::ensure(const std::span<std::string_view>& table_names, sql::ConnectionFactory& conn) {
  const auto cn = conn.create();
  for (auto table_name : table_names) {
    sql::checkTableName(table_name);
    if (ready_tables_.contains(table_name)) {
      continue;
    }
    generate(table_name);
    load(table_name, *cn);
  }
}

bool GeneratorState::contains(std::string_view table_name) const {
  sql::checkTableName(table_name);
  return available_tables().contains(table_name);
}


sql::RowCount generator::GeneratorState::generate(const std::string_view table_name) {
  sql::checkTableName(table_name);
  if (!contains(table_name)) {
    throw std::runtime_error(
        "Generator not found for table: " + std::string(table_name));
  }

  const auto file_name = csvPath(table_name);
  if (exists(file_name)) {
    PLOGI << "Table: " << table_name << " input already exists.";
    table(table_name).path = file_name;
  } else {
    PLOGI << "Table: " << table_name << " input data being generated to " + file_name.string() + "...";
    table(table_name).generator(*this);
    PLOGI << "Table: " << table_name << " input successfully generated";
  }

  return table(table_name).row_count;
}

sql::RowCount GeneratorState::load(const std::string_view table_name, sql::ConnectionBase& conn) {
  sql::checkTableName(table_name);
  const auto existing_rows = conn.tableRowCount(table_name);
  if (!existing_rows) {
    PLOGI << "Table: " << table_name << " does not exist. Constructing it from DDL";
    const auto table_ddl = table(table_name).ddl;
    conn.executeDdl(table_ddl);
  }
  const auto expected_rows = table(table_name).row_count;
  if (existing_rows.value_or(0) == 0) {
    PLOGI << "Table: " << table_name
    << " exists but has no rows. Loading it from file: " << table(table_name).path.string() << "...";
    conn.bulkLoad(table_name, {table(table_name).path});
    const auto generated_row_count = table(table_name).row_count;
    PLOGI << "Table: " << table_name
    << " is ready with " + std::to_string(generated_row_count) + " rows";
    return generated_row_count;
  }
  if (existing_rows.value() < 0.9 * expected_rows || existing_rows.value() > 1.1 * expected_rows) {
    std::string wrong_row_count_error = "Table: " + std::string(table_name)
                                        + " has " + std::to_string(existing_rows.value()) + " rows. ";
    wrong_row_count_error += "Which does not match the expected " + std::to_string(table(table_name).row_count);
    PLOGE << wrong_row_count_error;
    throw std::runtime_error(wrong_row_count_error);
  }

  PLOGI << "Table: " << table_name
    << " is already in the database with the correct count of: " + std::to_string(existing_rows.value()) + " rows";

  ready_tables_.emplace(table_name);
  return existing_rows.value();
}

void GeneratorState::registerGeneration(const std::string_view table_name, const std::filesystem::path& path) {
  sql::checkTableName(table_name);
  table(table_name).path = path;
  table(table_name).is_generated = true;
}

GeneratedTable& GeneratorState::table(
    const std::string_view table_name) const {
  sql::checkTableName(table_name);
  if (!contains(table_name)) {
    throw std::runtime_error(
        "Table not found: " + std::string(table_name) +
        ". Did you forget to call or include the appropriate REGISTER_GENERATOR");
  }
  return *available_tables().at(table_name);
}

Registrar::Registrar(const std::string_view table_name, const std::string_view ddl, const GeneratorFunc& f,
                     const sql::RowCount rows) {
  sql::checkTableName(table_name);
  available_tables().emplace(table_name, new GeneratedTable{table_name, ddl, f, rows});
}
}