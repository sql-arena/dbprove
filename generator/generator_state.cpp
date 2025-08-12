#include "generator_state.h"
#include <plog/Log.h>
#include "generated_table.h"

namespace generator {
std::map<std::string, GeneratedTable*>&
available_tables() {
  static std::map<std::string, GeneratedTable*> registry;
  return registry;
}


GeneratorState::GeneratorState(const std::filesystem::path& basePath)
  : basePath_(basePath) {
}

GeneratorState::~GeneratorState() {
}

bool GeneratorState::contains(std::string_view name) const {
  auto lower_name = to_lower(name);
  return available_tables().contains(lower_name);
}


sql::RowCount generator::GeneratorState::generate(const std::string_view name) {
  const auto low_name = to_lower(name);
  if (!contains(low_name)) {
    throw std::runtime_error(
        "Generator not found for table: " + std::string(name));
  }

  const auto file_name = csvPath(low_name);
  if (exists(file_name)) {
    PLOGI << "Table: " << name << " input already exists.";
    table(name).path = file_name;
  } else {
    PLOGI << "Table: " << name << " input data being generated to " + file_name.string() + "...";
    table(name).generator(*this);
    if (!contains(name)) {
      throw std::runtime_error("After generation the table " + std::string(name)
                               + " was not in the map.\n"
                               + "This likely means the generator forgot to call registerGeneration");
    }
    PLOGI << "Table: " << name << " input successfully generated";
  }

  return table(name).row_count;
}

sql::RowCount GeneratorState::load(std::string_view name, sql::ConnectionBase& conn) {
  const auto existing_rows = conn.tableRowCount(name);

  if (!existing_rows) {
    PLOGI << "Table: " << name << " does not exist. Constructing it from DDL";
    const auto table_ddl = table(name).ddl;
    conn.executeDdl(table_ddl);
  }
  const auto expected_rows = table(name).row_count;
  if (existing_rows.value_or(0) == 0) {
    PLOGI << "Table: " << name
    << " exists but has no rows. Loading it from file: " << table(name).path.string() << "...";
    conn.bulkLoad(name, {table(name).path});
    PLOGI << "Table: " << name
    << " is ready with " + std::to_string(existing_rows.value()) + " rows";
    return table(name).row_count;
  }
  if (existing_rows.value() != expected_rows) {
    std::string wrong_row_count_error = "Table: " + std::string(name)
                                        + " has " + std::to_string(existing_rows.value()) + " rows. ";
    wrong_row_count_error += "Which does not match the expected " + std::to_string(table(name).row_count);
    PLOGE << wrong_row_count_error;
    throw std::runtime_error(wrong_row_count_error);
  }

  PLOGI << "Table: " << name
    << " is already in the database with the correct count of: " + std::to_string(existing_rows.value()) + " rows";

  return existing_rows.value();
}

void GeneratorState::registerGeneration(const std::string_view name, const std::filesystem::path& path) {
  table(name).path = path;
  table(name).is_generated = true;
}

GeneratedTable& GeneratorState::table(
    const std::string_view name) const {
  auto lower_name = to_lower(name);
  if (!contains(lower_name)) {
    throw std::runtime_error(
        "Table not found: " + std::string(name) +
        ". Did you forget to call or include the appropriate REGISTER_GENERATOR");
  }
  return *available_tables().at(lower_name);
}

Registrar::Registrar(const std::string_view name, const std::string_view ddl, const GeneratorFunc& f,
                     const sql::RowCount rows) {
  auto lower_name = to_lower(name);
  available_tables().emplace(lower_name, new GeneratedTable{lower_name, ddl, f, rows});
}
}