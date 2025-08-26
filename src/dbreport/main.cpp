#include "dbreport/embedded_sql.h"
#include <dbprove/common/log_formatter.h>
#include <dbprove/common/file_utility.h>
#include <dbprove/sql/sql.h>


#include <plog/Log.h>
#include <CLI/CLI.hpp>

#include <iostream>
#include <filesystem>
#include <vector>
#include <algorithm>
#include <regex>
#include <string>
#include <pugixml.hpp>


namespace fs = std::filesystem;

void TerminateHandler() {
  try {
    const auto e = std::current_exception();
    if (e) {
      std::rethrow_exception(e);
    }
  } catch (const std::exception& e) {
    std::cerr << "Fatal Error:" << std::endl << e.what() << std::endl;
  } catch (...) {
    std::cerr << "Unhandled unknown exception\n";
  }
  std::exit(1);
}

using namespace pugi;

struct Data {
  std::unique_ptr<sql::ResultBase> result;
  std::vector<std::string> engines;
};

void writeData(const sql::RowBase& row, const size_t column, xml_node& td) {
  const auto unit = row.asString(row.columnCount() - 1);

  if (unit == "Plan") {
    td.append_child("pre").text().set(row.asString(column));
  } else {
    td.text().set(row.asString(column));
  }
}


void writeHtmlReport(const fs::path& input_path, Data& result) {
  xml_document doc;

  auto decl = doc.append_child(pugi::node_declaration);
  decl.set_name("html");

  // Create the root HTML element
  auto html = doc.append_child("html");
  html.append_child("head").append_child("title").text().set("Comparison Report");
  auto body = html.append_child("body");
  body.append_child("h1").text().set("Comparison Report");

  auto table = body.append_child("table");

  auto thead = table.append_child("thread");
  thead.append_child("th").text().set("Proof");
  for (auto& engine : result.engines) {
    thead.append_child("th").text().set(engine);
  }
  thead.append_child("th").text().set("Unit");
  auto tbody = table.append_child("tbody");
  std::string last_theorem;
  // TODO: It would be very nice if rows has name based indexers
  for (auto& row : result.result->rows()) {
    auto theorem = row.asString(1);
    if (theorem != last_theorem) {
      auto tr = tbody.append_child("tr");
      auto theorem_header = tr.append_child("td");
      theorem_header.append_attribute("class") = "theorem_header";
      theorem_header.append_attribute("colspan") = 2 + result.engines.size();
      theorem_header.append_child("b").append_child(node_pcdata).set_value(theorem);
      theorem_header.append_child("br");
      auto description = row.asString(2);
      theorem_header.append_child(node_pcdata).set_value(description);
    }

    last_theorem = std::move(theorem);
    auto tr = tbody.append_child("tr");
    tr.append_child("td").text().set(row.asString(3));
    for (size_t e = 0; e < result.engines.size(); ++e) {
      auto engine = result.engines[e];
      auto td = tr.append_child("td");
      writeData(row, 4 + e, td);
    }
    tr.append_child("td").text().set(row.asString(row.columnCount() - 1));
  }

  std::ofstream report_file(input_path / "report.html");
  doc.save(report_file, "  ");
  report_file.close();
}

/**
 * Find .CSV files needed to report
 * @param input_path
 * @return
 */
std::vector<fs::path> findInputFiles(const fs::path& input_path) {
  std::vector<fs::path> csvFiles;

  for (const auto& entry : fs::directory_iterator(input_path)) {
    if (!entry.is_regular_file()) {
      continue;
    }
    std::string extension = entry.path().extension().string();
    std::ranges::transform(extension, extension.begin(), ::tolower);
    if (extension == ".csv") {
      csvFiles.push_back(entry.path());
    }
  }
  return csvFiles;
}

std::unique_ptr<sql::ConnectionBase> connection(const fs::path& input_path) {
  const auto duck_file = (input_path / "dbreport.duck").string();
  sql::ConnectionFactory factory(sql::Engine("DuckDb"), sql::CredentialFile(duck_file));
  return factory.create();
}

void loadInputFiles(sql::ConnectionBase& conn, const std::vector<fs::path>& input_files) {
  conn.execute(resource::proof_sql);

  for (const auto& file : input_files) {
    PLOGI << "Loading " << file.string();
    conn.bulkLoad("proof", file);
  }
}

Data joinFiles(sql::ConnectionBase& conn) {
  const auto result = conn.fetchAll(resource::engines_sql);
  std::string first_engine;
  std::vector<std::string> engine_fragments;
  std::vector<std::string> engine_names;
  for (auto& row : result->rows()) {
    auto engine_name = row[0].get<sql::SqlString>().get();
    if (first_engine.empty()) {
      first_engine = engine_name;
    }
    engine_names.push_back(engine_name);
    auto engine_data = std::string(resource::engine_data_sql);
    engine_data = std::regex_replace(engine_data, std::regex("<engine>"), engine_name);
    engine_fragments.push_back(engine_data);
  }
  std::string sql = "SELECT ";
  sql += first_engine + ".theorem_type\n";
  sql += ", " + first_engine + ".theorem\n";
  sql += ", " + first_engine + ".description\n";
  sql += ", " + first_engine + ".proof\n";
  for (const auto& engine_name : engine_names) {
    sql += ", " + engine_name + ".value";
  }
  sql += ", " + first_engine + ".unit\n";
  sql += "FROM ";
  bool first = true;
  for (const auto& fragment : engine_fragments) {
    if (!first) {
      sql += "INNER JOIN ";
    }
    sql += fragment + "\n";
    if (!first) {
      sql += "\n  USING (theorem_type, theorem, proof)\n";
    }
    first = false;
  }
  sql += "\nORDER BY " + first_engine + ".id";
  PLOGI << "Running SQL to generate report:\n" << sql;
  Data d{conn.fetchAll(sql), engine_names};
  return d;
}

int main(const int argc, char** argv) {
  std::set_terminate(TerminateHandler);
  CLI::App app{"dbreport"};
  std::string input_arg;
  app.set_help_flag("-?", "--help");
  app.add_option("-i, --input", input_arg);
  CLI11_PARSE(app, argc, argv);

  const fs::path input_directory(input_arg);
  if (!fs::exists(input_directory)) {
    std::cout << "The specified path does not exist: " << input_directory.string();
    return 1;
  }
  if (!fs::is_directory(input_directory)) {
    std::cout << "The specified path is not a directory: " << input_directory.string() << std::endl;
    return 1;
  }

  PLOGI << "Loading from directory: " << input_directory.string();

  const auto log_directory = dbprove::common::make_directory("logs");
  const std::string log_file = log_directory.string() + "/dbreport.log";
  plog::init<plog::DBProveFormatter>(plog::info, log_file.c_str(), 1000000, 5);

  const auto input_csv = findInputFiles(input_directory);

  const auto conn = connection(input_directory);
  loadInputFiles(*conn, input_csv);

  auto data = joinFiles(*conn);
  writeHtmlReport(input_directory, data);
  return 0;
}