#include <dbprove/theorem/theorem.h>
#include <dbprove/ux/ux.h>
#include <dbprove/sql/sql.h>
#include <dbprove/common/null_stream.h>
#include <dbprove/common/log_formatter.h>
#include <dbprove/common/file_utility.h>
#include <CLI/CLI.hpp>
#include <iostream>
#include <string>
#include <version>
#include <format>
#include <ranges>
#include <plog/Log.h>
#include <plog/Initializers/RollingFileInitializer.h>

namespace fs = std::filesystem;

using namespace dbprove;

void TerminateHandler() {
  try {
    auto eptr = std::current_exception();
    if (eptr) {
      std::rethrow_exception(eptr);
    }
  } catch (const std::exception& e) {
    std::cerr << "Fatal Error:" << std::endl << e.what() << std::endl;
  } catch (...) {
    std::cerr << "Unhandled unknown exception\n";
  }
  std::exit(1);
}


sql::Credential parseCredentials(
    const sql::Engine& engine,
    const std::string& host,
    const uint16_t port,
    const std::string& database,
    const std::optional<std::string>& username,
    const std::optional<std::string>& password,
    const std::optional<std::string>& token) {
  const auto engine_name = engine.name();

  try {
    switch (engine.type()) {
      case sql::Engine::Type::MariaDB:
      case sql::Engine::Type::Postgres:
      case sql::Engine::Type::SQLServer:
      case sql::Engine::Type::Yellowbrick:
      case sql::Engine::Type::Oracle: {
        if (!username) {
          throw std::invalid_argument("Username is required for " + engine_name);
        }
        return sql::CredentialPassword(host, database, port, username.value(), password);
      }
      case sql::Engine::Type::Utopia:
        return sql::CredentialNone();
      case sql::Engine::Type::DuckDB:
        return sql::CredentialFile(database);
      case sql::Engine::Type::Databricks: {
        if (!token) {
          throw std::invalid_argument("Token is required for " + engine_name);
        }
        return sql::CredentialAccessToken(engine, host, database, token.value());
      }
    default:
      throw std::invalid_argument("Cannot generate credentials for engine: " + engine_name);
    }
  } catch (const std::invalid_argument& e) {
    std::cerr << e.what() << std::endl;
    std::exit(1);
  }
}

generator::GeneratorState configureDataGeneration() {
  return generator::GeneratorState("./table_data", CloudProvider::GCS, "gs://sql-arena-data");
}

int main(int argc, char** argv) {
  std::set_terminate(TerminateHandler);

  CLI::App app{"dbprove"};

  std::string platform;
  std::optional<std::string> database = std::nullopt;
  std::optional<std::string> username = std::nullopt;
  std::optional<std::string> password = std::nullopt;
  std::optional<std::string> host = std::nullopt;
  std::optional<std::string> token = std::nullopt;
  std::vector<std::string> all_theorems;
  std::string engine_arg;
  uint32_t port;

  app.set_help_flag("-?", "--help");
  app.add_option(
      "-e, --engine",
      engine_arg);

  app.add_option("-h,--host",
                 host, "Host or endpoint");
  app.add_option("-p,--port",
                 port, "Port to use")->default_val(0);
  app.add_option("-d,--database",
                 database, "Database to use");
  app.add_option("-U,--username",
                 username, "Username ");
  app.add_option("-P,--password",
                 password, "Password. If omitted, will prompt");
  app.add_option("-t,--access-token",
                 token, "Access Token");
  app.add_option("-T,--theorem",
                 all_theorems, "Which theorems to prove")->delimiter(',');

  CLI11_PARSE(app, argc, argv);

  const auto log_directory = dbprove::common::make_directory("logs");
  const std::string log_file = log_directory.string() + "/dbprove.log";
  plog::init<plog::DBProveFormatter>(plog::info, log_file.c_str(), 1000000, 5);

  const sql::Engine engine(engine_arg);
  ux::Terminal::configure();

  auto generator_state = configureDataGeneration();

  PLOGI << "Generating into directory: " << generator_state.basePath();

  database = engine.defaultDatabase(database);
  host = engine.defaultHost(host);
  port = engine.defaultPort(port);
  username = engine.defaultUsername(username);
  password = engine.defaultPassword(password);
  token = engine.defaultToken(token);

  PLOGI << "Using engine: " << engine.name();
  PLOGI << "  host      : " << host.value();
  PLOGI << "  port      : " << port;
  PLOGI << "  database  : " << database.value();

  auto credentials =
      engine.parseCredentials(
          host.value(),
          port,
          database.value(),
          username,
          password,
          token);
  theorem::init();
  auto theorems = theorem::parse(all_theorems);

  const auto proof_directory = common::make_directory("proof");
  const std::string proof_file = proof_directory.string() + "/" + engine.name() + "_proof.csv";
  std::ofstream proof_output_stream(proof_file);
  if (!proof_output_stream.is_open()) {
    throw std::runtime_error("Failed to open proof file for CSV dumping: " + proof_file);
  }

  auto input_state = theorem::RunCtx{engine, credentials, generator_state,
                                     std::cout, proof_output_stream};

  theorem::prove(theorems, input_state);
}