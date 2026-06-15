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
#include <plog/Initializers/ConsoleInitializer.h>

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

generator::GeneratorState configureDataGeneration(const sql::Engine& engine) {
  const auto tableDataPath = common::make_directory("table_data");
  return generator::GeneratorState(engine, tableDataPath, CloudProvider::GCS, "gs://sql-arena-data");
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
  std::optional<std::string> artifacts_path = std::nullopt;
  std::optional<std::string> capture_artifacts_path = std::nullopt;
  std::optional<std::string> parquet_dir = std::nullopt;
  std::vector<std::string> all_theorems;
  std::string engine_arg;
  uint32_t port;
  uint32_t query_timeout_seconds = 0;
  uint32_t timing_runs = 3;
  bool verbose = false;
  bool prepare_ee_join_scale = false;
  bool append_proof_csv = false;

  app.set_help_flag("-?", "--help");
  app.add_option(
      "-e, --engine",
      engine_arg);

  app.add_flag("-v, --verbose", verbose, "Log to stdout");

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
  CLI::Option* artifacts_opt = app.add_option("-a,--artifacts",
                                             artifacts_path, "Path to store/load explain artifacts")
                                   ->expected(0, 1);
  CLI::Option* capture_artifacts_opt = app.add_option("--write-artifacts",
                                                      capture_artifacts_path,
                                                      "Path to store explain artifacts while executing live queries")
                                          ->expected(0, 1);
  app.add_option("--parquet-dir",
                 parquet_dir, "Directory containing parquet benchmark files such as orders.parquet and lineitem.parquet");
  app.add_flag("--prepare-ee-join-scale",
               prepare_ee_join_scale,
               "Materialize EE join-scale parquet inputs on the host using in-process DuckDB");
  app.add_flag("--append-proof-csv",
               append_proof_csv,
               "Append proof rows to an existing proof CSV instead of overwriting it");
  app.add_option("-T,--theorem",
                 all_theorems, "Which theorems to prove")->delimiter(',');
  app.add_option("--query-timeout",
                 query_timeout_seconds, "Query timeout in seconds (0 disables timeout)")->default_val(0);
  app.add_option("--timing-runs",
                 timing_runs, "Number of measured executions per query theorem")->default_val(3);

  CLI11_PARSE(app, argc, argv);

  if (artifacts_opt->count() > 0 && (!artifacts_path || artifacts_path->empty() || *artifacts_path == "true")) {
    artifacts_path = (fs::current_path() / "artifacts").string();
  }
  if (capture_artifacts_opt->count() > 0
      && (!capture_artifacts_path || capture_artifacts_path->empty() || *capture_artifacts_path == "true")) {
    capture_artifacts_path = (fs::current_path() / "artifacts").string();
  }
  if (artifacts_path && capture_artifacts_path) {
    throw std::runtime_error("Use either -a/--artifacts for replay or --write-artifacts for capture, not both");
  }

  const auto log_directory = dbprove::common::make_directory("logs");
  const std::string log_file = log_directory.string() + "/dbprove.log";
  const auto log_level = verbose ? plog::debug : plog::info;

  plog::init<plog::DBProveFormatter>(log_level, log_file.c_str(), 1000000, 5);
  if (verbose) {
      plog::init<plog::DBProveFormatter>(plog::debug, plog::streamStdOut);
  } else {
      plog::init<plog::DBProveFormatter>(plog::info, plog::streamStdOut);
  }

  if (prepare_ee_join_scale) {
    theorem::ee::prepareJoinScaleArtifacts(std::cout, parquet_dir);
    return 0;
  }

  const sql::Engine engine(engine_arg);
  ux::Terminal::configure();

  auto generator_state = configureDataGeneration(engine);

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

  const bool artifact_mode = artifacts_path.has_value();
  const auto connection_artifacts_path = artifact_mode ? artifacts_path : capture_artifacts_path;
  sql::setArtifactReplayMode(artifact_mode);
  std::string engine_version = "unknown";
  if (!artifact_mode) {
    try {
      sql::ConnectionFactory factory(engine, credentials, connection_artifacts_path);
      auto connection = factory.create();
      engine_version = connection->version();
      connection->close();
    } catch (const std::exception& e) {
      PLOGW << "Failed to retrieve engine version: " << e.what();
    }
  }

  std::ofstream proof_output_stream;
  NullStream null_output_stream;
  std::ostream* csv_output_stream = &null_output_stream;
  bool write_csv_header = true;
  std::optional<fs::path> proof_directory = std::nullopt;
  if (!artifact_mode) {
    const auto proof_base_directory = common::make_directory("proof");
    proof_directory = common::make_directory(proof_base_directory.string() + "/" + engine.name() + "/" + engine_version);
    if (append_proof_csv) {
      PLOGW << "--append-proof-csv is deprecated and ignored; proofs are now written to one theorem-named CSV per proof";
    }
    PLOGI << "Writing theorem proof CSV files to: "
          << fs::absolute(*proof_directory).string();
  } else {
    PLOGI << "Artifact mode enabled: skipping engine version check and proof CSV output";
  }

  if (connection_artifacts_path) {
    PLOGI << (artifact_mode ? "Using replay artifacts directory: " : "Writing artifacts directory: ")
          << fs::absolute(connection_artifacts_path.value()).string();
  }

  auto input_state = theorem::RunCtx{engine, credentials, generator_state,
                                     std::cout, *csv_output_stream, connection_artifacts_path,
                                     query_timeout_seconds > 0 ? std::optional<uint32_t>(query_timeout_seconds) : std::nullopt,
                                     timing_runs,
                                     parquet_dir,
                                     write_csv_header,
                                     proof_directory,
                                     artifact_mode};

  return theorem::prove(theorems, input_state) ? 0 : 1;
}
