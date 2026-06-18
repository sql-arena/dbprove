#include <dbprove/theorem/theorem.h>
#include "../theorem/init.h"
#include <dbprove/common/docker.h>
#include <dbprove/ux/ux.h>
#include <dbprove/sql/sql.h>
#include <dbprove/common/log_formatter.h>
#include <dbprove/common/file_utility.h>
#include <dbprove/common/string.h>
#include <CLI/CLI.hpp>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
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
    const std::optional<std::string>& token,
    const std::string& data_bucket_uri) {
  const auto engine_name = engine.name();

  try {
    switch (engine.type()) {
      case sql::Engine::Type::MariaDB:
      case sql::Engine::Type::Postgres:
      case sql::Engine::Type::SQLServer:
      case sql::Engine::Type::ClickHouse:
      case sql::Engine::Type::Trino:
      case sql::Engine::Type::Yellowbrick:
      case sql::Engine::Type::Oracle: {
        if (!username) {
          throw std::invalid_argument("Username is required for " + engine_name);
        }
        return sql::CredentialPassword(host, database, port, username.value(), password);
      }
      case sql::Engine::Type::Utopia:
      case sql::Engine::Type::DataFusion:
        return sql::CredentialNone();
      case sql::Engine::Type::DuckDB:
        return sql::CredentialFile(database);
      case sql::Engine::Type::Databricks: {
        if (!token) {
          throw std::invalid_argument("Token is required for " + engine_name);
        }
        return sql::CredentialAccessToken(engine, host, database, token.value(), data_bucket_uri);
      }
    default:
      throw std::invalid_argument("Cannot generate credentials for engine: " + engine_name);
    }
  } catch (const std::invalid_argument& e) {
    std::cerr << e.what() << std::endl;
    std::exit(1);
  }
}

generator::GeneratorState configureDataGeneration(const sql::Engine& engine,
                                                  const std::string& data_bucket_uri,
                                                  const std::optional<std::string>& download_dir_override,
                                                  const dbprove::StorageVariant storage_variant) {
  const auto tableDataPath = download_dir_override.has_value()
                           ? common::make_directory(*download_dir_override)
                           : common::make_directory((fs::current_path() / "table_data").string());
  const auto lowered = to_lower(data_bucket_uri);
  CloudProvider provider = CloudProvider::NONE;
  if (lowered.starts_with("s3://")) {
    provider = CloudProvider::AWS;
  } else if (lowered.starts_with("gs://")) {
    provider = CloudProvider::GCS;
  } else {
    throw std::runtime_error("Unsupported --data-bucket URI: " + data_bucket_uri + ". Expected s3:// or gs://");
  }
  return generator::GeneratorState(engine, tableDataPath, provider, data_bucket_uri, storage_variant);
}

namespace {
fs::path proofBaseDirectory() {
  return common::make_directory("proof");
}

fs::path proofEngineDirectory(const sql::Engine& engine) {
  return common::make_directory((proofBaseDirectory() / engine.name()).string());
}

fs::path proofVersionDirectory(const sql::Engine& engine, const std::string_view engine_version) {
  return common::make_directory((proofEngineDirectory(engine) / engine_version).string());
}

fs::path defaultArtifactsDirectory(const sql::Engine& engine, const std::string_view engine_version) {
  return common::make_directory((proofVersionDirectory(engine, engine_version) / "artefacts").string());
}

fs::path defaultLogDirectory(const sql::Engine& engine) {
  return common::make_directory((proofEngineDirectory(engine) / "logs").string());
}

std::optional<dbprove::StorageVariant> parseStorageVariant(const std::optional<std::string>& value) {
  if (!value.has_value()) {
    return std::nullopt;
  }

  const auto lowered = to_lower(*value);
  if (lowered == "native") {
    return dbprove::StorageVariant::Native;
  }
  if (lowered == "iceberg") {
    return dbprove::StorageVariant::Iceberg;
  }
  throw std::runtime_error("Unknown storage variant: " + *value + ". Expected 'native' or 'iceberg'.");
}

std::optional<dbprove::StorageVariant> theoremStorageVariantRequirement(const std::vector<const theorem::Theorem*>& theorems) {
  std::optional<dbprove::StorageVariant> required_variant = std::nullopt;
  for (const auto* theorem : theorems) {
    const auto theorem_variant = theorem->requiredStorageVariant();
    if (!theorem_variant.has_value()) {
      continue;
    }
    if (!required_variant.has_value()) {
      required_variant = theorem_variant;
      continue;
    }
    if (*required_variant != *theorem_variant) {
      throw std::runtime_error(
          "Selected theorems require conflicting storage variants: "
          + std::string(to_string(*required_variant)) + " and "
          + std::string(to_string(*theorem_variant)));
    }
  }
  return required_variant;
}

bool requiresMountedTpchParquet(const sql::Engine& engine, const dbprove::StorageVariant variant) {
  if (variant != dbprove::StorageVariant::Iceberg) {
    return false;
  }

  switch (engine.type()) {
    case sql::Engine::Type::Trino:
    case sql::Engine::Type::DataFusion:
      return true;
    default:
      return false;
  }
}

std::string prettyCommaSeparatedList(std::string value) {
  if (value.empty()) {
    return value;
  }

  std::string formatted;
  formatted.reserve(value.size() + 8);
  for (size_t i = 0; i < value.size(); ++i) {
    formatted.push_back(value[i]);
    if (value[i] == ',' && i + 1 < value.size() && value[i + 1] != ' ') {
      formatted.push_back(' ');
    }
  }
  return formatted;
}

void listTheorems(std::ostream& out) {
  theorem::init();

  std::vector<ux::TheoremListingRow> rows;
  rows.reserve(theorem::allTheorems().size());

  for (const auto& theorem_ptr : std::views::values(theorem::allTheorems())) {
    rows.push_back(ux::TheoremListingRow{
        .name = theorem_ptr->name,
        .description = theorem_ptr->description,
        .tags = prettyCommaSeparatedList(theorem_ptr->tags_to_string()),
        .categories = prettyCommaSeparatedList(theorem_ptr->categories_to_string()),
    });
  }

  ux::Header(out, "Available Theorems");
  ux::TheoremListTable(out, rows);
}
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
  std::string data_bucket_uri = "s3://sql-arena";
  std::optional<std::string> download_dir_override = std::nullopt;
  std::optional<std::string> artefact_directory_override = std::nullopt;
  std::optional<std::string> log_directory_override = std::nullopt;
  std::optional<std::string> docker_variant_arg = std::nullopt;
  std::optional<std::string> parquet_dir = std::nullopt;
  std::vector<std::string> all_theorems;
  std::string engine_arg;
  uint32_t port;
  uint32_t query_timeout_seconds = 0;
  uint32_t timing_runs = 3;
  bool verbose = false;
  bool docker_mode = false;
  bool prepare_ee_join_scale = false;
  bool list_theorems = false;

  app.set_help_flag("-?", "--help");
  app.add_option(
      "-e, --engine",
      engine_arg);

  app.add_flag("-L,--list", list_theorems, "List available theorems");
  app.add_flag("-v, --verbose", verbose, "Log to stdout");
  app.add_flag("--docker", docker_mode, "Run against the local docker-managed engine image/service");

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
  app.add_option("--data-bucket",
                 data_bucket_uri,
                 "Bucket URI for benchmark data, for example s3://sql-arena or gs://sql-arena-data")->default_val("s3://sql-arena");
  app.add_option("--download-dir",
                 download_dir_override,
                 "Directory for downloaded source data such as staged CSV and zip files");
  app.add_option("--artefact-dir",
                 artefact_directory_override,
                 "Directory to replay explain artefacts from. Missing artefacts are treated as errors.");
  app.add_option("--log-dir",
                 log_directory_override,
                 "Directory to write dbprove log files to");
  app.add_option("--variant",
                 docker_variant_arg,
                 "Docker storage variant to use when --docker is enabled: native or iceberg");
  app.add_option("--parquet-dir",
                 parquet_dir, "Directory containing parquet benchmark files such as orders.parquet and lineitem.parquet");
  app.add_flag("--prepare-ee-join-scale",
               prepare_ee_join_scale,
               "Materialize EE join-scale parquet inputs on the host using in-process DuckDB");
  app.add_option("-T,--theorem",
                 all_theorems, "Which theorems to prove")->delimiter(',');
  app.add_option("--query-timeout",
                 query_timeout_seconds, "Query timeout in seconds (0 disables timeout)")->default_val(0);
  app.add_option("--timing-runs",
                 timing_runs, "Number of measured executions per query theorem")->default_val(3);

  CLI11_PARSE(app, argc, argv);

  if (list_theorems) {
    ux::Terminal::configure();
    listTheorems(std::cout);
    return 0;
  }

  const sql::Engine engine(engine_arg);

  const auto log_directory = log_directory_override
                           ? common::make_directory(*log_directory_override)
                           : defaultLogDirectory(engine);
  const std::string log_file = (log_directory / "dbprove.log").string();
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

  ux::Terminal::configure();

  database = engine.defaultDatabase(database);
  host = engine.defaultHost(host);
  port = engine.defaultPort(port, docker_mode);
  username = engine.defaultUsername(username);
  password = engine.defaultPassword(password);
  if (docker_mode && engine.type() == sql::Engine::Type::Postgres
      && (!password.has_value() || password->empty())) {
    password = "postgres";
  }
  token = engine.defaultToken(token);

  PLOGI << "Using engine: " << engine.name();
  PLOGI << "  host      : " << host.value();
  PLOGI << "  port      : " << port;
  PLOGI << "  database  : " << database.value();

  auto credentials =
      parseCredentials(
          engine,
          host.value(),
          port,
          database.value(),
          username,
          password,
          token,
          data_bucket_uri);
  theorem::init();
  auto theorems = theorem::parse(all_theorems);
  const auto theorem_required_variant = theoremStorageVariantRequirement(theorems);
  const auto requested_docker_variant = parseStorageVariant(docker_variant_arg);

  if (requested_docker_variant.has_value() && !docker_mode) {
    throw std::runtime_error("--variant requires --docker");
  }

  if (requested_docker_variant.has_value() && theorem_required_variant.has_value()
      && *requested_docker_variant != *theorem_required_variant) {
    throw std::runtime_error(
        "Selected theorems require storage variant '"
        + std::string(to_string(*theorem_required_variant))
        + "', but '--variant " + std::string(to_string(*requested_docker_variant)) + "' was requested");
  }

  std::optional<dbprove::StorageVariant> effective_docker_variant = std::nullopt;
  if (docker_mode) {
    effective_docker_variant = requested_docker_variant;
    if (!effective_docker_variant.has_value()) {
      effective_docker_variant = theorem_required_variant.has_value()
                               ? theorem_required_variant
                               : engine.defaultStorageVariant();
    }
    if (!effective_docker_variant.has_value()) {
      throw std::runtime_error(
          "Docker mode is not supported for engine '" + engine.name()
          + "' because there is no local docker service mapping for it");
    }
  }

  const auto effective_storage_variant = docker_mode
                                       ? *effective_docker_variant
                                       : engine.defaultStorageVariant().value_or(dbprove::StorageVariant::Native);

  auto generator_state = configureDataGeneration(engine, data_bucket_uri, download_dir_override, effective_storage_variant);

  PLOGI << "Generating into directory: " << generator_state.basePath();

  const bool artifact_mode = artefact_directory_override.has_value();
  sql::setArtifactReplayMode(artifact_mode);

  std::unique_ptr<common::DockerComposeSession> docker_session;
  if (docker_mode && !artifact_mode) {
    if (requiresMountedTpchParquet(engine, *effective_docker_variant)) {
      PLOGI << "Pre-staging TPCH CSV/parquet inputs under " << generator_state.basePath()
            << " before starting docker-managed " << engine.name();
      generator_state.ensureDatasetFiles("tpch_sf1");
    }

    const auto service_config = engine.dockerServiceConfig(*effective_docker_variant);
    if (!service_config.has_value()) {
      throw std::runtime_error(
          "Storage variant '" + std::string(to_string(*effective_docker_variant))
          + "' is not available for engine '" + engine.name() + "'");
    }

    PLOGI << "Docker mode enabled for engine '" << engine.name()
          << "' using variant '" << to_string(*effective_docker_variant)
          << "' and service '" << service_config->service_name << "'";
    docker_session = std::make_unique<common::DockerComposeSession>();
    docker_session->start(service_config->service_name);
    engine.waitForDockerReady(credentials, service_config->readiness_timeout);
  } else if (docker_mode && artifact_mode) {
    PLOGI << "Artifact replay mode enabled: skipping docker service startup";
  }

  std::string engine_version = "unknown";
  if (!artifact_mode) {
    try {
      sql::ConnectionFactory factory(engine, credentials, std::nullopt);
      auto connection = factory.create();
      engine_version = connection->version();
      connection->close();
    } catch (const std::exception& e) {
      PLOGW << "Failed to retrieve engine version: " << e.what();
    }
  }

  const auto connection_artifacts_path = artifact_mode
                                       ? artefact_directory_override
                                       : std::optional<std::string>(defaultArtifactsDirectory(engine, engine_version).string());

  std::optional<fs::path> proof_directory = std::nullopt;
  if (!artifact_mode) {
    proof_directory = proofVersionDirectory(engine, engine_version);
    PLOGI << "Writing theorem proof JSON files to: "
          << fs::absolute(*proof_directory).string();
  } else {
    PLOGI << "Artifact mode enabled: skipping engine version check and theorem proof JSON output";
  }

  if (connection_artifacts_path) {
    PLOGI << (artifact_mode ? "Using replay artefacts directory: " : "Writing artefacts directory: ")
          << fs::absolute(connection_artifacts_path.value()).string();
  }

  auto input_state = theorem::RunCtx{engine, credentials, generator_state,
                                     std::cout, engine_version, connection_artifacts_path,
                                     effective_storage_variant,
                                     query_timeout_seconds > 0 ? std::optional<uint32_t>(query_timeout_seconds) : std::nullopt,
                                     timing_runs,
                                     parquet_dir,
                                     proof_directory,
                                     artifact_mode};

  return theorem::prove(theorems, input_state) ? 0 : 1;
}
