#include "generator_state.h"

#include <fstream>
#include <vector>
#include <zip.h>

#include "dbprove/common/aws_bucket.h"
#include "dbprove/common/docker.h"
#include "dbprove/common/file_utility.h"
#include "dbprove/common/table_data_conventions.h"
#include "dbprove/generator/test.h"
#include "dbprove/sql/connection_factory.h"
#include "dbprove/sql/parsed_table.h"
#include "generated_table.h"
#include <dbprove/sql/sql.h>
#include <dbprove/ux/ux.h>
#include <curl/curl.h>
#include <google/cloud/storage/client.h>
#include <nlohmann/json.hpp>
#include <plog/Log.h>

namespace generator {
namespace {
using json = nlohmann::json;

struct ParsedBucketLocation {
  CloudProvider provider;
  std::string bucket;
  std::string prefix;
};

ParsedBucketLocation parseBucketLocation(std::string_view uri, const CloudProvider provider) {
  const auto scheme_end = uri.find("://");
  if (scheme_end == std::string_view::npos) {
    throw std::runtime_error("Invalid bucket URI: " + std::string(uri));
  }

  const auto remainder = uri.substr(scheme_end + 3);
  const auto slash = remainder.find('/');
  ParsedBucketLocation location{
      .provider = provider,
      .bucket = std::string(remainder.substr(0, slash)),
      .prefix = slash == std::string_view::npos ? std::string() : std::string(remainder.substr(slash + 1))};
  while (!location.prefix.empty() && location.prefix.back() == '/') {
    location.prefix.pop_back();
  }
  if (location.bucket.empty()) {
    throw std::runtime_error("Invalid bucket URI with empty bucket name: " + std::string(uri));
  }
  return location;
}

std::string joinObjectPath(const std::string& prefix, const std::string& object) {
  if (prefix.empty()) {
    return object;
  }
  return prefix + "/" + object;
}

std::vector<std::filesystem::path> expectedCsvPaths(const std::filesystem::path& base_path,
                                                    const GeneratedTable& table) {
  std::vector<std::filesystem::path> paths;
  paths.reserve(table.expected_file_count);
  for (size_t i = 0; i < table.expected_file_count; ++i) {
    paths.push_back(dbprove::common::stagedTablePath(
        base_path, table.name, dbprove::common::tableFileStem(table.name, i, table.expected_file_count) + ".csv"));
  }
  return paths;
}

std::vector<std::filesystem::path> expectedParquetPaths(const std::filesystem::path& base_path,
                                                        const GeneratedTable& table) {
  std::vector<std::filesystem::path> paths;
  paths.reserve(table.expected_file_count);
  for (size_t i = 0; i < table.expected_file_count; ++i) {
    paths.push_back(dbprove::common::stagedTablePath(
        base_path, table.name, dbprove::common::tableFileStem(table.name, i, table.expected_file_count) + ".parquet"));
  }
  return paths;
}

std::vector<std::filesystem::path> expectedSourceStems(const std::filesystem::path& base_path,
                                                       const GeneratedTable& table) {
  std::vector<std::filesystem::path> paths;
  paths.reserve(table.expected_file_count);
  for (size_t i = 0; i < table.expected_file_count; ++i) {
    paths.push_back(dbprove::common::stagedTablePath(
        base_path, table.name, dbprove::common::tableFileStem(table.name, i, table.expected_file_count)));
  }
  return paths;
}

std::filesystem::path downloadCachePath(const std::filesystem::path& base_path, std::string_view dataset_name,
                                        std::string_view object_name) {
  return base_path / ".downloads" / dbprove::common::schemaObjectPath(dataset_name) / std::string(object_name);
}

bool fileExistsAndNonEmpty(const std::filesystem::path& path) {
  return std::filesystem::exists(path) && std::filesystem::is_regular_file(path) &&
         std::filesystem::file_size(path) > 0;
}

void removeIfEmpty(const std::filesystem::path& path) {
  if (std::filesystem::exists(path) && std::filesystem::is_regular_file(path) &&
      std::filesystem::file_size(path) == 0) {
    PLOGW << "Removing zero-length file: " << path.string();
    std::filesystem::remove(path);
  }
}

void validateZipFile(const std::filesystem::path& zip_path) {
  int err = 0;
  zip_t* archive = zip_open(zip_path.string().c_str(), ZIP_RDONLY, &err);
  if (archive) {
    zip_close(archive);
    return;
  }
  PLOGW << "Existing zip archive " << zip_path.string() << " is corrupted or invalid. Deleting and re-downloading.";
  std::filesystem::remove(zip_path);
}

void extractZipEntry(const std::filesystem::path& zip_path, std::string_view entry_name,
                     const std::filesystem::path& output_path) {
  int err = 0;
  zip_t* archive = zip_open(zip_path.string().c_str(), ZIP_RDONLY, &err);
  if (!archive) {
    zip_error_t zerr;
    zip_error_init_with_code(&zerr, err);
    const auto error_msg =
        std::format("Failed to open zip archive: {} (libzip error: {})", zip_path.string(), zip_error_strerror(&zerr));
    zip_error_fini(&zerr);
    std::filesystem::remove(zip_path);
    throw std::runtime_error(error_msg);
  }

  zip_stat_t st{};
  if (zip_stat(archive, std::string(entry_name).c_str(), 0, &st) != 0) {
    zip_close(archive);
    throw std::runtime_error("File '" + std::string(entry_name) + "' not found inside zip: " + zip_path.string());
  }

  zip_file_t* file = zip_fopen(archive, std::string(entry_name).c_str(), 0);
  if (!file) {
    zip_close(archive);
    throw std::runtime_error("Failed to open '" + std::string(entry_name) + "' from zip: " + zip_path.string());
  }

  std::ofstream out(output_path, std::ios::binary);
  if (!out.is_open()) {
    zip_fclose(file);
    zip_close(archive);
    throw std::runtime_error("Failed to create extracted file: " + output_path.string());
  }

  std::vector<char> buffer(1 << 16);
  zip_int64_t bytes_read = 0;
  while ((bytes_read = zip_fread(file, buffer.data(), static_cast<zip_uint64_t>(buffer.size()))) > 0) {
    out.write(buffer.data(), bytes_read);
  }

  out.close();
  zip_fclose(file);
  zip_close(archive);
}

void downloadObject(CloudProvider provider, std::string_view bucket_uri, std::string_view object,
                    const std::filesystem::path& destination_path) {
  const auto location = parseBucketLocation(bucket_uri, provider);
  const auto full_object = joinObjectPath(location.prefix, std::string(object));

  std::filesystem::create_directories(destination_path.parent_path());
  if (provider == CloudProvider::GCS) {
    namespace gcs = ::google::cloud::storage;
    auto client = gcs::Client::CreateDefaultClient().value();
    std::ofstream out(destination_path, std::ios::binary);
    if (!out.is_open()) {
      throw std::runtime_error("Failed to open local file for writing: " + destination_path.string());
    }
    auto reader = client.ReadObject(location.bucket, full_object);
    if (!reader.status().ok()) {
      out.close();
      std::filesystem::remove(destination_path);
      throw std::runtime_error("GCS ReadObject failed: " + reader.status().message());
    }
    out << reader.rdbuf();
    if (reader.bad() || !out.good()) {
      out.close();
      std::filesystem::remove(destination_path);
      throw std::runtime_error("GCS ReadObject or file write failed during download: " + reader.status().message());
    }
    out.close();
    return;
  }

  if (provider == CloudProvider::AWS) {
    dbprove::common::AWSBucket(std::string(bucket_uri)).downloadFile(full_object, destination_path);
    return;
  }

  throw std::runtime_error("Local table cache miss for " + std::string(object) +
                           ", but no supported object-store provider is configured");
}

size_t writeHttpResponse(const char* ptr, const size_t size, const size_t nmemb, void* userdata) {
  auto* buffer = static_cast<std::string*>(userdata);
  buffer->append(ptr, size * nmemb);
  return size * nmemb;
}

void ensureCurl() {
  static const auto init = []() {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    return true;
  }();
  static_cast<void>(init);
}

std::string sqlTypeKindName(const sql::SqlTypeKind kind) {
  switch (kind) {
    case sql::SqlTypeKind::SMALLINT:
      return "SMALLINT";
    case sql::SqlTypeKind::INT:
      return "INT";
    case sql::SqlTypeKind::BIGINT:
      return "BIGINT";
    case sql::SqlTypeKind::REAL:
      return "REAL";
    case sql::SqlTypeKind::DOUBLE:
      return "DOUBLE";
    case sql::SqlTypeKind::DECIMAL:
      return "DECIMAL";
    case sql::SqlTypeKind::STRING:
      return "STRING";
    case sql::SqlTypeKind::DATE:
      return "DATE";
    case sql::SqlTypeKind::TIME:
      return "TIME";
    case sql::SqlTypeKind::DATETIME:
      return "DATETIME";
    case sql::SqlTypeKind::SQL_NULL:
      return "SQL_NULL";
    case sql::SqlTypeKind::UNKNOWN:
      return "UNKNOWN";
  }
  return "UNKNOWN";
}

}  // namespace

std::map<std::string_view, GeneratedTable*>& available_tables() {
  static std::map<std::string_view, GeneratedTable*> registry;
  return registry;
}

std::map<std::string_view, std::vector<std::string_view>>& available_datasets() {
  static std::map<std::string_view, std::vector<std::string_view>> registry;
  return registry;
}

GeneratorState::GeneratorState(const sql::Engine& engine, const std::filesystem::path& basePath,
                               const CloudProvider dataProvider, std::string dataPath,
                               const dbprove::StorageVariant storageVariant)
    : engine_(engine), basePath_(basePath), dataProvider_(dataProvider), dataPath_(std::move(dataPath)),
      storageVariant_(storageVariant) {}

GeneratorState::~GeneratorState() = default;

void GeneratorState::ensure(const std::string_view table_name, sql::ConnectionFactory& conn) {
  std::vector table_names{table_name};
  ensure(std::span(table_names), conn);
}

void GeneratorState::ensure(std::span<const std::string_view> table_names, sql::ConnectionFactory& conn) {
  for (auto table_name : table_names) {
    sql::checkTableName(table_name);
    if (ready_tables_.contains(table_name)) {
      continue;
    }

    std::unique_ptr<sql::ConnectionBase> cn = conn.create();
    PLOGD << "Ensuring table: " << table_name;

    if (!table(table_name).is_generated) {
      PLOGD << "Table: " << table_name << " is not marked as generated. Preparing input...";
      generate(table_name);
    }

    auto existing_rows = cn->tableRowCount(table_name);
    const auto expected_rows = table(table_name).row_count;

    if (existing_rows && *existing_rows == expected_rows) {
      PLOGI << "Table: " << table_name << " already exists with correct " << *existing_rows << " rows";
      ready_tables_.insert(std::string(table_name));
      continue;
    }

    if (existing_rows && expected_rows == 0 && *existing_rows > 0) {
      PLOGI << "Table: " << table_name << " already exists with " << *existing_rows
            << " rows; accepting existing contents because no expected row count was registered yet.";
      table(table_name).row_count = *existing_rows;
      ready_tables_.insert(std::string(table_name));
      continue;
    }

    if (existing_rows) {
      throw std::runtime_error(
          std::format("Table: {} already exists with {} rows, but {} rows were expected. "
                      "Generator contract does not permit constructTable to rebuild existing tables.",
                      table_name, *existing_rows, expected_rows));
    }

    PLOGI << "Constructing table: " << table_name << " (expected: " << expected_rows << ")";
    load(table_name, *cn);

    ready_tables_.insert(std::string(table_name));
  }
}

void GeneratorState::ensureDataset(const std::string_view dataset_name, sql::ConnectionFactory& conn) {
  if (!containsDataset(dataset_name)) {
    throw std::runtime_error("Dataset not found: " + std::string(dataset_name));
  }

  const auto& tables = available_datasets().at(dataset_name);

  if (tables.empty()) {
    return;
  }

  std::set<std::string> schemas;
  for (const auto table_name : tables) {
    auto [schema_name, _] = sql::splitTable(table_name);
    if (!schema_name.empty()) {
      schemas.insert(schema_name);
    }
  }

  if (!schemas.empty()) {
    auto schema_conn = conn.create();
    for (const auto& schema_name : schemas) {
      try {
        schema_conn->createSchema(schema_name);
      } catch (std::exception& e) {
        PLOGD << "Schema creation failed (might already exist): " << e.what();
      }
    }
  }

  ensure(std::span<const std::string_view>(tables), conn);
}

void GeneratorState::registerIcebergTable(const std::string_view ddl,
                                          const std::span<const std::filesystem::path> source_stems) {
  if (source_stems.empty()) {
    throw std::runtime_error("registerIcebergTable requires at least one staged source file stem");
  }

  const auto parsed = sql::ParsedTable(ddl);
  json payload;
  payload["table_name"] = parsed.tableName();
  payload["source_stems"] = json::array();
  payload["columns"] = json::array();

  for (const auto& stem : source_stems) {
    payload["source_stems"].push_back(stem.string());
  }

  for (const auto& column : parsed.columns()) {
    json column_json{
        {"name", column.name},
        {"kind", sqlTypeKindName(column.type.kind)},
        {"is_null", column.is_null},
    };
    if (column.type.kind == sql::SqlTypeKind::STRING &&
        std::holds_alternative<sql::SqlTypeModifier::String>(column.type.modifier.value)) {
      column_json["string_length"] = std::get<sql::SqlTypeModifier::String>(column.type.modifier.value).length;
    }
    if (column.type.kind == sql::SqlTypeKind::DECIMAL &&
        std::holds_alternative<sql::SqlTypeModifier::Decimal>(column.type.modifier.value)) {
      const auto decimal = std::get<sql::SqlTypeModifier::Decimal>(column.type.modifier.value);
      column_json["decimal_precision"] = decimal.precision;
      column_json["decimal_scale"] = decimal.scale;
    }
    payload["columns"].push_back(std::move(column_json));
  }

  ensureCurl();
  CURL* curl = curl_easy_init();
  if (!curl) {
    throw std::runtime_error("Failed to initialize curl for local iceberg registration");
  }

  const auto payload_text = payload.dump();
  std::string response_body;
  curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, "Content-Type: application/json");

  curl_easy_setopt(curl, CURLOPT_URL, "http://127.0.0.1:19130/register-table");
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload_text.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(payload_text.size()));
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeHttpResponse);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);

  const auto curl_result = curl_easy_perform(curl);
  long status_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status_code);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  if (curl_result != CURLE_OK) {
    throw std::runtime_error("Local iceberg registration request failed for table "
                             + parsed.tableName() + ": " + curl_easy_strerror(curl_result));
  }
  if (status_code < 200 || status_code >= 300) {
    throw std::runtime_error("Local iceberg registration failed for table " + parsed.tableName()
                             + " with HTTP " + std::to_string(status_code) + ": " + response_body);
  }
}

void GeneratorState::ensureDatasetFiles(const std::string_view dataset_name) {
  if (!containsDataset(dataset_name)) {
    throw std::runtime_error("Dataset not found: " + std::string(dataset_name));
  }

  const auto& tables = available_datasets().at(dataset_name);
  for (const auto table_name : tables) {
    sql::checkTableName(table_name);
    generate(table_name);
  }
}

bool GeneratorState::contains(const std::string_view table_name) {
  sql::checkTableName(table_name);
  return available_tables().contains(table_name);
}

bool GeneratorState::containsDataset(const std::string_view dataset_name) {
  return available_datasets().contains(dataset_name);
}

sql::RowCount GeneratorState::generate(const std::string_view table_name) {
  sql::checkTableName(table_name);
  if (!contains(table_name)) {
    throw std::runtime_error("Table not found: " + std::string(table_name));
  }

  auto& t = table(table_name);
  const auto target_row_count = t.row_count;
  const auto csv_paths = expectedCsvPaths(basePath_, t);
  const auto parquet_paths = expectedParquetPaths(basePath_, t);
  const auto schema_path = dbprove::common::schemaObjectPath(t.dataset);
  const auto base_table_name = dbprove::common::splitQualifiedTableName(table_name).table_name;

  dbprove::common::make_directory(basePath_.string());

  std::vector<std::filesystem::path> ready_csv_paths;
  std::vector<std::filesystem::path> ready_parquet_paths;
  ready_csv_paths.reserve(t.expected_file_count);
  ready_parquet_paths.reserve(t.expected_file_count);

  for (size_t i = 0; i < t.expected_file_count; ++i) {
    const auto stem = dbprove::common::tableFileStem(table_name, i, t.expected_file_count);
    const auto csv_file_name = stem + ".csv";
    const auto zip_object_name = stem + ".csv.zip";
    const auto parquet_object_name = stem + ".parquet";
    const auto relative_object_prefix = (schema_path.empty() ? std::string() : schema_path + "/") + base_table_name + "/";
    const auto zip_object_path = relative_object_prefix + zip_object_name;
    const auto parquet_object_path = relative_object_prefix + parquet_object_name;
    const auto zip_cache_path = downloadCachePath(basePath_, t.dataset, zip_object_name);
    const auto& csv_path = csv_paths[i];
    const auto& parquet_path = parquet_paths[i];

    removeIfEmpty(csv_path);
    removeIfEmpty(parquet_path);
    removeIfEmpty(zip_cache_path);

    if (!fileExistsAndNonEmpty(csv_path)) {
      if (std::filesystem::exists(zip_cache_path)) {
        validateZipFile(zip_cache_path);
      }
      if (!std::filesystem::exists(zip_cache_path)) {
        PLOGI << "Downloading table CSV archive " << zip_object_path << " to " << zip_cache_path.string();
        downloadObject(cloudProvider(), dataPath(), zip_object_path, zip_cache_path);
      }
      std::filesystem::create_directories(csv_path.parent_path());
      extractZipEntry(zip_cache_path, csv_file_name, csv_path);
      PLOGI << "CSV available at: " << csv_path.string();
    }

    if (!fileExistsAndNonEmpty(parquet_path)) {
      PLOGI << "Downloading table parquet " << parquet_object_path << " to " << parquet_path.string();
      downloadObject(cloudProvider(), dataPath(), parquet_object_path, parquet_path);
    }

    ready_csv_paths.push_back(csv_path);
    ready_parquet_paths.push_back(parquet_path);
  }

  registerGeneration(table_name, std::move(ready_csv_paths), std::move(ready_parquet_paths));
  return target_row_count;
}

sql::RowCount GeneratorState::load(const std::string_view table_name, sql::ConnectionBase& conn) {
  sql::checkTableName(table_name);
  auto& t = table(table_name);
  const auto expected_rows = t.row_count;
  const auto source_stems = expectedSourceStems(basePath_, t);

  PLOGI << "Constructing table: " << table_name << "...";
  conn.constructTable(t.ddl, source_stems, storageVariant(), &GeneratorState::registerIcebergTable);

  if (expected_rows == 0) {
    PLOGI << "Table: " << table_name << " constructed with unknown expected row count; skipping full COUNT(*) verification.";
    return 0;
  }

  auto actual_rows = conn.tableRowCount(table_name).value_or(0);
  PLOGI << "Table: " << table_name << " constructed. Now has " << actual_rows << " rows";

  if (actual_rows != expected_rows) {
    const auto error_msg = std::format("Table: {} still has incorrect row count after constructTable. Got {}, expected {}",
                                       table_name, actual_rows, expected_rows);
    PLOGE << error_msg;
    throw std::runtime_error(error_msg);
  }

  return actual_rows;
}

void GeneratorState::registerGeneration(std::string_view table_name, std::vector<std::filesystem::path> csv_paths,
                                        std::vector<std::filesystem::path> parquet_paths) const {
  sql::checkTableName(table_name);

  if (csv_paths.empty()) {
    throw std::runtime_error("Cannot register generated table without CSV paths: " + std::string(table_name));
  }
  if (csv_paths.size() != parquet_paths.size()) {
    throw std::runtime_error("CSV and parquet file counts differ for table: " + std::string(table_name));
  }

  for (size_t i = 0; i < csv_paths.size(); ++i) {
    if (!fileExistsAndNonEmpty(csv_paths[i])) {
      throw std::runtime_error("Cannot register generated table with missing or empty CSV file: " +
                               csv_paths[i].string());
    }

    if (!fileExistsAndNonEmpty(parquet_paths[i])) {
      throw std::runtime_error("Cannot register generated table with missing or empty parquet file: " +
                               parquet_paths[i].string());
    }
  }

  table(table_name).csv_paths = std::move(csv_paths);
  table(table_name).parquet_paths = std::move(parquet_paths);
  table(table_name).is_generated = true;
}

void GeneratorState::printSummary(std::ostream& out) const {
  if (ready_tables_.empty()) {
    return;
  }

  dbprove::ux::Header(out, "Tables Loaded");
  std::vector<dbprove::ux::RowStats> stats;
  for (const auto& table_name : ready_tables_) {
    stats.push_back({table_name, table(table_name).row_count});
  }
  dbprove::ux::RowStatTable(out, stats);
}

GeneratedTable& GeneratorState::table(const std::string_view table_name) const {
  sql::checkTableName(table_name);
  if (!contains(table_name)) {
    throw std::runtime_error("Table not found: " + std::string(table_name) +
                             ". Did you forget to call or include the appropriate REGISTER_TABLE");
  }
  return *available_tables().at(table_name);
}

Registrar::Registrar(const std::string_view table_name, const std::string_view dataset_name, const std::string_view ddl,
                     const sql::RowCount rows, const size_t expected_file_count, TableMetadata metadata) {
  const auto qualified_table_name = dbprove::common::qualifyRegisteredTableName(table_name, dataset_name);
  sql::checkTableName(qualified_table_name);
  auto* table =
      new GeneratedTable{qualified_table_name, dataset_name, ddl, rows, expected_file_count, std::move(metadata)};
  available_tables().emplace(table->name, table);
  available_datasets()[dataset_name].push_back(table->name);
}

}  // namespace generator
