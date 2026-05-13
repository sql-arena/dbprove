#include "connection.h"

#include "result.h"
#include "sql_exceptions.h"

#include <dbprove/common/string.h>
#include <nlohmann/json.hpp>

#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <regex>
#include <sstream>
#ifdef _WIN32
#include <process.h>
#else
#include <sys/wait.h>
#endif

namespace sql::datafusion {
using ordered_json = nlohmann::ordered_json;
using json = nlohmann::json;

namespace {
#ifdef _WIN32
constexpr auto dbprove_popen = _popen;
constexpr auto dbprove_pclose = _pclose;
#else
constexpr auto dbprove_popen = popen;
constexpr auto dbprove_pclose = pclose;
#endif

struct CommandResult {
  int exit_code = 0;
  std::string output;
};

std::string shellQuote(std::string_view value) {
  std::string quoted = "'";
  for (const char c : value) {
    if (c == '\'') {
      quoted += "'\\''";
    } else {
      quoted += c;
    }
  }
  quoted += "'";
  return quoted;
}

class TemporarySqlFile {
  std::filesystem::path path_;

public:
  explicit TemporarySqlFile(std::string_view sql) {
    const auto temp_dir = std::filesystem::temp_directory_path();
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();

    for (size_t attempt = 0; attempt < 32; ++attempt) {
      auto candidate = temp_dir / ("dbprove-datafusion-" + std::to_string(now) + "-" +
                                   std::to_string(std::rand()) + "-" + std::to_string(attempt) + ".sql");
      if (std::filesystem::exists(candidate)) {
        continue;
      }

      std::ofstream out(candidate, std::ios::binary | std::ios::trunc);
      if (!out.is_open()) {
        continue;
      }
      out << sql;
      out.close();
      path_ = std::move(candidate);
      return;
    }

    throw std::runtime_error("Failed to create temporary SQL file for DataFusion");
  }

  ~TemporarySqlFile() {
    std::error_code ec;
    std::filesystem::remove(path_, ec);
  }

  const std::filesystem::path& path() const {
    return path_;
  }
};

CommandResult runCommand(std::string_view command) {
  FILE* pipe = dbprove_popen((std::string(command) + " 2>&1").c_str(), "r");
  if (pipe == nullptr) {
    throw std::runtime_error("Failed to launch process: " + std::string(command));
  }

  std::array<char, 4096> buffer{};
  std::string output;
  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    output += buffer.data();
  }

  const int raw_status = dbprove_pclose(pipe);
#ifdef _WIN32
  const int exit_code = raw_status;
#else
  const int exit_code = raw_status == -1 ? raw_status : WEXITSTATUS(raw_status);
#endif
  return CommandResult{exit_code, output};
}

std::string trimOutput(std::string output) {
  while (!output.empty() && std::isspace(static_cast<unsigned char>(output.back())) != 0) {
    output.pop_back();
  }
  return output;
}

SqlTypeKind inferType(const SqlVariant& value) {
  if (value.is<SqlSmallInt>()) {
    return SqlTypeKind::SMALLINT;
  }
  if (value.is<SqlInt>()) {
    return SqlTypeKind::INT;
  }
  if (value.is<SqlBigInt>()) {
    return SqlTypeKind::BIGINT;
  }
  if (value.is<SqlFloat>()) {
    return SqlTypeKind::REAL;
  }
  if (value.is<SqlDouble>()) {
    return SqlTypeKind::DOUBLE;
  }
  if (value.is<SqlDecimal>()) {
    return SqlTypeKind::DECIMAL;
  }
  if (value.is<SqlString>()) {
    return SqlTypeKind::STRING;
  }
  return SqlTypeKind::SQL_NULL;
}

SqlVariant jsonValueToVariant(const ordered_json& value) {
  if (value.is_null()) {
    return SqlVariant();
  }
  if (value.is_number_integer()) {
    const auto numeric = value.get<int64_t>();
    if (numeric >= std::numeric_limits<int16_t>::min() && numeric <= std::numeric_limits<int16_t>::max()) {
      return SqlVariant(static_cast<int16_t>(numeric));
    }
    if (numeric >= std::numeric_limits<int32_t>::min() && numeric <= std::numeric_limits<int32_t>::max()) {
      return SqlVariant(static_cast<int32_t>(numeric));
    }
    return SqlVariant(numeric);
  }
  if (value.is_number_unsigned()) {
    return SqlVariant(static_cast<int64_t>(value.get<uint64_t>()));
  }
  if (value.is_number_float()) {
    return SqlVariant(value.get<double>());
  }
  if (value.is_boolean()) {
    return SqlVariant(value.get<bool>() ? "true" : "false");
  }
  if (value.is_string()) {
    const auto text = value.get<std::string>();
    static const std::regex decimal_pattern(R"(^-?\d+\.\d+$)");
    if (std::regex_match(text, decimal_pattern)) {
      return SqlVariant(SqlDecimal(text));
    }
    return SqlVariant(text);
  }
  return SqlVariant(value.dump());
}

[[noreturn]] void throwForCommandError(const Credential& credential, std::string_view statement, const std::string& output) {
  const auto lowered = to_lower(output);
  if (lowered.contains("error during planning")) {
    if (lowered.contains("unsupported") || lowered.contains("not implemented")) {
      throw NotImplementedException(output);
    }
    if (lowered.contains("table") && lowered.contains("not found")) {
      throw InvalidObjectException(output);
    }
    throw SyntaxException(output, statement);
  }
  if (lowered.contains("schema error") || lowered.contains("table") && lowered.contains("not found")) {
    throw InvalidObjectException(output);
  }
  if (lowered.contains("not implemented")) {
    throw NotImplementedException(output);
  }
  throw ConnectionException(credential, output);
}
}

class Connection::Pimpl {
  CredentialNone credential_;
  std::string image_name_;
  bool closed_ = false;

  [[nodiscard]] std::string dockerImage() const {
    return image_name_;
  }

public:
  explicit Pimpl(CredentialNone credential)
    : credential_(std::move(credential))
    , image_name_(std::getenv("DBPROVE_DATAFUSION_IMAGE") != nullptr
                    ? std::getenv("DBPROVE_DATAFUSION_IMAGE")
                    : "dbprove-datafusion:latest") {
  }

  void ensureOpen() const {
    if (closed_) {
      throw ConnectionClosedException(credential_);
    }
  }

  void close() {
    closed_ = true;
  }

  CommandResult runCli(std::string_view sql) const {
    ensureOpen();
    TemporarySqlFile query_file(sql);
    const auto mounted = query_file.path().string() + ":/tmp/query.sql:ro";
    const std::string command = "docker run --rm -v " + shellQuote(mounted) + " " +
                                shellQuote(dockerImage()) +
                                " -q --format json -b 1000000 -f /tmp/query.sql";
    return runCommand(command);
  }

  CommandResult runPhysicalPlanHelper(std::string_view sql) const {
    ensureOpen();
    TemporarySqlFile query_file(sql);
    const auto mounted = query_file.path().string() + ":/tmp/query.sql:ro";
    const std::string command = "docker run --rm -v " + shellQuote(mounted) +
                                " --entrypoint datafusion-plan-json " + shellQuote(dockerImage()) +
                                " --sql-file /tmp/query.sql";
    return runCommand(command);
  }
};

Connection::Connection(const CredentialNone& credential, const Engine& engine, std::optional<std::string> artifacts_path)
  : ConnectionBase(credential, engine, std::move(artifacts_path))
  , impl_(std::make_unique<Pimpl>(credential)) {
}

Connection::~Connection() = default;

const ConnectionBase::TypeMap& Connection::typeMap() const {
  static const TypeMap map = {};
  return map;
}

void Connection::execute(std::string_view statement) {
  const auto result = impl_->runCli(statement);
  if (result.exit_code != 0) {
    throwForCommandError(credential, statement, result.output);
  }
}

std::unique_ptr<ResultBase> Connection::fetchJsonQuery(std::string_view statement) {
  const auto result = impl_->runCli(statement);
  if (result.exit_code != 0) {
    throwForCommandError(credential, statement, result.output);
  }

  ordered_json payload;
  try {
    payload = ordered_json::parse(trimOutput(result.output));
  } catch (const std::exception& e) {
    throw ProtocolException("Failed to parse DataFusion JSON output: " + std::string(e.what()) + "\n" + result.output);
  }

  if (!payload.is_array()) {
    throw ProtocolException("Expected DataFusion CLI to return a JSON array");
  }

  std::vector<std::vector<SqlVariant>> rows;
  std::vector<std::string> column_names;
  std::vector<SqlTypeKind> column_types;
  for (const auto& row_json : payload) {
    if (!row_json.is_object()) {
      throw ProtocolException("Expected DataFusion row payload to be an object");
    }

    if (column_names.empty()) {
      for (auto it = row_json.begin(); it != row_json.end(); ++it) {
        column_names.push_back(it.key());
        column_types.push_back(SqlTypeKind::SQL_NULL);
      }
    }

    std::vector<SqlVariant> row;
    row.reserve(column_names.size());
    for (size_t index = 0; index < column_names.size(); ++index) {
      const auto& value = row_json.at(column_names[index]);
      auto variant = jsonValueToVariant(value);
      if (column_types[index] == SqlTypeKind::SQL_NULL && !variant.is<SqlNull>()) {
        column_types[index] = inferType(variant);
      }
      row.push_back(std::move(variant));
    }
    rows.push_back(std::move(row));
  }

  return std::make_unique<Result>(std::move(rows), std::move(column_types));
}

std::unique_ptr<ResultBase> Connection::fetchAll(std::string_view statement) {
  return fetchJsonQuery(statement);
}

void Connection::bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);
  throw NotImplementedException("The DataFusion driver uses a pre-mounted dataset container and does not implement bulk load");
}

void Connection::analyse(std::string_view table_name) {
  const auto statement = "SELECT COUNT(*) FROM " + std::string(table_name);
  execute(statement);
}

bool Connection::shouldSkipDatasetTuning(std::string_view dataset) {
  return dataset == "tpch";
}

std::string Connection::version() {
  return fetchScalar("SELECT version()").asString();
}

void Connection::close() {
  impl_->close();
}

json Connection::fetchPhysicalPlanJson(std::string_view statement) const {
  const auto result = impl_->runPhysicalPlanHelper(statement);
  if (result.exit_code != 0) {
    throwForCommandError(credential, statement, result.output);
  }

  try {
    return json::parse(trimOutput(result.output));
  } catch (const std::exception& e) {
    throw ProtocolException("Failed to parse DataFusion physical plan JSON: " + std::string(e.what()) + "\n" + result.output);
  }
}

}
