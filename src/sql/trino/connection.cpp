#include "connection.h"

#include "include/dbprove/sql/parsed_table.h"
#include "result.h"
#include "sql_exceptions.h"

#include <dbprove/common/string.h>
#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <optional>
#include <plog/Log.h>
#include <regex>
#include <chrono>
#include <sstream>

namespace sql::trino {
using json = nlohmann::json;

namespace {
struct TrinoColumnMeta {
  std::string name;
  std::string raw_type;
};

struct TrinoQueryResult {
  std::vector<TrinoColumnMeta> columns;
  std::vector<std::vector<SqlVariant>> rows;
};

size_t writeCallback(const char* ptr, const size_t size, const size_t nmemb, void* userdata) {
  auto* buffer = static_cast<std::string*>(userdata);
  buffer->append(ptr, size * nmemb);
  return size * nmemb;
}

std::string extractRawType(const json& column_json) {
  if (column_json.contains("typeSignature") &&
      column_json["typeSignature"].is_object() &&
      column_json["typeSignature"].contains("rawType")) {
    return column_json["typeSignature"]["rawType"].get<std::string>();
  }

  if (column_json.contains("type") && column_json["type"].is_string()) {
    const auto type_name = column_json["type"].get<std::string>();
    const auto paren = type_name.find('(');
    return paren == std::string::npos ? type_name : type_name.substr(0, paren);
  }

  return "unknown";
}

std::string jsonScalarAsString(const json& value) {
  if (value.is_string()) {
    return value.get<std::string>();
  }
  if (value.is_boolean()) {
    return value.get<bool>() ? "true" : "false";
  }
  if (value.is_number_integer()) {
    return std::to_string(value.get<int64_t>());
  }
  if (value.is_number_unsigned()) {
    return std::to_string(value.get<uint64_t>());
  }
  if (value.is_number_float()) {
    return std::to_string(value.get<double>());
  }
  return value.dump();
}

std::string defaultTypeName(const SqlTypeKind kind) {
  switch (kind) {
    case SqlTypeKind::SMALLINT:
      return "SMALLINT";
    case SqlTypeKind::INT:
      return "INT";
    case SqlTypeKind::BIGINT:
      return "BIGINT";
    case SqlTypeKind::REAL:
      return "REAL";
    case SqlTypeKind::DOUBLE:
      return "DOUBLE";
    case SqlTypeKind::DECIMAL:
      return "DECIMAL";
    case SqlTypeKind::STRING:
      return "TEXT";
    case SqlTypeKind::DATE:
      return "DATE";
    case SqlTypeKind::TIME:
      return "TIME";
    case SqlTypeKind::DATETIME:
      return "TIMESTAMP";
    case SqlTypeKind::SQL_NULL:
      return "NULL";
    case SqlTypeKind::UNKNOWN:
      return "UNKNOWN";
  }
  return "UNKNOWN";
}

std::string renderType(const SqlTypeMeta& type, const ConnectionBase::TypeMap& type_map) {
  std::string rendered = type_map.contains(type.kind)
                       ? std::string(type_map.at(type.kind))
                       : defaultTypeName(type.kind);

  if (type.kind == SqlTypeKind::STRING && std::holds_alternative<SqlTypeModifier::String>(type.modifier.value)) {
    const auto length = std::get<SqlTypeModifier::String>(type.modifier.value).length;
    return rendered + "(" + std::to_string(length) + ")";
  }

  if (type.kind == SqlTypeKind::DECIMAL && std::holds_alternative<SqlTypeModifier::Decimal>(type.modifier.value)) {
    const auto decimal = std::get<SqlTypeModifier::Decimal>(type.modifier.value);
    return rendered + "(" + std::to_string(decimal.precision) + ", " + std::to_string(decimal.scale) + ")";
  }

  return rendered;
}

}

class Connection::Pimpl {
  Connection& connection;
  const CredentialPassword credential_;
  bool closed_ = false;

  static void ensureCurl() {
    static const auto init = []() {
      curl_global_init(CURL_GLOBAL_DEFAULT);
      return true;
    }();
    (void)init;
  }

public:
  explicit Pimpl(Connection& connection, CredentialPassword credential)
    : connection(connection)
    , credential_(std::move(credential)) {
    ensureCurl();
  }

  void ensureOpen() const {
    if (closed_) {
      throw ConnectionClosedException(credential_);
    }
  }

  void close() {
    closed_ = true;
  }

  [[nodiscard]] std::string currentSchema() const {
    return credential_.database == "tpch" ? "tpch_sf1" : "default";
  }

  [[nodiscard]] std::string baseUrl() const {
    if (credential_.host.rfind("http://", 0) == 0 || credential_.host.rfind("https://", 0) == 0) {
      return credential_.host;
    }
    return "http://" + credential_.host + ":" + std::to_string(credential_.port);
  }

  [[nodiscard]] std::string rewriteStatement(std::string_view statement) const {
    std::string sql = trim_trailing_semicolons(statement);

    if (credential_.database == "tpch") {
      static const std::regex tpch_schema(R"(\btpch_sf1\.)", std::regex::icase);
      sql = std::regex_replace(sql, tpch_schema, "");
    }

    static const std::regex character_varying(R"(\bcharacter\s+varying\b)", std::regex::icase);
    sql = std::regex_replace(sql, character_varying, "varchar");

    static const std::regex left_fn(R"(\bLEFT\s*\(\s*([^,()]+)\s*,\s*([^)]+?)\s*\))", std::regex::icase);
    sql = std::regex_replace(sql, left_fn, "SUBSTRING($1, 1, $2)");

    static const std::regex iso_date(R"('(\d{4}-\d{2}-\d{2})')");
    sql = std::regex_replace(sql, iso_date, "DATE '$1'");
    return sql;
  }

  void throwForError(const json& error, std::string_view statement) const {
    const auto message = error.value("message", "Unknown Trino error");
    const auto error_name = error.value("errorName", "");

    if (error_name == "SYNTAX_ERROR") {
      throw SyntaxException(message, statement);
    }
    if (error_name == "PERMISSION_DENIED") {
      throw PermissionDeniedException(message);
    }
    if (error_name == "NOT_SUPPORTED") {
      throw NotImplementedException(message);
    }
    if (error_name == "TABLE_NOT_FOUND" ||
        error_name == "COLUMN_NOT_FOUND" ||
        error_name == "SCHEMA_NOT_FOUND") {
      throw InvalidObjectException(message);
    }
    if (message.contains("does not exist") || message.contains("cannot be resolved")) {
      throw InvalidObjectException(message);
    }
    throw InvalidException(message);
  }

  [[nodiscard]] long requestTimeoutSeconds(const std::optional<std::chrono::steady_clock::time_point>& deadline) const {
    if (!deadline.has_value()) {
      return 600L;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= *deadline) {
      return 1L;
    }
    const auto remaining = std::chrono::duration_cast<std::chrono::seconds>(*deadline - now).count();
    return std::max<long>(1L, static_cast<long>(remaining));
  }

  json httpRequest(std::string_view url,
                   std::optional<std::string_view> body = std::nullopt,
                   std::optional<std::chrono::steady_clock::time_point> deadline = std::nullopt) const {
    ensureOpen();

    auto* curl = curl_easy_init();
    if (!curl) {
      throw ConnectionException(credential_, "Failed to initialize curl for Trino");
    }

    std::string response_body;
    std::string request_body;
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, ("X-Trino-User: " + credential_.username).c_str());
    headers = curl_slist_append(headers, ("X-Trino-Catalog: " + credential_.database).c_str());
    headers = curl_slist_append(headers, ("X-Trino-Schema: " + currentSchema()).c_str());
    if (body.has_value()) {
      request_body = std::string(*body);
      headers = curl_slist_append(headers, "Content-Type: text/plain; charset=utf-8");
      curl_easy_setopt(curl, CURLOPT_POST, 1L);
      curl_easy_setopt(curl, CURLOPT_POSTFIELDS, request_body.c_str());
      curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, request_body.size());
    }

    curl_easy_setopt(curl, CURLOPT_URL, std::string(url).c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, requestTimeoutSeconds(deadline));

    const auto res = curl_easy_perform(curl);
    long status_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status_code);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
      throw ConnectionException(credential_, curl_easy_strerror(res));
    }
    if (status_code >= 400) {
      throw ConnectionException(credential_, "Trino returned HTTP " + std::to_string(status_code) + ": " + response_body);
    }

    try {
      return json::parse(response_body);
    } catch (const std::exception& e) {
      throw ProtocolException("Failed to parse Trino JSON response: " + std::string(e.what()));
    }
  }

  void cancelQuery(std::string_view next_uri) const {
    ensureOpen();

    auto* curl = curl_easy_init();
    if (!curl) {
      return;
    }

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, ("X-Trino-User: " + credential_.username).c_str());
    headers = curl_slist_append(headers, ("X-Trino-Catalog: " + credential_.database).c_str());
    headers = curl_slist_append(headers, ("X-Trino-Schema: " + currentSchema()).c_str());

    std::string response_body;
    curl_easy_setopt(curl, CURLOPT_URL, std::string(next_uri).c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);

    const auto res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      PLOGW << "Failed to cancel Trino query via " << next_uri << ": " << curl_easy_strerror(res);
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }

  [[noreturn]] void throwTimeout(std::optional<std::string_view> cancel_uri, const uint32_t timeout_seconds) const {
    if (cancel_uri.has_value() && !cancel_uri->empty()) {
      cancelQuery(*cancel_uri);
    }
    throw std::runtime_error("Query timed out after " + std::to_string(timeout_seconds) + " seconds");
  }

  SqlVariant jsonValueToVariant(const json& value, const std::string& raw_type) const {
    if (value.is_null()) {
      return SqlVariant();
    }

    const auto raw_upper = to_upper(raw_type);
    if (raw_upper == "BIGINT") {
      return value.is_number() ? SqlVariant(value.get<int64_t>()) : SqlVariant(static_cast<int64_t>(std::stoll(jsonScalarAsString(value))));
    }
    if (raw_upper == "INTEGER") {
      return value.is_number() ? SqlVariant(value.get<int32_t>()) : SqlVariant(static_cast<int32_t>(std::stoi(jsonScalarAsString(value))));
    }
    if (raw_upper == "SMALLINT") {
      return value.is_number() ? SqlVariant(static_cast<int16_t>(value.get<int>())) : SqlVariant(static_cast<int16_t>(std::stoi(jsonScalarAsString(value))));
    }
    if (raw_upper == "DOUBLE" || raw_upper == "REAL") {
      return value.is_number() ? SqlVariant(value.get<double>()) : SqlVariant(std::stod(jsonScalarAsString(value)));
    }
    if (raw_upper == "DECIMAL") {
      return SqlVariant(SqlDecimal(jsonScalarAsString(value)));
    }
    if (raw_upper == "VARCHAR" || raw_upper == "CHAR" || raw_upper == "DATE" || raw_upper == "TIME" ||
        raw_upper == "TIMESTAMP" || raw_upper == "TIMESTAMP WITH TIME ZONE" || raw_upper == "JSON" ||
        raw_upper == "VARBINARY" || raw_upper == "IPADDRESS" || raw_upper == "UUID" || raw_upper == "BOOLEAN") {
      return SqlVariant(jsonScalarAsString(value));
    }

    if (value.is_number_integer()) {
      return SqlVariant(value.get<int64_t>());
    }
    if (value.is_number_float()) {
      return SqlVariant(value.get<double>());
    }
    return SqlVariant(jsonScalarAsString(value));
  }

  TrinoQueryResult runStatement(std::string_view statement) const {
    const auto rewritten = rewriteStatement(statement);
    const auto timeout_seconds = connection.queryTimeoutSeconds();
    const auto deadline = timeout_seconds.has_value()
                            ? std::optional(std::chrono::steady_clock::now() + std::chrono::seconds(*timeout_seconds))
                            : std::nullopt;

    json page = httpRequest(baseUrl() + "/v1/statement", rewritten, deadline);

    // Extract query ID from the initial response for reliable cancellation via /v1/query/{id}.
    std::string query_id;
    if (page.contains("id") && page["id"].is_string()) {
      query_id = page["id"].get<std::string>();
    }

    const auto cancelCurrentQuery = [&]() {
      if (!query_id.empty()) {
        cancelQuery(baseUrl() + "/v1/query/" + query_id);
      } else if (page.contains("nextUri") && !page["nextUri"].is_null()) {
        cancelQuery(page["nextUri"].get<std::string>());
      }
    };

    TrinoQueryResult result;
    while (true) {
      if (deadline.has_value() && std::chrono::steady_clock::now() >= *deadline) {
        cancelCurrentQuery();
        throwTimeout(std::nullopt, *timeout_seconds);
      }

      if (page.contains("error")) {
        throwForError(page["error"], statement);
      }

      if (result.columns.empty() && page.contains("columns") && page["columns"].is_array()) {
        for (const auto& column_json : page["columns"]) {
          result.columns.push_back({column_json.value("name", ""), extractRawType(column_json)});
        }
      }

      if (page.contains("data") && page["data"].is_array()) {
        for (const auto& row_json : page["data"]) {
          std::vector<SqlVariant> row;
          if (row_json.is_array()) {
            row.reserve(row_json.size());
            for (size_t i = 0; i < row_json.size(); ++i) {
              const auto raw_type = i < result.columns.size() ? result.columns[i].raw_type : "unknown";
              row.push_back(jsonValueToVariant(row_json[i], raw_type));
            }
          }
          result.rows.push_back(std::move(row));
        }
      }

      if (!page.contains("nextUri") || page["nextUri"].is_null()) {
        break;
      }
      const auto next_uri = page["nextUri"].get<std::string>();
      try {
        page = httpRequest(next_uri, std::nullopt, deadline);
      } catch (const ConnectionException& e) {
        if (std::string_view(e.what()).find("Timeout was reached") != std::string_view::npos) {
          cancelCurrentQuery();
          if (timeout_seconds.has_value()) {
            throwTimeout(std::nullopt, *timeout_seconds);
          }
        }
        throw;
      }
    }

    return result;
  }

  std::string version() const {
    const auto info = httpRequest(baseUrl() + "/v1/info");
    if (info.contains("nodeVersion") && info["nodeVersion"].is_object() && info["nodeVersion"].contains("version")) {
      return info["nodeVersion"]["version"].get<std::string>();
    }
    return "unknown";
  }
};

Connection::Connection(const CredentialPassword& credential, const Engine& engine, std::optional<std::string> artifacts_path)
  : ConnectionBase(credential, engine, std::move(artifacts_path))
  , impl_(std::make_unique<Pimpl>(*this, credential)) {
}

Connection::~Connection() = default;

const ConnectionBase::TypeMap& Connection::typeMap() const {
  static const TypeMap map = {
      {SqlTypeKind::STRING, "VARCHAR"},
  };
  return map;
}

void Connection::execute(const std::string_view statement) {
  impl_->runStatement(statement);
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  auto result = impl_->runStatement(statement);
  return std::make_unique<Result>(std::move(result.rows), result.columns.size());
}

void Connection::bulkLoad(const std::string_view, const std::vector<std::filesystem::path>) {
  throw NotImplementedException(
      "Trino bulk load is not implemented. dbprove expects Trino datasets to be mounted from staged parquet.");
}

void Connection::constructTable(std::string_view ddl,
                                const std::span<const std::filesystem::path> source_stems,
                                const dbprove::StorageVariant storage_variant,
                                const IcebergRegistrationCallback register_iceberg_table) {
  if (storage_variant != dbprove::StorageVariant::Iceberg) {
    ConnectionBase::constructTable(ddl, source_stems, storage_variant, register_iceberg_table);
    return;
  }

  if (source_stems.empty()) {
    throw std::runtime_error("constructTable requires at least one staged source file stem");
  }

  if (register_iceberg_table == nullptr) {
    throw std::runtime_error("Trino iceberg table construction requires an iceberg registration callback");
  }
  register_iceberg_table(ddl, source_stems);
}

std::string Connection::version() {
  return impl_->version();
}

void Connection::close() {
  impl_->close();
  ConnectionBase::close();
}
}
