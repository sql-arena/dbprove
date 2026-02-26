#include "connection.h"
#include "result.h"
#include "row.h"
#include "dbprove/sql/sql_exceptions.h"
#include <dbprove/sql/sql.h>

#include <iostream>
#include <curl/curl.h>
#include <duckdb/common/exception.hpp>
#include <nlohmann/json.hpp>


namespace sql::databricks {
using namespace nlohmann;

static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* s) {
  size_t newLength = size * nmemb;
  try {
    s->append(static_cast<char*>(contents), newLength);
    return newLength;
  } catch (std::bad_alloc&) {
    // Handle memory problem
    return 0;
  }
}

/**
 *
 * Deal with various response code coming back from DataBricks (Because HTTP error codes are so last year)
 */
static void handleDatabricksResponse(const CredentialAccessToken& credential, json& response) {
  if (!response.contains("error_code")) {
    return;
  }
  const auto error_code = response["error_code"].get<std::string>();
  const auto message = response["message"].get<std::string>();
  if (error_code == "PERMISSION_DENIED") {
    throw ConnectionException(credential, "Permission denied " + message);
  }
  if (error_code == "INVALID_TOKEN") {
    throw PermissionDeniedException("Invalid token " + message);
  }
  if (error_code == "ENDPOINT_NOT_FOUND") {
    throw ConnectionException(credential, "Could not find host at: " + credential.endpoint_url);
  }
  if (error_code == "INVALID_PARAMETER_VALUE") {
    throw ConnectionException(
        credential,
        "An invalid parameter was passed while connecting to: " + credential.endpoint_url + " the error was: " +
        message);
  }
  if (error_code == "TABLE_OR_VIEW_NOT_FOUND" || error_code == "RESOURCE_DOES_NOT_EXIST") {
    throw InvalidObjectException(message);
  }
  // No idea, we should probably have handled differently
  throw std::runtime_error("Databricks error: " + message);
}


class Connection::Pimpl {
  Connection& connection_;
  CURL* curl_;
  const std::string token_;
  std::string endpoint_;
  std::string warehouse_id_;

public:
  explicit Pimpl(Connection& connection, const CredentialAccessToken& token)
    : connection_(connection)
    , curl_(nullptr)
    , token_(token.token)
    , endpoint_(token.endpoint_url)
    , warehouse_id_(token.database) {
    curl_ = curl_easy_init();
    assert(!endpoint_.empty());
    if (!curl_) {
      throw std::runtime_error("Failed to initialize curl");
    }

    if (!to_lower(endpoint_).starts_with("http")) {
      endpoint_ = "https://" + endpoint_;
    }
  }

  ~Pimpl() {
    if (curl_) {
      curl_easy_cleanup(curl_);
    }
  }

  json sendQuery(const std::string_view query, const std::map<std::string, std::string>& tags = {}) {
    if (!curl_) {
      throw std::runtime_error("Curl not initialized");
    }

    std::string readBuffer;

    // Prepare JSON payload with the SQL query
    json payload = {{"statement", std::string(query)},
                    {"warehouse_id", warehouse_id_},
                    {"wait_timeout", "30s"},
                    {"disposition", "INLINE"}
                    };

    if (!tags.empty()) {
      json tags_array = json::array();
      for (const auto& [key, value] : tags) {
        tags_array.push_back({{"key", key}, {"value", value}});
      }
      payload["query_tags"] = tags_array;
    }

    const std::string jsonPayload = payload.dump();

    // Set curl options
    curl_easy_setopt(curl_, CURLOPT_URL, endpoint_.c_str());
    curl_easy_setopt(curl_, CURLOPT_POST, 1L);
    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &readBuffer);
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, jsonPayload.c_str());

    // Set up headers
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");

    // Add the PAT token as Authorization header
    const std::string authHeader = "Authorization: Bearer " + token_;
    headers = curl_slist_append(headers, authHeader.c_str());
    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);

    const CURLcode res = curl_easy_perform(curl_);

    curl_slist_free_all(headers);

    // Check for errors
    if (res != CURLE_OK) {
      std::stringstream errorMsg;
      errorMsg << "curl_easy_perform() failed: " << curl_easy_strerror(res);
      throw std::runtime_error(errorMsg.str());
    }

    return json::parse(readBuffer);
  }

  json getRequest(const std::string& path) {
    if (!curl_) {
      throw std::runtime_error("Curl not initialized");
    }

    std::string readBuffer;
    
    // Construct full URL
    std::string base_url = endpoint_;
    size_t api_pos = base_url.find("/api/2.0/");
    if (api_pos != std::string::npos) {
      base_url = base_url.substr(0, api_pos);
    }
    std::string url = base_url + path;

    // Set curl options
    curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &readBuffer);

    // Set up headers
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Accept: application/json");

    // Add the PAT token as Authorization header
    const std::string authHeader = "Authorization: Bearer " + token_;
    headers = curl_slist_append(headers, authHeader.c_str());
    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);

    const CURLcode res = curl_easy_perform(curl_);

    curl_slist_free_all(headers);

    // Check for errors
    if (res != CURLE_OK) {
      std::stringstream errorMsg;
      errorMsg << "curl_easy_perform() (GET) failed: " << curl_easy_strerror(res);
      throw std::runtime_error(errorMsg.str());
    }

    return json::parse(readBuffer);
  }

  json getHistory() {
    return getRequest("/api/2.0/sql/history/queries?max_results=10&include_metrics=true");
  }
};

Connection::Connection(const CredentialAccessToken& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>(*this, credential))
  , token_(credential) {
}

Connection::~Connection() {
}

void Connection::execute(std::string_view statement) {
  auto response = impl_->sendQuery(statement);
  handleDatabricksResponse(token_, response);
}

std::string Connection::execute(std::string_view statement, const std::map<std::string, std::string>& tags) {
  auto response = impl_->sendQuery(statement, tags);
  handleDatabricksResponse(token_, response);
  
  std::string statement_id;
  std::string query_id;
  
  if (response.contains("statement_id")) {
    statement_id = response["statement_id"].get<std::string>();
  }

  return statement_id;
}

json Connection::getHistory() {
  return impl_->getHistory();
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  auto response = impl_->sendQuery(statement);
  handleDatabricksResponse(token_, response);
  return std::make_unique<Result>(response);
}

std::unique_ptr<RowBase> Connection::fetchRow(const std::string_view statement) {
  auto response = impl_->sendQuery(statement);
  handleDatabricksResponse(token_, response);
  auto result = new Result(response);
  const auto row_count = result->rowCount();
  if (row_count == 0) {
    throw EmptyResultException(statement);
  }
  if (row_count > 1) {
    throw InvalidRowsException("Expected to find a single row in the data, but found: " + std::to_string(row_count),
                               statement);
  }
  return std::make_unique<Row>(result, 0, true);
}

SqlVariant Connection::fetchScalar(const std::string_view statement) {
  const auto row = fetchRow(statement);
  if (row->columnCount() != 1) {
    throw InvalidColumnsException("Expected to find a single column in the data", statement);
  }
  return row->asVariant(0);
}

void Connection::bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) {
  // TODO: should have a general way to map table to its location

  const auto [schema_name, table_name] = splitTable(table);

  const std::string statement =
      "COPY INTO " + std::string(table) + " " +
      "FROM 's3://sql-arena-data/tpc-h/sf1' " +
      "FILEFORMAT = PARQUET " +
      "FILES = ('" + table_name + ".parquet')";

  auto response = impl_->sendQuery(statement);
  handleDatabricksResponse(token_, response);
}

const ConnectionBase::TypeMap& Connection::typeMap() const {
  static const TypeMap map = {{"INT", "BIGINT"}};
  return map;
}

void Connection::analyse(const std::string_view table_name) {
  execute("ANALYZE TABLE " + std::string(table_name) + " COMPUTE STATISTICS");
}
}
