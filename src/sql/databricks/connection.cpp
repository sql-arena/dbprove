#include "connection.h"
#include "result.h"
#include "row.h"
#include "dbprove/sql/sql_exceptions.h"
#include <dbprove/sql/sql.h>

#include <iostream>
#include <curl/curl.h>
#include <duckdb/common/exception.hpp>
#include <nlohmann/json.hpp>

#include "plog/Log.h"


namespace sql::databricks
{
    using namespace nlohmann;

    static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* s)
    {
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
    static void handleDatabricksResponse(const CredentialAccessToken& credential, json& response)
    {
        PLOGD << "Handling Databricks response: " << response.dump();
        if (response.contains("status")) {
            const auto& status = response["status"];
            if (status.contains("state")) {
                const auto state = status["state"].get<std::string>();
                if (state == "FAILED") {
                    const auto& error = status["error"];
                    const auto error_code = error["error_code"].get<std::string>();
                    const auto message = error["message"].get<std::string>();

                    if (message.find("ALREADY_EXISTS") != std::string::npos || error_code == "SCHEMA_ALREADY_EXISTS" || error_code == "TABLE_OR_VIEW_ALREADY_EXISTS") {
                        // These are expected during idempotent operations
                        PLOGD << "Idempotent error ignored: " << error_code;
                        return;
                    }

                    if (error_code == "TABLE_OR_VIEW_NOT_FOUND" || message.find("TABLE_OR_VIEW_NOT_FOUND") != std::string::npos) {
                        throw InvalidObjectException(message);
                    }
                    if (error_code == "SCHEMA_NOT_FOUND" || message.find("SCHEMA_NOT_FOUND") != std::string::npos) {
                        throw InvalidObjectException(message);
                    }
                    throw std::runtime_error("Databricks Query FAILED: " + message);
                }
            }
        }

        if (response.contains("error_code")) {
            const auto error_code = response["error_code"].get<std::string>();
            const auto message = response["message"].get<std::string>();

            if (error_code == "SCHEMA_ALREADY_EXISTS" || error_code == "TABLE_OR_VIEW_ALREADY_EXISTS") {
                // These are expected during idempotent operations
                PLOGD << "Idempotent API error ignored: " << error_code;
                return;
            }

            if (error_code == "PERMISSION_DENIED") {
                throw ConnectionException(credential, "Permission denied " + message);
            }
            if (error_code == "INVALID_TOKEN") {
                throw PermissionDeniedException("Invalid token " + message);
            }
            if (error_code == "ENDPOINT_NOT_FOUND") {
                throw ConnectionException(credential, "Could not find host at: " + credential.endpoint_url + " the error was: " + message);
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
    }


    class Connection::Pimpl
    {
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
            , endpoint_("https://" + token.endpoint_url)
            , warehouse_id_(token.database)
        {
            curl_ = curl_easy_init();
            assert(!endpoint_.empty());
            if (!curl_) {
                throw std::runtime_error("Failed to initialize curl");
            }
        }

        ~Pimpl()
        {
            if (curl_) {
                curl_easy_cleanup(curl_);
            }
        }

        json sendQuery(const std::string_view query, const std::map<std::string, std::string>& tags = {})
        {
            std::string readBuffer;
            std::string apiUrl = endpoint_ + "/api/2.0/sql/statements/";
            PLOGI << "Sending Databricks query to " << apiUrl;
            PLOGI << "Query: " << query;

            // Prepare JSON payload with the SQL query
            json payload = {
                {"statement", std::string(query)},
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
            curl_easy_setopt(curl_, CURLOPT_URL, apiUrl.c_str());
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

            curl_easy_reset(curl_);
            return json::parse(readBuffer);
        }

        json queryHistory(const std::string& statement_id) const
        {
            std::string readBuffer;
            std::string apiUrl = endpoint_ + "/api/2.0/sql/history/queries";

            PLOGI << "Sending Databricks GET request to" << apiUrl;

            // Set curl options
            curl_easy_setopt(curl_, CURLOPT_URL, apiUrl.c_str());
            curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
            curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &readBuffer);
            curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "GET");

            json payload = {
                {"max_results", 1},
                {"filter_by",json{
                    {"statement_ids", json::array({ statement_id})}}}
            };
            std::string body = payload.dump();
            curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, body.c_str());

            // Set up headers
            curl_slist* headers = nullptr;
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
            curl_easy_reset(curl_);
            return json::parse(readBuffer);
        }
    };

    Connection::Connection(const CredentialAccessToken& credential, const Engine& engine, std::optional<std::string> artifacts_path)
        : ConnectionBase(credential, engine, std::move(artifacts_path))
        , impl_(std::make_unique<Pimpl>(*this, credential))
        , token_(credential)
    {
    }

    Connection::~Connection()
    {
    }

    void Connection::execute(std::string_view statement)
    {
        auto response = impl_->sendQuery(statement);
        handleDatabricksResponse(token_, response);
    }

    std::string Connection::execute(std::string_view statement, const std::map<std::string, std::string>& tags)
    {
        auto response = impl_->sendQuery(statement, tags);
        handleDatabricksResponse(token_, response);

        std::string statement_id;
        std::string query_id;

        if (response.contains("statement_id")) {
            statement_id = response["statement_id"].get<std::string>();
        }

        return statement_id;
    }


    std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement)
    {
        auto response = impl_->sendQuery(statement);
        handleDatabricksResponse(token_, response);
        auto out = std::make_unique<Result>(response);
        return out;
    }

    void Connection::bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths)
    {
        // TODO: should have a general way to map table to its location

        const auto [schema_name, table_name] = splitTable(table);

        const std::string statement = "COPY INTO " + std::string(table) + " " + "FROM 's3://sql-arena-data/tpc-h/sf1' "
            + "FILEFORMAT = PARQUET " + "FILES = ('" + table_name + ".parquet')";

        auto response = impl_->sendQuery(statement);
        handleDatabricksResponse(token_, response);
    }

    const ConnectionBase::TypeMap& Connection::typeMap() const
    {
        static const TypeMap map = {{"INT", "BIGINT"}};
        return map;
    }

    std::optional<RowCount> Connection::tableRowCount(const std::string_view table)
    {
        try {
            auto result = ConnectionBase::tableRowCount(table);
            PLOGD << "Table row count for " << table << ": " << (result ? std::to_string(*result) : "nullopt");
            return result;
        } catch (const InvalidObjectException& e) {
            PLOGD << "Table " << table << " not found (InvalidObjectException): " << e.what();
            return std::nullopt;
        } catch (const std::exception& e) {
            PLOGD << "Table row count for " << table << " failed with exception: " << e.what();
            throw;
        }
    }

    void Connection::analyse(const std::string_view table_name)
    {
        execute("ANALYZE TABLE " + std::string(table_name) + " COMPUTE STATISTICS");
    }

    json Connection::queryHistory(const std::string& statement_id) const
    {
        unsigned remaining_retry = 3;
        while (remaining_retry-- > 0) {
            PLOGI << "Collecting history query history API - remaining retries: " << remaining_retry;
            sleep(1);
            auto response =  impl_->queryHistory(statement_id);

            if (response["res"].size() == 0) {
                /* Not there yet*/
                continue;
            }
            if (!response["res"][0].value("is_final", false)) {
                /* Not "consistent" yet (this is so fucked up) */
                continue;
            }
            return response;
        }
        throw std::runtime_error("Could not find statement_id" + statement_id + " in history API.");
    }
}
