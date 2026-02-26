#include <filesystem>
#include <iostream>
#include <thread>
#include <chrono>
#include <cstdio>
#include <array>
#include <iomanip>
#include <sstream>

#include "connection.h"
#include <dbprove/sql/explain/plan.h>
#include "select.h"
#include <dbprove/common/config.h>

#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <plog/Log.h>

#include <thread>
#include <chrono>
#include <random>

namespace sql::databricks
{
    using namespace sql::explain;
    using nlohmann::json;

    namespace
    {
        std::string generate_uuid()
        {
            static std::random_device rd;
            static std::mt19937 gen(rd());
            static std::uniform_int_distribution<> dis(0, 15);
            static const char* digits = "0123456789abcdef";
            std::string uuid = "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx";
            for (char& c : uuid) {
                if (c == 'x')
                    c = digits[dis(gen)];
                else if (c == 'y')
                    c = digits[(dis(gen) & 0x3) | 0x8];
            }
            return uuid;
        }

        std::string get_wall_clock()
        {
            auto now = std::chrono::system_clock::now();
            auto in_time_t = std::chrono::system_clock::to_time_t(now);
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
            std::stringstream ss;
            ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X") << "." << std::setfill('0') << std::setw(3)
                << ms.count();
            return ss.str();
        }

        static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* s)
        {
            size_t newLength = size * nmemb;
            try {
                s->append(static_cast<char*>(contents), newLength);
                return newLength;
            } catch (std::bad_alloc&) {
                return 0;
            }
        }

        static size_t HeaderCallback(char* buffer, size_t size, size_t nitems, void* userdata)
        {
            std::string* headers = static_cast<std::string*>(userdata);
            headers->append(buffer, size * nitems);
            return size * nitems;
        }


        std::string runNodeDumpPlan(const std::string& workspace, const std::string& orgId, const std::string& queryId,
                                    const std::string& startTimeMs)
        {
            std::string scriptPath = "dump_dbx_plan.mjs";

            // Try to find the script in the same directory as the current working directory,
            // or a few levels up if we are running from a build/run directory.
            std::filesystem::path p = std::filesystem::current_path();
            bool found = false;
            for (int i = 0; i < 5; ++i) {
                if (std::filesystem::exists(p / "dump_dbx_plan.mjs")) {
                    scriptPath = (p / "dump_dbx_plan.mjs").string();
                    found = true;
                    break;
                }
                if (p.has_parent_path()) {
                    p = p.parent_path();
                } else {
                    break;
                }
            }

            if (!found) {
                // Fallback to checking DBPROVE_HOME or similar if we had one,
                // but for now we'll just log and hope 'node' can find it or it's in CWD
                std::cout << "Warning: could not find dump_dbx_plan.mjs in search path, using relative path." <<
                    std::endl;
            }

            std::string cmd = "node " + scriptPath + " --workspace " + workspace + " --o " + orgId + " --queryId " +
                queryId + " --queryStartTimeMs " + startTimeMs + " --headless 1 --verbose 1";

            std::cout << "Running external plan scraper: " << cmd << std::endl;

            std::array<char, 128> buffer;
            std::string result;
            std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
            if (!pipe) {
                throw std::runtime_error("popen() failed!");
            }
            while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
                result += buffer.data();
            }
            return result;
        }
    }

    std::unique_ptr<Plan> Connection::explain(const std::string_view statement)
    {
        try {
            // 1. Generate a unique ID to use as a query tag.
            // This allows us to find the query in system.query.history reliably.
            std::string query_uuid = generate_uuid();

            std::cout << "[" << get_wall_clock() << "] Explaining statement with ID: " << query_uuid << std::endl;

            // 1a. Execute the query with the unique tag in statement_parameters
            std::map<std::string, std::string> tags = {{"dbprove", query_uuid}};
            // NOTE: We must disable caching, if not, we end up with a query_id that is not the actual one that has the plan
            std::string statement_id = execute("SET use_cached_result = false;" + std::string(statement), tags);

            // 2. Poll the history REST API until the query metadata is available
            auto info = getQueryHistoryInfo(statement_id);
            std::string start_time_ms = std::to_string(info.start_time_ms);

            // 3. Retrieve Workspace URL and Org ID
            std::string workspace;
            std::string org_id = getOrgId(workspace);

            std::string actual_statement_id = (info.cache_query_id.empty()
                                                  ? info.query_id
                                                  : info.cache_query_id);
            std::string scraped_json = runNodeDumpPlan(workspace, org_id, actual_statement_id, start_time_ms);
            if (!scraped_json.empty() && scraped_json[0] == '{') {
                std::cout << "--- START SCRAPER OUTPUT ---" << std::endl;
                std::cout << scraped_json << std::endl;
                std::cout << "--- END SCRAPER OUTPUT ---" << std::endl;
                try {
                    auto plan_json = json::parse(scraped_json);
                    std::cout << "Scraped Plan JSON captured successfully." << std::endl;
                } catch (const std::exception& ex) {
                    std::cout << "Scraped output was not valid JSON: " << ex.what() << std::endl;
                }
            } else {
                std::cout << "Scraper did not return valid JSON or was empty. Scraper output: " << scraped_json <<
                    std::endl;
            }
        } catch (const std::exception& ex) {
            std::cout << "Failed to retrieve JSON plan from Databricks History: " << ex.what() << std::endl;
        }

        // Fallback to EXPLAIN EXTENDED for now if history failed, or just return empty for now to see what we get
        // Let's keep the old EXPLAIN EXTENDED as fallback or additional info.
        const std::string explain_stmt = std::string("EXPLAIN EXTENDED ") + std::string(statement);

        try {
            auto result = fetchAll(explain_stmt);
            for (const auto& row : result->rows()) {
                if (row.columnCount() > 0) {
                    std::cout << row[0].asString() << std::endl;
                }
            }
        } catch (const std::exception& ex) {
            std::cout << "Failed to retrieve EXPLAIN EXTENDED from Databricks: " << ex.what() << std::endl;
        }

        // Return an empty minimal plan for now so we can wire up the flow end-to-end
        auto root = std::make_unique<Select>();
        return std::make_unique<Plan>(std::move(root));
    }

    std::string Connection::getOrgId(std::string& workspace_url)
    {
        workspace_url = token_.endpoint_url;
        size_t api_pos = workspace_url.find("/api/");
        if (api_pos != std::string::npos) {
            workspace_url = workspace_url.substr(0, api_pos);
        }
        if (workspace_url.rfind("http", 0) != 0) {
            workspace_url = "https://" + workspace_url;
        }

        CURL* curl = curl_easy_init();
        if (!curl)
            return "";

        std::string headers;
        curl_easy_setopt(curl, CURLOPT_URL, workspace_url.c_str());
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L); // HEAD request
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, HeaderCallback);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, &headers);

        // Follow redirects as sometimes it's needed
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

        const CURLcode res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);

        if (res == CURLE_OK) {
            size_t pos = headers.find("x-databricks-org-id:");
            if (pos == std::string::npos) {
                pos = headers.find("X-Databricks-Org-Id:");
            }
            if (pos != std::string::npos) {
                const size_t start = headers.find_first_not_of(" \t", pos + 20);
                const size_t end = headers.find_first_of("\r\n", start);
                if (start != std::string::npos && end != std::string::npos) {
                    std::string org_id = headers.substr(start, end - start);
                    std::cout << "[" << get_wall_clock() << "] Found org_id in headers: " << org_id << std::endl;
                    return org_id;
                }
            }
        }
        throw std::runtime_error(
            "Failed to retrieve x-databricks-org-id from " + workspace_url + ". Headers: " + headers);
    }

    Connection::QueryHistoryInfo Connection::getQueryHistoryInfo(const std::string& statement_id)
    {
        std::cout << "--- getQueryHistoryInfo looking for " << statement_id << std::endl;
        auto response = getHistory();
        std::cout << response.dump(2) << std::endl;

        if (!response.contains("res") || !response["res"].is_array()) {
            throw std::runtime_error("History API response missing 'res' array.");
        }
        std::string query_id = "";
        std::string cache_query_id = "";
        int64_t start_time_ms = -1;

        for (const auto& q : response["res"]) {
            query_id = q["query_id"].get<std::string>();
            if (query_id == statement_id) {
                std::cout << "found query" << std::endl;
                std::cout << q.dump(2) << std::endl;
            }

            if (q.contains("cache_query_id") && q["cache_query_id"].get<std::string>() == statement_id) {
                std::cout << "found cache" << std::endl;
                std::cout << q.dump(2) << std::endl;
            }
        }



        for (const auto& q : response["res"]) {
            // In the History API, 'query_id' corresponds to the 'statement_id' from the execution API

            if (!q.contains("query_id")) {
                continue;
            }

            query_id = q["query_id"].get<std::string>();

            if (query_id != statement_id) {
                continue;
            }

            std::cout << q.dump(2) << std::endl;

            if (q.contains("cache_query_id")) {
                cache_query_id = q["cache_query_id"].get<std::string>();
            }

            if (q.contains("query_start_time_ms")) {
                start_time_ms = q["query_start_time_ms"].get<int64_t>();
            }

            if (q.contains("query_start_time_ms")) {
                start_time_ms = q["query_start_time_ms"].get<int64_t>();
            }
            PLOGI << "Found query in history API. statement_id: " << statement_id << ", query_id: " << query_id <<
     ", cache_query_id: " << cache_query_id << ", start_time_ms: " << start_time_ms;
            std::cout << "--- queryid:" << query_id << " cache:" << cache_query_id << std::endl;

        }

        return { query_id, cache_query_id, start_time_ms};
        throw std::runtime_error("Failed to find query in history API. statement_id: " + statement_id);

    }
}
