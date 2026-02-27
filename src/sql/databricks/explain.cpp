#include <filesystem>
#include <iostream>
#include <thread>
#include <array>

#include "connection.h"
#include <dbprove/sql/explain/plan.h>
#include "select.h"
#include <dbprove/common/config.h>

#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <plog/Log.h>



namespace sql::databricks
{
    using namespace sql::explain;
    using nlohmann::json;

    static size_t HeaderCallback(char* buffer, size_t size, size_t nitems, void* userdata)
    {
        std::string* headers = static_cast<std::string*>(userdata);
        headers->append(buffer, size * nitems);
        return size * nitems;
    }


    std::unique_ptr<Plan> Connection::explain(const std::string_view statement)
    {
        std::map<std::string, std::string> tags = {{"dbprove", "DBPROVE"}};
        // NOTE: We must disable caching, if not, we end up with a query_id that is not the actual one that has the plan
        std::string statement_id = execute(statement, tags);

        PLOGI << "Gathering data for Statement ID: " << statement_id;
        auto info = getQueryHistoryInfo(statement_id);
        std::string start_time_ms = std::to_string(info.start_time_ms);

        PLOGI << "Finding Org ID: " << statement_id;
        std::string org_id = getOrgId();

        // find the actual execution using the ugly, undocumented API
        std::string actual_statement_id = (info.cache_query_id.empty() ? info.query_id : info.cache_query_id);
        std::string scraped_json = runNodeDumpPlan(actual_statement_id, start_time_ms);
        auto plan_json = json::parse(scraped_json);

        // Find the estimated query plan to get row estimates
        PLOGI << "Running Estimation EXPLAIN";
        const std::string explain_stmt = std::string("EXPLAIN COST ") + std::string(statement);

        auto r =  fetchAll(explain_stmt);
        std::string estimate_data;
        for (auto& row : r->rows()) {
            estimate_data += row[0].asString();
            break;
        }

        // Return an empty minimal plan for now so we can wire up the flow end-to-end
        auto root = std::make_unique<Select>();
        return std::make_unique<Plan>(std::move(root));
    }



    std::string Connection::getOrgId() const
    {
        std::string workspace_url = token_.endpoint_url;
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
                    return org_id;
                }
            }
        }
        throw std::runtime_error(
            "Failed to retrieve x-databricks-org-id from " + workspace_url + ". Headers: " + headers);
    }

    std::string Connection::runNodeDumpPlan(const std::string& statement_id, const std::string& startTimeMs) const
    {
        std::string workspace = token_.endpoint_url;
        std::string orgId = getOrgId();

        std::string scriptPath = "scripts/dump_databricks_plan.mjs";

        // Try to find the script in the same directory as the current working directory,
        // or a few levels up if we are running from a build/run directory.
        std::filesystem::path p = std::filesystem::current_path();
        bool found = false;
        for (int i = 0; i < 5; ++i) {
            if (std::filesystem::exists(p / "scripts/dump_databricks_plan.mjs")) {
                scriptPath = (p / "scripts/dump_databricks_plan.mjs").string();
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
            PLOGW << "Could not find scripts/dump_databricks_plan.mjs in search path, using relative path.";
        }

        std::string cmd = "node " + scriptPath + " --workspace " + workspace + " --o " + orgId + " --queryId " + statement_id
            + " --queryStartTimeMs " + startTimeMs + " --headless 1";

        PLOGI << "Running external plan scraper: " << cmd;

        std::array<char, 128> buffer;
        std::string result;
        std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
        if (!pipe) {
            throw std::runtime_error("popen() failed when trying to grab stdout from scraper!");
        }
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
            result += buffer.data();
        }
        return std::move(result);
    }


    Connection::QueryHistoryInfo Connection::getQueryHistoryInfo(const std::string& statement_id)
    {
        auto response = queryHistory(statement_id);
        if (!response.contains("res") || !response["res"].is_array()) {
            throw std::runtime_error("History API response missing 'res' array.");
        }
        std::string query_id = "";
        std::string cache_query_id = "";
        int64_t start_time_ms = -1;

        for (const auto& q : response["res"]) {
            // In the History API, 'query_id' corresponds to the 'statement_id' from the execution API

            if (!q.contains("query_id")) {
                continue;
            }

            query_id = q["query_id"].get<std::string>();

            if (query_id != statement_id) {
                continue;
            }
            if (q.contains("cache_query_id")) {
                cache_query_id = q["cache_query_id"].get<std::string>();
            }

            if (q.contains("query_start_time_ms")) {
                start_time_ms = q["query_start_time_ms"].get<int64_t>();
            }

            if (q.contains("query_start_time_ms")) {
                start_time_ms = q["query_start_time_ms"].get<int64_t>();
            }
        }

        if (query_id.empty()) {
            throw std::runtime_error("Could not find statement_id" + statement_id + " in history API.");
        }
        return {query_id, cache_query_id, start_time_ms};
    }
}
