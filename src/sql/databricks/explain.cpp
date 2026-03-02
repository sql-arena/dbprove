#include <filesystem>
#include <iostream>
#include <thread>
#include <array>

#include "connection.h"
#include "group_by.h"
#include "join.h"
#include "scan.h"
#include "sort.h"
#include "explain/node.h"
#include "explain/plan.h"
#include <unordered_set>
#include "select.h"
#include <dbprove/common/config.h>

#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <plog/Log.h>
#include <regex>
#include <fstream>

#include "explain_context.h"

namespace sql::databricks
{
    using namespace sql::explain;
    using nlohmann::json;

    void parseExplainCost(const std::string& estimate_data, ExplainContext& context)
    {
        // Databricks EXPLAIN COST output typically contains lines like:
        // == Optimized Logical Plan ==
        // +- OperatorName ..., Statistics(sizeInBytes=..., rowCount=5.86E+6, ...)
        //
        // We want to capture the operator name and the row estimate.
        
        // Match operator name (skipping prefix characters like +-, |, and spaces)
        // and then skip everything until "rowCount=" followed by a number (including scientific notation).
        std::regex line_regex(R"(([A-Z][A-Za-z]+).*Statistics\(.*rowCount=([0-9.E\+\-]+))");
        std::smatch match;

        std::istringstream stream(estimate_data);
        std::string line;
        while (std::getline(stream, line)) {
            if (std::regex_search(line, match, line_regex)) {
                if (match.size() == 3) {
                    std::string op_name = match[1].str();
                    try {
                        double rows = std::stod(match[2].str());
                        
                        // Since multiple operators might have the same name, we might need a more unique key later,
                        // but for now, we follow the instruction: "populate the context with a map of nodes and their row estimates"
                        context.row_estimates[op_name] = rows;
                        PLOGD << "Found estimate: " << op_name << " -> " << rows;
                    } catch (const std::exception& e) {
                        PLOGW << "Failed to parse row count '" << match[2].str() << "' for operator " << op_name << ": " << e.what();
                    }
                }
            }
        }
    }

    std::unique_ptr<Node> createNodeFromSparkType(const std::string& name, const json& node_json, const ExplainContext& ctx) {
        std::unique_ptr<Node> node;
        if (name.find("Scan") != std::string::npos) {
            std::string table = name;
            size_t last_space = name.find_last_of(' ');
            if (last_space != std::string::npos) {
                table = name.substr(last_space + 1);
            }
            node = std::make_unique<Scan>(table);
            node->rows_estimated = ctx.row_estimates.contains("Relation") ? ctx.row_estimates.at("Relation") : NAN;
        }
        else if (name.find("Aggregate") != std::string::npos) {
            node = std::make_unique<GroupBy>(GroupBy::Strategy::HASH, std::vector<Column>{}, std::vector<Column>{});
            node->rows_estimated = ctx.row_estimates.contains("Aggregate") ? ctx.row_estimates.at("Aggregate") : NAN;
        }
        else if (name == "Sort") {
            node = std::make_unique<Sort>(std::vector<Column>{});
            node->rows_estimated = ctx.row_estimates.contains("Sort") ? ctx.row_estimates.at("Sort") : NAN;
        }
        else if (name == "Project") {
            node = std::make_unique<Select>();
            node->rows_estimated = ctx.row_estimates.contains("Project") ? ctx.row_estimates.at("Project") : NAN;
        }
        else if (name == "Filter") {
            node = std::make_unique<Select>();
            node->rows_estimated = ctx.row_estimates.contains("Filter") ? ctx.row_estimates.at("Filter") : NAN;
        }
        else if (name.find("Join") != std::string::npos) {
             node = std::make_unique<Join>(Join::Type::INNER, Join::Strategy::HASH, "");
        }
        else {
            node = std::make_unique<Select>();
        }

        if (node && node_json.contains("metadata") && node_json["metadata"].is_object()) {
            const auto& metadata = node_json["metadata"];
            // Databricks often puts actual rows in "number of output rows" or similar fields
            for (auto it = metadata.begin(); it != metadata.end(); ++it) {
                std::string key = it.key();
                if (key.find("rows") != std::string::npos || key.find("Rows") != std::string::npos) {
                    try {
                        std::string val_str = it.value().get<std::string>();
                        // Remove commas and other formatting
                        val_str.erase(std::remove(val_str.begin(), val_str.end(), ','), val_str.end());
                        node->rows_actual = std::stod(val_str);
                        PLOGD << "Extracted actual rows for " << name << ": " << node->rows_actual;
                    } catch (...) {}
                }
            }
        }

        return node;
    }

    std::unique_ptr<Plan> buildExplainPlan(const ExplainContext& ctx) {
        const json* current = &ctx.scraped_plan;
        std::function<const json*(const json&)> findGraphContainer = [&](const json& j) -> const json* {
            if (j.is_object()) {
                if (j.contains("nodes") && j["nodes"].is_array() && 
                    j.contains("edges") && j["edges"].is_array()) {
                    return &j;
                }
                for (auto it = j.begin(); it != j.end(); ++it) {
                    const json* res = findGraphContainer(it.value());
                    if (res) return res;
                }
            } else if (j.is_array()) {
                for (const auto& item : j) {
                    const json* res = findGraphContainer(item);
                    if (res) return res;
                }
            }
            return nullptr;
        };

        const json* graph_json = findGraphContainer(ctx.scraped_plan);
        if (!graph_json) {
             throw std::runtime_error("No execution graph found in scraped JSON");
        }

        PLOGD << "Building plan from graph with " << (*graph_json)["nodes"].size() << " nodes";

        std::map<std::string, std::unique_ptr<Node>> nodes;
        std::map<std::string, std::string> child_to_parent;
        std::unordered_set<std::string> all_node_ids;

        for (const auto& node_json : (*graph_json)["nodes"]) {
            std::string id = node_json.contains("id") ? node_json["id"].is_string() ? node_json["id"].get<std::string>() : node_json["id"].dump() : "unknown";
            if (id.starts_with("\"") && id.ends_with("\"")) id = id.substr(1, id.size() - 2);

            std::string name = node_json.contains("name") ? node_json["name"].get<std::string>() : "unknown";
            
            auto node = createNodeFromSparkType(name, node_json, ctx);
            nodes[id] = std::move(node);
            all_node_ids.insert(id);
        }

        for (const auto& edge : (*graph_json)["edges"]) {
            std::string fromId, toId;
            if (edge.contains("fromId")) {
                fromId = edge["fromId"].is_string() ? edge["fromId"].get<std::string>() : edge["fromId"].dump();
            }
            if (edge.contains("toId")) {
                toId = edge["toId"].is_string() ? edge["toId"].get<std::string>() : edge["toId"].dump();
            }
            if (fromId.starts_with("\"") && fromId.ends_with("\"")) fromId = fromId.substr(1, fromId.size() - 2);
            if (toId.starts_with("\"") && toId.ends_with("\"")) toId = toId.substr(1, toId.size() - 2);
            
            if (!fromId.empty() && !toId.empty()) {
                child_to_parent[fromId] = toId;
            }
        }

        // Identify root (node with no consumer)
        std::string root_id;
        for (const auto& id : all_node_ids) {
            bool is_producer = false;
            bool is_consumer = false;
            for (const auto& edge : (*graph_json)["edges"]) {
                std::string from, to;
                if (edge.contains("fromId")) from = edge["fromId"].is_string() ? edge["fromId"].get<std::string>() : edge["fromId"].dump();
                if (edge.contains("toId")) to = edge["toId"].is_string() ? edge["toId"].get<std::string>() : edge["toId"].dump();
                if (from.starts_with("\"") && from.ends_with("\"")) from = from.substr(1, from.size() - 2);
                if (to.starts_with("\"") && to.ends_with("\"")) to = to.substr(1, to.size() - 2);
                
                if (from == id) is_producer = true;
                if (to == id) is_consumer = true;
            }
            // Root is the final consumer (it has children/producers feeding it, but it feeds nothing else)
            if (is_consumer && !is_producer) {
                 root_id = id;
            }
        }

        // Build the tree: Parent (toId) -> addChild(Child (fromId))
        // In Databricks Edges: Producer (Child) -> Consumer (Parent)
        for (const auto& edge : (*graph_json)["edges"]) {
            std::string child_id, parent_id;
            if (edge.contains("fromId")) child_id = edge["fromId"].is_string() ? edge["fromId"].get<std::string>() : edge["fromId"].dump();
            if (edge.contains("toId")) parent_id = edge["toId"].is_string() ? edge["toId"].get<std::string>() : edge["toId"].dump();
            if (child_id.starts_with("\"") && child_id.ends_with("\"")) child_id = child_id.substr(1, child_id.size() - 2);
            if (parent_id.starts_with("\"") && parent_id.ends_with("\"")) parent_id = parent_id.substr(1, parent_id.size() - 2);

            if (nodes.contains(parent_id) && nodes.contains(child_id)) {
                if (nodes[parent_id] && nodes[child_id]) {
                    nodes[parent_id]->addChild(std::move(nodes[child_id]));
                }
            }
        }

        if (root_id.empty() || !nodes.contains(root_id) || !nodes[root_id]) {
            throw std::runtime_error("Could not identify root node of the Databricks plan");
        }

        auto root = std::move(nodes[root_id]);
        return std::make_unique<Plan>(std::move(root));
    }

    void walkPlanJson(const json& plan_json) {
        // Deep recursive search for anything that looks like a graph
        std::function<void(const json&)> findGraphs = [&](const json& j) {
            if (j.is_object()) {
                if (j.contains("nodes") && j["nodes"].is_array() && 
                    j.contains("edges") && j["edges"].is_array()) {
                    
                    PLOGD << "Found a graph structure with " << j["nodes"].size() << " nodes";
                    return;
                }
                for (auto it = j.begin(); it != j.end(); ++it) {
                    findGraphs(it.value());
                }
            } else if (j.is_array()) {
                for (const auto& item : j) {
                    findGraphs(item);
                }
            }
        };

        findGraphs(plan_json);
    }

    static size_t HeaderCallback(char* buffer, size_t size, size_t nitems, void* userdata)
    {
        std::string* headers = static_cast<std::string*>(userdata);
        headers->append(buffer, size * nitems);
        return size * nitems;
    }


    std::unique_ptr<Plan> Connection::explain(const std::string_view statement)
    {
        std::string stmt_str(statement);
        size_t stmt_hash = std::hash<std::string>{}(stmt_str);
        std::string base_name = "databricks_" + std::to_string(stmt_hash);
        
        std::optional<std::filesystem::path> json_path;
        std::optional<std::filesystem::path> explain_path;
        
        if (artifacts_path_) {
            json_path = std::filesystem::path(*artifacts_path_) / (base_name + "_json");
            explain_path = std::filesystem::path(*artifacts_path_) / (base_name + "_raw_explain");
            
            if (std::filesystem::exists(*json_path) && std::filesystem::exists(*explain_path)) {
                PLOGI << "Loading artifacts from " << *artifacts_path_;
                ExplainContext context;
                
                std::ifstream jf(*json_path);
                std::string scraped_json_str((std::istreambuf_iterator<char>(jf)), std::istreambuf_iterator<char>());
                context.scraped_plan = json::parse(scraped_json_str);
                
                std::ifstream ef(*explain_path);
                context.raw_explain = std::string((std::istreambuf_iterator<char>(ef)), std::istreambuf_iterator<char>());
                
                parseExplainCost(context.raw_explain, context);
                context.dump();
                walkPlanJson(context.scraped_plan);
                return buildExplainPlan(context);
            }
        }

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
        std::string scraped_json_str = runNodeDumpPlan(actual_statement_id, start_time_ms);
        
        ExplainContext context;
        try {
            context.scraped_plan = json::parse(scraped_json_str);
        } catch (const std::exception& e) {
            PLOGE << "Failed to parse scraped JSON: " << e.what();
            PLOGE << "Raw JSON snippet: " << scraped_json_str.substr(0, 1000);
            throw;
        }

        // Find the estimated query plan to get row estimates
        PLOGI << "Running Estimation EXPLAIN";
        const std::string explain_stmt = std::string("EXPLAIN COST ") + std::string(statement);

        std::string estimate_data;
        try {
            auto r = fetchAll(explain_stmt);
            for (auto& row : r->rows()) {
                estimate_data += row[0].asString();
                estimate_data += "\n";
            }
        } catch (const std::exception& e) {
            PLOGE << "EXPLAIN COST failed: " << e.what();
            // We might still be able to build a plan without estimates
        }

        context.raw_explain = estimate_data;
        
        if (artifacts_path_) {
            PLOGI << "Saving artifacts to " << *artifacts_path_;
            std::filesystem::create_directories(*artifacts_path_);
            std::ofstream jf(*json_path);
            jf << context.scraped_plan.dump();
            std::ofstream ef(*explain_path);
            ef << context.raw_explain;
        }

        parseExplainCost(context.raw_explain, context);
        context.dump();

        PLOGI << "Walking Scraped JSON Plan Tree";
        walkPlanJson(context.scraped_plan);

        return buildExplainPlan(context);
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
