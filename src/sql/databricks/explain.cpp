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
#include "projection.h"

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
                        PLOGW << "Failed to parse row count '" << match[2].str() << "' for operator " << op_name << ": "
 << e.what();
                    }
                }
            }
        }
    }

    std::unique_ptr<Node> createNodeFromSparkType(const std::string& name, const json& node_json,
                                                  const ExplainContext& ctx)
    {
        std::unique_ptr<Node> node;
        std::string tag = node_json.value("tag", "");

        if (name.find("Scan") != std::string::npos || tag.find("SCAN") != std::string::npos) {
            std::string table = name;
            size_t last_space = name.find_last_of(' ');
            if (last_space != std::string::npos) {
                table = name.substr(last_space + 1);
            }
            node = std::make_unique<Scan>(table);
            node->rows_estimated = ctx.row_estimates.contains("Relation") ? ctx.row_estimates.at("Relation") : NAN;
        } else if (name.find("Aggregate") != std::string::npos || tag.find("AGGREGATE") != std::string::npos) {
            node = std::make_unique<GroupBy>(GroupBy::Strategy::HASH, std::vector<Column>{}, std::vector<Column>{});
            node->rows_estimated = ctx.row_estimates.contains("Aggregate") ? ctx.row_estimates.at("Aggregate") : NAN;
        } else if (name == "Sort" || tag.find("SORT") != std::string::npos) {
            node = std::make_unique<Sort>(std::vector<Column>{});
            node->rows_estimated = ctx.row_estimates.contains("Sort") ? ctx.row_estimates.at("Sort") : NAN;
        } else if (name == "Project" || tag.find("PROJECT") != std::string::npos) {
            node = std::make_unique<Select>();
            node->rows_estimated = ctx.row_estimates.contains("Project") ? ctx.row_estimates.at("Project") : NAN;
        } else if (name == "Filter" || tag.find("FILTER") != std::string::npos) {
            node = std::make_unique<Select>();
            node->rows_estimated = ctx.row_estimates.contains("Filter") ? ctx.row_estimates.at("Filter") : NAN;
        } else if (name.find("Join") != std::string::npos || tag.find("JOIN") != std::string::npos) {
            Join::Type type = Join::Type::INNER;
            if (name.find("Left Outer") != std::string::npos) type = Join::Type::LEFT_OUTER;
            else if (name.find("Right Outer") != std::string::npos) type = Join::Type::RIGHT_OUTER;
            else if (name.find("Full Outer") != std::string::npos) type = Join::Type::FULL;

            std::string condition;
            if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
                std::string left_keys, right_keys;
                for (const auto& meta : node_json["metaData"]) {
                    std::string key = meta.value("key", "");
                    if (key == "LEFT_KEYS") {
                        if (meta.contains("values") && meta["values"].is_array() && !meta["values"].empty()) {
                            left_keys = meta["values"][0].get<std::string>();
                        }
                    } else if (key == "RIGHT_KEYS") {
                        if (meta.contains("values") && meta["values"].is_array() && !meta["values"].empty()) {
                            right_keys = meta["values"][0].get<std::string>();
                        }
                    }
                }
                if (!left_keys.empty() && !right_keys.empty()) {
                    condition = left_keys + " = " + right_keys;
                }
            }

            node = std::make_unique<Join>(type, Join::Strategy::HASH, condition);
            node->rows_estimated = ctx.row_estimates.contains("Join") ? ctx.row_estimates.at("Join") : NAN;
        } else if (name.find("Exchange") != std::string::npos || tag.find("EXCHANGE") != std::string::npos || tag.find("SHUFFLE") != std::string::npos) {
            node = std::make_unique<Projection>(std::vector<Column>{}); // Map Exchange to Projection for now
        }

        if (!node) {
            // Fallback for unknown nodes that might be important (stages, adaptive plan wrappers, etc.)
            if (tag.find("SINK") != std::string::npos || tag.find("RESULT") != std::string::npos || 
                tag.find("STAGE") != std::string::npos || tag.find("PLAN") != std::string::npos) {
                 node = std::make_unique<Projection>(std::vector<Column>{});
            }
        }

        if (node && node_json.contains("metrics") && node_json["metrics"].is_array()) {
            for (const auto& metric : node_json["metrics"]) {
                std::string key = metric.value("key", "");
                if (key == "NUMBER_OUTPUT_ROWS") {
                    try {
                        std::string val_str = metric.value("value", "0");
                        val_str.erase(std::remove(val_str.begin(), val_str.end(), ','), val_str.end());
                        node->rows_actual = std::stod(val_str);
                        PLOGD << "Extracted actual rows for " << name << ": " << node->rows_actual;
                    } catch (...) {
                    }
                }
            }
        }

        return node;
    }

    std::unique_ptr<Plan> buildExplainPlan(const ExplainContext& ctx)
    {
        const json* graph_json = nullptr;
        std::function<const json*(const json&)> findGraphContainer = [&](const json& j) -> const json* {
            if (j.is_object()) {
                if (j.contains("nodes") && j["nodes"].is_array() && j.contains("edges") && j["edges"].is_array()) {
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

        graph_json = findGraphContainer(ctx.scraped_plan);
        if (!graph_json) {
            throw std::runtime_error("No execution graph found in scraped JSON");
        }

        PLOGD << "Building plan from graph with " << (*graph_json)["nodes"].size() << " nodes";

        struct NodeInfo {
            std::string id;
            std::string name;
            std::string tag;
            std::unique_ptr<Node> node;
            bool is_root = true;
        };

        std::map<std::string, NodeInfo> info_map;
        // First pass: create all nodes and map them by ID
        for (const auto& node_json : (*graph_json)["nodes"]) {
            std::string id = node_json.value("id", "unknown");
            if (id.starts_with("\"") && id.ends_with("\"")) id = id.substr(1, id.size() - 2);

            std::string name = node_json.value("name", "unknown");
            std::string tag = node_json.value("tag", "");

            auto node = createNodeFromSparkType(name, node_json, ctx);
            if (node) {
                info_map[id] = {id, name, tag, std::move(node), true};
            }
        }

        // Second pass: Identify all parent-child relationships
        std::map<std::string, std::vector<std::string>> parent_to_children;
        for (const auto& edge : (*graph_json)["edges"]) {
            std::string fromId = edge.value("fromId", "");
            std::string toId = edge.value("toId", "");
            if (fromId.starts_with("\"") && fromId.ends_with("\"")) fromId = fromId.substr(1, fromId.size() - 2);
            if (toId.starts_with("\"") && toId.ends_with("\"")) toId = toId.substr(1, toId.size() - 2);

            if (info_map.contains(fromId) && info_map.contains(toId)) {
                parent_to_children[fromId].push_back(toId);
                info_map[toId].is_root = false;
            }
        }

        // Recursive helper to link nodes bottom-up
        std::function<void(const std::string&)> linkRecursively = [&](const std::string& parent_id) {
            if (!parent_to_children.contains(parent_id)) return;

            auto children_ids = parent_to_children[parent_id];
            for (const auto& child_id : children_ids) {
                // Link children of this child first
                linkRecursively(child_id);

                if (info_map[parent_id].node && info_map[child_id].node) {
                    PLOGD << "Linking consumer " << parent_id << " (" << info_map[parent_id].name << ") to producer " << child_id << " (" << info_map[child_id].name << ")";
                    info_map[parent_id].node->addChild(std::move(info_map[child_id].node));
                }
            }
        };

        // Identify the best root candidate among those that are marked as is_root
        std::string root_id = "";
        for (const auto& [id, info] : info_map) {
            if (info.is_root) {
                // Prefer stages that sound like output/result
                if (info.tag.find("RESULT") != std::string::npos || info.tag.find("SINK") != std::string::npos) {
                    root_id = id;
                    break;
                }
                if (root_id.empty()) root_id = id;
            }
        }

        if (!root_id.empty()) {
            linkRecursively(root_id);
        }

        if (root_id.empty() || !info_map.contains(root_id) || !info_map[root_id].node) {
            throw std::runtime_error("Could not identify root node of the Databricks plan");
        }

        PLOGD << "Identified root candidate: " << root_id << " (" << info_map[root_id].name << ")";

        auto root = std::move(info_map[root_id].node);
        return std::make_unique<Plan>(std::move(root));
    }

    void walkPlanJson(const json& plan_json)
    {
        // Deep recursive search for anything that looks like a graph
        std::function<void(const json&)> findGraphs = [&](const json& j) {
            if (j.is_object()) {
                if (j.contains("nodes") && j["nodes"].is_array() && j.contains("edges") && j["edges"].is_array()) {
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

    static size_t HeaderCallback(const char* buffer, const size_t size, const size_t nitems, void* userdata)
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
                context.raw_explain = std::string((std::istreambuf_iterator<char>(ef)),
                                                  std::istreambuf_iterator<char>());

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

        std::string cmd = "node " + scriptPath + " --workspace " + workspace + " --o " + orgId + " --queryId " +
            statement_id + " --queryStartTimeMs " + startTimeMs + " --headless 1";

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
