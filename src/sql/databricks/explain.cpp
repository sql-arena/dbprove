#include <filesystem>
#include <iostream>
#include <thread>
#include <array>

#include "connection.h"
#include "group_by.h"
#include "join.h"
#include "scan.h"
#include "sort.h"
#include "union.h"
#include "explain/node.h"
#include "explain/plan.h"
#include "distribution.h"
#include <unordered_set>
#include "select.h"
#include <dbprove/common/config.h>

#include <curl/curl.h>
#include "explain/limit.h"
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
                    // Map Spark logical operator names to our estimate keys if they differ
                    if (op_name == "GlobalLimit" || op_name == "LocalLimit") {
                         // We keep the specific names to allow prioritization during node mapping,
                         // but also keep "Limit" for general mapping if needed.
                         context.row_estimates["Limit"] = std::stod(match[2].str());
                    }
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

    std::unique_ptr<Node> handleScan(const std::string& name, const json& node_json, const ExplainContext& ctx)
    {
        std::string table = name;
        if (name == "Local Table Scan" || name == "LocalTableScan") {
            table = "LocalTable";
        } else {
            size_t last_space = name.find_last_of(' ');
            if (last_space != std::string::npos) {
                table = name.substr(last_space + 1);
            }
        }
        auto node = std::make_unique<Scan>(table);
        node->rows_estimated = ctx.row_estimates.contains("Relation") ? ctx.row_estimates.at("Relation") : NAN;
        if (std::isnan(node->rows_estimated) && ctx.row_estimates.contains("LocalRelation")) {
            node->rows_estimated = ctx.row_estimates.at("LocalRelation");
        }

        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                std::string key = meta.value("key", "");
                if (key == "PUSHED_FILTERS" || key == "PARTITION_FILTERS" || key == "DATA_FILTERS") {
                    if (meta.contains("values") && meta["values"].is_array() && !meta["values"].empty()) {
                        node->setFilter(meta["values"][0].get<std::string>());
                    } else if (meta.contains("value")) {
                        node->setFilter(meta["value"].get<std::string>());
                    }
                }
            }
        }

        return node;
    }

    std::unique_ptr<Node> handleAggregate(const json& node_json, const ExplainContext& ctx)
    {
        std::vector<Column> group_keys;
        std::vector<Column> aggregate_exprs;
        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                std::string key = meta.value("key", "");
                if (key == "GROUPING_EXPRESSIONS") {
                    if (meta.contains("values") && meta["values"].is_array()) {
                        for (const auto& val : meta["values"]) {
                            std::string col_name = val.get<std::string>();
                            group_keys.push_back(Column(col_name));
                        }
                    }
                } else if (key == "AGGREGATE_EXPRESSIONS") {
                    if (meta.contains("values") && meta["values"].is_array()) {
                        for (const auto& val : meta["values"]) {
                            std::string agg_name = val.get<std::string>();
                            aggregate_exprs.push_back(Column(agg_name));
                        }
                    }
                }
            }
        }
        GroupBy::Strategy strategy = group_keys.empty() ? GroupBy::Strategy::SIMPLE : GroupBy::Strategy::HASH;
        auto node = std::make_unique<GroupBy>(strategy, group_keys, aggregate_exprs);
        node->rows_estimated = ctx.row_estimates.contains("Aggregate") ? ctx.row_estimates.at("Aggregate") : NAN;
        return node;
    }

    std::unique_ptr<Node> handleSort(const json& node_json, const ExplainContext& ctx)
    {
        std::vector<Column> sort_keys;
        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                std::string key = meta.value("key", "");
                if (key == "SORT_ORDER") {
                    if (meta.contains("values") && meta["values"].is_array()) {
                        for (const auto& val : meta["values"]) {
                            sort_keys.push_back(Column(val.get<std::string>()));
                        }
                    }
                }
            }
        }
        auto node = std::make_unique<Sort>(sort_keys);
        node->rows_estimated = ctx.row_estimates.contains("Sort") ? ctx.row_estimates.at("Sort") : NAN;
        return node;
    }

    std::unique_ptr<Node> handleProject(const ExplainContext& ctx)
    {
        auto node = std::make_unique<Select>();
        node->rows_estimated = ctx.row_estimates.contains("Project") ? ctx.row_estimates.at("Project") : NAN;
        return node;
    }

    std::unique_ptr<Node> handleFilter(const json& node_json, const ExplainContext& ctx)
    {
        auto node = std::make_unique<Select>();
        node->rows_estimated = ctx.row_estimates.contains("Filter") ? ctx.row_estimates.at("Filter") : NAN;

        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                std::string key = meta.value("key", "");
                if (key == "CONDITION") {
                    if (meta.contains("value")) {
                        node->setFilter(meta["value"].get<std::string>());
                    } else if (meta.contains("values") && meta["values"].is_array() && !meta["values"].empty()) {
                        node->setFilter(meta["values"][0].get<std::string>());
                    }
                }
            }
        }

        return node;
    }

    std::unique_ptr<Node> handleJoin(const std::string& name, const json& node_json, const ExplainContext& ctx)
    {
        Join::Type type = Join::Type::INNER;
        if (name.find("Left Outer") != std::string::npos || name.find("LeftOuter") != std::string::npos)
            type = Join::Type::LEFT_OUTER;
        else if (name.find("Right Outer") != std::string::npos || name.find("RightOuter") != std::string::npos)
            type = Join::Type::RIGHT_OUTER;
        else if (name.find("Full Outer") != std::string::npos || name.find("FullOuter") != std::string::npos)
            type = Join::Type::FULL;
        else if (name.find("Left Semi") != std::string::npos || name.find("LeftSemi") != std::string::npos)
            type = Join::Type::LEFT_SEMI_INNER;
        else if (name.find("Left Anti") != std::string::npos || name.find("LeftAnti") != std::string::npos)
            type = Join::Type::LEFT_ANTI;
        else if (name.find("Right Semi") != std::string::npos || name.find("RightSemi") != std::string::npos)
            type = Join::Type::RIGHT_SEMI_INNER;
        else if (name.find("Right Anti") != std::string::npos || name.find("RightAnti") != std::string::npos)
            type = Join::Type::RIGHT_ANTI;

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

        auto node = std::make_unique<Join>(type, Join::Strategy::HASH, condition);
        node->rows_estimated = ctx.row_estimates.contains("Join") ? ctx.row_estimates.at("Join") : NAN;
        return node;
    }

    std::unique_ptr<Node> handleExchange(const std::string& name, const json& node_json)
    {
        Distribute::Strategy strategy = Distribute::Strategy::HASH;
        std::vector<Column> dist_cols;
        std::string tag = node_json.value("tag", "");

        if (name.find("Broadcast") != std::string::npos || tag.find("BROADCAST") != std::string::npos) {
            strategy = Distribute::Strategy::BROADCAST;
        }

        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                std::string key = meta.value("key", "");
                if (key == "PARTITIONING_TYPE") {
                    std::string val = meta.value("value", "");
                    if (val == "BroadcastPartitioning" || val == "BROADCAST") {
                        strategy = Distribute::Strategy::BROADCAST;
                    } else if (val == "RoundRobinPartitioning" || val == "RoundRobin") {
                        strategy = Distribute::Strategy::ROUND_ROBIN;
                    } else if (val == "Single") {
                        strategy = Distribute::Strategy::GATHER;
                    }
                } else if (key == "PARTITIONING_EXPRESSIONS" || key == "SHUFFLE_ATTRIBUTES") {
                    if (meta.contains("values") && meta["values"].is_array()) {
                        for (const auto& val_json : meta["values"]) {
                            dist_cols.push_back(Column(val_json.get<std::string>()));
                        }
                    } else if (meta.contains("value")) {
                        dist_cols.push_back(Column(meta["value"].get<std::string>()));
                    }
                }
            }
        }

        return std::make_unique<Distribute>(strategy, dist_cols);
    }

    std::unique_ptr<Node> handleLimit(const std::string& name, const json& node_json, const ExplainContext& ctx)
    {
        uint64_t limit_val = 0;
        std::vector<Column> sort_keys;

        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                std::string key = meta.value("key", "");
                if (key == "LIMIT" || key == "limit") {
                    try {
                        std::string val_str = meta.value("value", "0");
                        limit_val = std::stoull(val_str);
                    } catch (...) {
                    }
                } else if (key == "SORT_ORDER") {
                    if (meta.contains("values") && meta["values"].is_array()) {
                        for (const auto& val : meta["values"]) {
                            sort_keys.push_back(Column(val.get<std::string>()));
                        }
                    }
                }
            }
        }

        if (limit_val == 0) {
             if (ctx.row_estimates.contains("GlobalLimit")) limit_val = static_cast<uint64_t>(ctx.row_estimates.at("GlobalLimit"));
             else if (ctx.row_estimates.contains("Limit")) limit_val = static_cast<uint64_t>(ctx.row_estimates.at("Limit"));
             else if (ctx.row_estimates.contains("LocalLimit")) limit_val = static_cast<uint64_t>(ctx.row_estimates.at("LocalLimit"));
        }
        
        std::unique_ptr<Node> limit_node = std::make_unique<Limit>(static_cast<RowCount>(limit_val));
        limit_node->rows_estimated = ctx.row_estimates.contains("Limit") ? ctx.row_estimates.at("Limit") : NAN;
        if (std::isnan(limit_node->rows_estimated)) {
             limit_node->rows_estimated = ctx.row_estimates.contains("GlobalLimit") ? ctx.row_estimates.at("GlobalLimit") : NAN;
        }
        if (std::isnan(limit_node->rows_estimated)) {
             limit_node->rows_estimated = ctx.row_estimates.contains("LocalLimit") ? ctx.row_estimates.at("LocalLimit") : NAN;
        }

        if (!sort_keys.empty()) {
             auto sort_node = std::make_unique<Sort>(sort_keys);
             sort_node->rows_estimated = ctx.row_estimates.contains("Sort") ? ctx.row_estimates.at("Sort") : NAN;
             sort_node->addChild(std::move(limit_node));
             return sort_node;
        }

        return limit_node;
    }

    std::unique_ptr<Node> handleUnion(const ExplainContext& ctx)
    {
        auto node = std::make_unique<Union>(Union::Type::ALL);
        node->rows_estimated = ctx.row_estimates.contains("Union") ? ctx.row_estimates.at("Union") : NAN;
        return node;
    }

    std::unique_ptr<Node> createNodeFromSparkType(const std::string& name, const json& node_json,
                                                  const ExplainContext& ctx)
    {
        std::unique_ptr<Node> node;
        std::string tag = node_json.value("tag", "");

        if (name.find("Scan") != std::string::npos || tag.find("SCAN") != std::string::npos) {
            node = handleScan(name, node_json, ctx);
        } else if (name.find("Aggregate") != std::string::npos || tag.find("AGGREGATE") != std::string::npos) {
            node = handleAggregate(node_json, ctx);
        } else if (name == "Sort" || tag.find("SORT") != std::string::npos) {
            node = handleSort(node_json, ctx);
        } else if (name.find("Project") != std::string::npos || tag.find("PROJECT") != std::string::npos) {
            node = handleProject(ctx);
        } else if (name.find("Filter") != std::string::npos || tag.find("FILTER") != std::string::npos) {
            node = handleFilter(node_json, ctx);
        } else if (name.find("Join") != std::string::npos || tag.find("JOIN") != std::string::npos) {
            node = handleJoin(name, node_json, ctx);
        } else if (name.find("Exchange") != std::string::npos || tag.find("EXCHANGE") != std::string::npos || tag.find("SHUFFLE") != std::string::npos) {
            node = handleExchange(name, node_json);
        } else if (name.find("Union") != std::string::npos || tag.find("UNION") != std::string::npos) {
            node = handleUnion(ctx);
        } else if (name.find("Limit") != std::string::npos || tag.find("LIMIT") != std::string::npos ||
                   name.find("TopK") != std::string::npos || tag.find("TOPK") != std::string::npos) {
            node = handleLimit(name, node_json, ctx);
        } else if (name == "Arrow Conversion" || name == "Columnar to Row" || name == "Columnar To Row" ||
                   name == "Result Query Stage" || name == "Arrow Result Stage" || name == "Whole Stage Codegen") {
            PLOGD << "Explicitly ignoring node " << name;
            return nullptr;
        }

        if (!node) {
            // Fallback for unknown nodes that might be important (stages, adaptive plan wrappers, etc.)
            PLOGE << "Failed to parse node " << name << " with tag " << tag;
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

    void propagateEstimates(Node* node)
    {
        if (!node) return;

        for (auto child : node->children()) {
            propagateEstimates(child);
        }

        if (std::isnan(node->rows_estimated) || std::isnan(node->rows_actual)) {
            for (auto child : node->children()) {
                if (std::isnan(node->rows_estimated) && !std::isnan(child->rows_estimated)) {
                    node->rows_estimated = child->rows_estimated;
                }
                if (std::isnan(node->rows_actual) && !std::isnan(child->rows_actual)) {
                    node->rows_actual = child->rows_actual;
                }
            }
        }
    }

    std::unique_ptr<Node> removeTopLevelProject(std::unique_ptr<Node> root)
    {
        if (!root) return nullptr;

        while (root->type == sql::explain::NodeType::SELECT || root->type == sql::explain::NodeType::PROJECTION) {
            if (root->childCount() == 1) {
                PLOGD << "Removing top-level " << to_string(root->type) << " node";
                root = root->takeChild(0);
                root->setParentToSelf();
            } else {
                break;
            }
        }

        return root;
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
                PLOGD << "Created node " << id << " (" << name << ") with tag " << tag;
                info_map[id] = {id, name, tag, std::move(node), true};
            } else {
                PLOGD << "Skipped node " << id << " (" << name << ") with tag " << tag;
                // If we skipped a node, we still need to record it so edges can pass through it
                info_map[id] = {id, name, tag, nullptr, true};
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
                PLOGD << "Found edge: " << fromId << " -> " << toId;
                parent_to_children[fromId].push_back(toId);
                info_map[toId].is_root = false;
            }
        }

        // Recursive helper to link nodes bottom-up
        std::function<std::vector<std::unique_ptr<Node>>(const std::string&)> linkRecursively =
            [&](const std::string& current_id) -> std::vector<std::unique_ptr<Node>> {
            std::vector<std::unique_ptr<Node>> current_nodes;

            // Get all linked nodes from children first
            std::vector<std::unique_ptr<Node>> children_nodes;
            if (parent_to_children.contains(current_id)) {
                for (const auto& child_id : parent_to_children[current_id]) {
                    auto linked_children = linkRecursively(child_id);
                    for (auto& child_node : linked_children) {
                        children_nodes.push_back(std::move(child_node));
                    }
                }
            }

            if (info_map[current_id].node) {
                // If this is a real node, add children to it and return it
                if (info_map[current_id].node->type != NodeType::SCAN) {
                    for (auto& child_node : children_nodes) {
                        PLOGD << "Linking consumer " << current_id << " (" << info_map[current_id].name << ") to producer " << child_node->typeName();
                        
                        // We need to be careful if the node already has children (e.g., Sort wrapping Limit for TopK)
                        // We want to attach graph-level children to the leaf of our canonical technical subtree.
                        Node* target = info_map[current_id].node.get();
                        while (target->childCount() > 0 && target->type != NodeType::SCAN && target->type != NodeType::JOIN && target->type != NodeType::UNION && target->type != NodeType::SEQUENCE) {
                            target = target->firstChild();
                        }
                        target->addChild(std::move(child_node));
                    }
                } else {
                    if (!children_nodes.empty()) {
                        PLOGW << "Scan node " << current_id << " has unexpected children, ignoring them to keep SCAN a leaf";
                    }
                }
                current_nodes.push_back(std::move(info_map[current_id].node));
            } else {
                // If this node was skipped, just pass the children up
                current_nodes = std::move(children_nodes);
            }

            return current_nodes;
        };

        // Identify the best root candidate among those that are marked as is_root
        std::string root_id = "";
        for (const auto& [id, info] : info_map) {
            if (info.is_root) {
                PLOGD << "Root candidate: " << id << " (" << info.name << ") tag: " << info.tag;
                // Prefer stages that sound like output/result AND have children (to avoid empty result islands)
                if ((info.tag.find("RESULT") != std::string::npos || 
                     info.tag.find("SINK") != std::string::npos ||
                     info.name.find("Result") != std::string::npos) &&
                    parent_to_children.contains(id)) {
                    root_id = id;
                    // Usually there's only one "Result" stage with children
                }
                if (root_id.empty() && parent_to_children.contains(id)) root_id = id;
            }
        }

        // Fallback: if no roots with children found, just pick the first root
        if (root_id.empty() && !info_map.empty()) {
            root_id = info_map.begin()->first;
        }

        std::unique_ptr<Node> root;
        if (!root_id.empty()) {
            auto linked_roots = linkRecursively(root_id);
            if (!linked_roots.empty()) {
                root = std::move(linked_roots[0]);
            }
        }

        if (!root) {
            throw std::runtime_error("Could not identify root node of the Databricks plan");
        }

        PLOGD << "Identified root candidate: " << root_id << " (" << info_map[root_id].name << ")";

        // Post-processing
        propagateEstimates(root.get());
        root = removeTopLevelProject(std::move(root));

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


    std::unique_ptr<Plan> Connection::explain(const std::string_view statement, std::optional<std::string_view> name)
    {
        std::string name_str;
        if (name) {
            name_str = std::string(*name);
        } else {
            std::string stmt_str(statement);
            size_t stmt_hash = std::hash<std::string>{}(stmt_str);
            name_str = std::to_string(stmt_hash);
        }

        ExplainContext context;

        auto json_artefact = getArtefact(name_str, "json");
        if (json_artefact) {
            PLOGI << "Loading scraped plan artifact for " << name_str;
            context.scraped_plan = json::parse(*json_artefact);
        } else {
            context.scraped_plan = getLiveScrapedPlan(statement);
            storeArtefact(name_str, "json", context.scraped_plan.dump());
        }

        auto explain_artefact = getArtefact(name_str, "raw_explain");
        if (explain_artefact) {
            PLOGI << "Loading EXPLAIN COST artifact for " << name_str;
            context.raw_explain = *explain_artefact;
        } else {
            context.raw_explain = getLiveExplainCost(statement);
            storeArtefact(name_str, "raw_explain", context.raw_explain);
        }

        parseExplainCost(context.raw_explain, context);
        context.dump();

        PLOGI << "Walking Scraped JSON Plan Tree";
        walkPlanJson(context.scraped_plan);

        return buildExplainPlan(context);
    }

    nlohmann::json Connection::getLiveScrapedPlan(std::string_view statement)
    {
        std::map<std::string, std::string> tags = {{"dbprove", "DBPROVE"}};
        // NOTE: We must disable caching, if not, we end up with a query_id that is not the actual one that has the plan
        std::string statement_id = execute(statement, tags);

        PLOGI << "Gathering data for Statement ID: " << statement_id;
        auto info = getQueryHistoryInfo(statement_id);
        std::string start_time_ms = std::to_string(info.start_time_ms);

        PLOGI << "Finding Org ID: " << statement_id;
        // find the actual execution using the ugly, undocumented API
        std::string actual_statement_id = (info.cache_query_id.empty() ? info.query_id : info.cache_query_id);
        std::string scraped_json_str = runNodeDumpPlan(actual_statement_id, start_time_ms);

        try {
            return json::parse(scraped_json_str);
        } catch (const std::exception& e) {
            PLOGE << "Failed to parse scraped JSON: " << e.what();
            PLOGE << "Raw JSON snippet: " << scraped_json_str.substr(0, 1000);
            throw;
        }
    }

    std::string Connection::getLiveExplainCost(std::string_view statement)
    {
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

        return estimate_data;
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
