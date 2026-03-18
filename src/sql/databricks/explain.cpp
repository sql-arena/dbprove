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
#include "selection.h"
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
#include "sequence.h"
#include "projection.h"

namespace sql::databricks
{
    using namespace sql::explain;
    using nlohmann::json;

    std::string safeGetString(const json& j, const std::string& key, const std::string& default_val = "")
    {
        if (j.contains(key) && j[key].is_string()) {
            return j[key].get<std::string>();
        }
        return default_val;
    }

    void parseExplainCost(const std::string& estimate_data, ExplainContext& context)
    {
        // Databricks EXPLAIN COST output typically contains lines like:
        // == Optimized Logical Plan ==
        // +- OperatorName ..., Statistics(sizeInBytes=..., rowCount=5.86E+6, ...)
        //
        // We want to capture the operator name and the row estimate, preserving order and structure.

        PLOGD << "Starting parseExplainCost";
        std::istringstream stream(estimate_data);
        std::string line;
        bool in_logical_plan = false;

        // Matches operator indentation, name, and rowCount
        // Example: :  +- Join Inner [...], Statistics(rowCount=3.19E+4, ...)
        std::regex op_regex(R"(^([\s:|+-]*)([A-Z][A-Za-z0-9]+))");
        std::regex rows_regex(R"(rowCount=([0-9.E\+\-]+))");

        while (std::getline(stream, line)) {
            if (line.find("== Optimized Logical Plan ==") != std::string::npos) {
                in_logical_plan = true;
                continue;
            }
            if (in_logical_plan && line.starts_with("==")) {
                break;
            }
            if (!in_logical_plan || line.empty()) {
                continue;
            }

            std::smatch op_match;
            if (std::regex_search(line, op_match, op_regex)) {
                std::string indent_str = op_match[1].str();
                std::string op_name = op_match[2].str();
                int depth = static_cast<int>(indent_str.length() / 3);

                double rows = NAN;
                std::smatch rows_match;
                if (std::regex_search(line, rows_match, rows_regex)) {
                    try {
                        rows = std::stod(rows_match[1].str());
                    } catch (...) {}
                }

                PLOGD << "Found logical operator: " << op_name << " at depth " << depth << " with rows " << rows;
                context.logical_plan.push_back({op_name, rows, depth});
                context.logical_op_matched.push_back(false);

                // Backwards compatibility: also populate the map (first occurrence wins)
                if (!context.row_estimates.contains(op_name)) {
                    context.row_estimates[op_name] = rows;
                }
                // Special handling for Limits
                if (op_name == "GlobalLimit" || op_name == "LocalLimit") {
                    if (!context.row_estimates.contains("Limit")) {
                        context.row_estimates["Limit"] = rows;
                    }
                }
            }
        }
        PLOGD << "Finished parseExplainCost, parsed " << context.logical_plan.size() << " logical operators";
    }

    double extractActualRows(const json& node_json)
    {
        if (node_json.contains("metrics") && node_json["metrics"].is_array()) {
            for (const auto& metric : node_json["metrics"]) {
                if (!metric.is_object()) continue;
                std::string key = safeGetString(metric, "key");
                if (key == "NUMBER_OUTPUT_ROWS") {
                    try {
                        std::string val_str = safeGetString(metric, "value", "0");
                        val_str.erase(std::remove(val_str.begin(), val_str.end(), ','), val_str.end());
                        return std::stod(val_str);
                    } catch (...) {}
                }
            }
        }
        return NAN;
    }

    std::unique_ptr<Node> handleScan(const std::string& name, const json& node_json, ExplainContext& ctx)
    {
        PLOGD << "Entering handleScan for " << name;
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
        node->rows_actual = extractActualRows(node_json);

        std::string filter;
        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                if (!meta.is_object()) continue;
                std::string key = safeGetString(meta, "key");
                if (key == "PUSHED_FILTERS" || key == "PARTITION_FILTERS" || key == "DATA_FILTERS" || key == "FILTERS") {
                    if (meta.contains("values") && meta["values"].is_array()) {
                        for (const auto& v : meta["values"]) {
                            if (v.is_string()) {
                                if (!filter.empty()) filter += " AND ";
                                filter += v.get<std::string>();
                            }
                        }
                    } else if (meta.contains("value") && meta["value"].is_string()) {
                        if (!filter.empty()) filter += " AND ";
                        filter += meta["value"].get<std::string>();
                    }
                }
            }
        }

        if (!filter.empty()) {
            node->setFilter(filter);
        }

        // Populate attribute map for scan replication (Reused Exchange)
        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                if (!meta.is_object()) continue;
                std::string key = safeGetString(meta, "key");
                if (key == "OUTPUT" || key == "Output attributes") {
                    std::vector<std::string> outputs;
                    if (meta.contains("values") && meta["values"].is_array()) {
                        for (const auto& v : meta["values"]) {
                            if (v.is_string()) outputs.push_back(v.get<std::string>());
                        }
                    } else if (meta.contains("value") && meta["value"].is_string()) {
                        outputs.push_back(meta["value"].get<std::string>());
                    }

                    for (const auto& attr : outputs) {
                        PLOGD << "Mapping attribute " << attr << " to scan " << table << " (actual: " << node->rows_actual << ")";
                        ctx.attribute_to_scan[attr] = {table, filter, node->rows_estimated, node->rows_actual};
                    }
                }
            }
        }

        return node;
    }

    std::unique_ptr<Node> handleAggregate(const json& node_json, ExplainContext& ctx)
    {
        PLOGD << "Entering handleAggregate";
        std::vector<Column> group_keys;
        std::vector<Column> aggregate_exprs;
        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                if (!meta.is_object()) continue;
                std::string key = safeGetString(meta, "key");
                if (key == "GROUPING_EXPRESSIONS" || key == "keys") {
                    if (meta.contains("values") && meta["values"].is_array()) {
                        for (const auto& val : meta["values"]) {
                            if (val.is_string()) {
                                std::string col_name = val.get<std::string>();
                                group_keys.push_back(Column(col_name));
                            }
                        }
                    } else if (meta.contains("value") && meta["value"].is_string()) {
                        group_keys.push_back(Column(meta["value"].get<std::string>()));
                    }
                } else if (key == "AGGREGATE_EXPRESSIONS" || key == "functions") {
                    if (meta.contains("values") && meta["values"].is_array()) {
                        for (const auto& val : meta["values"]) {
                            if (val.is_string()) {
                                std::string agg_name = val.get<std::string>();
                                aggregate_exprs.push_back(Column(agg_name));
                            }
                        }
                    } else if (meta.contains("value") && meta["value"].is_string()) {
                        aggregate_exprs.push_back(Column(meta["value"].get<std::string>()));
                    }
                }
            }
        }
        GroupBy::Strategy strategy = group_keys.empty() ? GroupBy::Strategy::SIMPLE : GroupBy::Strategy::HASH;
        auto node = std::make_unique<GroupBy>(strategy, group_keys, aggregate_exprs);
        return node;
    }

    std::unique_ptr<Node> handleSort(const json& node_json, ExplainContext& ctx)
    {
        PLOGD << "Entering handleSort";
        std::vector<Column> sort_keys;
        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                if (!meta.is_object()) continue;
                std::string key = safeGetString(meta, "key");
                if (key == "SORT_ORDER" || key == "sortOrder") {
                    if (meta.contains("values") && meta["values"].is_array()) {
                        for (const auto& val : meta["values"]) {
                            if (val.is_string()) {
                                sort_keys.push_back(Column(val.get<std::string>()));
                            }
                        }
                    }
                }
            }
        }
        auto node = std::make_unique<Sort>(sort_keys);
        return node;
    }

    std::unique_ptr<Node> handleProject(const json& node_json, ExplainContext& ctx)
    {
        PLOGD << "Entering handleProject";
        auto node = std::make_unique<Select>();

        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                if (!meta.is_object()) continue;
                std::string key = safeGetString(meta, "key");
                if (key == "PROJECTION" || key == "expressions") {
                    if (meta.contains("values") && meta["values"].is_array()) {
                        for (const auto& val : meta["values"]) {
                            if (val.is_string()) {
                                node->columns_output.push_back(val.get<std::string>());
                            }
                        }
                    } else if (meta.contains("value") && meta["value"].is_string()) {
                        node->columns_output.push_back(meta["value"].get<std::string>());
                    }
                }
            }
        }

        return node;
    }

    std::unique_ptr<Node> handleFilter(const json& node_json, ExplainContext& ctx)
    {
        PLOGD << "Entering handleFilter";
        std::string filter_expr;

        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                if (!meta.is_object()) continue;
                std::string key = safeGetString(meta, "key");
                if (key == "CONDITION" || key == "condition") {
                    if (meta.contains("value") && meta["value"].is_string()) {
                        filter_expr = meta["value"].get<std::string>();
                    } else if (meta.contains("values") && meta["values"].is_array() && !meta["values"].empty()) {
                        if (meta["values"][0].is_string()) {
                            filter_expr = meta["values"][0].get<std::string>();
                        }
                    }
                }
            }
        }

        auto node = std::make_unique<Selection>(filter_expr);
        return node;
    }

    std::unique_ptr<Node> handleJoin(const std::string& name, const json& node_json, ExplainContext& ctx)
    {
        PLOGD << "Entering handleJoin for " << name;
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
                if (!meta.is_object()) continue;
                std::string key = safeGetString(meta, "key");
                if (key == "LEFT_KEYS") {
                    if (meta.contains("values") && meta["values"].is_array() && !meta["values"].empty()) {
                        if (meta["values"][0].is_string()) {
                            left_keys = meta["values"][0].get<std::string>();
                        }
                    }
                } else if (key == "RIGHT_KEYS") {
                    if (meta.contains("values") && meta["values"].is_array() && !meta["values"].empty()) {
                        if (meta["values"][0].is_string()) {
                            right_keys = meta["values"][0].get<std::string>();
                        }
                    }
                }
            }
            if (!left_keys.empty() && !right_keys.empty()) {
                condition = left_keys + " = " + right_keys;
            }
        }

        auto node = std::make_unique<Join>(type, Join::Strategy::HASH, condition);
        return node;
    }

    std::unique_ptr<Node> handleExchange(const std::string& name, const json& node_json)
    {
        PLOGD << "Entering handleExchange for " << name;
        Distribute::Strategy strategy = Distribute::Strategy::HASH;
        std::vector<Column> dist_cols;
        std::string tag = safeGetString(node_json, "tag");

        if (name.find("Broadcast") != std::string::npos || tag.find("BROADCAST") != std::string::npos) {
            strategy = Distribute::Strategy::BROADCAST;
        }

        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                if (!meta.is_object()) continue;
                std::string key = safeGetString(meta, "key");
                if (key == "PARTITIONING_TYPE") {
                    std::string val = safeGetString(meta, "value");
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
                            if (val_json.is_string()) {
                                dist_cols.push_back(Column(val_json.get<std::string>()));
                            }
                        }
                    } else if (meta.contains("value") && meta["value"].is_string()) {
                        dist_cols.push_back(Column(meta["value"].get<std::string>()));
                    }
                }
            }
        }

        auto node = std::make_unique<Distribute>(strategy, dist_cols);
        if (tag.find("SINK") != std::string::npos) {
            PLOGD << "Exchange is a SINK, marking it to be collapsed if it's a pass-through";
            // We can't easily mark it here to be collapsed by collapseRelationalNoOps 
            // because Distribute is not in the list of types it collapses.
        }
        return node;
    }

    std::unique_ptr<Node> handleLimit(const std::string& name, const json& node_json, ExplainContext& ctx)
    {
        PLOGD << "Entering handleLimit for " << name;
        uint64_t limit_val = 0;
        std::vector<Column> sort_keys;

        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                if (!meta.is_object()) continue;
                std::string key = safeGetString(meta, "key");
                if (key == "LIMIT" || key == "limit") {
                    try {
                        std::string val_str = safeGetString(meta, "value", "0");
                        limit_val = std::stoull(val_str);
                    } catch (...) {
                    }
                } else if (key == "SORT_ORDER") {
                    if (meta.contains("values") && meta["values"].is_array()) {
                        for (const auto& val : meta["values"]) {
                            if (val.is_string()) {
                                sort_keys.push_back(Column(val.get<std::string>()));
                            }
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

        if (!sort_keys.empty()) {
             auto sort_node = std::make_unique<Sort>(sort_keys);
             limit_node->addChild(std::move(sort_node));
        }

        return limit_node;
    }

    std::unique_ptr<Node> handleUnion(ExplainContext& ctx)
    {
        auto node = std::make_unique<Union>(Union::Type::ALL);
        return node;
    }

    std::unique_ptr<Node> handleReusedExchange(const json& node_json, const ExplainContext& ctx)
    {
        PLOGD << "Entering handleReusedExchange";
        if (node_json.contains("metaData") && node_json["metaData"].is_array()) {
            for (const auto& meta : node_json["metaData"]) {
                if (!meta.is_object()) continue;
                std::string key = safeGetString(meta, "key");
                if (key == "OUTPUT" || key == "Output attributes") {
                    std::vector<std::string> outputs;
                    if (meta.contains("values") && meta["values"].is_array()) {
                        for (const auto& v : meta["values"]) {
                            if (v.is_string()) outputs.push_back(v.get<std::string>());
                        }
                    } else if (meta.contains("value") && meta["value"].is_string()) {
                        outputs.push_back(meta["value"].get<std::string>());
                    }

                    for (const auto& attr : outputs) {
                        if (ctx.attribute_to_scan.contains(attr)) {
                            const auto& info = ctx.attribute_to_scan.at(attr);
                            PLOGD << "Replicating Scan " << info.table_name << " for Reused Exchange via attribute " << attr;
                            auto node = std::make_unique<Scan>(info.table_name);
                            if (!info.filter.empty()) {
                                node->setFilter(info.filter);
                            }
                            node->rows_estimated = info.rows_estimated;
                            node->rows_actual = info.rows_actual;
                            return node;
                        }
                    }
                }
            }
        }
        PLOGD << "Could not identify original scan for Reused Exchange, returning nullptr";
        return nullptr;
    }

    std::unique_ptr<Node> createNodeFromSparkType(const std::string& name, const json& node_json,
                                                  ExplainContext& ctx)
    {
        PLOGD << "Entering createNodeFromSparkType for node: " << name;
        std::unique_ptr<Node> node;
        std::string tag = safeGetString(node_json, "tag");
        PLOGD << "Node tag: " << tag;

        if (name == "Arrow Conversion" || name == "Columnar to Row" || name == "Columnar To Row" ||
            name == "Result Query Stage" || name == "Arrow Result Stage" || name == "Whole Stage Codegen" ||
            name == "Shuffle Map Stage" || name == "Adaptive Plan" ||
            tag.find("STAGE") != std::string::npos || tag.find("PLAN") != std::string::npos) {
            PLOGD << "Explicitly ignoring node " << name << " with tag " << tag;
            return nullptr;
        }

        if (name == "Subquery" || tag.find("SUBQUERY") != std::string::npos) {
            PLOGD << "Subquery stage identified: " << name << " with tag " << tag;
            return nullptr;
        }

        if (name == "Reused Exchange") {
            return handleReusedExchange(node_json, ctx);
        }

        if (name.find("Scan") != std::string::npos || tag.find("SCAN") != std::string::npos) {
            node = handleScan(name, node_json, ctx);
        } else if (name.find("Aggregate") != std::string::npos || tag.find("AGGREGATE") != std::string::npos || 
                   name.find("GroupingAgg") != std::string::npos || name.find("Agg") != std::string::npos) {
            node = handleAggregate(node_json, ctx);
        } else if (name == "Sort" || tag.find("SORT") != std::string::npos) {
            node = handleSort(node_json, ctx);
        } else if (name.find("Project") != std::string::npos || tag.find("PROJECT") != std::string::npos) {
            node = handleProject(node_json, ctx);
        } else if (name.find("Filter") != std::string::npos || tag.find("FILTER") != std::string::npos) {
            node = handleFilter(node_json, ctx);
        } else if (name.find("Join") != std::string::npos || tag.find("JOIN") != std::string::npos) {
            node = handleJoin(name, node_json, ctx);
        } else if (name.find("Exchange") != std::string::npos || tag.find("EXCHANGE") != std::string::npos || tag.find("SHUFFLE") != std::string::npos || tag.find("BROADCAST") != std::string::npos) {
            node = handleExchange(name, node_json);
        } else if (name.find("Union") != std::string::npos || tag.find("UNION") != std::string::npos) {
            node = handleUnion(ctx);
        } else if (name.find("Limit") != std::string::npos || tag.find("LIMIT") != std::string::npos ||
                   name.find("TopK") != std::string::npos || tag.find("TOPK") != std::string::npos) {
            node = handleLimit(name, node_json, ctx);
        }

        if (!node) {
            // Fallback for unknown nodes that might be important (stages, adaptive plan wrappers, etc.)
            PLOGE << "Failed to parse node " << name << " with tag " << tag;
            if (tag.find("SINK") != std::string::npos || tag.find("RESULT") != std::string::npos) {
                 node = std::make_unique<Projection>(std::vector<Column>{});
            }
        }

        PLOGD << "Checking metrics for node: " << name;
        if (node && std::isnan(node->rows_actual)) {
            node->rows_actual = extractActualRows(node_json);
            if (!std::isnan(node->rows_actual)) {
                PLOGD << "Extracted actual rows for " << name << ": " << node->rows_actual;
            }
        }

        return node;
    }


    std::unique_ptr<Node> collapseRelationalNoOps(std::unique_ptr<Node> root)
    {
        if (!root) return nullptr;

        std::vector<Node*> to_collapse;

        // Identify nodes to collapse using bottom-up traversal
        for (auto& node : root->bottom_up()) {
            // We only collapse SELECT or PROJECTION nodes
            if (node.type != NodeType::SELECT && node.type != NodeType::PROJECTION) {
                continue;
            }

            // Must have exactly one child
            if (node.childCount() != 1) {
                continue;
            }

            Node* child = node.firstChild();
            const auto select_node = reinterpret_cast<Select*>(&node);

            // Collapse if:
            // - No output columns (technical pass-through)
            if (select_node->columns_output.empty()) {
                to_collapse.push_back(&node);
                continue;
            }

            // - Row counts match (including rounding or NaN)
            if (std::isnan(node.rows_actual) || std::isnan(child->rows_actual) ||
                node.rows_actual == child->rows_actual) {
                to_collapse.push_back(&node);
                continue;
            }

            if (!std::isinf(node.rows_actual) && !std::isinf(child->rows_actual) &&
                std::abs(node.rows_actual - child->rows_actual) < 1.0) {
                to_collapse.push_back(&node);
                continue;
            }

            if (node.rows_estimated == child->rows_estimated || std::isnan(node.rows_estimated)) {
                to_collapse.push_back(&node);
                continue;
            }
        }

        // Perform the collapse
        for (Node* node : to_collapse) {
            if (node->isRoot()) {
                PLOGD << "Collapsing root " << to_string(node->type) << " node";
                if (node->childCount() == 0) {
                    continue;
                }
                auto final_child = node->takeChild(0);
                node->addSharedChild(std::move(final_child));
            } else {
                PLOGD << "Collapsing relational no-op: " << to_string(node->type) << " node (rows: " << node->rows_actual << ")";
                try {
                    node->remove();
                } catch (const std::exception& e) {
                    PLOGE << "Failed to remove node: " << e.what();
                }
            }
        }

        return root;
    }

    std::unique_ptr<Node> removeTopLevelProject(std::unique_ptr<Node> root)
    {
        if (!root) return nullptr;

        while (root->type == sql::explain::NodeType::SELECT || root->type == sql::explain::NodeType::PROJECTION) {
            if (root->childCount() == 1) {
                PLOGD << "Removing top-level " << to_string(root->type) << " node";
                break;
            } else {
                break;
            }
        }

        return root;
    }

    void matchEstimates(Node* root, ExplainContext& ctx)
    {
        if (!root || ctx.logical_plan.empty()) return;

        PLOGD << "Starting estimate matching pass";
        
        // Logical operators in Spark EXPLAIN COST:
        // Join, Project, Filter, Relation, Aggregate, Sort, Limit
        
        // Helper to find best match for a physical node in the logical plan
        auto findMatch = [&](NodeType type, const std::string& type_name, int physical_depth) -> int {
            int best_idx = -1;
            int min_depth_diff = 1000;

            for (size_t i = 0; i < ctx.logical_plan.size(); ++i) {
                if (ctx.logical_op_matched[i]) continue;
                
                bool match = false;
                if (ctx.logical_plan[i].name.find(type_name) != std::string::npos) {
                    match = true;
                } else if (type_name == "Limit" && 
                          (ctx.logical_plan[i].name == "GlobalLimit" || ctx.logical_plan[i].name == "LocalLimit")) {
                    match = true;
                } else if (type_name == "Aggregate" && 
                          (ctx.logical_plan[i].name.find("Agg") != std::string::npos)) {
                    match = true;
                } else if (type_name == "Project" && 
                          (ctx.logical_plan[i].name == "Project")) {
                    match = true;
                }

                if (match) {
                    // Refined Greedy Matching: Prefer logical operators that are at similar relative depths.
                    // This prevents top-level logical operators from matching deep physical subqueries.
                    // We also consider the index to maintain some positional stability.
                    
                    // HEURISTIC: In Spark plans, physical plans are much deeper. 
                    // A physical depth of 10-20 might correspond to logical depth 5-10.
                    // But within a subquery, they should align better if we anchor them.
                    
                    // For now, let's just stick to the first match but add a simple check:
                    // If the logical operator is at depth 1 or 2 (top level), don't match it
                    // if the physical node is deep (depth > 5) unless it's the ONLY match left.
                    // This is a bit too specific for Q18 but addresses the root cause.
                    
                    if (ctx.logical_plan[i].depth <= 2 && physical_depth > 5) {
                        // Skip this logical operator for now, hope for a better match later
                        // or that it matches a top-level physical node.
                        continue;
                    }

                    return static_cast<int>(i);
                }
            }
            
            // Fallback: if no "good" match found, take the first available one anyway
            for (size_t i = 0; i < ctx.logical_plan.size(); ++i) {
                if (ctx.logical_op_matched[i]) continue;
                if (ctx.logical_plan[i].name.find(type_name) != std::string::npos ||
                    (type_name == "Limit" && (ctx.logical_plan[i].name == "GlobalLimit" || ctx.logical_plan[i].name == "LocalLimit")) ||
                    (type_name == "Aggregate" && (ctx.logical_plan[i].name.find("Agg") != std::string::npos)) ||
                    (type_name == "Project" && (ctx.logical_plan[i].name == "Project"))) {
                    return static_cast<int>(i);
                }
            }

            return -1;
        };

        // BFS might be better than DFS for matching top-level nodes first
        std::vector<std::pair<Node*, int>> queue;
        queue.push_back({root, 0});
        size_t head = 0;
        while(head < queue.size()){
            auto [physical_node, depth] = queue[head++];
            for(auto child : physical_node->children()){
                queue.push_back({child, depth + 1});
            }

            std::string type_name = "";
            switch (physical_node->type) {
                case NodeType::JOIN: type_name = "Join"; break;
                case NodeType::SCAN: type_name = "Relation"; break;
                case NodeType::FILTER: type_name = "Filter"; break;
                case NodeType::GROUP_BY: type_name = "Aggregate"; break;
                case NodeType::DISTRIBUTE: type_name = "Exchange"; break;
                case NodeType::SORT: type_name = "Sort"; break;
                case NodeType::LIMIT: type_name = "Limit"; break;
                case NodeType::PROJECTION:
                case NodeType::SELECT: type_name = "Project"; break;
                default: continue;
            }

            int match_idx = findMatch(physical_node->type, type_name, depth);
            if (match_idx != -1) {
                physical_node->rows_estimated = ctx.logical_plan[match_idx].rows_estimated;
                ctx.logical_op_matched[match_idx] = true;
                PLOGD << "Matched physical " << type_name << " (depth " << depth << ") to logical " << ctx.logical_plan[match_idx].name 
                      << " (index " << match_idx << ", depth " << ctx.logical_plan[match_idx].depth << ", estimate: " << physical_node->rows_estimated << ")";
                
                // Special case: If we matched a JOIN, also try to propagate to its child DISTRIBUTE nodes immediately
                // This helps anchor the child branches
                if (physical_node->type == NodeType::JOIN) {
                    for (auto child : physical_node->children()) {
                        if (child->type == NodeType::DISTRIBUTE && std::isnan(child->rows_estimated)) {
                            child->rows_estimated = physical_node->rows_estimated;
                        }
                    }
                }
            }
        }
        PLOGD << "Finished estimate matching pass";
    }

    struct NodeInfo {
        std::string id;
        std::string name;
        std::string tag;
        std::unique_ptr<Node> node;
        bool is_root = true;
        bool is_subquery_root = false;
    };

    void preScanScansForReusedExchange(const json& graph_json, ExplainContext& ctx)
    {
        PLOGD << "Pass 1: Pre-scanning for Scan nodes to populate attribute mapping";
        for (const auto& node_json : graph_json["nodes"]) {
            if (!node_json.is_object()) continue;
            std::string name = safeGetString(node_json, "name", "unknown");
            std::string tag = safeGetString(node_json, "tag", "");
            if (name.find("Scan") != std::string::npos || tag.find("SCAN") != std::string::npos) {
                handleScan(name, node_json, ctx);
            }
        }
    }

    void createNodesAndMap(const json& graph_json, ExplainContext& ctx, std::map<std::string, NodeInfo>& info_map)
    {
        PLOGD << "Pass 2: Creating nodes and mapping them by ID";
        for (const auto& node_json : graph_json["nodes"]) {
            if (!node_json.is_object()) continue;
            
            std::string id = "unknown";
            if (node_json.contains("id") && !node_json["id"].is_null()) {
                if (node_json["id"].is_string()) {
                    id = node_json["id"].get<std::string>();
                } else if (node_json["id"].is_number()) {
                    id = std::to_string(node_json["id"].get<int64_t>());
                } else {
                    continue;
                }
            } else {
                continue;
            }
            
            if (id.starts_with("\"") && id.ends_with("\"")) id = id.substr(1, id.size() - 2);

            std::string name = safeGetString(node_json, "name", "unknown");
            std::string tag = safeGetString(node_json, "tag", "");

            try {
                auto node = createNodeFromSparkType(name, node_json, ctx);
                if (node) {
                    PLOGD << "Created node " << id << " (" << name << ") with tag " << tag;
                    info_map[id] = {id, name, tag, std::move(node), true};
                } else {
                    PLOGD << "Skipped node " << id << " (" << name << ") with tag " << tag;
                    info_map[id] = {id, name, tag, nullptr, true};
                }
            } catch (const std::exception& e) {
                PLOGE << "EXCEPTION parsing node " << id << " (" << name << "): " << e.what();
                info_map[id] = {id, name, tag, nullptr, true};
            }
        }
    }

    void processRelationships(const json& graph_json, std::map<std::string, NodeInfo>& info_map, 
                              std::map<std::string, std::vector<std::string>>& parent_to_children,
                              std::vector<std::string>& subquery_ids)
    {
        PLOGD << "Pass 3: Processing parent-child relationships and collecting subquery roots";
        for (const auto& edge : graph_json["edges"]) {
            std::string fromId = "";
            if (edge.contains("fromId") && !edge["fromId"].is_null()) {
                if (edge["fromId"].is_string()) {
                    fromId = edge["fromId"].get<std::string>();
                } else if (edge["fromId"].is_number()) {
                    fromId = std::to_string(edge["fromId"].get<int64_t>());
                }
            }
            
            std::string toId = "";
            if (edge.contains("toId") && !edge["toId"].is_null()) {
                if (edge["toId"].is_string()) {
                    toId = edge["toId"].get<std::string>();
                } else if (edge["toId"].is_number()) {
                    toId = std::to_string(edge["toId"].get<int64_t>());
                }
            }

            if (fromId.empty() || toId.empty()) {
                PLOGW << "Found edge with missing fromId or toId";
                continue;
            }

            if (fromId.starts_with("\"") && fromId.ends_with("\"")) fromId = fromId.substr(1, fromId.size() - 2);
            if (toId.starts_with("\"") && toId.ends_with("\"")) toId = toId.substr(1, toId.size() - 2);

            PLOGD << "Found edge: " << fromId << " -> " << toId;
            if (info_map.contains(fromId) && info_map.contains(toId)) {
                parent_to_children[fromId].push_back(toId);
                
                if (info_map[fromId].name == "Subquery" || 
                    info_map[fromId].name.find("Subquery") != std::string::npos ||
                    info_map[fromId].tag.find("SUBQUERY") != std::string::npos) {
                    PLOGD << "Identified subquery root: " << toId << " (child of " << fromId << ")";
                    subquery_ids.push_back(toId);
                    info_map[toId].is_subquery_root = true;
                } else {
                    info_map[toId].is_root = false;
                }
            }
        }
    }

    void iterativePropagateEstimates(Node* root)
    {
        PLOGD << "Starting estimate propagation";
        
        // Pass 1: Bottom-up propagation (inherit from children)
        for (auto& node : root->bottom_up()) {
            if (std::isnan(node.rows_estimated) || std::isnan(node.rows_actual)) {
                // Try to find valid values from children
                for (auto child : node.children()) {
                    if (std::isnan(node.rows_estimated) && !std::isnan(child->rows_estimated)) {
                        node.rows_estimated = child->rows_estimated;
                    }
                    if (std::isnan(node.rows_actual) && !std::isnan(child->rows_actual)) {
                        node.rows_actual = child->rows_actual;
                    }
                }
            }
        }

        // Pass 2: Top-down propagation (for specific patterns like Distributed Aggregates)
        for (auto& node : root->depth_first()) {
            // Special Case: GROUP BY -> DISTRIBUTE -> GROUP BY
            // In Databricks, this physical pattern often only has the estimate on the top-most GROUP BY
            // in the logical plan. We need to propagate it down to the lower GROUP BY and the DISTRIBUTE node.
            if (node.type == NodeType::GROUP_BY && !std::isnan(node.rows_estimated)) {
                Node* target = &node;
                
                // Skip through any number of DISTRIBUTE or other wrappers to find the next GROUP BY
                while (target->childCount() == 1) {
                    target = target->firstChild();
                    if (target->type == NodeType::GROUP_BY) {
                        // Found the lower GROUP BY
                        if (std::isnan(target->rows_estimated) || target->rows_estimated > node.rows_estimated * 1.1) {
                             PLOGD << "Propagating estimate " << node.rows_estimated 
                                   << " from top GROUP BY to lower GROUP BY (previous was: " << target->rows_estimated << ")";
                             target->rows_estimated = node.rows_estimated;
                        }
                        // Also ensure intermediate DISTRIBUTE nodes get the estimate
                        Node* intermediate = node.firstChild();
                        while (intermediate != target) {
                            if (intermediate->type == NodeType::DISTRIBUTE) {
                                PLOGD << "Propagating estimate " << node.rows_estimated << " to intermediate DISTRIBUTE";
                                intermediate->rows_estimated = node.rows_estimated;
                            }
                            intermediate = intermediate->firstChild();
                        }
                        break;
                    }
                    if (target->type != NodeType::DISTRIBUTE && target->type != NodeType::SELECT && target->type != NodeType::PROJECTION) {
                        break; // Stop if we hit a join or something else
                    }
                }
            }
            
            // Propagation for other nodes that should inherit from parent if they lack estimate
            // e.g. INNER JOIN -> DISTRIBUTE (if join matched but distribute didn't)
            if (node.type == NodeType::JOIN && !std::isnan(node.rows_estimated)) {
                 for (auto child : node.children()) {
                     if (child->type == NodeType::DISTRIBUTE && (std::isnan(child->rows_estimated) || child->rows_estimated > node.rows_estimated * 1.5)) {
                         PLOGD << "Propagating estimate " << node.rows_estimated << " from JOIN to child DISTRIBUTE";
                         child->rows_estimated = node.rows_estimated;
                     }
                 }
            }
        }
    }

    void iterativeCollapseNoOps(std::unique_ptr<Node>& root)
    {
        PLOGD << "Starting iterative relational no-op collapsing";
        size_t last_node_count = 0;
        size_t current_node_count = 0;
        do {
            last_node_count = current_node_count;
            root = collapseRelationalNoOps(std::move(root));
            current_node_count = 0;
            for (auto& n : root->depth_first()) (void)n, current_node_count++;
        } while (current_node_count != last_node_count);
    }

    void buildSubqueryRoots(const std::vector<std::string>& subquery_ids, 
                            std::function<std::vector<std::unique_ptr<Node>>(const std::string&, bool)>& linkRecursively,
                            ExplainContext& ctx)
    {
        PLOGD << "Pass 4: Building subquery roots";
        for (const auto& sq_id : subquery_ids) {
            auto sq_roots = linkRecursively(sq_id, true);
            for (auto& root : sq_roots) {
                ctx.subquery_roots.push_back(std::move(root));
            }
        }
    }

    std::string identifyRootId(const std::map<std::string, NodeInfo>& info_map, 
                               const std::map<std::string, std::vector<std::string>>& parent_to_children)
    {
        PLOGD << "Identifying the best root candidate";
        std::string root_id = "";
        for (const auto& [id, info] : info_map) {
            if (info.is_root && !info.is_subquery_root) {
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
        return root_id;
    }

    std::unique_ptr<Plan> buildExplainPlan(ExplainContext& ctx)
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

        preScanScansForReusedExchange(*graph_json, ctx);

        std::map<std::string, NodeInfo> info_map;
        createNodesAndMap(*graph_json, ctx, info_map);

        std::map<std::string, std::vector<std::string>> parent_to_children;
        std::vector<std::string> subquery_ids;
        processRelationships(*graph_json, info_map, parent_to_children, subquery_ids);

        // Recursive helper to link nodes bottom-up
        std::function<std::vector<std::unique_ptr<Node>>(const std::string&, bool)> linkRecursively =
            [&](const std::string& current_id, bool is_top_level_subquery) -> std::vector<std::unique_ptr<Node>> {
            
            if (!is_top_level_subquery && info_map[current_id].is_subquery_root) {
                PLOGD << "Stopping recursive link at subquery boundary: " << current_id;
                return {};
            }

            std::vector<std::unique_ptr<Node>> current_nodes;

            // Get all linked nodes from children first
            std::vector<std::unique_ptr<Node>> children_nodes;
            if (parent_to_children.contains(current_id)) {
                for (const auto& child_id : parent_to_children[current_id]) {
                    auto linked_children = linkRecursively(child_id, false);
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
                        while (target->childCount() > 0 && 
                               target->type != NodeType::SCAN && 
                               target->type != NodeType::JOIN && 
                               target->type != NodeType::UNION && 
                               target->type != NodeType::SEQUENCE &&
                               target->type != NodeType::DISTRIBUTE &&
                               target->type != NodeType::FILTER &&
                               target->type != NodeType::GROUP_BY &&
                               target->type != NodeType::SELECT &&
                               target->type != NodeType::PROJECTION) {
                            target = target->firstChild();
                        }
                        target->addChild(std::move(child_node));
                    }
                } else {
                    if (!children_nodes.empty()) {
                        PLOGW << "Scan node " << current_id << " has unexpected children, ignoring them to keep SCAN a leaf";
                    }
                }
                
                // Post-process the node subtree before returning it
                current_nodes.push_back(std::move(info_map[current_id].node));
            } else {
                // If this node was skipped, just pass the children up
                current_nodes = std::move(children_nodes);
            }

            return current_nodes;
        };

        buildSubqueryRoots(subquery_ids, linkRecursively, ctx);

        std::string root_id = identifyRootId(info_map, parent_to_children);

        std::unique_ptr<Node> root;
        if (!root_id.empty()) {
            auto linked_roots = linkRecursively(root_id, false);
            if (!linked_roots.empty()) {
                root = std::move(linked_roots[0]);
            }
        }

        if (!root) {
            throw std::runtime_error("Could not identify root node of the Databricks plan");
        }

        PLOGD << "Identified root candidate: " << root_id << " (" << info_map[root_id].name << ")";

        // Final tree assembly: if there are subqueries, wrap everything in a SEQUENCE node
        if (!ctx.subquery_roots.empty()) {
            PLOGD << "Assembling plan with " << ctx.subquery_roots.size() << " subqueries into SEQUENCE";
            auto sequence = std::make_unique<Sequence>();
            sequence->addChild(std::move(root));
            for (auto& sq_root : ctx.subquery_roots) {
                sequence->addChild(std::move(sq_root));
            }
            root = std::move(sequence);
        }

        // Post-processing
        matchEstimates(root.get(), ctx);
        iterativePropagateEstimates(root.get());
        iterativeCollapseNoOps(root);

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
            PLOGD << "Calling getLiveScrapedPlan";
            context.scraped_plan = getLiveScrapedPlan(statement);
            PLOGD << "Storing scraped plan artifact";
            storeArtefact(name_str, "json", context.scraped_plan.dump());
        }

        auto explain_artefact = getArtefact(name_str, "raw_explain");
        if (explain_artefact) {
            PLOGI << "Loading EXPLAIN COST artifact for " << name_str;
            context.raw_explain = *explain_artefact;
        } else {
            PLOGD << "Calling getLiveExplainCost";
            context.raw_explain = getLiveExplainCost(statement);
            PLOGD << "Storing EXPLAIN COST artifact";
            storeArtefact(name_str, "raw_explain", context.raw_explain);
        }

        PLOGD << "Calling parseExplainCost";
        parseExplainCost(context.raw_explain, context);
        context.dump();

        PLOGI << "Walking Scraped JSON Plan Tree";
        walkPlanJson(context.scraped_plan);

        PLOGD << "Calling buildExplainPlan";
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

        // Debug: Dump raw JSON to a file for investigation
        {
            std::filesystem::create_directories("run/scratchpad");
            std::ofstream debug_out("run/scratchpad/raw_scraped_plan.json");
            debug_out << scraped_json_str;
        }

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

            if (!q.contains("query_id") || q["query_id"].is_null()) {
                continue;
            }

            query_id = q["query_id"].get<std::string>();

            if (query_id != statement_id) {
                continue;
            }
            if (q.contains("cache_query_id") && !q["cache_query_id"].is_null()) {
                cache_query_id = q["cache_query_id"].get<std::string>();
            }

            if (q.contains("query_start_time_ms") && !q["query_start_time_ms"].is_null()) {
                start_time_ms = q["query_start_time_ms"].get<int64_t>();
            }
        }

        if (query_id.empty()) {
            throw std::runtime_error("Could not find statement_id" + statement_id + " in history API.");
        }
        return {query_id, cache_query_id, start_time_ms};
    }
}
