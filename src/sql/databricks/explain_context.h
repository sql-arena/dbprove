#pragma once
#include <map>
#include <string>
#include <nlohmann/json.hpp>

namespace sql::explain { class Node; }

namespace sql::databricks {

struct LogicalOperator {
    std::string name;
    double rows_estimated;
    int depth;
};

struct ExplainContext {
    /**
     * @brief Information needed to replicate a Scan node.
     */
    struct ScanInfo {
        std::string table_name;
        std::string filter;
        double rows_estimated;
        double rows_actual;
    };

    /**
     * @brief A map of node identifiers to their row estimates.
     */
    std::map<std::string, double> row_estimates;

    /**
     * @brief The parsed logical plan from EXPLAIN COST.
     */
    std::vector<LogicalOperator> logical_plan;

    /**
     * @brief Tracks which logical operators have been matched to physical nodes.
     */
    std::vector<bool> logical_op_matched;

    /**
     * @brief Mapping from output attribute names to the Scan node that produced them.
     */
    std::map<std::string, ScanInfo> attribute_to_scan;

    /**
     * @brief The raw output from EXPLAIN COST.
     */
    std::string raw_explain;

    /**
     * @brief The captured plan payload JSON from the scraper.
     */
    nlohmann::json scraped_plan;

    /**
     * @brief Roots of subqueries identified during plan building.
     */
    std::vector<std::unique_ptr<sql::explain::Node>> subquery_roots;

    /**
     * @brief Dump the context information to PLOGD.
     */
    void dump() const;
};

} // namespace sql::databricks
