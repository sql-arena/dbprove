#pragma once
#include <map>
#include <string>
#include <nlohmann/json.hpp>

namespace sql::databricks {

struct ExplainContext {
    /**
     * @brief A map of node identifiers to their row estimates.
     */
    std::map<std::string, double> row_estimates;

    /**
     * @brief The raw output from EXPLAIN COST.
     */
    std::string raw_explain;

    /**
     * @brief The captured plan payload JSON from the scraper.
     */
    nlohmann::json scraped_plan;

    /**
     * @brief Dump the context information to PLOGD.
     */
    void dump() const;
};

} // namespace sql::databricks
