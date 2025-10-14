#include "connection.h"
#include <explain_nodes.h>
#include <nlohmann/json.hpp>
#include <regex>

#include "explain/plan.h"

namespace sql::clickhouse {
using namespace sql::explain;
using namespace nlohmann;


std::string sanitiseColumn(const std::string& nonsense) {
  /* Clickhouse prefixes column names with __tableX which point at… Nothing…
   * There doesn't seem to be a way to get from __tableX to the actual table name.
   * Instead, strip it off.
   */
  return std::regex_replace(nonsense, std::regex("__table\\d+\\.*"), "");
}


void parseProjections(std::vector<Column>& columns, const json& node_json) {
  if (!node_json.contains("Actions")) {
    return;
  }
  for (auto& action : node_json["Actions"]) {
    auto action_type = action["Node Type"].get<std::string>();
    if (action_type == "INPUT") {
      continue;
    }
    if (action_type == "COLUMN") {
      auto column = sanitiseColumn(action["Column"].get<std::string>());
      columns.emplace_back(column);
    } else if (action_type == "FUNCTION") {
      auto function = sanitiseColumn(action["Function Name"].get<std::string>());
      columns.emplace_back(function);
    } else {
      throw ExplainException("Unknown action type: " + action_type);
    }
  }
}

void parseSorting(std::vector<Column>& columns, const json& node_json) {
  if (!node_json.contains("Sort Description")) {
    return;
  }
  for (auto& column : node_json["Sort Description"]) {
    const auto name = column["Column"].get<std::string>();
    const auto sorting = column["Ascending"].get<bool>() ? Column::Sorting::ASC : Column::Sorting::DESC;
    columns.emplace_back(sanitiseColumn(name), sorting);
  }
}

void parseGroupKeys(std::vector<Column>& columns, const json& node_json) {
  if (!node_json.contains("Keys")) {
    return;
  }
  auto keys = node_json["Keys"];
  if (!keys.is_array()) {
    throw ExplainException("Expected keys in group by to be an array");
  }
  const auto key_count = node_json["Keys"].size();
  for (size_t i = 0; i < key_count; ++i) {
    columns.emplace_back(sanitiseColumn(keys[i].get<std::string>()));
  }
}

void parseAggregations(std::vector<Column>& columns, const json& node_json) {
  if (!node_json.contains("Aggregations")) {
    return;
  }
  auto aggregations = node_json["Aggregations"];
  if (!aggregations.is_array()) {
    throw ExplainException("Expected aggregations in group by to be an array");
  }
  const auto aggregation_count = node_json["Aggregations"].size();
  for (size_t i = 0; i < aggregation_count; ++i) {
    columns.emplace_back(sanitiseColumn(aggregations[i]["Name"].get<std::string>()));
  }
}

std::string parseClauses(const std::string& clause) {
  auto result = std::regex_replace(clause, std::regex("[\\[\\]]"), "");
  //  result = std::regex_replace(result, std::regex("\\,"), " AND");
  result = sanitiseColumn(result);
  return result;
}


std::unique_ptr<Node> createNodeFromJson(const json& node_json) {
  const auto node_type = node_json["Node Type"].get<std::string>();
  std::unique_ptr<Node> node = nullptr;
  if (node_type == "Expression") {
    std::vector<Column> columns_projected;
    parseProjections(columns_projected, node_json);
    if (columns_projected.empty()) {
      // Clickhouse will do these odd projections where it just reorganizes column order.
      return nullptr;
    }
    node = std::make_unique<Projection>(columns_projected);
  } else if (node_type == "ReadFromMergeTree") {
    auto table_name = node_json["Description"].get<std::string>();
    // TODO: Potentially add a filter node here based on what we filter from scan
    node = std::make_unique<Scan>(table_name);
  } else if (node_type == "Sorting") {
    std::vector<Column> columns_sorted;
    parseSorting(columns_sorted, node_json);
    node = std::make_unique<Sort>(columns_sorted);
  } else if (node_type == "Aggregating") {
    std::vector<Column> columns_aggregated;
    std::vector<Column> columns_grouped;
    parseGroupKeys(columns_grouped, node_json);
    parseAggregations(columns_aggregated, node_json);
    node = std::make_unique<GroupBy>(GroupBy::Strategy::HASH, columns_grouped, columns_aggregated);
  } else if (node_type == "Limit") {
    auto limit_value = node_json["Limit"].get<int64_t>();
    node = std::make_unique<Limit>(limit_value);
  } else if (node_type == "Filter") {
    const auto filter_condition = sanitiseColumn(node_json["Filter Column"].get<std::string>());
    node = std::make_unique<Selection>(filter_condition);
  } else if (node_type == "Join") {
    const auto join_strategy = node_json["Algorithm"].get<std::string>().contains("Hash")
                                 ? Join::Strategy::HASH
                                 : Join::Strategy::LOOP;
    std::string join_condition;
    const auto clauses = node_json["Clauses"];
    join_condition += parseClauses(clauses);

    const auto join_type = Join::typeFromString(node_json["Type"].get<std::string>());
    node = std::make_unique<Join>(join_type, join_strategy, join_condition);
  } else if (node_type == "CreatingSets") {
    // These are prep nodes that simply construct layouts
    return nullptr;
  }
  if (!node) {
    throw ExplainException("Could not parse ClickHouse node of type: " + node_type);
  }

  if (node_json.contains("Indexes")) {
    auto index_json = node_json["Indexes"];
    if (index_json.is_array() && index_json.size() > 0 && index_json[0].contains("Selected Granules")) {
      constexpr RowCount granule_size = 8192;
      node->rows_actual = index_json[0]["Selected Granules"].get<int>() * granule_size;
    }
  }

  return node;
}


std::unique_ptr<Node> buildExplainNode(json& node_json) {
  auto node = createNodeFromJson(node_json);
  while (node == nullptr) {
    if (!node_json.contains("Plans")) {
      throw std::runtime_error(
          "EXPLAIN tries to skip past a node that has no children. You must handle all leaf nodes");
    }
    if (node_json["Plans"].size() > 1) {
      throw std::runtime_error(
          "EXPLAIN parsing Tried to skip past a node with >1 children. You have to deal with this case");
    }
    node_json = node_json["Plans"][0];
    node = createNodeFromJson(node_json);
  }

  if (node_json.contains("Plans")) {
    for (auto& child_json : node_json["Plans"]) {
      auto child_node = buildExplainNode(child_json);
      node->addChild(std::move(child_node));
    }
  }

  return node;
}


std::unique_ptr<Plan> buildExplainPlan(json& json) {
  if (!json.is_array()) {
    throw ExplainException("ClickHouse plans are supposed to be inside an array");
  }
  const auto top_plan = json[0];
  if (!top_plan.contains("Plan")) {
    throw ExplainException("Invalid EXPLAIN format. Expected to find a Plans node");
  }

  auto plan_json = top_plan["Plan"];

  /* Construct the tree*/
  auto root_node = buildExplainNode(plan_json);

  if (!root_node) {
    throw std::runtime_error("Invalid EXPLAIN plan output, could not construct a plan");
  }

  auto plan = std::make_unique<Plan>(std::move(root_node));
  plan->planning_time = 0;
  plan->execution_time = 0;

  return plan;
}


std::unique_ptr<Plan> Connection::explain(const std::string_view statement) {
  const std::string explain_stmt = "EXPLAIN PLAN json = 1, actions = 1, header = 1, indexes = 1\n" +
                                   std::string(statement) + "\nFORMAT TSVRaw";
  auto string_explain = fetchScalar(explain_stmt).asString();
  auto json_explain = json::parse(string_explain);
  auto plan = buildExplainPlan(json_explain);

  plan->flipJoins();
  return plan;
}
}