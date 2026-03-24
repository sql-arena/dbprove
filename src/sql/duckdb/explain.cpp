#include "connection.h"
#include <dbprove/sql/sql.h>
#include <explain_nodes.h>
#include <materialise.h>
#include <sequence.h>

#include <nlohmann/json.hpp>
#include <memory>
#include <regex>
#include <scan_materialised.h>
#include <stdexcept>

#include "explain/plan.h"

namespace sql::duckdb {
using namespace sql::explain;
using namespace nlohmann;

struct ExplainContext {
  /**
   * Delimiters are expression used to materialize scans
   */
  std::map<size_t, std::string> delimiter_expressions;
  std::map<size_t, std::string> cte_names;
  std::stack<Node*> propagate_estimate;
  std::stack<Node*> propagate_actual;
};

bool hasBalancedExpressionFragments(const std::string& expression) {
  int paren_depth = 0;
  bool in_single_quote = false;
  for (size_t i = 0; i < expression.size(); ++i) {
    const char c = expression[i];
    if (in_single_quote) {
      if (c == '\'' && i + 1 < expression.size() && expression[i + 1] == '\'') {
        ++i;
        continue;
      }
      if (c == '\'') {
        in_single_quote = false;
      }
      continue;
    }
    if (c == '\'') {
      in_single_quote = true;
      continue;
    }
    if (c == '(') {
      ++paren_depth;
    } else if (c == ')') {
      --paren_depth;
    }
  }
  return !in_single_quote && paren_depth == 0;
}

std::vector<std::string> parseExpressionList(const json& value) {
  if (value.is_string()) {
    return {value.get<std::string>()};
  }
  if (!value.is_array()) {
    throw ExplainException("Expected string or array of expression fragments: " + value.dump());
  }

  std::vector<std::string> expressions;
  std::string current_expression;
  for (const auto& item : value) {
    const auto fragment = item.get<std::string>();
    if (!current_expression.empty()) {
      current_expression += " ";
    }
    current_expression += fragment;
    if (hasBalancedExpressionFragments(current_expression)) {
      expressions.push_back(current_expression);
      current_expression.clear();
    }
  }
  if (!current_expression.empty()) {
    expressions.push_back(current_expression);
  }
  return expressions;
}

bool isRawProjectionReference(const std::string& expression) {
  return std::regex_match(expression, std::regex("#[0-9]+"));
}

bool isInternalProjectionExpression(const std::string& expression) {
  return std::regex_match(expression, std::regex("__internal_.*"));
}

bool isSyntheticNullFillProjection(const std::vector<std::string>& projections) {
  bool saw_null = false;
  bool saw_raw_or_internal = false;
  for (const auto& projection : projections) {
    if (projection == "NULL") {
      saw_null = true;
      continue;
    }
    if (isRawProjectionReference(projection) || isInternalProjectionExpression(projection)) {
      saw_raw_or_internal = true;
      continue;
    }
    return false;
  }
  return saw_null && saw_raw_or_internal;
}

std::string parseFilter(json filter) {
  const auto filters = parseExpressionList(filter);
  std::string result;
  for (size_t i = 0; i < filters.size(); ++i) {
    if (i > 0) {
      result += " AND ";
    }
    result += filters[i];
  }
  return result;
}

std::pair<std::string, Column::Sorting> parseSortString(const std::string& column_data) {
  if (column_data.ends_with(" ASC")) {
    return std::make_pair(column_data.substr(0, column_data.size() - 4), Column::Sorting::ASC);
  }
  if (column_data.ends_with(" DESC")) {
    return std::make_pair(column_data.substr(0, column_data.size() - 5), Column::Sorting::DESC);
  }
  return std::make_pair(column_data, Column::Sorting::ASC);
}

std::vector<Column> parseOrderColumns(const json& node_json) {
  auto extra_info = node_json["extra_info"];
  std::vector<Column> sort_keys;
  std::unique_ptr<Sort> sortNode = nullptr;

  if (extra_info.contains("Order By")) {
    for (const auto& sort_key : parseExpressionList(extra_info["Order By"])) {
      auto [column, sort_order] = parseSortString(sort_key);
      sort_keys.push_back(Column(column, sort_order));
    }
  }
  return sort_keys;
}

std::string extractConditions(const json& extra_info) {
  const auto conditions = parseExpressionList(extra_info["Conditions"]);
  std::string result;
  for (size_t i = 0; i < conditions.size(); ++i) {
    result += conditions[i];
    if (i + 1 < conditions.size()) {
      result += " AND ";
    }
  }
  return result;
}

void linkScanMaterialisedToSourceMaterialise(Node& root) {
  std::map<int, const Materialise*> materialise_by_id;
  std::map<std::string, const Materialise*> materialise_by_name;
  for (auto& n : root.depth_first()) {
    if (n.type != NodeType::MATERIALISE) {
      continue;
    }
    const auto* materialise = dynamic_cast<const Materialise*>(&n);
    if (materialise == nullptr) {
      continue;
    }
    if (materialise->node_id >= 0) {
      materialise_by_id[materialise->node_id] = materialise;
    }
    if (!materialise->materialised_node_name.empty()) {
      materialise_by_name[materialise->materialised_node_name] = materialise;
    }
  }
  for (auto& n : root.depth_first()) {
    if (n.type != NodeType::SCAN_MATERIALISED) {
      continue;
    }
    auto* scan_materialised = dynamic_cast<ScanMaterialised*>(&n);
    if (scan_materialised == nullptr || scan_materialised->sourceMaterialise() != nullptr) {
      continue;
    }
    const Materialise* source = nullptr;
    if (!scan_materialised->primary_node_name.empty() &&
        materialise_by_name.contains(scan_materialised->primary_node_name)) {
      source = materialise_by_name.at(scan_materialised->primary_node_name);
    } else if (scan_materialised->primary_node_id >= 0 &&
               materialise_by_id.contains(scan_materialised->primary_node_id)) {
      source = materialise_by_id.at(scan_materialised->primary_node_id);
    }
    if (source != nullptr) {
      scan_materialised->setSourceMaterialise(source);
    }
  }
}

std::unique_ptr<Node> buildExplainNode(json& node_json, ExplainContext& ctx);
std::unique_ptr<Node> buildCteNode(json& node_json, ExplainContext& ctx);

std::unique_ptr<Node> createNodeFromJson(json& node_json, ExplainContext& ctx) {
  const auto operator_name = node_json["operator_name"].get<std::string>();

  const double actual_rows = node_json["operator_cardinality"].get<double>();
  const auto& extra_info = node_json["extra_info"];
  std::unique_ptr<Node> node;
  if (operator_name.contains("SEQ_SCAN")) {
    // NOTE: Duckdb has an extra space in the SEQ_SCAN
    const auto table_name = node_json["extra_info"]["Table"].get<std::string>();

    std::string filter;
    if (extra_info.contains("Filters")) {
      filter = parseFilter(extra_info["Filters"]);
    }
    node = std::make_unique<Scan>(table_name);
    node->setFilter(filter);
  } else if (operator_name.contains("DELIM_SCAN")) {
    // TODO: There is a reference inside this node which points at the thing which is being materialised
    std::string materialised_expression;
    auto index = extra_info["Delim Index"].get<std::string>();
    auto delim_index = std::atol(index.c_str());

    if (ctx.delimiter_expressions.contains(delim_index)) {
      materialised_expression = ctx.delimiter_expressions[delim_index];
    }
    node = std::make_unique<ScanMaterialised>(-1, materialised_expression);
  } else if (operator_name == "COLUMN_DATA_SCAN") {
    node = std::make_unique<ScanMaterialised>();
  } else if (operator_name == "CTE_SCAN") {
    const auto cte_index = std::atoi(extra_info["CTE Index"].get<std::string>().c_str());
    const auto cte_name = ctx.cte_names.contains(cte_index) ? ctx.cte_names.at(cte_index) : "";
    node = std::make_unique<ScanMaterialised>(cte_index, "", cte_name);
  } else if (operator_name.contains("PROJECTION")) {
    std::vector<Column> projection_columns;
    // DuckDB uses a special projection node to “flip types” and compress.
    size_t internal_projections = 0;
    const auto projection_expressions = parseExpressionList(extra_info["Projections"]);
    if (isSyntheticNullFillProjection(projection_expressions)) {
      return nullptr;
    }
    for (const auto& column_name : projection_expressions) {
      if (std::regex_match(column_name, std::regex(".*error\\(.*"))) {
        // DuckDb internal error handling
        return nullptr;
      }
      projection_columns.push_back(Column(column_name));
      bool is_internal = isRawProjectionReference(column_name);
      is_internal |= isInternalProjectionExpression(column_name);
      internal_projections += is_internal ? 1 : 0;
    }
    if (internal_projections == projection_columns.size()) {
      // If this is just an internal projection, we don't need it to understand the plan.
      return nullptr;
    }
    node = std::make_unique<Projection>(projection_columns);
  } else if (operator_name.contains("HASH_JOIN") || operator_name == "NESTED_LOOP_JOIN") {
    auto join_condition = extractConditions(extra_info);

    const auto join_type_str = extra_info["Join Type"].get<std::string>();

    auto join_type = Join::typeFromString(join_type_str);
    auto strategy = operator_name.contains("HASH_JOIN") ? Join::Strategy::HASH : Join::Strategy::LOOP;
    node = std::make_unique<Join>(join_type, strategy, join_condition);
  } else if (operator_name.contains("RIGHT_DELIM_JOIN") || operator_name.contains("LEFT_DELIM_JOIN")) {
    const auto join_type_str = extra_info["Join Type"].get<std::string>();
    auto join_type = join_type_str.contains("RIGHT") ? Join::Type::RIGHT_SEMI_INNER : Join::Type::LEFT_SEMI_INNER;
    auto join_condition = extractConditions(extra_info);

    auto index = extra_info["Delim Index"].get<std::string>();
    auto delim_index = std::atol(index.c_str());
    ctx.delimiter_expressions[delim_index] = join_condition;
    node = std::make_unique<Join>(join_type, Join::Strategy::HASH, join_condition);
  } else if (operator_name == "TOP_N") {
    // DuckDB uses a special sort + limit node when asking for Top N.
    std::unique_ptr<Sort> sortNode;
    auto sort_keys = parseOrderColumns(node_json);
    if (!sort_keys.empty()) {
      sortNode = std::make_unique<Sort>(sort_keys);
    }

    auto limit_string = extra_info["Top"].get<std::string>();
    auto limit = std::atol(limit_string.c_str());
    node = std::make_unique<Limit>(limit);
    // For some reason, Duck doesn't estimate a cardinality for this type.
    node->rows_estimated = limit;
    if (sortNode) {
      sortNode->rows_actual = NAN;
      sortNode->rows_estimated = limit;
      ctx.propagate_actual.push(sortNode.get());
      node->addChild(std::move(sortNode));
    }
  } else if (operator_name == "ORDER_BY") {
    auto sort_keys = parseOrderColumns(node_json);
    node = std::make_unique<Sort>(sort_keys);
  } else if (operator_name == "FILTER") {
    auto filter_condition = extra_info["Expression"].get<std::string>();
    node = std::make_unique<Selection>(filter_condition);
  } else if (operator_name == "UNGROUPED_AGGREGATE") {
    // An aggregate without group by
    std::vector<Column> aggregate_columns;
    if (extra_info.contains("Aggregates")) {
      for (const auto& column : parseExpressionList(extra_info["Aggregates"])) {
        aggregate_columns.push_back(Column(column));
      }
    }
    node = std::make_unique<GroupBy>(GroupBy::Strategy::SIMPLE, std::vector<Column>({}), aggregate_columns);
  } else if (operator_name == "HASH_GROUP_BY" || operator_name == "PERFECT_HASH_GROUP_BY") {
    std::vector<Column> group_by_columns;
    if (extra_info.contains("Groups")) {
      for (const auto& column : parseExpressionList(extra_info["Groups"])) {
        group_by_columns.push_back(Column(column));
      }
    }
    std::vector<Column> aggregate_columns;
    if (extra_info.contains("Aggregates")) {
      for (const auto& column : parseExpressionList(extra_info["Aggregates"])) {
        aggregate_columns.push_back(Column(column));
      }
    }
    node = std::make_unique<GroupBy>(GroupBy::Strategy::HASH, group_by_columns, aggregate_columns);
  } else if (operator_name == "UNION") {
    node = std::make_unique<Union>(Union::Type::ALL);
  } else if (operator_name == "EMPTY_RESULT" || operator_name == "DUMMY_SCAN") {
    node = std::make_unique<ScanEmpty>();
  } else if (operator_name == "EXPLAIN_ANALYZE") {
    // Duck adds this fake node when explaining queries
    return nullptr;
  }

  if (node == nullptr) {
    throw std::runtime_error("Unknown operator type: " + operator_name);
  }

  node->rows_actual = actual_rows;
  if (extra_info.contains("Estimated Cardinality")) {
    auto cardinality_string = extra_info["Estimated Cardinality"].get<std::string>();
    double estimate = std::atof(cardinality_string.c_str());
    node->rows_estimated = estimate;
    if (std::isnan(node->rows_actual)) {
      node->rows_actual = actual_rows;
    }
    while (!ctx.propagate_estimate.empty()) {
      ctx.propagate_estimate.top()->rows_estimated = estimate;
      ctx.propagate_estimate.pop();
    }
  } else if (node->rows_estimated == 0) {
    // HACK: Likely a bug in DuckDB. Some nodes don't have estimates, even though they care about the value.
    ctx.propagate_estimate.push(node.get());
  }

  if (std::isnan(node->rows_actual)) {
    while (!ctx.propagate_actual.empty()) {
      ctx.propagate_actual.top()->rows_actual = actual_rows;
      ctx.propagate_actual.pop();
    }
  }

  return node;
}

std::unique_ptr<Node> buildCteNode(json& node_json, ExplainContext& ctx) {
  const auto& extra_info = node_json["extra_info"];
  const auto cte_index = std::atoi(extra_info["Table Index"].get<std::string>().c_str());
  const auto cte_name = extra_info.contains("CTE Name") ? extra_info["CTE Name"].get<std::string>() : "";
  if (!cte_name.empty()) {
    ctx.cte_names[cte_index] = cte_name;
  }

  if (!node_json.contains("children") || node_json["children"].size() < 2) {
    throw std::runtime_error("CTE operator must have at least two children");
  }

  auto sequence = std::make_unique<Sequence>();
  auto materialise = std::make_unique<Materialise>("CTE", cte_index, cte_name);

  auto producer_json = node_json["children"][0];
  auto producer = buildExplainNode(producer_json, ctx);
  materialise->rows_actual = producer->rows_actual;
  materialise->rows_estimated = producer->rows_estimated;
  materialise->addChild(std::move(producer));
  sequence->addChild(std::move(materialise));

  for (size_t i = 1; i < node_json["children"].size(); ++i) {
    auto consumer_json = node_json["children"][i];
    sequence->addChild(buildExplainNode(consumer_json, ctx));
  }

  return sequence;
}


std::unique_ptr<Node> buildExplainNode(json& node_json, ExplainContext& ctx) {
  // Determine the node type from the “Node Type” field
  if (!node_json.contains("operator_name")) {
    throw std::runtime_error("Missing 'Node Type' in EXPLAIN node");
  }
  if (node_json["operator_name"].get<std::string>() == "CTE") {
    return buildCteNode(node_json, ctx);
  }
  auto node = createNodeFromJson(node_json, ctx);
  while (node == nullptr) {
    if (!node_json.contains("children")) {
      throw std::runtime_error(
          "EXPLAIN tries to skip past a node that has no children. You must handle all leaf nodes");
    }
    if (node_json["children"].size() > 1) {
      throw std::runtime_error(
          "EXPLAIN parsing Tried to skip past a node with >1 children. You have to deal with this case");
    }
    node_json = node_json["children"][0];
    const auto operator_name = node_json["operator_name"].get<std::string>();
    if (operator_name == "CTE") {
      return buildCteNode(node_json, ctx);
    }
    node = createNodeFromJson(node_json, ctx);
  }

  Node* last_child = node.get();
  while (last_child->childCount() > 0) {
    last_child = last_child->firstChild();
  }

  if (node_json.contains("children")) {
    auto children = node_json["children"];
    for (auto& child_json : children) {
      auto child_node = buildExplainNode(child_json, ctx);
      last_child->addChild(std::move(child_node));
    }
  }

  return node;
}


std::unique_ptr<Plan> buildExplainPlan(json& json) {
  if (!json.contains("children")) {
    throw std::runtime_error("Invalid EXPLAIN format. Expected to find a 'children' node");
  }

  // High-level stats about the query
  double planning_time = 0.0;
  double execution_time = 0.0;

  if (json.contains("planner")) {
    planning_time = json["planner"].get<double>();
  }
  if (json.contains("cpu_time")) {
    execution_time = json["cpu_time"].get<double>();
  }

  auto& plan_json = json["children"];

  ExplainContext ctx;
  /* Construct the tree*/
  auto root_node = buildExplainNode(plan_json[0], ctx);

  if (!root_node) {
    throw std::runtime_error("Invalid EXPLAIN plan output, could not construct a plan");
  }

  // Create and return the plan object with timing information
  auto plan = std::make_unique<Plan>(std::move(root_node));
  plan->planning_time = planning_time;
  plan->execution_time = execution_time;
  plan->flipJoins();
  linkScanMaterialisedToSourceMaterialise(plan->planTree());
  return plan;
}


std::unique_ptr<Plan> Connection::explain(const std::string_view statement, std::optional<std::string_view> name) {
  const std::string explain_query = "PRAGMA enable_profiling = 'json';\n" "PRAGMA profiling_mode = 'detailed';\n"
                                    "EXPLAIN (ANALYSE, FORMAT JSON)\n" + std::string(statement);
  const auto result = fetchRow(explain_query);

  std::string explain_raw = result->asString(1);
  auto explain_json = json::parse(explain_raw);

  return buildExplainPlan(explain_json);
}
}
