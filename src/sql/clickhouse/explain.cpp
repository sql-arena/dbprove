#include "connection.h"
#include <explain_nodes.h>
#include <nlohmann/json.hpp>
#include <regex>
#include <set>

#include "explain/plan.h"
#include <plog/Log.h>

namespace sql::clickhouse {
using namespace sql::explain;
using namespace nlohmann;


struct ExplainCtx {
  std::map<std::string, std::set<Node*>> nodeBroadCast;
  std::string schema_name;

  void addNode(const std::string& node_name, Node* node) {
    if (!nodeBroadCast.contains(node_name)) {
      nodeBroadCast[node_name] = {};
    }
    nodeBroadCast[node_name].insert(node);
  }
};

std::string cleanFilter(std::string filter) {
  filter = std::regex_replace(filter, std::regex(R"(and\()"), "funcAnd(");
  filter = std::regex_replace(filter, std::regex(R"(or\()"), "funcOr(");
  filter = std::regex_replace(filter, std::regex(R"(like\()"), "funcLike(");
  // Odd _TYPE "casts"
  filter = std::regex_replace(filter, std::regex(R"(Nullable\(([^)]*)\))"), "$1");
  filter = std::regex_replace(filter, std::regex(R"(_(String|UInt8|Int8|Float64))"), "");
  /* Clickhouse prefixes column names with __tableX which point at… Nothing…
   * There doesn't seem to be a way to get from __tableX to the actual table name.
   * Instead, strip it off.
   */
  filter = std::regex_replace(filter, std::regex("__table\\d+\\.*"), "");
  return filter;
}


void parseProjections(std::vector<Column>& columns, const json& node_json) {
  if (!node_json.contains("Expression")) {
    return;
  }
  const auto expression = node_json["Expression"];
  if (!expression.contains("Actions")) {
    return;
  }
  const auto actions = expression["Actions"];
  const auto inputs = expression["Inputs"];
  for (auto& action : actions) {
    const auto action_type = action["Node Type"].get<std::string>();
    const auto result_name = action["Result Name"].get<std::string>();
    if (action_type == "INPUT") {
      continue;
    }
    if (action_type == "COLUMN") {
      continue;
    }
    if (action_type == "FUNCTION") {
      auto function = cleanFilter(result_name);
      if (!function.empty()) {
        columns.emplace_back(function);
      }
      continue;
    }
    if (action_type == "ALIAS") {
      const auto input_offset = action["Arguments"][0].get<int64_t>();
      const auto input_ch_clean = cleanFilter(inputs[input_offset]["Name"].get<std::string>());
      const auto input_name = cleanExpression(input_ch_clean);
      const auto alias = cleanExpression(result_name);
      if (input_name != alias) {
        columns.emplace_back(Column(input_name, alias));
      }
      continue;
    }
    throw ExplainException("Unknown action type: " + action_type);
  }
}

void parseSorting(std::vector<Column>& columns, const json& node_json) {
  if (!node_json.contains("Sort Description")) {
    return;
  }
  for (auto& column : node_json["Sort Description"]) {
    const auto name = column["Column"].get<std::string>();
    const auto sorting = column["Ascending"].get<bool>() ? Column::Sorting::ASC : Column::Sorting::DESC;
    columns.emplace_back(cleanFilter(name), sorting);
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
    columns.emplace_back(cleanFilter(keys[i].get<std::string>()));
  }
}

void parseAggregations(std::vector<Column>& columns, const json& node_json) {
  if (!node_json.contains("Aggregates")) {
    return;
  }
  auto aggregations = node_json["Aggregates"];
  if (!aggregations.is_array()) {
    throw ExplainException("Expected aggregations in group by to be an array");
  }
  const auto aggregation_count = node_json["Aggregates"].size();
  for (size_t i = 0; i < aggregation_count; ++i) {
    columns.emplace_back(cleanFilter(aggregations[i]["Name"].get<std::string>()));
  }
}

std::string parseClauses(const std::string& clause) {
  auto result = std::regex_replace(clause, std::regex("[\\[\\]]"), "");
  //  result = std::regex_replace(result, std::regex("\\,"), " AND");
  result = cleanFilter(result);
  return result;
}

std::string parseScanClause(const json& node_json) {
  if (!node_json.contains("Prewhere info")) {
    return "";
  }
  const auto prewhere_info = node_json["Prewhere info"];
  if (!prewhere_info.contains("Prewhere filter")) {
    return "";
  }
  const auto prewhere_filter = prewhere_info["Prewhere filter"];
  if (!prewhere_filter.contains("Prewhere filter column")) {
    return "";
  }
  const auto filter = prewhere_filter["Prewhere filter column"].get<std::string>();

  return cleanFilter(filter);
}

std::unique_ptr<Node> createNodeFromJson(const json& node_json, ExplainCtx& ctx) {
  const auto node_type = node_json["Node Type"].get<std::string>();
  std::unique_ptr<Node> node = nullptr;
  auto ch_node_id = node_json["Node Id"].get<std::string>();
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
    std::string where_condition = parseScanClause(node_json);
    node = std::make_unique<Scan>(table_name);
    node->setFilter(where_condition);
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
    const auto filter_condition = cleanFilter(node_json["Filter Column"].get<std::string>());
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

  ctx.addNode(ch_node_id, node.get());
  return node;
}


std::unique_ptr<Node> buildExplainNode(json& node_json, ExplainCtx& ctx) {
  auto node = createNodeFromJson(node_json, ctx);
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
    node = createNodeFromJson(node_json, ctx);
  }

  if (node_json.contains("Plans")) {
    for (auto& child_json : node_json["Plans"]) {
      auto child_node = buildExplainNode(child_json, ctx);
      node->addChild(std::move(child_node));
    }
  }

  return node;
}


std::unique_ptr<Plan> buildExplainPlan(json& json, ExplainCtx& ctx) {
  if (!json.is_array()) {
    throw ExplainException("ClickHouse plans are supposed to be inside an array");
  }
  const auto top_plan = json[0];
  if (!top_plan.contains("Plan")) {
    throw ExplainException("Invalid EXPLAIN format. Expected to find a Plans node");
  }

  auto plan_json = top_plan["Plan"];

  /* Construct the tree*/
  auto root_node = buildExplainNode(plan_json, ctx);

  if (!root_node) {
    throw std::runtime_error("Invalid EXPLAIN plan output, could not construct a plan");
  }

  auto plan = std::make_unique<Plan>(std::move(root_node));
  plan->planning_time = 0;
  plan->execution_time = 0;
  return plan;
}

std::string findSemiJoinCondition(Node& n) {
  const auto treeToSearch = n.firstChild();
  for (auto& c : treeToSearch->breadth_first()) {
    if (c.type == NodeType::FILTER) {
      /* The SEMI was using a filter, remove that filter and move it to the join node */
      const auto filter = dynamic_cast<const Selection*>(&c);
      std::string condition = filter->filterCondition();
      c.remove();
      return condition;
    }
    if (c.type == NodeType::SCAN) {
      const auto scan = dynamic_cast<const Scan*>(&c);
      return scan->filterCondition();
    }
  }
  return "";
}

void cleanupExists(Node& root) {
  for (auto& n : root.depth_first()) {
    if (n.type == NodeType::FILTER || n.type == NodeType::JOIN) {
      std::string cleaned = removeExpressionFunction(n.filterCondition(), "exists");
      n.setFilter(cleaned);
    }
  }
}

/**
 * ClickHouse does semi join with a LEFT JOIN followed by a strange exixts() filter.
 * If we observe this structure, we can collapse it into a proper LEFT SEMI JOIN.
 *
 * The structure we are looking for is:
 *
 * LEFT JOIN (... exists() )
 * FILTER (... actual join condition ...)
 *
 * And we want to convert it to:
 *
 * LEFT SEMI JOIN (.. filter...)
 * @param root
 */
void fixSemiJoin(Node& root) {
  std::vector<Join*> to_fix;

  for (Node& n : root.depth_first()) {
    if (n.type != NodeType::JOIN) {
      continue;
    }
    auto join = dynamic_cast<Join*>(&n);
    if (join->type != Join::Type::LEFT_OUTER) {
      continue;
    }
    if (!join->condition.contains("EXISTS()")) {
      continue;
    }
    to_fix.push_back(join);
  }
  /* We now have a list of joins that are actually semi joins, turn them into that... */
  for (const auto n : to_fix) {
    auto condition = findSemiJoinCondition(*n);
    auto parent_expression = n->parent().filterCondition();
    auto join_type = parent_expression.contains("NOT (exist") ? Join::Type::LEFT_ANTI : Join::Type::LEFT_SEMI_INNER;
    auto semi = std::make_unique<Join>(join_type, Join::Strategy::HASH, condition);
    n->replaceWith(std::move(semi));
  }
  cleanupExists(root);
}

void fixActuals(Node& root, Connection* cn, ExplainCtx& ctx) {
  for (auto& n : root.depth_first()) {
    auto sql = n.treeSQL(0);
    sql = "SELECT COUNT(*)\nFROM " + sql;
    PLOGI << "Gathering Actuals with SQL:";
    PLOGI << "\n" << sql;
    const auto actual = cn->fetchScalar(sql).asInt8();
    n.rows_actual = actual;
  }
}

/**
 * ClickHouse will broadcast certain subtrees. We can recognise those by their Node ID.
 *
 * A broadcast subtree has the same nodeID is their original tree
 */
void pruneBroadCast(Node& root, ExplainCtx& ctx) {
  std::vector<Node*> to_prune;
  for (auto nodes : ctx.nodeBroadCast | std::views::values) {
    if (nodes.size() == 1) {
      continue;
    }
    std::vector<Node*> vec(nodes.begin(), nodes.end());
    std::ranges::sort(vec, [](const Node* a, const Node* b) {
      return a->depth() < b->depth();
    });
    // keep the most shallow (which is the original tree), prune the rest which are broadcast
    for (size_t i = 1; i < vec.size(); ++i) {
      to_prune.push_back(vec[i]);
    }
  }

  // Nuke the broadcast nodes
  for (const auto n : to_prune) {
    n->remove();
  }

  // With those nodes gone, we may end up with an empty join (Because the join subtree is NOT broadcast!)
  while (true) {
    std::vector<Node*> junctions_to_prune;
    size_t pruned = 0;
    for (auto& n : root.depth_first()) {
      if (n.type != NodeType::JOIN && n.type != NodeType::UNION) {
        continue;
      }
      if (n.childCount() > 1) {
        continue;
      }
      junctions_to_prune.push_back(&n);
    }
    for (const auto n : junctions_to_prune) {
      n->remove();
      ++pruned;
    }
    if (pruned == 0) {
      break;
    }
  }
}

std::unique_ptr<Plan> Connection::explain(const std::string_view statement) {
  const std::string explain_stmt = "EXPLAIN PLAN json = 1, actions = 1, header = 1, description = 1, projections = 1\n"
                                   + std::string(statement) + "\nFORMAT TSVRaw";
  auto string_explain = fetchScalar(explain_stmt).asString();
  auto json_explain = json::parse(string_explain);

  ExplainCtx ctx;
  auto plan = buildExplainPlan(json_explain, ctx);

  plan->flipJoins();

  pruneBroadCast(plan->planTree(), ctx);

  fixSemiJoin(plan->planTree());

  fixActuals(plan->planTree(), this, ctx);

  return plan;
}
}