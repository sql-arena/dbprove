#include <iostream>

#include "connection.h"

#include "group_by.h"
#include "join.h"
#include "limit.h"
#include "scan.h"
#include "scan_materialised.h"
#include "select.h"
#include "sort.h"
#include "union.h"
#include "explain/node.h"
#include "explain/plan.h"
#include <unordered_set>
#include <regex>
#include <nlohmann/json.hpp>
#include <plog/Log.h>

#include "sequence.h"
#include "duckdb/common/helper.hpp"

namespace sql::postgresql {
using namespace sql::explain;
using namespace nlohmann;

struct ExplainCtx {
  std::vector<std::unique_ptr<Node>> initPlans_;
  std::unordered_set<Node*> subPlanNodes_;
  std::unordered_set<std::string> aliases_;
};

/**
 * A custom dialect for PostgreSQL expressions that is aware of aliases.
 */
struct PgDialect final : EngineDialect {
  explicit PgDialect(const ExplainCtx& ctx) : ctx_(ctx) {}

  const std::map<std::string_view, std::string_view>& engineFunctions() const override {
    static const std::map<std::string_view, std::string_view> fn = {
      {"\"LEFT\"", "LEFT"},
    };
    return fn;
  }

  static bool isIdentifierLike(const std::string& value) {
    if (value.empty()) {
      return false;
    }
    const unsigned char first = static_cast<unsigned char>(value.front());
    return std::isalpha(first) || value.front() == '_' || value.front() == '"';
  }

  void preRender(std::vector<Token>& tokens) override {
    for (auto& token : tokens) {
      if (token.type == Token::Type::Literal) {
        // Handle qualified names: table.column or alias.column
        const size_t dot_pos = token.value.find('.');
        if (dot_pos != std::string::npos) {
          const std::string qualifier = token.value.substr(0, dot_pos);
          const std::string column = token.value.substr(dot_pos + 1);

          // Decimal literals (e.g. 0.2) also contain dots; only treat identifier-like
          // tokens as qualified names that can be alias-stripped.
          if (!isIdentifierLike(qualifier) || !isIdentifierLike(column)) {
            continue;
          }

          // If the qualifier is not an alias, we strip it.
          // Otherwise, we keep it as a two-part name.
          if (!ctx_.aliases_.contains(qualifier)) {
            token.value = column;
          }
        }
      }
    }
  }

private:
  const ExplainCtx& ctx_;
};

/**
 * Alias-aware expression cleaner for PostgreSQL.
 */
std::string cleanPgExpression(std::string expression, const ExplainCtx& ctx) {
  PgDialect dialect(ctx);
  return cleanExpression(std::move(expression), &dialect);
}

static std::string firstOutputColumn(const Node& node) {
  if (!node.columns_output.empty()) {
    return node.columns_output.front();
  }
  return "";
}

static std::optional<std::string> extractSubplanJoinCondition(const std::string& raw_filter,
                                                              const Node& subplan_node,
                                                              const ExplainCtx& ctx) {
  const auto subplan_column = firstOutputColumn(subplan_node);
  if (subplan_column.empty()) {
    return std::nullopt;
  }

  // Example:
  //   (NOT (ANY (partsupp.ps_suppkey = (hashed SubPlan 1).col1)))
  static const std::regex hashed_any(R"(ANY\s*\(\s*(.*?)\s*=\s*\(hashed\s+SubPlan\s+\d+\)\.col\d+\s*\))",
                                     std::regex::icase);
  std::smatch match;
  if (std::regex_search(raw_filter, match, hashed_any) && match.size() > 1) {
    const auto lhs = cleanPgExpression(match[1].str(), ctx);
    const auto rhs = cleanPgExpression(subplan_column, ctx);
    return lhs + " = " + rhs;
  }

  // Fallback for non-hashed SubPlan references.
  static const std::regex plain_subplan(R"(\(\s*(.*?)\s*=\s*\(SubPlan\s+\d+\)\s*\))", std::regex::icase);
  if (std::regex_search(raw_filter, match, plain_subplan) && match.size() > 1) {
    const auto lhs = cleanPgExpression(match[1].str(), ctx);
    const auto rhs = cleanPgExpression(subplan_column, ctx);
    return lhs + " = " + rhs;
  }

  return std::nullopt;
}

static std::optional<std::string> extractCorrelatedIndexCond(const json& node_json, const ExplainCtx& ctx) {
  if (node_json.contains("Index Cond")) {
    const auto cond = cleanPgExpression(node_json["Index Cond"].get<std::string>(), ctx);
    if (!cond.empty()) {
      return cond;
    }
  }

  if (node_json.contains("Plans") && node_json["Plans"].is_array()) {
    for (const auto& child : node_json["Plans"]) {
      if (const auto cond = extractCorrelatedIndexCond(child, ctx); cond.has_value()) {
        return cond;
      }
    }
  }
  return std::nullopt;
}

static std::optional<double> subplanTotalActualRows(const json& node_json) {
  if (!node_json.contains("Actual Rows")) {
    return std::nullopt;
  }

  const auto actual_rows = node_json["Actual Rows"].get<double>();
  const auto loops = node_json.contains("Actual Loops") ? node_json["Actual Loops"].get<double>() : 1.0;
  return std::max(actual_rows * loops, loops);
}

/**
 * Create the appropriate node type based on PostgreSQL node type
 * Skip past nodes we currently don't care about.
 */
std::unique_ptr<Node> createNodeFromPgType(const json& node_json, ExplainCtx& ctx) {
  const auto pg_node_type = node_json["Node Type"].get<std::string>();
  std::unique_ptr<Node> node;
  bool needsLoopCorrection = true; // See later comment
  double actual_override = -1;

  if (pg_node_type == "Seq Scan" || pg_node_type == "Index Scan" || pg_node_type == "Index Only Scan") {
    std::string relation_name;
    std::string base_relation_name;

    if (node_json.contains("Parallel Aware")) {
      const auto parallel_aware = node_json["Parallel Aware"].get<bool>();
      if (!parallel_aware && pg_node_type == "Seq Scan") {
        // Apparently, the single threaded Seq Scan correctly reports Actual without loops
        needsLoopCorrection = false;
      }
    }

    const auto strategy = pg_node_type == "Seq Scan" ? Scan::Strategy::SCAN : Scan::Strategy::SEEK;
    if (node_json.contains("Relation Name")) {
      relation_name = node_json["Relation Name"].get<std::string>();
      base_relation_name = relation_name;
    }
    if (node_json.contains("Schema")) {
      relation_name = node_json["Schema"].get<std::string>() + "." + relation_name;
    }
    std::string alias;
    if (node_json.contains("Alias")) {
      alias = node_json["Alias"].get<std::string>();
      // If alias equals relation name, it is not a meaningful query alias.
      // Keep qualifier stripping behavior for expressions like supplier.s_comment.
      if (alias != base_relation_name) {
        ctx.aliases_.insert(alias);
      }
    }
    node = std::make_unique<Scan>(relation_name, strategy, alias);
  } else if (pg_node_type == "Bitmap Heap Scan" || pg_node_type == "Bitmap Index Scan") {
    /* PG does a bitmap intersection of indexes using e special operators.
     * They can be chained together and be arbitrarily nested
     * We just care about the top one as that is the actual outcome of the scan.
     */
    std::string relation_name;
    std::string base_relation_name;
    if (node_json.contains("Relation Name")) {
      relation_name = node_json["Relation Name"].get<std::string>();
      base_relation_name = relation_name;
    }
    std::string alias;
    if (node_json.contains("Alias")) {
      alias = node_json["Alias"].get<std::string>();
      if (alias != base_relation_name) {
        ctx.aliases_.insert(alias);
      }
    }
    if (relation_name.empty()) {
      // One of the lower bitmap operations, we already rendered the scan.
      return nullptr;
    }
    node = std::make_unique<Scan>(relation_name, Scan::Strategy::SEEK, alias);
  } else if (pg_node_type == "CTE Scan" || pg_node_type == "WorkTable Scan") {
    std::string cte_name;
    if (node_json.contains("CTE Name")) {
      cte_name = node_json["CTE Name"].get<std::string>();
    } else if (node_json.contains("Alias")) {
      cte_name = node_json["Alias"].get<std::string>();
    }

    if (node_json.contains("Alias")) {
      const auto alias = node_json["Alias"].get<std::string>();
      if (!cte_name.empty() && alias != cte_name) {
        ctx.aliases_.insert(alias);
      }
    }

    node = std::make_unique<ScanMaterialised>(-1, "", cte_name);
  } else if (pg_node_type == "Hash Join" || pg_node_type == "Nested Loop" || pg_node_type == "Merge Join") {
    std::string join_condition;

    Join::Strategy join_strategy;
    if (pg_node_type == "Hash Join") {
      // We treat
      needsLoopCorrection = false;
      join_strategy = Join::Strategy::HASH;
    } else if (pg_node_type == "Nested Loop") {
      join_strategy = Join::Strategy::LOOP;
      // Loops often hold join keys inside inner index probes.
      // Walk the inner subtree and extract the first available Index Cond.
      const auto extract_loop_cond = [&](const auto& self, const json& j)-> std::string {
        if (j.contains("Index Cond")) {
          return cleanPgExpression(j["Index Cond"].get<std::string>(), ctx);
        }
        if (j.contains("Node Type")) {
          const auto t = j["Node Type"].get<std::string>();
          if ((t == "Index Scan" || t == "Index Only Scan") && j.contains("Index Cond")) {
            return cleanPgExpression(j["Index Cond"].get<std::string>(), ctx);
          }
        }
        if (j.contains("Plans") && j["Plans"].is_array()) {
          for (const auto& child : j["Plans"]) {
            const auto cond = self(self, child);
            if (!cond.empty()) {
              return cond;
            }
          }
        }
        return "";
      };
      if (node_json.contains("Plans") && node_json["Plans"].size() >= 2) {
        join_condition = extract_loop_cond(extract_loop_cond, node_json["Plans"][1]);
      }
    } else if (pg_node_type == "Merge Join") {
      join_strategy = Join::Strategy::MERGE;
      join_condition = node_json["Merge Cond"].get<std::string>();
    }

    auto join_type = Join::Type::INNER;
    if (node_json.contains("Join Type")) {
      join_type = Join::typeFromString(node_json["Join Type"].get<std::string>());
    }

    if (node_json.contains("Join Filter")) {
      auto condition = node_json["Join Filter"].get<std::string>();
      condition = cleanPgExpression(condition, ctx);
      if (join_condition.empty()) {
        join_condition = condition;
      } else {
        join_condition.append(" AND ").append(condition);
      }
    }
    if (node_json.contains("Hash Cond")) {
      join_condition = cleanPgExpression(node_json["Hash Cond"].get<std::string>(), ctx);
    }
    if (!join_condition.empty() && join_condition.front() == '(' && join_condition.back() == ')') {
      // Only strip if they are a matching pair
      int depth = 0;
      bool matching = true;
      for (size_t i = 0; i < join_condition.length() - 1; ++i) {
        if (join_condition[i] == '(') depth++;
        else if (join_condition[i] == ')') depth--;
        if (depth == 0) {
          matching = false;
          break;
        }
      }
      if (matching && depth == 1) {
        join_condition = join_condition.substr(1, join_condition.length() - 2);
        join_condition = cleanPgExpression(join_condition, ctx);
      }
    }
    if (join_condition.empty() && join_type == Join::Type::INNER) {
      // PG doesn't use the notion of cross-join, it simply has loop join without conditions conditions.
      join_type = Join::Type::CROSS;
    }
    if (node_json.contains("Join Filter")) {
      /* Join filters are additional conditions evaluated in the join that are not usable for seeks/lookups */
      join_condition += " AND " + node_json["Join Filter"].get<std::string>();
    }

    PgDialect dialect(ctx);
    node = std::make_unique<Join>(join_type, join_strategy, join_condition, &dialect);
  } else if (pg_node_type == "Sort") {
    PgDialect dialect(ctx);
    std::vector<Column> sort_keys;
    for (const auto& col : node_json["Sort Key"]) {
      const std::string desc_suffix = " DESC";
      auto sort_order = Column::Sorting::ASC;
      auto name = col.get<std::string>();
      if (name.size() >= desc_suffix.size() && name.substr(name.size() - desc_suffix.size()) == desc_suffix) {
        // Sort keys end with “DESC” (yeah, a string) if they’re descending.
        name = name.substr(0, name.size() - desc_suffix.size());
        sort_order = Column::Sorting::DESC;
      }
      name = cleanPgExpression(name, ctx);
      sort_keys.emplace_back(Column(name, sort_order, &dialect));
    }
    node = std::make_unique<Sort>(sort_keys);
  } else if (pg_node_type == "Limit") {
    int64_t limit_count = -1;
    if (node_json.contains("Plan Rows")) {
      limit_count = node_json["Plan Rows"].get<int64_t>();
    }
    node = std::make_unique<Limit>(limit_count);
  } else if (pg_node_type == "Aggregate") {
    std::vector<Column> group_keys;

    auto group_strategy = GroupBy::Strategy::UNKNOWN;
    if (node_json.contains("Strategy")) {
      const auto strategy = node_json["Strategy"].get<std::string>();
      if (strategy == "Hashed") {
        group_strategy = GroupBy::Strategy::HASH;
      } else if (strategy == "Sorted") {
        group_strategy = GroupBy::Strategy::SORT_MERGE;
      } else if (strategy == "Plain") {
        group_strategy = GroupBy::Strategy::SIMPLE;
      } else {
        throw std::runtime_error("Did not know GROUP BY strategy " + strategy);
      }
    } else if (node_json.contains("Group Key")) {
      // The strategy field is only present if the strategy is Hash or if an aggregate is present
      group_strategy = GroupBy::Strategy::SORT_MERGE;
    }

    if (node_json.contains("Group Key")) {
      PgDialect dialect(ctx);
      for (const auto& key : node_json["Group Key"]) {
        group_keys.push_back(Column(cleanPgExpression(key.get<std::string>(), ctx), &dialect));
      }
    }
    std::vector<Column> aggregates;
    if (node_json.contains("Output")) {
      PgDialect dialect(ctx);
      std::vector<Column> out;
      for (const auto& key : node_json["Output"]) {
        out.push_back(Column(cleanPgExpression(key.get<std::string>(), ctx), &dialect));
      }
      std::unordered_set group_keys_set(group_keys.begin(), group_keys.end());
      for (const auto& column : out) {
        // If the column is not found in group_keys_set, add it to non_group_keys
        if (!group_keys_set.contains(column)) {
          aggregates.push_back(column);
        }
      }
    }
    node = std::make_unique<GroupBy>(group_strategy, group_keys, aggregates);
  } else if (pg_node_type == "Result") {
    node = std::make_unique<Select>();
  } else if (pg_node_type == "Append") {
    node = std::make_unique<Union>(Union::Type::ALL);
  } else if (pg_node_type == "Merge Append") {
    /* PG has a special node for distinct unions when the input is sorted. */
    node = std::make_unique<Union>(Union::Type::DISTINCT);
  } else {
    return nullptr;
  }

  /* Cost and estimation*/
  if (node_json.contains("Total Cost")) {
    node->cost = node_json["Total Cost"].get<double>();
  }

  /* Row estimate and actual */
  if (node_json.contains("Plan Rows")) {
    node->rows_estimated = node_json["Plan Rows"].get<double>();
  }
  if (actual_override > 0) {
    node->rows_actual = actual_override;
  } else if (node_json.contains("Actual Rows")) {
    node->rows_actual = node_json["Actual Rows"].get<double>();
  }

  /* PG can loop over the same sub-plan multiple times.
   * It doesn't estimate the expected loops, but it does report the actual loops during ANALYSE.
   * Because we run the sub-plan multiple times, the row counts we're dealing with going through
   * the node is: (#loops * #rows)
   *
   * Since we don't want to unfairly skew estimate vs actual, we multiply both by the loop count.
   *
   * The `rows_actual` is the AVERAGE per loop.
   * Instead of using a double for an average value, like a sane human, Postgres uses an integer.
   * That then means that the value rounds DOWN to zero - misrepresenting the actual. For those cases
   * we then have to fall back to the loop count.
   */
  if (needsLoopCorrection && node_json.contains("Actual Loops")) {
    auto loop_count = node_json["Actual Loops"].get<double>();
    node->rows_actual *= loop_count;
    node->rows_actual = std::max(node->rows_actual, loop_count);
    node->rows_estimated *= loop_count;
  }

  // Filter conditions
  if (node_json.contains("Filter")) {
    PgDialect dialect(ctx);
    node->setFilter(cleanPgExpression(node_json["Filter"].get<std::string>(), ctx), &dialect);
  }

  // Output columns
  if (node_json.contains("Output") && node_json["Output"].is_array()) {
    PgDialect dialect(ctx);
    for (const auto& col : node_json["Output"]) {
      node->columns_output.push_back(cleanPgExpression(col.get<std::string>(), ctx));
    }
  }

  return node;
}


std::unique_ptr<Node> buildExplainNode(json& node_json, ExplainCtx& ctx) {
  // Determine the node type from the “Node Type” field
  if (!node_json.contains("Node Type")) {
    throw std::runtime_error("Missing 'Node Type' in EXPLAIN node");
  }

  auto node = createNodeFromPgType(node_json, ctx);
  const auto node_type = node_json["Node Type"].get<std::string>();
  if (node == nullptr && (node_type == "BitmapAnd" || node_type == "Bitmap Index Scan" || node_type ==
                          "Bitmap Heap Scan")) {
    // We already handled the first bitmap operation.
    return nullptr;
  }
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
    node = createNodeFromPgType(node_json, ctx);
  }

  if (node_json.contains("Plans") && node_json["Plans"].is_array()) {
    const auto plans = node_json["Plans"];
    for (size_t i = 0; i < plans.size(); ++i) {
      auto child_json = plans[i];
      auto child_node = buildExplainNode(child_json, ctx);
      if (child_node == nullptr) {
        continue;
      }

      bool is_subplan = false;
      if (child_json.contains("Parent Relationship")) {
        auto rel = child_json["Parent Relationship"].get<std::string>();
        if (rel == "SubPlan" || rel == "InitPlan") {
          is_subplan = true;
          ctx.subPlanNodes_.insert(child_node.get());
          if (const auto total_rows = subplanTotalActualRows(child_json); total_rows.has_value()) {
            child_node->rows_actual = *total_rows;
          }
        }
      }

      // If the parent has a subplan in its filter, we should
      // wrap it in a join to represent the dependency correctly.
      // But we only do this for Scans and other non-Join nodes.
      // Joins will have the subplan added as a 3rd child and then hoisted.
      const auto filter = node->filterCondition();
      if (is_subplan && (filter.find("SubPlan") != std::string::npos) &&
          (node->type == NodeType::SCAN || node->type == NodeType::SCAN_EMPTY || node->type == NodeType::SCAN_MATERIALISED)) {
        std::string join_condition;
        if (node_json.contains("Filter")) {
          if (const auto extracted = extractSubplanJoinCondition(node_json["Filter"].get<std::string>(), *child_node, ctx);
              extracted.has_value()) {
            join_condition = *extracted;
          }
        }
        if (join_condition.empty()) {
          // Correlated subplans often encode dependency via Index Cond in a nested scan.
          // Use that as semi/anti join condition when scalar SubPlan filter parsing is insufficient.
          if (const auto correlated = extractCorrelatedIndexCond(child_json, ctx); correlated.has_value()) {
            join_condition = *correlated;
          }
        }

        auto join_type = Join::Type::LEFT_SEMI_INNER;
        if (filter.find("NOT (IN") != std::string::npos || filter.find("NOT EXISTS") != std::string::npos) {
          join_type = Join::Type::LEFT_ANTI;
        }

        auto join = std::make_unique<Join>(join_type, Join::Strategy::LOOP, join_condition);
        join->rows_actual = node->rows_actual;
        join->rows_estimated = node->rows_estimated;
        join->addChild(std::move(node));
        join->addChild(std::move(child_node));
        node = std::move(join);
        continue;
      }

      // Subplans in PG are sometimes attached as extra children in the JSON.
      // We add them all as children here, and later hoist them if the node 
      // has more children than its type expects.
      node->addChild(std::move(child_node));
    }
  }

  return node;
}


/**
 * PostgreSQL often attaches subqueries (SubPlan) to operators as extra children.
 * This transformation hoists these extra children to a root Sequence node.
 */
void hoistSubPlans(std::unique_ptr<Node>& root, ExplainCtx& ctx) {
  std::vector<std::shared_ptr<Node>> hoisted;

  // Traverse the tree and find nodes marked as subplans in the context.
  for (auto& n : root->depth_first()) {
    for (size_t i = 0; i < n.childCount(); ++i) {
      Node* child = n.children()[i];
      if (ctx.subPlanNodes_.contains(child)) {
        // This is a subplan that should be hoisted.
        // But we only hoist it if it's NOT part of a join we synthesized earlier for Scans.
        // Synthesized joins for scans have the subplan as their second child.
        if (n.type == NodeType::JOIN && n.childCount() == 2 && n.children()[1] == child) {
            // Check if first child is a SCAN
            if (n.children()[0]->type == NodeType::SCAN || n.children()[0]->type == NodeType::SCAN_EMPTY || n.children()[0]->type == NodeType::SCAN_MATERIALISED) {
                // This is a Scan-Join we want to keep, do not hoist.
                continue;
            }
        }

        hoisted.push_back(n.takeChild(i));
        --i; // Adjust index after removal
      }
    }
  }

  if (!hoisted.empty()) {
    auto sequence = std::make_unique<Sequence>();
    for (auto& h : hoisted) {
      sequence->addSharedChild(std::move(h));
    }
    sequence->addChild(std::move(root));
    root = std::move(sequence);
    root->setParentToSelf();
  }
}

/**
* Loop joins in PG have the lookup table on the wrong side, flip that
*/
void flipJoins(Node& root) {
  std::vector<Node*> join_nodes;
  for (auto& node : root.depth_first()) {
    if (node.type == NodeType::JOIN) {
      const auto join_node = static_cast<Join*>(&node);
      if (join_node->strategy == Join::Strategy::HASH) {
        join_nodes.push_back(&node);
      }
    }
  }

  for (const auto node : join_nodes) {
    node->reverseChildren();
  }
}

/**
 * Propagate row counts for Sequence nodes.
 */
void fixRowCounts(Node* node) {
  for (auto& n : node->bottom_up()) {
    if (n.type == NodeType::SEQUENCE) {
      if (n.childCount() > 0) {
        const Node* last_child = n.lastChild();
        n.rows_actual = last_child->rows_actual;
        n.rows_estimated = last_child->rows_estimated;
      }
    }
  }
}

std::unique_ptr<Plan> buildExplainPlan(json& explain_json) {
  if (!explain_json.is_array() || explain_json.empty() || !explain_json[0].contains("Plan")) {
    throw std::runtime_error("Invalid EXPLAIN JSON format");
  }

  ExplainCtx ctx;
  // High level stats about the query
  double planning_time = 0.0;
  double execution_time = 0.0;


  if (explain_json[0].contains("Planning Time")) {
    planning_time = explain_json[0]["Planning Time"].get<double>();
  }
  if (explain_json[0].contains("Execution Time")) {
    execution_time = explain_json[0]["Execution Time"].get<double>();
  }

  auto& plan_json = explain_json[0]["Plan"];

  // Construct the tree
  auto top_node = buildExplainNode(plan_json, ctx);

  if (!top_node) {
    throw std::runtime_error("Invalid EXPLAIN plan output, could not construct a plan from ");
  }

  std::unique_ptr<Node> root_node;
  if (ctx.initPlans_.size() > 0) {
    /* Init plans are these strange constructs in PG that run before the main plan
     * We treat those as sequences of queries with a special sequence node
     */
    root_node = std::make_unique<Sequence>();
    for (auto& init_plan : ctx.initPlans_) {
      root_node->addChild(std::move(init_plan));
    }
    root_node->addChild(std::move(top_node));
  }
  else {
    root_node = std::move(top_node);
  }

  flipJoins(*root_node.get());
  hoistSubPlans(root_node, ctx);
  fixRowCounts(root_node.get());

  // Create and return the plan object with timing information
  auto plan = std::make_unique<Plan>(std::move(root_node));
  plan->planning_time = planning_time;
  plan->execution_time = execution_time;
  return plan;
}

std::unique_ptr<Plan> Connection::explain(const std::string_view statement, std::optional<std::string_view> name) {
  const std::string artifact_name = name.has_value() ? std::string(*name) : std::to_string(std::hash<std::string_view>{}(statement));
  const auto cached_json = getArtefact(artifact_name, "json");
  if (cached_json) {
    PLOGI << "Using cached execution plan artifact for: " << artifact_name;
    auto explain_json = json::parse(*cached_json);
    return buildExplainPlan(explain_json);
  }

  const std::string explain_modded = "EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON)\n" + std::string(statement);

  const auto result = fetchScalar(explain_modded);
  assert(result.is<SqlString>());

  auto explain_string = result.get<SqlString>().get();
  storeArtefact(artifact_name, "json", explain_string);
  auto explain_json = json::parse(explain_string);

  return buildExplainPlan(explain_json);
}
}
