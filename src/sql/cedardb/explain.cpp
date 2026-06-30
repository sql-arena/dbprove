#include "connection.h"

#include "group_by.h"
#include "join.h"
#include "limit.h"
#include "scan.h"
#include "select.h"
#include "selection.h"
#include "sort.h"
#include "union.h"
#include "explain/node.h"
#include "explain/plan.h"
#include <nlohmann/json.hpp>
#include <plog/Log.h>

namespace sql::cedardb {
using namespace sql::explain;
using namespace nlohmann;

// ---------------------------------------------------------------------------
// Expression rendering
// ---------------------------------------------------------------------------

/**
 * Render a CedarDB expression object to a compact SQL-like string.
 * CedarDB expressions are trees: {"expression":"compare","left":{...},"right":{...},"direction":"="}
 */
static std::string renderExpr(const json& expr) {
  if (!expr.is_object() || !expr.contains("expression")) {
    return "";
  }
  const auto kind = expr["expression"].get<std::string>();

  if (kind == "iuref") {
    return expr.value("iu", "");
  }

  if (kind == "const") {
    if (!expr.contains("value")) return "null";
    const auto& val = expr["value"];
    if (val.contains("null") && val["null"].get<bool>()) return "null";
    if (!val.contains("value")) return "";
    const auto& v = val["value"];
    if (v.is_string()) return v.get<std::string>();
    if (v.is_number_integer()) return std::to_string(v.get<int64_t>());
    if (v.is_number_float()) return std::to_string(v.get<double>());
    if (v.is_boolean()) return v.get<bool>() ? "true" : "false";
    return "";
  }

  if (kind == "compare") {
    const auto left = renderExpr(expr.contains("left") ? expr["left"] : json{});
    const auto right = renderExpr(expr.contains("right") ? expr["right"] : json{});
    if (left.empty() || right.empty()) return "";
    return left + " " + expr.value("direction", "=") + " " + right;
  }

  if (kind == "cast") {
    return renderExpr(expr.contains("input") ? expr["input"] : json{});
  }

  if (kind == "parameter") {
    return "$" + std::to_string(expr.value("id", 0) + 1);
  }

  if (kind == "and" || kind == "or") {
    const auto sep = (kind == "and") ? " AND " : " OR ";
    std::string result;
    for (const auto& part : expr.value("input", json::array())) {
      const auto s = renderExpr(part);
      if (!s.empty()) {
        if (!result.empty()) result += sep;
        result += s;
      }
    }
    return result;
  }

  if (kind == "mul" || kind == "sub" || kind == "div") {
    const auto op = (kind == "mul") ? " * " : (kind == "sub") ? " - " : " / ";
    const auto left = renderExpr(expr.contains("left") ? expr["left"] : json{});
    const auto right = renderExpr(expr.contains("right") ? expr["right"] : json{});
    if (left.empty() || right.empty()) return "";
    return left + op + right;
  }

  if (kind == "extractyear") {
    const auto input = renderExpr(expr.contains("input") ? expr["input"] : json{});
    return input.empty() ? "" : "EXTRACT(YEAR FROM " + input + ")";
  }

  if (kind == "between") {
    const auto& inputs = expr.value("input", json::array());
    if (inputs.size() < 3) return "";
    const auto val = renderExpr(inputs[0]);
    const auto lo  = renderExpr(inputs[1]);
    const auto hi  = renderExpr(inputs[2]);
    if (val.empty() || lo.empty() || hi.empty()) return "";
    return val + " BETWEEN " + lo + " AND " + hi;
  }

  if (kind == "in") {
    const auto& inputs = expr.value("input", json::array());
    if (inputs.empty()) return "";
    const auto col = renderExpr(inputs[0]);
    if (col.empty()) return "";
    // values[] items are {type, value} objects (not const expressions)
    std::string vals;
    for (const auto& v : expr.value("values", json::array())) {
      const auto& raw = v.value("value", json{});
      std::string s;
      if (raw.is_string())         s = raw.get<std::string>();
      else if (raw.is_number_integer()) s = std::to_string(raw.get<int64_t>());
      else if (raw.is_number_float())   s = std::to_string(raw.get<double>());
      if (!s.empty()) { if (!vals.empty()) vals += ", "; vals += s; }
    }
    return vals.empty() ? "" : col + " IN (" + vals + ")";
  }

  if (kind == "contains") {
    const auto& inputs = expr.value("input", json::array());
    if (inputs.size() < 2) return "";
    const auto str = renderExpr(inputs[0]);
    const auto sub = renderExpr(inputs[1]);
    if (str.empty() || sub.empty()) return "";
    return str + " LIKE '%" + sub + "%'";
  }

  if (kind == "left") {
    const auto& inputs = expr.value("input", json::array());
    if (inputs.size() < 2) return "";
    const auto str = renderExpr(inputs[0]);
    const auto len = renderExpr(inputs[1]);
    if (str.empty() || len.empty()) return "";
    return "LEFT(" + str + ", " + len + ")";
  }

  if (kind == "searchedcase") {
    std::string result = "CASE";
    for (const auto& c : expr.value("cases", json::array())) {
      const auto cond = renderExpr(c.value("condition", json{}));
      const auto res  = renderExpr(c.value("result", json{}));
      if (!cond.empty() && !res.empty())
        result += " WHEN " + cond + " THEN " + res;
    }
    if (expr.contains("else")) {
      const auto else_val = renderExpr(expr["else"]);
      if (!else_val.empty()) result += " ELSE " + else_val;
    }
    return result + " END";
  }

  return ""; // unknown — return empty so callers skip
}

// ---------------------------------------------------------------------------
// Restriction / filter rendering for tablescan nodes
// ---------------------------------------------------------------------------

/**
 * Build a filter condition string from CedarDB's restrictions array.
 *
 * Each entry: {"attribute": <index>, "mode": "="|">="|"<="|"[]"|"is"|"isnotnull"|"joinfilter", ...}
 *
 * We skip:
 *   - "isnotnull" : NULL-elimination probes injected by the optimizer for join safety
 *   - "joinfilter": Bloom-filter probes injected from the hash-join build side
 */
static std::string renderRestrictions(const json& node) {
  const auto& restrictions = node["restrictions"];
  const auto& attributes = node["attributes"];

  auto attrName = [&](int idx) -> std::string {
    if (idx >= 0 && idx < static_cast<int>(attributes.size())) {
      return attributes[idx].value("name", "?");
    }
    return "?";
  };

  std::string filter;
  for (const auto& r : restrictions) {
    const auto mode = r.value("mode", "");
    if (mode == "isnotnull" || mode == "joinfilter") continue;

    const int attr_idx = r.value("attribute", -1);
    const std::string col = attrName(attr_idx);

    std::string clause;
    if (mode == "is") {
      const bool is_null = r.contains("value") && r["value"].contains("value")
                           && r["value"]["value"].contains("null")
                           && r["value"]["value"]["null"].get<bool>();
      clause = col + (is_null ? " IS NULL" : " IS NOT NULL");
    } else if (mode == "[]") {
      const auto lo = renderExpr(r.value("value", json{}));
      const auto hi = renderExpr(r.value("upper", json{}));
      if (lo.empty() || hi.empty()) continue;
      clause = col + " BETWEEN " + lo + " AND " + hi;
    } else {
      // =, >=, <=
      const auto val_str = renderExpr(r.value("value", json{}));
      if (val_str.empty()) continue;
      clause = col + " " + mode + " " + val_str;
    }

    if (!clause.empty()) {
      if (!filter.empty()) filter += " AND ";
      filter += clause;
    }
  }

  // Residuals are post-scan filter expressions not pushed into index
  if (node.contains("residuals") && node["residuals"].is_array()) {
    for (const auto& res : node["residuals"]) {
      const auto clause = renderExpr(res);
      if (!clause.empty() && clause != "?") {
        if (!filter.empty()) filter += " AND ";
        filter += clause;
      }
    }
  }

  return filter;
}

// ---------------------------------------------------------------------------
// Physical join strategy mapping
// ---------------------------------------------------------------------------

static Join::Strategy joinStrategy(const std::string& phys_op) {
  if (phys_op == "hashjoin")    return Join::Strategy::HASH;
  if (phys_op == "bnljoin")     return Join::Strategy::LOOP;
  if (phys_op == "indexnljoin") return Join::Strategy::LOOP;
  if (phys_op == "singletonjoin") return Join::Strategy::LOOP;
  return Join::Strategy::HASH; // safe default
}

// ---------------------------------------------------------------------------
// Node construction
// ---------------------------------------------------------------------------

std::unique_ptr<Node> buildCedarNode(const json& node_json);

/**
 * Build a single plan node from a CedarDB JSON object.
 * Returns nullptr for operators we skip (e.g. window, unknown).
 */
static std::unique_ptr<Node> makeCedarNode(const json& j) {
  const auto op = j.value("operator", "");
  const auto phys = j.value("physicalOperator", "");

  // ---- Scan ---------------------------------------------------------------
  if (op == "tablescan") {
    const auto table_name = j.value("tablename", "");
    const auto strategy = (phys == "indexscan") ? Scan::Strategy::SEEK : Scan::Strategy::SCAN;
    auto node = std::make_unique<Scan>(table_name, strategy);

    if (j.contains("restrictions") && j.contains("attributes")) {
      const auto filter = renderRestrictions(j);
      if (!filter.empty()) {
        node->setFilter(filter);
      }
    }
    return node;
  }

  // ---- Join ---------------------------------------------------------------
  if (op == "join") {
    const auto join_type_str = j.value("type", "inner");
    Join::Type join_type = Join::Type::INNER;
    if      (join_type_str == "inner")       join_type = Join::Type::INNER;
    else if (join_type_str == "rightouter")  join_type = Join::Type::RIGHT_OUTER;
    else if (join_type_str == "leftouter")   join_type = Join::Type::LEFT_OUTER;
    else if (join_type_str == "rightsemi")   join_type = Join::Type::RIGHT_SEMI_INNER;
    else if (join_type_str == "leftsemi")    join_type = Join::Type::LEFT_SEMI_INNER;
    else if (join_type_str == "rightanti")   join_type = Join::Type::RIGHT_ANTI;
    else if (join_type_str == "leftanti")    join_type = Join::Type::LEFT_ANTI;
    else if (join_type_str == "full")        join_type = Join::Type::FULL;

    std::string condition;
    if (j.contains("condition")) {
      condition = renderExpr(j["condition"]);
    }
    if (condition.empty() && join_type == Join::Type::INNER) {
      join_type = Join::Type::CROSS;
    }

    return std::make_unique<Join>(join_type, joinStrategy(phys), condition);
  }

  // ---- GroupBy / ungrouped aggregation ------------------------------------
  if (op == "groupby") {
    std::vector<Column> group_keys;
    std::vector<Column> aggregates;

    // "key" references indices into "values"
    const auto& values_arr = j.contains("values") ? j["values"] : json::array();
    if (j.contains("key") && j["key"].is_array()) {
      for (const auto& k : j["key"]) {
        const int arg = k.value("arg", -1);
        std::string name;
        if (arg >= 0 && arg < static_cast<int>(values_arr.size())) {
          name = renderExpr(values_arr[arg]);
        } else {
          name = k.value("iu", "?");
        }
        group_keys.emplace_back(name);
      }
    }

    if (j.contains("aggregates") && j["aggregates"].is_array()) {
      for (const auto& agg : j["aggregates"]) {
        const auto agg_op = agg.value("op", "");
        const auto agg_iu = agg.value("iu", agg_op);
        aggregates.emplace_back(agg_op.empty() ? agg_iu : agg_op);
      }
    }

    const auto strategy = (phys == "ungroupedaggregation")
                          ? GroupBy::Strategy::SIMPLE
                          : GroupBy::Strategy::HASH;
    return std::make_unique<GroupBy>(strategy, group_keys, aggregates);
  }

  // ---- Sort / TopK / Limit ------------------------------------------------
  if (op == "sort") {
    if (phys == "limit") {
      // Pure LIMIT without ORDER BY; use a Limit node
      int64_t limit_count = -1;
      if (j.contains("limit")) {
        const auto lim_expr = j["limit"];
        if (lim_expr.contains("value") && lim_expr["value"].contains("value")) {
          limit_count = lim_expr["value"]["value"].get<int64_t>();
        }
      }
      return std::make_unique<Limit>(limit_count);
    }

    std::vector<Column> sort_cols;
    if (j.contains("order") && j["order"].is_array()) {
      for (const auto& entry : j["order"]) {
        const auto name = renderExpr(entry.value("value", json{}));
        const bool desc = entry.value("descending", false);
        sort_cols.emplace_back(name, desc ? Column::Sorting::DESC : Column::Sorting::ASC);
      }
    }
    auto node = std::make_unique<Sort>(sort_cols);

    // Attach limit count if present (simpletopk)
    if (j.contains("limit") && j["limit"].contains("value") && j["limit"]["value"].contains("value")) {
      // Store as filter condition for display (Sort doesn't carry a limit field)
      const int64_t lim = j["limit"]["value"]["value"].get<int64_t>();
      node->setFilter("LIMIT " + std::to_string(lim));
    }
    return node;
  }

  // ---- Selection (HAVING filter, scalar result) ---------------------------
  if (op == "select") {
    return std::make_unique<Selection>("");
  }

  // ---- Set operations (UNION / INTERSECT / EXCEPT) -------------------------
  if (op == "setoperation") {
    const auto set_op = j.value("operation", "unionall");
    const auto union_type = (set_op == "unionall") ? Union::Type::ALL : Union::Type::DISTINCT;
    return std::make_unique<Union>(union_type);
  }

  // ---- Pipeline breaker scan: materialized intermediate result ------------
  // pipelineBreaker.input child holds the subplan that was materialized;
  // scannedOperator reference (no child) is a leaf scan of that temp result.
  if (op == "pipelinebreakerscan") {
    // The subplan variant embeds the child as pipelineBreaker.input — handled
    // transparently in buildCedarNode; returning nullptr triggers that fallback.
    // The reference variant (scannedOperator) has no tree child — emit a Scan.
    if (!j.contains("pipelineBreaker")) {
      return std::make_unique<Scan>("<temp>", Scan::Strategy::SCAN);
    }
    return nullptr; // pass through to pipelineBreaker.input below
  }

  // ---- Late materialisation: fetch rows by TID after a sort/filter pass ---
  // Transparent: children are attached via 'left' in buildCedarNode.
  if (op == "latematerialization") {
    return nullptr; // pass through to left child
  }

  // ---- Window functions: not a canonical node; fold into input ------------
  // (handled in buildCedarNode by passing through to child)

  return nullptr; // unknown / unhandled
}

/**
 * Recursively build the plan tree from a CedarDB JSON node.
 * Unknown operator nodes are transparent: we try their child instead.
 */
std::unique_ptr<Node> buildCedarNode(const json& node_json) {
  auto node = makeCedarNode(node_json);

  // For skipped / unknown operators pass through to child
  if (node == nullptr) {
    // pipelinebreakerscan with subplan: recurse into the materialized subtree
    if (node_json.value("operator", "") == "pipelinebreakerscan"
        && node_json.contains("pipelineBreaker")
        && node_json["pipelineBreaker"].contains("input")) {
      return buildCedarNode(node_json["pipelineBreaker"]["input"]);
    }
    // latematerialization: pass through to left child
    if (node_json.value("operator", "") == "latematerialization"
        && node_json.contains("left")) {
      return buildCedarNode(node_json["left"]);
    }
    if (node_json.contains("input")) {
      return buildCedarNode(node_json["input"]);
    }
    const auto op = node_json.value("operator", "?");
    throw std::runtime_error("CedarDB EXPLAIN: unhandled operator '" + op + "' with no input child");
  }

  // Attach row counts
  node->rows_estimated = node_json.value("cardinality", std::numeric_limits<double>::quiet_NaN());
  if (node_json.contains("analyzePlanCardinality")) {
    node->rows_actual = node_json["analyzePlanCardinality"].get<double>();
  }

  // ---- Recurse into children -------------------------------------------

  const auto op = node_json.value("operator", "");

  // Binary: join has left + right
  if (op == "join") {
    if (node_json.contains("left")) {
      if (auto child = buildCedarNode(node_json["left"])) {
        node->addChild(std::move(child));
      }
    }
    if (node_json.contains("right")) {
      if (auto child = buildCedarNode(node_json["right"])) {
        node->addChild(std::move(child));
      }
    }
    return node;
  }

  // N-ary: setoperation has arguments[]
  if (op == "setoperation") {
    if (node_json.contains("arguments") && node_json["arguments"].is_array()) {
      for (const auto& arg : node_json["arguments"]) {
        if (arg.contains("input")) {
          if (auto child = buildCedarNode(arg["input"])) {
            node->addChild(std::move(child));
          }
        }
      }
    }
    return node;
  }

  // Unary: everything else uses "input"
  if (node_json.contains("input")) {
    if (auto child = buildCedarNode(node_json["input"])) {
      node->addChild(std::move(child));
    }
  }

  return node;
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

static std::unique_ptr<Plan> buildExplainPlan(const json& root_json) {
  if (!root_json.contains("plan")) {
    throw std::runtime_error("CedarDB EXPLAIN JSON missing 'plan' key");
  }

  auto top_node = buildCedarNode(root_json["plan"]);
  if (!top_node) {
    throw std::runtime_error("CedarDB EXPLAIN: could not construct plan tree");
  }

  double execution_time = 0.0;
  if (root_json.contains("analyzePlanPipelines")) {
    for (const auto& pipeline : root_json["analyzePlanPipelines"]) {
      execution_time += pipeline.value("duration", 0.0);
    }
    // CedarDB reports durations in microseconds; convert to milliseconds
    execution_time /= 1000.0;
  }

  auto plan = std::make_unique<Plan>(std::move(top_node));
  plan->execution_time = execution_time;
  return plan;
}

std::unique_ptr<Plan> Connection::explain(const std::string_view statement,
                                           std::optional<std::string_view> name) {
  const std::string artifact_name = name.has_value()
    ? std::string(*name)
    : std::to_string(std::hash<std::string_view>{}(statement));

  const auto cached = getArtefact(artifact_name, "json");
  if (cached) {
    PLOGI << "Using cached execution plan artifact for: " << artifact_name;
    return buildExplainPlan(json::parse(*cached));
  }

  const std::string explain_sql = "EXPLAIN (ANALYZE, FORMAT JSON)\n" + std::string(statement);
  const auto result = fetchScalar(explain_sql);
  assert(result.is<SqlString>());

  const auto explain_string = result.get<SqlString>().get();
  storeArtefact(artifact_name, "json", explain_string);
  return buildExplainPlan(json::parse(explain_string));
}
} // namespace sql::cedardb
