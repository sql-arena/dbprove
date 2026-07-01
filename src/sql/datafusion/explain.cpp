#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <string_view>
#include <vector>

#include "connection.h"
#include "sql_exceptions.h"
#include <dbprove/common/json_utility.h>
#include <dbprove/common/string.h>
#include <explain/distribution.h>
#include <explain/group_by.h>
#include <explain/join.h>
#include <explain/limit.h>
#include <explain/plan.h>
#include <explain/projection.h>
#include <explain/scan.h>
#include <explain/select.h>
#include <explain/selection.h>
#include <explain/sort.h>
#include <nlohmann/json.hpp>

namespace sql::datafusion {
using json = nlohmann::json;
using namespace sql::explain;
using namespace dbprove::common;

namespace {
constexpr std::string_view kUnknownExpression = "<expr>";

std::string quoteSqlString(const std::string& value) {
  std::string quoted = "'";
  for (const char c : value) {
    if (c == '\'') {
      quoted += "''";
    } else {
      quoted += c;
    }
  }
  quoted += "'";
  return quoted;
}

std::string expressionToString(const json& expression);

bool isSimpleOutputName(std::string_view value) {
  if (value.empty()) {
    return false;
  }
  for (const char c : value) {
    if (std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_' || c == '.') {
      continue;
    }
    return false;
  }
  return true;
}

std::string expressionListToString(const json& expressions, std::string_view separator = ", ") {
  if (!expressions.is_array()) {
    return expressionToString(expressions);
  }

  std::ostringstream out;
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (i > 0) {
      out << separator;
    }
    out << expressionToString(expressions[i]);
  }
  return out.str();
}

std::string literalToString(const json& literal) {
  if (!literal.is_object() || literal.empty()) {
    return "NULL";
  }

  if (literal.contains("boolValue")) {
    return literal["boolValue"].get<bool>() ? "TRUE" : "FALSE";
  }
  if (literal.contains("utf8ViewValue")) {
    return quoteSqlString(literal["utf8ViewValue"].get<std::string>());
  }
  if (literal.contains("utf8Value")) {
    return quoteSqlString(literal["utf8Value"].get<std::string>());
  }
  if (literal.contains("int32Value")) {
    return std::to_string(literal["int32Value"].get<int32_t>());
  }
  if (literal.contains("int64Value")) {
    return literal["int64Value"].get<std::string>();
  }
  if (literal.contains("uint32Value")) {
    return std::to_string(literal["uint32Value"].get<uint32_t>());
  }
  if (literal.contains("uint64Value")) {
    return literal["uint64Value"].get<std::string>();
  }
  if (literal.contains("float32Value")) {
    return std::to_string(literal["float32Value"].get<float>());
  }
  if (literal.contains("float64Value")) {
    return std::to_string(literal["float64Value"].get<double>());
  }
  if (literal.contains("date32Value")) {
    return json_date32_to_string(literal["date32Value"].get<int64_t>());
  }
  if (literal.contains("decimal128Value")) {
    return json_decimal128_to_string(literal["decimal128Value"]);
  }
  if (literal.contains("nullValue")) {
    return "NULL";
  }
  return std::string(kUnknownExpression);
}

std::string binaryOperatorToString(std::string_view op) {
  if (op == "Eq") {
    return "=";
  }
  if (op == "NotEq") {
    return "<>";
  }
  if (op == "Lt") {
    return "<";
  }
  if (op == "LtEq") {
    return "<=";
  }
  if (op == "Gt") {
    return ">";
  }
  if (op == "GtEq") {
    return ">=";
  }
  if (op == "And") {
    return "AND";
  }
  if (op == "Or") {
    return "OR";
  }
  if (op == "Plus") {
    return "+";
  }
  if (op == "Minus") {
    return "-";
  }
  if (op == "Multiply") {
    return "*";
  }
  if (op == "Divide") {
    return "/";
  }
  if (op == "Modulo") {
    return "%";
  }
  if (op == "IsDistinctFrom") {
    return "IS DISTINCT FROM";
  }
  if (op == "IsNotDistinctFrom") {
    return "IS NOT DISTINCT FROM";
  }
  return std::string(op);
}

std::string caseToString(const json& case_expr) {
  std::ostringstream out;
  out << "CASE";
  if (case_expr.contains("expr") && !case_expr["expr"].is_null() && !case_expr["expr"].empty()) {
    out << " " << expressionToString(case_expr["expr"]);
  }
  for (const auto& entry : case_expr.value("whenThenExpr", json::array())) {
    out << " WHEN " << expressionToString(entry["whenExpr"]);
    out << " THEN " << expressionToString(entry["thenExpr"]);
  }
  if (case_expr.contains("elseExpr") && !case_expr["elseExpr"].is_null()) {
    out << " ELSE " << expressionToString(case_expr["elseExpr"]);
  }
  out << " END";
  return out.str();
}

std::string expressionToString(const json& expression) {
  if (!expression.is_object() || expression.empty()) {
    return std::string(kUnknownExpression);
  }

  if (expression.contains("column")) {
    const auto& column = expression["column"];
    return column.value("name", std::string(kUnknownExpression));
  }
  if (expression.contains("literal")) {
    return literalToString(expression["literal"]);
  }
  if (expression.contains("binaryExpr")) {
    const auto& binary = expression["binaryExpr"];
    const auto op = binary.value("op", "");
    if (op == "Modulo") {
      return "MOD(" + expressionToString(binary["l"]) + ", " + expressionToString(binary["r"]) + ")";
    }
    return "(" + expressionToString(binary["l"]) + " " + binaryOperatorToString(op) + " " +
           expressionToString(binary["r"]) + ")";
  }
  if (expression.contains("cast")) {
    const auto& cast = expression["cast"];
    return "CAST(" + expressionToString(cast["expr"]) + " AS " + json_arrow_type_to_string(cast["arrowType"]) + ")";
  }
  if (expression.contains("sort")) {
    const auto& sort = expression["sort"];
    auto rendered = expressionToString(sort["expr"]);
    if (sort.value("asc", true) == false) {
      rendered += " DESC";
    } else {
      rendered += " ASC";
    }
    if (sort.contains("nullsFirst")) {
      rendered += sort["nullsFirst"].get<bool>() ? " NULLS FIRST" : " NULLS LAST";
    }
    return rendered;
  }
  if (expression.contains("case")) {
    return caseToString(expression["case"]);
  }
  if (expression.contains("inList")) {
    const auto& in_list = expression["inList"];
    std::ostringstream out;
    out << expressionToString(in_list["expr"]);
    if (in_list.value("negated", false)) {
      out << " NOT";
    }
    out << " IN (";
    const auto& list = in_list["list"];
    for (size_t i = 0; i < list.size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << expressionToString(list[i]);
    }
    out << ")";
    return out.str();
  }
  if (expression.contains("likeExpr")) {
    const auto& like = expression["likeExpr"];
    std::ostringstream out;
    out << expressionToString(like["expr"]);
    if (like.value("negated", false)) {
      out << " NOT";
    }
    out << " LIKE " << expressionToString(like["pattern"]);
    return out.str();
  }
  if (expression.contains("scalarUdf")) {
    const auto& udf = expression["scalarUdf"];
    std::ostringstream out;
    out << udf.value("name", "udf") << "(";
    const auto& args = udf.value("args", json::array());
    for (size_t i = 0; i < args.size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << expressionToString(args[i]);
    }
    out << ")";
    return out.str();
  }
  if (expression.contains("aggregateExpr")) {
    const auto& aggregate = expression["aggregateExpr"];
    if (aggregate.contains("userDefinedAggrFunction") && aggregate.contains("expr")) {
      std::ostringstream out;
      out << aggregate["userDefinedAggrFunction"].get<std::string>() << "(";
      const auto& args = aggregate["expr"];
      for (size_t i = 0; i < args.size(); ++i) {
        if (i > 0) {
          out << ", ";
        }
        out << expressionToString(args[i]);
      }
      out << ")";
      return out.str();
    }
    if (aggregate.contains("humanDisplay")) {
      return aggregate["humanDisplay"].get<std::string>();
    }
  }
  if (expression.contains("hashExpr")) {
    const auto& hash = expression["hashExpr"];
    if (hash.is_array()) {
      return "HASH(" + expressionListToString(hash) + ")";
    }
    if (hash.is_object()) {
      const auto name = to_upper(hash.value("description", "hash"));
      if (hash.contains("onColumns")) {
        return name + "(" + expressionListToString(hash["onColumns"]) + ")";
      }
      if (hash.contains("hashExpr")) {
        return name + "(" + expressionListToString(hash["hashExpr"]) + ")";
      }
      return name + "()";
    }
  }
  if (expression.contains("isNullExpr")) {
    return expressionToString(expression["isNullExpr"]["expr"]) + " IS NULL";
  }
  if (expression.contains("isNotNullExpr")) {
    return expressionToString(expression["isNotNullExpr"]["expr"]) + " IS NOT NULL";
  }
  return std::string(kUnknownExpression);
}

std::vector<Column> columnsFromExpressions(const json& expressions, const json* aliases = nullptr) {
  std::vector<Column> columns;
  if (!expressions.is_array()) {
    return columns;
  }

  columns.reserve(expressions.size());
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (aliases != nullptr && aliases->is_array() && i < aliases->size() && (*aliases)[i].is_string()) {
      const auto alias = (*aliases)[i].get<std::string>();
      if (isSimpleOutputName(alias)) {
        columns.emplace_back(alias);
        continue;
      }
    }
    columns.emplace_back(expressionToString(expressions[i]));
  }
  return columns;
}

std::vector<Column> columnsFromAggregateExpressions(const json& expressions, const json* aliases = nullptr) {
  std::vector<Column> columns;
  if (!expressions.is_array()) {
    return columns;
  }

  columns.reserve(expressions.size());
  for (size_t i = 0; i < expressions.size(); ++i) {
    if (aliases != nullptr && aliases->is_array() && i < aliases->size() && (*aliases)[i].is_string()) {
      const auto alias = (*aliases)[i].get<std::string>();
      if (isSimpleOutputName(alias)) {
        columns.emplace_back(alias);
        continue;
      }
    }
    columns.emplace_back(expressionToString(expressions[i]));
  }
  return columns;
}

std::vector<Column> columnsFromSortExpressions(const json& expressions) {
  std::vector<Column> columns;
  if (!expressions.is_array()) {
    return columns;
  }

  columns.reserve(expressions.size());
  for (const auto& entry : expressions) {
    if (!entry.is_object() || !entry.contains("sort")) {
      columns.emplace_back(expressionToString(entry));
      continue;
    }

    const auto& sort = entry["sort"];
    const auto sorting = sort.value("asc", true) ? Column::Sorting::ASC : Column::Sorting::DESC;
    columns.emplace_back(expressionToString(sort["expr"]), sorting);
  }
  return columns;
}

std::optional<double> extractNumRows(const json& statistics) {
  if (!statistics.is_object() || !statistics.contains("numRows")) {
    return std::nullopt;
  }

  const auto& num_rows = statistics["numRows"];
  if (num_rows.contains("val") && num_rows["val"].is_object()) {
    for (const auto& [_, value] : num_rows["val"].items()) {
      if (const auto numeric = json_as_double(value)) {
        return *numeric;
      }
    }
  }
  return std::nullopt;
}

std::optional<double> extractDbproveRowCount(const json& body, std::string_view key) {
  if (!body.is_object() || !body.contains("dbprove") || !body["dbprove"].is_object()) {
    return std::nullopt;
  }

  const auto& dbprove = body["dbprove"];
  if (key == "estimated") {
    if (!dbprove.contains("statistics") || !dbprove["statistics"].is_object()) {
      return std::nullopt;
    }
    const auto& statistics = dbprove["statistics"];
    if (!statistics.contains("numRows") || !statistics["numRows"].is_object()) {
      return std::nullopt;
    }
    const auto& num_rows = statistics["numRows"];
    if (num_rows.contains("value")) {
      return json_as_double(num_rows["value"]);
    }
    return std::nullopt;
  }

  if (key == "actual") {
    if (!dbprove.contains("metrics") || !dbprove["metrics"].is_object()) {
      return std::nullopt;
    }
    const auto& metrics = dbprove["metrics"];
    if (!metrics.contains("outputRows")) {
      return std::nullopt;
    }
    return json_as_double(metrics["outputRows"]);
  }

  return std::nullopt;
}

std::optional<double> estimateFromDbprove(const json& body) { return extractDbproveRowCount(body, "estimated"); }

std::string tableNameFromPath(std::string_view path) {
  const auto slash = path.find_last_of('/');
  const auto base = slash == std::string_view::npos ? path : path.substr(slash + 1);
  const auto dot = base.rfind('.');
  return std::string(dot == std::string_view::npos ? base : base.substr(0, dot));
}

std::string extractTableName(const json& parquet_scan) {
  if (!parquet_scan.is_object() || !parquet_scan.contains("baseConf")) {
    return "unknown";
  }
  const auto& file_groups = parquet_scan["baseConf"].value("fileGroups", json::array());
  if (file_groups.empty()) {
    return "unknown";
  }
  const auto& files = file_groups[0].value("files", json::array());
  if (files.empty() || !files[0].contains("path")) {
    return "unknown";
  }
  return tableNameFromPath(files[0]["path"].get<std::string>());
}

std::optional<RowCount> extractFetch(const json& op_body) {
  if (!op_body.is_object() || !op_body.contains("fetch")) {
    return std::nullopt;
  }
  const auto fetch = json_as_int64(op_body["fetch"]);
  if (!fetch.has_value() || *fetch < 0) {
    return std::nullopt;
  }
  return static_cast<RowCount>(*fetch);
}

Join::Type parseJoinType(const json& join_body) {
  const auto join_type = to_lower(join_body.value("joinType", "INNER"));
  if (join_type == "inner") {
    return Join::Type::INNER;
  }
  if (join_type == "left") {
    return Join::Type::LEFT_OUTER;
  }
  if (join_type == "right") {
    return Join::Type::RIGHT_OUTER;
  }
  if (join_type == "full") {
    return Join::Type::FULL;
  }
  if (join_type == "cross") {
    return Join::Type::CROSS;
  }
  if (join_type == "leftsemi" || join_type == "leftsemiinner") {
    return Join::Type::LEFT_SEMI_INNER;
  }
  if (join_type == "rightsemi" || join_type == "rightsemiinner") {
    return Join::Type::RIGHT_SEMI_INNER;
  }
  if (join_type == "leftanti") {
    return Join::Type::LEFT_ANTI;
  }
  if (join_type == "rightanti") {
    return Join::Type::RIGHT_ANTI;
  }
  return Join::typeFromString(join_type);
}

std::string hashJoinCondition(const json& join_body) {
  std::vector<std::string> parts;

  for (const auto& pair : join_body.value("on", json::array())) {
    parts.push_back(expressionToString(pair["left"]) + " = " + expressionToString(pair["right"]));
  }

  if (join_body.contains("filter") && join_body["filter"].is_object()) {
    const auto& filter = join_body["filter"];
    if (filter.contains("expression")) {
      parts.push_back(expressionToString(filter["expression"]));
    } else if (!filter.empty()) {
      parts.push_back(expressionToString(filter));
    }
  }

  if (parts.empty()) {
    return "";
  }

  std::ostringstream out;
  for (size_t i = 0; i < parts.size(); ++i) {
    if (i > 0) {
      out << " AND ";
    }
    out << parts[i];
  }
  return out.str();
}

std::string joinFilterExpression(const json& filter) {
  if (!filter.is_object()) {
    return expressionToString(filter);
  }
  if (filter.contains("expression")) {
    return expressionToString(filter["expression"]);
  }
  return expressionToString(filter);
}

std::unique_ptr<Node> buildCanonicalNode(const json& wrapper);

std::unique_ptr<Node> wrapFetch(std::unique_ptr<Node> node, const json& op_body) {
  const auto fetch = extractFetch(op_body);
  if (!fetch.has_value()) {
    return node;
  }

  auto limit = std::make_unique<Limit>(*fetch);
  limit->rows_estimated = node->rows_estimated;
  limit->rows_actual = node->rows_actual;
  limit->addChild(std::move(node));
  return limit;
}

bool sameEstimateAsChild(const Node& node) {
  if (node.childCount() == 0 || node.firstChild() == nullptr) {
    return false;
  }
  const auto child_estimated = node.firstChild()->rows_estimated;
  return !std::isnan(node.rows_estimated) && !std::isnan(child_estimated) &&
         std::abs(node.rows_estimated - child_estimated) < 0.5;
}

void clearUninformativeEstimate(Node& node) { node.rows_estimated = NAN; }

void dropAggregateFallbackEstimateIfNeeded(Node& node, const json& body) {
  if (!body.contains("groupExpr")) {
    return;
  }
  const auto& group_expr = body["groupExpr"];
  if (!group_expr.is_array() || group_expr.empty()) {
    return;
  }
  if (sameEstimateAsChild(node)) {
    clearUninformativeEstimate(node);
  }
}

void dropPassthroughEstimate(Node& node) { clearUninformativeEstimate(node); }

void applyDbproveRowCounts(Node& node, const json& body) {
  if (const auto estimated = estimateFromDbprove(body)) {
    node.rows_estimated = *estimated;
  }
  if (const auto actual = extractDbproveRowCount(body, "actual")) {
    node.rows_actual = *actual;
  }
}

void propagateMissingEstimates(Node& root) {
  for (auto& node : root.bottom_up()) {
    if (!std::isnan(node.rows_estimated)) {
      continue;
    }
    if (node.childCount() != 1 || node.firstChild() == nullptr) {
      continue;
    }
    const auto child_estimated = node.firstChild()->rows_estimated;
    if (std::isnan(child_estimated) || std::isinf(child_estimated)) {
      continue;
    }
    node.rows_estimated = child_estimated;
  }
}

std::unique_ptr<Node> buildCanonicalNode(const json& wrapper) {
  if (!wrapper.is_object() || wrapper.empty()) {
    throw InvalidPlanException("DataFusion physical plan node was not an object");
  }

  const auto op = wrapper.begin().key();
  const auto& body = wrapper.begin().value();
  std::unique_ptr<Node> result;

  if (op == "parquetScan") {
    auto scan = std::make_unique<Scan>(extractTableName(body));
    if (body.contains("predicate")) {
      scan->setFilter(expressionToString(body["predicate"]));
    }
    applyDbproveRowCounts(*scan, body);
    if (body.contains("baseConf") && body["baseConf"].contains("limit") &&
        body["baseConf"]["limit"].contains("limit")) {
      auto limit = std::make_unique<Limit>(body["baseConf"]["limit"]["limit"].get<RowCount>());
      limit->addChild(std::move(scan));
      return limit;
    }
    return scan;
  }

  if (op == "filter") {
    result = std::make_unique<Selection>(expressionToString(body["expr"]));
    result->addChild(buildCanonicalNode(body["input"]));
    applyDbproveRowCounts(*result, body);
    return result;
  }

  if (op == "projection") {
    const json* aliases = body.contains("exprName") ? &body["exprName"] : nullptr;
    result = std::make_unique<Projection>(columnsFromExpressions(body["expr"], aliases));
    result->addChild(buildCanonicalNode(body["input"]));
    applyDbproveRowCounts(*result, body);
    dropPassthroughEstimate(*result);
    return result;
  }

  if (op == "aggregate") {
    auto strategy = GroupBy::Strategy::HASH;
    const auto mode = body.value("mode", "");
    if (mode.contains("PARTIAL")) {
      strategy = GroupBy::Strategy::PARTIAL;
    }
    const json* group_aliases = body.contains("groupExprName") ? &body["groupExprName"] : nullptr;
    const json* aggr_aliases = body.contains("aggrExprName") ? &body["aggrExprName"] : nullptr;
    result = std::make_unique<GroupBy>(
        strategy, columnsFromExpressions(body.value("groupExpr", json::array()), group_aliases),
        columnsFromAggregateExpressions(body.value("aggrExpr", json::array()), aggr_aliases));
    result->addChild(buildCanonicalNode(body["input"]));
    applyDbproveRowCounts(*result, body);
    dropAggregateFallbackEstimateIfNeeded(*result, body);
    return result;
  }

  if (op == "hashJoin") {
    result = std::make_unique<Join>(parseJoinType(body), Join::Strategy::HASH, hashJoinCondition(body));
    // DataFusion HashJoinExec always uses right as the build side.
    // Our convention: firstChild()=build, lastChild()=probe.
    result->addChild(buildCanonicalNode(body["right"]));
    result->addChild(buildCanonicalNode(body["left"]));
    applyDbproveRowCounts(*result, body);
    return result;
  }

  if (op == "nestedLoopJoin") {
    std::string condition;
    if (body.contains("filter")) {
      condition = joinFilterExpression(body["filter"]);
    }
    result = std::make_unique<Join>(parseJoinType(body), Join::Strategy::LOOP, condition);
    // DataFusion NestedLoopJoin: right is the inner (build) side.
    result->addChild(buildCanonicalNode(body["right"]));
    result->addChild(buildCanonicalNode(body["left"]));
    applyDbproveRowCounts(*result, body);
    return result;
  }

  if (op == "repartition") {
    const auto& partitioning = body["partitioning"];
    if (partitioning.contains("hash")) {
      result = std::make_unique<Distribute>(
          Distribute::Strategy::HASH, columnsFromExpressions(partitioning["hash"].value("hashExpr", json::array())));
    } else if (partitioning.contains("roundRobin")) {
      result = std::make_unique<Distribute>(Distribute::Strategy::ROUND_ROBIN);
    } else {
      result = std::make_unique<Distribute>(Distribute::Strategy::GATHER);
    }
    result->addChild(buildCanonicalNode(body["input"]));
    applyDbproveRowCounts(*result, body);
    dropPassthroughEstimate(*result);
    return result;
  }

  if (op == "merge") {
    result = std::make_unique<Distribute>(Distribute::Strategy::GATHER);
    result->addChild(buildCanonicalNode(body["input"]));
    applyDbproveRowCounts(*result, body);
    dropPassthroughEstimate(*result);
    return wrapFetch(std::move(result), body);
  }

  if (op == "sort") {
    result = std::make_unique<Sort>(columnsFromSortExpressions(body["expr"]));
    result->addChild(buildCanonicalNode(body["input"]));
    applyDbproveRowCounts(*result, body);
    dropPassthroughEstimate(*result);
    return wrapFetch(std::move(result), body);
  }

  if (op == "sortPreservingMerge") {
    result = std::make_unique<Sort>(columnsFromSortExpressions(body["expr"]));
    result->addChild(buildCanonicalNode(body["input"]));
    applyDbproveRowCounts(*result, body);
    dropPassthroughEstimate(*result);
    return wrapFetch(std::move(result), body);
  }

  result = std::make_unique<Select>();
  if (body.is_object()) {
    if (body.contains("input")) {
      result->addChild(buildCanonicalNode(body["input"]));
    } else {
      if (body.contains("left")) {
        result->addChild(buildCanonicalNode(body["left"]));
      }
      if (body.contains("right")) {
        result->addChild(buildCanonicalNode(body["right"]));
      }
    }
  }
  applyDbproveRowCounts(*result, body);
  return result;
}
}  // namespace

std::unique_ptr<Plan> Connection::explain(std::string_view statement, std::optional<std::string_view> name) {
  const std::lock_guard<std::recursive_mutex> lock(driverMutex());
  const std::string artifact_name =
      name.has_value() ? std::string(*name) : std::to_string(std::hash<std::string_view>{}(statement));

  json physical_json;
  if (const auto cached = getArtefact(artifact_name + "_physical", "json")) {
    physical_json = json::parse(*cached);
  } else {
    physical_json = fetchPhysicalPlanJson(statement);
    storeArtefact(artifact_name + "_physical", "json", physical_json.dump());
  }

  auto root = buildCanonicalNode(physical_json);
  propagateMissingEstimates(*root);
  return std::make_unique<Plan>(std::move(root));
}
}  // namespace sql::datafusion
