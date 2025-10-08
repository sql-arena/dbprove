#include "connection.h"
#include "explain/plan.h"
#include <memory>
#include <pugixml.hpp>
#include <regex>
#include <set>
#include <unordered_set>

#include "group_by.h"
#include "join.h"
#include "projection.h"
#include "scan.h"
#include "selection.h"
#include "sort.h"
#include "sql_exceptions.h"

using namespace sql::explain;
using namespace pugi;

namespace sql::msodbc {
void parseRowCount(Node* node, xml_node node_xml) {
  const double rows_estimated = node_xml.attribute("EstimateRows").as_double();
  node->rows_estimated = rows_estimated;

  const auto runtime_info = node_xml.child("RunTimeInformation");
  if (runtime_info) {
    const auto per_thread = runtime_info.child("RunTimeCountersPerThread");
    if (per_thread) {
      node->rows_actual = per_thread.attribute("ActualRows").as_double();
    }
  }
}

/**
 * SQL Server for some reason does not add actual row counts to projections.
 * But these cannot change the actual, so we can correct by simply taking the count of the child
 * @param node
 */
void fixProjection(Node* node) {
  for (auto& child : node->depth_first()) {
    if (child.type == NodeType::PROJECTION || child.type == NodeType::SELECTION) {
      child.rows_actual = child.firstChild()->rows_actual;
    }
  }
}

std::string definedValueExpression(const xml_node& defined_value) {
  const auto scalar_op = defined_value.child("ScalarOperator");
  std::string name = scalar_op.attribute("ScalarString").as_string();
  const auto column_ref = defined_value.child("ColumnReference");
  if (column_ref) {
    name += " AS " + std::string(column_ref.attribute("Column").as_string());
  }

  return name;
}

static std::string qname(const xml_node& column_reference) {
  auto get = [&](const char* a)-> std::string {
    auto v = column_reference.attribute(a);
    return v ? v.value() : "";
  };
  const std::string database = get("Database");
  const std::string schema = get("Schema");
  const std::string table = get("Table");
  const std::string column = get("Column");
  return database + "." + schema + "." + table + "." + column;
}

const std::regex bloom_regex(R"(PROBE\(\[*Opt_Bitmap[0-9]+\]*,(.*)\))");

std::string cleanFilter(std::string filter) {
  size_t old_size = 0;
  do {
    old_size = filter.size();
    filter = std::regex_replace(filter, bloom_regex, "BLOOM($1)");
  } while (old_size != filter.size());
  return filter;
}

static std::vector<std::pair<std::string, std::string>>
extractLoopJoinPairs(const xml_node& inner_rel_op, const std::unordered_set<std::string>& outer_refs) {
  std::vector<std::pair<std::string, std::string>> pairs; // (inner_col, outer_col)

  // Find all "IndexSeek/SeekPredicates" (or Key/Clustered) under the inner subtree.
  // We rely on structure: Prefix/RangeColumns (inner) aligned with Prefix/RangeExpressions (outer).
  // Depending on version and node types in the tree, we may have to look several places
  auto seek_predicates = inner_rel_op.select_nodes(".//*[local-name()='SeekPredicates']");

  if (seek_predicates.empty()) {
    seek_predicates = inner_rel_op.select_nodes(".//*[local-name()='SeekPredicateNew']");
  }
  if (seek_predicates.empty()) {
    seek_predicates = inner_rel_op.select_nodes(".//*[local-name()='Predicate' ]");
  }

  for (auto sp : seek_predicates) {
    xml_node spNode = sp.node();

    // Collect inner columns in order
    std::vector<xml_node> innerCols;
    for (auto n : spNode.select_nodes(".//*[local-name()='RangeColumns']/*[local-name()='ColumnReference']"))
      innerCols.push_back(n.node());

    // Collect outer expressions (identifiers) in order
    std::vector<xml_node> outerCols;
    for (auto n : spNode.select_nodes(
             ".//*[local-name()='RangeExpressions']//*[local-name()='Identifier']/*[local-name()='ColumnReference']"))
      outerCols.push_back(n.node());

    // Zip them by position; typical plans align these 1:1 for equi-join prefixes
    size_t m = std::min(innerCols.size(), outerCols.size());
    for (size_t i = 0; i < m; ++i) {
      std::string innerQN = qname(innerCols[i]);
      std::string outerQN = qname(outerCols[i]);
      // Only record if the “outer” side actually matches one of NL's OuterReferences
      if (outer_refs.contains(outerQN)) {
        pairs.emplace_back(innerQN, outerQN);
      }
    }
  }

  return pairs;
}

bool isScanOp(std::string physical_op) {
  std::set<std::string> scan_ops = {"Clustered Index Scan", "Clustered Index Seek", "Table Scan"};
  return scan_ops.contains(physical_op);
}

std::pair<std::unique_ptr<Node>, std::vector<xml_node>> createNodeFromXML(const xml_node& node_xml,
                                                                          std::string pushed_filter = "") {
  const auto physical_op = std::string(node_xml.attribute("PhysicalOp").as_string());
  const auto logical_op = std::string(node_xml.attribute("LogicalOp").as_string());

  const std::map<std::string, Join::Type> join_map = {{"Inner Join", Join::Type::INNER},
                                                      {"Right Semi Join", Join::Type::RIGHT_SEMI_INNER}};

  std::unique_ptr<Node> node;
  std::vector<xml_node> children;
  if (logical_op == "Filter") {
    const auto filter = node_xml.child("Filter");
    const auto child = filter.child("RelOp");
    const auto child_op = std::string(child.attribute("PhysicalOp").as_string());
    const auto predicate = filter.child("Predicate");
    std::string condition;
    if (predicate) {
      const auto scalar_op = predicate.child("ScalarOperator");
      if (scalar_op) {
        condition = scalar_op.attribute("ScalarString").as_string();
      }
    }
    if (isScanOp(child_op)) {
      // We are calculating bloom filters here. Get rid of those
      condition = std::regex_replace(condition, bloom_regex, "");
      return createNodeFromXML(child, condition);
    }
    condition = cleanFilter(condition);
    children.push_back(filter.child("RelOp"));
    node = std::make_unique<Selection>(condition);
  } else if (logical_op == "Sort") {
    std::vector<Column> columns_sorted;
    auto sort = node_xml.child("Sort");
    for (auto column : sort.child("OrderBy")) {
      const Column::Sorting sorting = column.attribute("Ascending").as_bool()
                                        ? Column::Sorting::ASC
                                        : Column::Sorting::DESC;
      const auto name = std::string(column.child("ColumnReference").attribute("Column").as_string());
      columns_sorted.push_back(Column(name, sorting));
      children.push_back(sort.child("RelOp"));
    }
    node = std::make_unique<Sort>(columns_sorted);
  } else if (logical_op == "Aggregate" && physical_op == "Hash Match") {
    std::vector<Column> columns_grouped;
    std::vector<Column> columns_aggregated;

    const auto hash_child = node_xml.child("Hash");
    for (auto defined_value : hash_child.child("DefinedValues")) {
      auto name = definedValueExpression(defined_value);
      columns_aggregated.push_back(Column(name));
    }
    for (auto hash_key_build : hash_child.child("HashKeyBuild")) {
      auto column_ref = hash_key_build.child("ColumnReference");
      const auto name = std::string(column_ref.attribute("Column").as_string());
      columns_grouped.push_back(Column(name));
    }
    node = std::make_unique<GroupBy>(GroupBy::Strategy::HASH, columns_grouped, columns_aggregated);
    children.push_back(hash_child.child("RelOp"));
  } else if (logical_op == "Aggregate" && physical_op == "Stream Aggregate") {
    std::vector<Column> columns_grouped;
    std::vector<Column> columns_aggregated;

    const auto stream_aggregate_child = node_xml.child("StreamAggregate");
    for (auto defined_value : stream_aggregate_child.child("DefinedValues")) {
      auto name = definedValueExpression(defined_value);
      columns_aggregated.push_back(Column(name));
    }
    for (auto hash_key_build : stream_aggregate_child.child("GroupBy")) {
      auto column_ref = hash_key_build.child("ColumnReference");
      const auto name = std::string(column_ref.attribute("Column").as_string());
      columns_grouped.push_back(Column(name));
    }
    node = std::make_unique<GroupBy>(GroupBy::Strategy::HASH, columns_grouped, columns_aggregated);
    children.push_back(stream_aggregate_child.child("RelOp"));
  } else if (logical_op == "Inner Join" && physical_op == "Merge Join") {
    const auto merge = node_xml.child("Merge");
    const auto residual = merge.child("Residual");
    const auto scalar_operator = residual.child("ScalarOperator");
    for (auto child : merge.children("RelOp")) {
      children.push_back(child);
    }

    std::string condition = scalar_operator.attribute("ScalarString").as_string();
    node = std::make_unique<Join>(Join::Type::INNER, Join::Strategy::MERGE, condition);
  } else if (physical_op == "Hash Match") {
    const auto hash = node_xml.child("Hash");
    const auto probe = hash.child("ProbeResidual");
    const auto scalar_operator = probe.child("ScalarOperator");
    for (auto child : hash.children("RelOp")) {
      children.push_back(child);
    }
    std::string condition = scalar_operator.attribute("ScalarString").as_string();

    node = std::make_unique<Join>(Join::Type::INNER, Join::Strategy::HASH, condition);
  } else if (logical_op == "Inner Join" && physical_op == "Nested Loops") {
    std::string condition;
    const auto nested_loops = node_xml.child("NestedLoops");
    for (auto child : nested_loops.children("RelOp")) {
      children.push_back(child);
    }
    std::unordered_set<std::string> columns_outer;
    for (auto outer_ref : nested_loops.child("OuterReferences")) {
      columns_outer.insert(qname(outer_ref));
    }
    auto inner_rel_op = children.back();
    auto pairs = extractLoopJoinPairs(inner_rel_op, columns_outer);
    for (const auto& [inner, outer] : pairs) {
      if (!condition.empty()) {
        condition += " AND ";
      }
      condition += outer + " = " + inner;
    }
    std::ranges::reverse(children); // Loop joins are the wrong way around in SQL Server plans
    node = std::make_unique<Join>(Join::Type::INNER, Join::Strategy::LOOP, condition);
  } else if (isScanOp(physical_op)) {
    const auto output_columns = node_xml.child("OutputList");
    std::string table_name;
    for (auto column : output_columns.children()) {
      table_name = std::string(column.attribute("Table").as_string());
      break;
    }
    const auto index_scan = node_xml.child("IndexScan");
    if (table_name.empty()) {
      if (index_scan) {
        const auto object = index_scan.child("Object");
        if (object) {
          table_name = object.attribute("Table").as_string();
        }
      }
    }

    if (table_name.empty()) {
      const auto output_list = node_xml.child("OutputList");
      for (auto column : output_list.children()) {
        table_name = std::string(column.attribute("Table").as_string());
        break;
      }
    }

    if (table_name.empty()) {
      throw ExplainException("Table name not found for node: " + physical_op);
    }

    node = std::make_unique<Scan>(table_name);
    std::string filter;
    if (index_scan) {
      const auto predicate = index_scan.child("Predicate");
      filter = predicate.child("ScalarOperator").attribute("ScalarString").as_string();
    }

    if (!pushed_filter.empty()) {
      if (filter.empty()) {
        filter = pushed_filter;
      } else {
        filter = "(" + filter + ") AND (" + pushed_filter + ")";
      }
    }
    filter = cleanFilter(filter);
    node->setFilter(filter);
  } else if (logical_op == "Compute Scalar") {
    const auto compute_scalar = node_xml.child("ComputeScalar");
    std::vector<Column> columns_computed;

    for (auto defined_value : compute_scalar.child("DefinedValues")) {
      auto name = definedValueExpression(defined_value);
      columns_computed.push_back(Column(name));
    }

    node = std::make_unique<Projection>(columns_computed);
    children.push_back(compute_scalar.child("RelOp"));
  } else {
    throw ExplainException("Unknown node type: " + logical_op + " (" + physical_op + ")");
  }

  parseRowCount(node.get(), node_xml);

  return std::pair{std::move(node), children};
}

std::unique_ptr<Node> buildExplainNode(const xml_node& node_xml) {
  auto [node, children] = createNodeFromXML(node_xml);
  for (auto child : children) {
    node->addChild(buildExplainNode(child));
  }
  return std::move(node);
}


std::unique_ptr<Plan> buildExplainPlan(const std::string& explain_output) {
  xml_document doc;
  const auto result = doc.load_buffer(explain_output.c_str(), explain_output.size());
  if (!result) {
    throw ExplainException("Parsing of XML from SQL Server failed on error: " + std::string(result.description()));
  }

  /* High lever stats about the query*/

  /* Find the actual statement we want to explain. A single roundtrip may contain multiple statements */
  const auto statements = doc.child("ShowPlanXML").child("BatchSequence").child("Batch").child("Statements");
  xml_node statement_node;
  for (auto statement : statements.children()) {
    const auto statement_type = std::string(statement.attribute("StatementType").as_string());
    if (statement_type == "SELECT") {
      statement_node = statement;
      break;
    }
  }
  const auto plan_node = statement_node.child("QueryPlan");

  const auto query_stats = plan_node.child("QueryTimeStats");
  const double execution_time = query_stats.attribute("ElapsedTime").as_double();
  const auto first_operator = plan_node.child("RelOp");

  std::unique_ptr<Node> root_node = buildExplainNode(first_operator);
  fixProjection(root_node.get());

  auto plan = std::make_unique<Plan>(std::move(root_node));
  plan->planning_time = 0;
  plan->execution_time = execution_time;

  return plan;
}


std::unique_ptr<explain::Plan> Connection::explain(const std::string_view statement) {
  /* NOTE: We have to do multiple roundtrips here due to a limitation in SQL Servers handling of batches wiuth SET statements*/
  execute("SET STATISTICS XML ON");
  const auto result = fetchAll(statement);
  result->drain();
  const auto plan = result->nextResult();
  if (!plan) {
    throw ExplainException("No execution plan returned from SQL Server");
  }
  std::string explain_string;
  for (auto& row : plan->rows()) {
    explain_string = row.asString(0);
    break;
  }
  execute("SET STATISTICS XML OFF");
  return buildExplainPlan(explain_string);
}
}