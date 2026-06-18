#include "connection.h"
#include <dbprove/sql/sql.h>
#include <memory>
#include <cassert>
#include <regex>
#include <pugixml.hpp>
#include <selection.h>
#include <set>
#include "join.h"
#include "scan.h"
#include "sort.h"
#include "group_by.h"
#include "sequence.h"
#include "include/dbprove/sql/parsed_table.h"
#include "sql_exceptions.h"
#include "explain/plan.h"
#include <sstream>

namespace sql::explain {
class GroupBy;
}

namespace sql::yellowbrick {
namespace {
std::string defaultTypeName(const SqlTypeKind kind) {
  switch (kind) {
    case SqlTypeKind::SMALLINT:
      return "SMALLINT";
    case SqlTypeKind::INT:
      return "INT";
    case SqlTypeKind::BIGINT:
      return "BIGINT";
    case SqlTypeKind::REAL:
      return "REAL";
    case SqlTypeKind::DOUBLE:
      return "DOUBLE";
    case SqlTypeKind::DECIMAL:
      return "DECIMAL";
    case SqlTypeKind::STRING:
      return "TEXT";
    case SqlTypeKind::DATE:
      return "DATE";
    case SqlTypeKind::TIME:
      return "TIME";
    case SqlTypeKind::DATETIME:
      return "DATETIME";
    case SqlTypeKind::SQL_NULL:
      return "NULL";
    case SqlTypeKind::UNKNOWN:
      return "UNKNOWN";
  }
  return "UNKNOWN";
}

std::string renderType(const SqlTypeMeta& type, const ConnectionBase::TypeMap& type_map) {
  std::string rendered = type_map.contains(type.kind)
                       ? std::string(type_map.at(type.kind))
                       : defaultTypeName(type.kind);

  if (type.kind == SqlTypeKind::STRING && std::holds_alternative<SqlTypeModifier::String>(type.modifier.value)) {
    const auto length = std::get<SqlTypeModifier::String>(type.modifier.value).length;
    return rendered + "(" + std::to_string(length) + ")";
  }

  if (type.kind == SqlTypeKind::DECIMAL && std::holds_alternative<SqlTypeModifier::Decimal>(type.modifier.value)) {
    const auto decimal = std::get<SqlTypeModifier::Decimal>(type.modifier.value);
    return rendered + "(" + std::to_string(decimal.precision) + ", " + std::to_string(decimal.scale) + ")";
  }

  return rendered;
}
}

Connection::Connection(const CredentialPassword& credential, const Engine& engine, std::optional<std::string> artifacts_path)
  : postgresql::Connection(credential, engine, std::move(artifacts_path)) {
}

std::string Connection::version() {
  const auto version_string = fetchScalar("SELECT version()").get<SqlString>().get();
  const std::regex version_regex(R"(\d+\.\d+\.\d+)");
  std::smatch match;
  if (std::regex_search(version_string, match, version_regex)) {
    return match[1];
  }
  return "Unknown";
}

void Connection::constructTable(const std::string_view ddl,
                                const std::span<const std::filesystem::path> source_stems,
                                const dbprove::StorageVariant storage_variant) {
  static_cast<void>(storage_variant);

  if (source_stems.empty()) {
    throw std::runtime_error("constructTable requires at least one staged source file stem");
  }

  const auto parsed = ParsedTable(ddl);
  const auto& table = parsed.tableName();
  std::ostringstream out;
  out << "CREATE TABLE " << table << "\n(\n";
  for (size_t i = 0; i < parsed.columns().size(); ++i) {
    const auto& column = parsed.columns()[i];
    out << "    " << column.name << " " << renderType(column.type, typeMap());
    out << (column.is_null ? " NULL" : " NOT NULL");
    if (i + 1 < parsed.columns().size()) {
      out << ",";
    }
    out << "\n";
  }
  out << ")\nDISTRIBUTE REPLICATE;";
  const auto create_table = out.str();
  execute(create_table);

  std::vector<std::filesystem::path> csv_paths;
  csv_paths.reserve(source_stems.size());
  for (const auto& stem : source_stems) {
    auto csv_path = stem;
    csv_path += ".csv";
    csv_paths.push_back(std::move(csv_path));
  }
  bulkLoad(table, csv_paths);
}

using namespace pugi;
using namespace explain;


std::unique_ptr<Node> createNodeFromYbXml(const xml_node& xml_node) {
  const auto yb_node_type = std::string(xml_node.name());

  static const std::set<std::string> ignore_nodes = {"SELECT",
                                                     "warnings",
                                                     "BUILD",
                                                     "columns",
                                                     "column",
                                                     "DISTRIBUTE",
                                                     "WRITE_TEMP",
                                                     "WRITE_HASH",
                                                     "EXPRESSION",
                                                     "partition_stats"};

  if (ignore_nodes.contains(yb_node_type)) {
    return nullptr;
  }

  std::unique_ptr<Node> node;

  if (yb_node_type == "SORT") {
    std::vector<Column> columns_sorted = {};
    node = std::make_unique<Sort>(columns_sorted);
  } else if (yb_node_type == "JOIN") {
    const auto strategy_xml = std::string_view(xml_node.attribute("strategy").as_string());
    const auto type_xml = std::string_view(xml_node.attribute("type").as_string());
    auto condition_xml = std::string(xml_node.attribute("criteria").as_string());
    /* Yellowbrick puts the string "ON " in front of join criteria.*/
    condition_xml = sql::cleanExpression(condition_xml.substr(3, condition_xml.size() - 3));

    auto strategy = (strategy_xml == "hash") ? Join::Strategy::HASH : Join::Strategy::LOOP;
    auto type = Join::typeFromString(type_xml);
    node = std::make_unique<Join>(type, strategy, condition_xml);
  } else if (yb_node_type == "GROUP_BY") {
    // TODO: Handle the case of simple strategy
    // TODO: Fish out the grouping and aggregate keys
    std::vector<Column> columns_keys = {};
    std::vector<Column> aggregations = {};
    node = std::make_unique<GroupBy>(GroupBy::Strategy::HASH, columns_keys, aggregations);
  } else if (yb_node_type == "SCAN") {
    const auto table_name = xml_node.attribute("table_name").as_string();

    node = std::make_unique<Scan>(table_name);

    const auto filter = xml_node.attribute("filter");
    if (!filter.empty()) {
      node->setFilter(filter.as_string());
    }
  } else if (yb_node_type == "SEQUENCE") {
    node = std::make_unique<Sequence>();
  } else if (yb_node_type == "FILTER") {
    const auto filter = std::string(xml_node.attribute("criteria").as_string());
    node = std::make_unique<Selection>(filter);
  }

  if (node == nullptr) {
    throw ExplainException("Could not parse the node of type : " + std::string(yb_node_type));
  }
  node->rows_actual = xml_node.attribute("actual").as_double();
  node->rows_estimated = xml_node.attribute("estimate").as_double();

  return node;
}

std::unique_ptr<Node> buildExplainNode(xml_node& node_xml) {
  const std::string yb_node_type = node_xml.name();

  /* The YBXML contains special meta nodes that hold metadata about their parent node, these are handled by
   * the individual explain nodes.
   * We can ignore them from this visitor function.
   */
  static const std::set<std::string> meta_nodes = {"columns",
                                                   "column",
                                                   "warnings",
                                                   "warning",
                                                   "partition_stats"
  };
  std::unique_ptr<Node> node = createNodeFromYbXml(node_xml);
  while (node == nullptr && !node_xml.children().empty()) {
    xml_node last_child;
    for (auto child : node_xml.children()) {
      const std::string child_name = child.name();
      if (meta_nodes.contains(child_name)) {
        continue;
      }
      node = createNodeFromYbXml(child);
      last_child = child;
    }
    node_xml = last_child;
  };

  for (auto child_xml : node_xml.children()) {
    auto child_node = buildExplainNode(child_xml);
    if (child_node) {
      node->addChild(std::move(child_node));
    }
  }

  return node;
}

void flipJoins(Node& root) {
  std::vector<Node*> join_nodes;
  for (auto& node : root.depth_first()) {
    if (node.type == NodeType::JOIN) {
      join_nodes.push_back(&node);
    }
  }

  for (const auto node : join_nodes) {
    node->reverseChildren();
  }
}


std::unique_ptr<Plan> buildExplainPlan(const std::string& explain_output) {
  xml_document doc;
  const auto result = doc.load_buffer(explain_output.c_str(), explain_output.size());
  if (!result) {
    throw ExplainException(
        "Failed to parse explain XML from Yellowbrick with error: " + std::string(result.description()));
  }

  const auto root = doc.child("PLAN");
  const double execution_time = root.attribute("time_ms").as_double();
  xml_node plan_xml;
  for (auto root_child = root.begin(); root_child != root.end(); ++root_child) {
    if (std::next(root_child) == root.end()) {
      // The last node is the executing plan.
      plan_xml = *root_child;
    }
  }
  auto root_node = buildExplainNode(plan_xml);

  if (!root_node) {
    throw ExplainException("Invalid EXPLAIN plan output, could not construct a plan from query");
  }
  flipJoins(*root_node);
  // Create and return the plan object with timing information
  auto plan = std::make_unique<Plan>(std::move(root_node));
  plan->execution_time = execution_time;
  return plan;
}

std::unique_ptr<Plan> Connection::explain(const std::string_view statement, std::optional<std::string_view> name) {
  const std::string explain_modded = "EXPLAIN (ANALYSE, VERBOSE, FORMAT YBXML)\n" + std::string(statement);
  const auto result = fetchScalar(explain_modded);
  assert(result.is<SqlString>());

  const auto explain_string = result.get<SqlString>().get();

  return buildExplainPlan(explain_string);
}

void Connection::analyse(const std::string_view table_name) {
  execute("ANALYSE " + std::string(table_name) + ";\nYFLUSH " + std::string(table_name));
}
}
