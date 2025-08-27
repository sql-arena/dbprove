#include "connection.h"
#include <dbprove/sql/sql.h>
#include <memory>
#include <cassert>
#include <join.h>
#include <regex>
#include <pugixml.hpp>
#include <scan.h>
#include <set>
#include <sort.h>

#include "group_by.h"

namespace sql::explain {
class GroupBy;
}

namespace sql::yellowbrick {
Connection::Connection(const CredentialPassword& credential, const Engine& engine)
  : postgres::Connection(credential, engine) {
}

std::string Connection::version() {
  const auto versionString = fetchScalar("SELECT version()").get<SqlString>().get();
  const std::regex versionRegex(R"(\d+\.\d+\.\d+)");
  std::smatch match;
  if (std::regex_search(versionString, match, versionRegex)) {
    return match[1];
  }
  return "Unknown";
}

std::string Connection::translateDialectDdl(const std::string_view ddl) const {
  const std::string pg = postgres::Connection::translateDialectDdl(ddl);
  const std::regex re(";");
  auto r = std::regex_replace(pg, re, " DISTRIBUTE REPLICATE;");
  return r;
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
    const auto condition_xml = std::string(xml_node.attribute("criteria").as_string());

    auto strategy = (strategy_xml == "hash") ? Join::Strategy::HASH : Join::Strategy::LOOP;
    static const std::map<std::string_view, Join::Type> xml2type = {
        {"inner", Join::Type::INNER},
        {"left", Join::Type::LEFT},
        {"right", Join::Type::RIGHT},
        {"full", Join::Type::FULL}
    };
    if (!xml2type.contains(type_xml)) {
      throw ExplainException("Unrecognised JOIN type in Yellowbrick explain: " + std::string(type_xml));
    }
    auto type = xml2type.at(type_xml);
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

  // Create and return the plan object with timing information
  auto plan = std::make_unique<Plan>(std::move(root_node));
  plan->execution_time = execution_time;
  return plan;
}

std::unique_ptr<Plan> Connection::explain(const std::string_view statement) {
  const std::string explain_modded = "EXPLAIN (ANALYSE, VERBOSE, FORMAT YBXML)\n" + std::string(statement);
  const auto result = fetchScalar(explain_modded);
  assert(result.is<SqlString>());

  const auto explain_string = result.get<SqlString>().get();

  return buildExplainPlan(explain_string);
}
}