#include "connection.h"

#include <explain/limit.h>

#include "result.h"
#include <nlohmann/json.hpp>
#include "credential.h"
#include <duckdb.hpp>
#include <memory>
#include <stdexcept>

#include "sql_exceptions.h"
#include "explain/group_by.h"
#include "explain/join.h"
#include "explain/projection.h"
#include "explain/scan.h"
#include "explain/scan_empty.h"
#include "explain/selection.h"
#include "explain/sort.h"
#include "explain/union.h"


namespace sql::duckdb {
void handleDuckError(::duckdb::QueryResult* result) {
  if (result->HasError()) {
    throw std::runtime_error("DuckDB query execution failed: " + result->GetError());
  }
}

class Connection::Pimpl {
public:
  Connection& connection;
  const CredentialFile credential;
  std::unique_ptr<::duckdb::DuckDB> db;
  std::unique_ptr<::duckdb::Connection> db_connection;

  explicit Pimpl(Connection& connection, const CredentialFile& credential)
    : connection(connection)
    , credential(credential) {
    try {
      // Open database connection using the file path from credential
      db = std::make_unique<::duckdb::DuckDB>(credential.path);
      db_connection = std::make_unique<::duckdb::Connection>(*db);
    } catch (const ::duckdb::IOException& e) {
      throw ConnectionException(credential, std::string(e.what()));
    }
    catch (const ::duckdb::Exception& e) {
      throw std::runtime_error("Failed to connect to DuckDB: " + std::string(e.what()));
    }
  }

  std::unique_ptr<::duckdb::QueryResult> execute(const std::string_view statement) const {
    try {
      const auto mapped_statement = connection.mapTypes(statement);
      auto result = db_connection->Query(std::string(mapped_statement));

      handleDuckError(result.get());
      return result;
    } catch (const ::duckdb::ConnectionException& e) {
      throw ConnectionException(credential, std::string(e.what()));
    } catch (const ::duckdb::CatalogException& e) {
      throw std::runtime_error("DuckDB catalog error (table might not exist): " + std::string(e.what()));
    } catch (const ::duckdb::ParserException& e) {
      throw SyntaxException("DuckDB SQL parsing error: " + std::string(e.what()));
    } catch (const ::duckdb::BinderException& e) {
      throw std::runtime_error("DuckDB binding error (column mismatch): " + std::string(e.what()));
    } catch (const ::duckdb::IOException& e) {
      throw std::runtime_error("DuckDB I/O error accessing file: " + std::string(e.what()));
    } catch (const ::duckdb::TransactionException& e) {
      throw TransactionException("DuckDB transaction error: " + std::string(e.what()));
    }
  }
};

Connection::Connection(const CredentialFile& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>(*this, credential)) {
}

const ConnectionBase::TypeMap& Connection::typeMap() const {
  // Duck has great type aliasing, so no need to map
  static const TypeMap map = {};
  return map;
}

Connection::~Connection() {
}

void Connection::execute(const std::string_view statement) {
  auto result = impl_->execute(statement);
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  auto result = impl_->execute(statement);
  return std::make_unique<Result>(std::move(result));
}

std::unique_ptr<ResultBase> Connection::fetchMany(const std::string_view statement) {
  return fetchAll(statement);
}

std::unique_ptr<RowBase> Connection::fetchRow(const std::string_view statement) {
  auto result = new Result(impl_->execute(statement));
  const auto row_count = result->rowCount();
  if (row_count == 0) {
    throw EmptyResultException(statement);
  }
  if (row_count > 1) {
    throw InvalidRowsException("Expected to find a single row in the data, but found: " + std::to_string(row_count),
                               statement);
  }
  auto& firstRow = result->nextRow();
  auto row = std::make_unique<Row>(static_cast<const Row&>(firstRow), result);
  return row;
}

SqlVariant Connection::fetchScalar(const std::string_view statement) {
  const auto row = fetchRow(statement);
  if (row->columnCount() != 1) {
    throw InvalidColumnsException("Expected to find a single column in the data", statement);
  }
  return row->asVariant(0);
}

void Connection::bulkLoad(const std::string_view table, std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);

  for (const auto& path : source_paths) {
    std::string copy_statement = "COPY " + std::string(table) +
                                 " FROM '" + path.string() +
                                 "' WITH (FORMAT 'csv', AUTO_DETECT true, HEADER true)";
    auto result = impl_->execute(copy_statement);
  }
}


using namespace sql::explain;
using namespace nlohmann;

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
    if (extra_info["Order By"].is_array()) {
      for (auto& sort_key : extra_info["Order By"]) {
        auto [column, sort_order] = parseSortString(sort_key.get<std::string>());
        sort_keys.push_back(Column(column, sort_order));
      }
    } else {
      const auto sort_key = extra_info["Order By"].get<std::string>();
      auto [column, sort_order] = parseSortString(sort_key);
      sort_keys.push_back(Column(column, sort_order));
    }
  }
  return sort_keys;
}

std::unique_ptr<Node> createNodeFromJson(json& node_json) {
  const auto operator_name = node_json["operator_name"].get<std::string>();

  const auto& extra_info = node_json["extra_info"];
  std::unique_ptr<Node> node;
  if (operator_name.contains("SEQ_SCAN")) {
    // NOTE: Duckdb has an extra space in the SEQ_SCAN
    const auto table_name = node_json["extra_info"]["Table"].get<std::string>();
    node = std::make_unique<Scan>(table_name);
  }
  if (operator_name.contains("PROJECTION")) {
    std::vector<Column> projection_columns;
    for (auto& column : extra_info["Projections"]) {
      const std::string column_name = column.get<std::string>();
      projection_columns.push_back(Column(column_name));
    }
    node = std::make_unique<Projection>(projection_columns);
  }
  if (operator_name.contains("HASH_JOIN")) {
    auto join_condition = extra_info["Conditions"].get<std::string>();

    auto join_type = Join::Type::CROSS;

    const auto join_type_str = extra_info["Join Type"].get<std::string>();

    if (join_type_str.contains("INNER")) {
      join_type = Join::Type::INNER;
    } else if (join_type_str.contains("LEFT")) {
      join_type = Join::Type::LEFT;
    } else if (join_type_str.contains("RIGHT")) {
      join_type = Join::Type::RIGHT;
    } else if (join_type_str.contains("FULL")) {
      join_type = Join::Type::FULL;
    }
    node = std::make_unique<Join>(join_type, Join::Strategy::HASH, join_condition);
  }
  if (operator_name == "TOP_N") {
    // DuckDB uses a special sort + limit node when asking for Top N
    std::unique_ptr<Sort> sortNode = nullptr;
    auto sort_keys = parseOrderColumns(node_json);
    if (sort_keys.size() > 0) {
      sortNode = std::make_unique<Sort>(sort_keys);
    }

    auto limit_string = extra_info["Top"].get<std::string>();
    auto limit = std::atol(limit_string.c_str());
    auto limitNode = std::make_unique<Limit>(limit);
    // For some reason, Duck does not estimate a cardinality for this type
    limitNode->rows_estimated = limit;

    if (sortNode) {
      sortNode->addChild(std::move(limitNode));
      node = std::move(sortNode);
    } else {
      node = std::move(limitNode);
    }
  }
  if (operator_name == "ORDER_BY") {
    auto sort_keys = parseOrderColumns(node_json);
    node = std::make_unique<Sort>(sort_keys);
  }
  if (operator_name == "FILTER") {
    auto filter_condition = extra_info["Expression"].get<std::string>();
    node = std::make_unique<Selection>(filter_condition);
  }
  if (operator_name == "HASH_GROUP_BY" || operator_name == "PERFECT_HASH_GROUP_BY") {
    std::vector<Column> group_by_columns;
    if (extra_info.contains("Groups")) {
      for (auto& column : extra_info["Groups"]) {
        group_by_columns.push_back(Column(column.get<std::string>()));
      }
    }
    std::vector<Column> aggregate_columns;
    if (extra_info.contains("Aggregates")) {
      for (auto& column : extra_info["Aggregates"]) {
        aggregate_columns.push_back(Column(column.get<std::string>()));
      }
    }
    node = std::make_unique<GroupBy>(GroupBy::Strategy::HASH, group_by_columns, aggregate_columns);
  }
  if (operator_name == "UNION") {
    node = std::make_unique<Union>(Union::Type::ALL);
  }
  if (operator_name == "EMPTY_RESULT") {
    node = std::make_unique<ScanEmpty>();
  }
  if (operator_name == "EXPLAIN_ANALYZE") {
    // Duck adds this fake node when explaining queries
    return nullptr;
  }

  if (node == nullptr) {
    throw std::runtime_error("Unknown operator type: " + operator_name);
  }

  if (extra_info.contains("Estimated Cardinality")) {
    auto cardinality_string = extra_info["Estimated Cardinality"].get<std::string>();
    node->rows_estimated = std::atof(cardinality_string.c_str());
  }
  node->rows_actual = node_json["operator_cardinality"].get<double>();
  return node;
}


std::unique_ptr<Node> buildExplainNode(json& node_json) {
  // Determine the node type from "Node Type" field
  if (!node_json.contains("operator_name")) {
    throw std::runtime_error("Missing 'Node Type' in EXPLAIN node");
  }
  auto node = createNodeFromJson(node_json);
  while (node == nullptr) {
    if (!node_json.contains("children")) {
      throw std::runtime_error(
          "EXPLAIN tries to skip past a node that has no children. You must handle all leaf nodes");
    }
    if (node_json["Plans"].size() > 1) {
      throw std::runtime_error(
          "EXPLAIN parsing Tried to skip past a node with >1 children. You have to deal with this case");
    }
    node_json = node_json["children"][0];
    const auto operator_name = node_json["operator_name"].get<std::string>();
    node = createNodeFromJson(node_json);
  }

  if (node_json.contains("children")) {
    for (auto& child_json : node_json["children"]) {
      auto child_node = buildExplainNode(child_json);
      node->addChild(std::move(child_node));
    }
  }

  return node;
}

/**
 * In DuckDB, joins are the "wrong way around"
 *  By convention, the build side is the FIRST child, but duck makes it
 *  the second.
 *
 *  Correct for this
 */
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

std::unique_ptr<Plan> buildExplainPlan(json& json) {
  if (!json.contains("children")) {
    throw std::runtime_error("Invalid EXPLAIN format. Expected to find a children node");
  }

  /* High lever stats about the query */
  double planning_time = 0.0;
  double execution_time = 0.0;

  if (json.contains("planner")) {
    planning_time = json["planner"].get<double>();
  }
  if (json.contains("cpu_time")) {
    execution_time = json["cpu_time"].get<double>();
  }

  auto& plan_json = json["children"];

  /* Construct the tree*/
  auto root_node = buildExplainNode(plan_json[0]);

  if (!root_node) {
    throw std::runtime_error("Invalid EXPLAIN plan output, could not construct a plan");
  }

  // Create and return the plan object with timing information
  auto plan = std::make_unique<Plan>(std::move(root_node));
  plan->planning_time = planning_time;
  plan->execution_time = execution_time;

  flipJoins(plan->planTree());
  return plan;
}

std::unique_ptr<Plan> Connection::explain(const std::string_view statement) {
  const std::string explain_query = "PRAGMA enable_profiling = 'json';\n"
                                    "PRAGMA profiling_mode = 'detailed';\n"
                                    "EXPLAIN (ANALYSE, FORMAT JSON)\n" + std::string(statement);
  const auto result = fetchRow(explain_query);

  std::string explain_raw = result->asString(1);
  auto explain_json = json::parse(explain_raw);

  return buildExplainPlan(explain_json);
}
}