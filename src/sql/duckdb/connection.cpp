#include "connection.h"
#include "result.h"
#include <dbprove/sql/sql.h>
#include <explain_nodes.h>

#include <nlohmann/json.hpp>
#include <duckdb.hpp>
#include <memory>
#include <regex>
#include <scan_materialised.h>
#include <stdexcept>

#include "explain/plan.h"


namespace sql::duckdb {
void handleDuckError(::duckdb::QueryResult* result) {
  if (!result->HasError()) {
    return;
  }

  switch (result->GetErrorType()) {
    case ::duckdb::ExceptionType::INVALID:
      break;
    case ::duckdb::ExceptionType::OUT_OF_RANGE:
      break;
    case ::duckdb::ExceptionType::CONVERSION:
      break;
    case ::duckdb::ExceptionType::UNKNOWN_TYPE:
      break;
    case ::duckdb::ExceptionType::DECIMAL:
      break;
    case ::duckdb::ExceptionType::MISMATCH_TYPE:
      break;
    case ::duckdb::ExceptionType::DIVIDE_BY_ZERO:
      break;
    case ::duckdb::ExceptionType::OBJECT_SIZE:
      break;
    case ::duckdb::ExceptionType::INVALID_TYPE:
      break;
    case ::duckdb::ExceptionType::SERIALIZATION:
      break;
    case ::duckdb::ExceptionType::TRANSACTION:
      break;
    case ::duckdb::ExceptionType::NOT_IMPLEMENTED:
      break;
    case ::duckdb::ExceptionType::EXPRESSION:
      break;
    case ::duckdb::ExceptionType::CATALOG: {
      // TODO: we can do better here
      throw InvalidObjectException(result->GetError());
      break;
    }
    case ::duckdb::ExceptionType::PARSER:
      break;
    case ::duckdb::ExceptionType::PLANNER:
      break;
    case ::duckdb::ExceptionType::SCHEDULER:
      break;
    case ::duckdb::ExceptionType::EXECUTOR:
      break;
    case ::duckdb::ExceptionType::CONSTRAINT:
      break;
    case ::duckdb::ExceptionType::INDEX:
      break;
    case ::duckdb::ExceptionType::STAT:
      break;
    case ::duckdb::ExceptionType::CONNECTION:
      break;
    case ::duckdb::ExceptionType::SYNTAX:
      break;
    case ::duckdb::ExceptionType::SETTINGS:
      break;
    case ::duckdb::ExceptionType::BINDER:
      break;
    case ::duckdb::ExceptionType::NETWORK:
      break;
    case ::duckdb::ExceptionType::OPTIMIZER:
      break;
    case ::duckdb::ExceptionType::NULL_POINTER:
      break;
    case ::duckdb::ExceptionType::IO:
      break;
    case ::duckdb::ExceptionType::INTERRUPT:
      break;
    case ::duckdb::ExceptionType::FATAL:
      break;
    case ::duckdb::ExceptionType::INTERNAL:
      break;
    case ::duckdb::ExceptionType::INVALID_INPUT:
      break;
    case ::duckdb::ExceptionType::OUT_OF_MEMORY:
      break;
    case ::duckdb::ExceptionType::PERMISSION:
      break;
    case ::duckdb::ExceptionType::PARAMETER_NOT_RESOLVED:
      break;
    case ::duckdb::ExceptionType::PARAMETER_NOT_ALLOWED:
      break;
    case ::duckdb::ExceptionType::DEPENDENCY:
      break;
    case ::duckdb::ExceptionType::HTTP:
      break;
    case ::duckdb::ExceptionType::MISSING_EXTENSION:
      break;
    case ::duckdb::ExceptionType::AUTOLOAD:
      break;
    case ::duckdb::ExceptionType::SEQUENCE:
      break;
    case ::duckdb::ExceptionType::INVALID_CONFIGURATION:
      break;
  }

  throw std::runtime_error("DuckDB query execution failed: " + result->GetError());
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
    } catch (const ::duckdb::Exception& e) {
      throw std::runtime_error("Failed to connect to DuckDB: " + std::string(e.what()));
    }
  }

  void close() {
    db_connection = nullptr;
  }

  void check_connection() {
    if (!db_connection) {
      throw ConnectionClosedException(credential);
    }
  }

  [[nodiscard]] std::unique_ptr<::duckdb::QueryResult> execute(const std::string_view statement) {
    check_connection();
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
                                 " FROM '" + path.string() + "'"
                                 "\nWITH (FORMAT 'csv', "
                                 "DELIM '|', "
                                 "AUTO_DETECT true, "
                                 "HEADER true, "
                                 "STRICT_MODE false "
                                 ")";
    auto result = impl_->execute(copy_statement);
  }
}


using namespace sql::explain;
using namespace nlohmann;

std::string parseFilter(json filter) {
  if (filter.is_string()) {
    return filter.get<std::string>();
  }
  if (!filter.is_array()) {
    throw ExplainException("Cannot parse filter: " + filter.dump());
  }
  std::string result;
  for (size_t i = 0; i < filter.size(); ++i) {
    if (i > 0) {
      result += " AND ";
    }
    result += filter[i].get<std::string>();
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

struct ExplainContext {
  /**
   * Delimiters are expression that are used to materialise scans
   */
  std::map<size_t, std::string> delimiter_expressions;
  std::stack<Node*> propagate_estimate;
  std::stack<Node*> propagate_actual;
};

std::string extractConditions(const json& extra_info) {
  if (!extra_info["Conditions"].is_array()) {
    return extra_info["Conditions"].get<std::string>();
  }

  std::string result;
  for (size_t i = 0; i < extra_info["Conditions"].size(); ++i) {
    auto c = extra_info["Conditions"][i];
    result += c.get<std::string>();
    if (c < extra_info["Conditions"].size() - 1) {
      result += " AND ";
    }
  }
  return result;
}


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
    node = std::make_unique<ScanMaterialised>(materialised_expression);
  } else if (operator_name == "COLUMN_DATA_SCAN") {
    node = std::make_unique<ScanMaterialised>();
  } else if (operator_name.contains("PROJECTION")) {
    std::vector<Column> projection_columns;
    for (auto& column : extra_info["Projections"]) {
      const std::string column_name = column.get<std::string>();
      projection_columns.push_back(Column(column_name));
    }
    node = std::make_unique<Projection>(projection_columns);
  } else if (operator_name.contains("HASH_JOIN")
             || operator_name == "NESTED_LOOP_JOIN") {
    auto join_condition = extractConditions(extra_info);

    const auto join_type_str = extra_info["Join Type"].get<std::string>();

    auto join_type = Join::typeFromString(join_type_str);
    auto strategy = operator_name.contains("HASH_JOIN") ? Join::Strategy::HASH : Join::Strategy::LOOP;
    node = std::make_unique<Join>(join_type, strategy, join_condition);
  } else if (operator_name.contains("RIGHT_DELIM_JOIN")
             ||
             operator_name.contains("LEFT_DELIM_JOIN")) {
    const auto join_type_str = extra_info["Join Type"].get<std::string>();
    auto join_type = join_type_str.contains("RIGHT")
                       ? Join::Type::RIGHT_SEMI_INNER
                       : Join::Type::LEFT_SEMI_INNER;
    auto join_condition = extractConditions(extra_info);

    auto index = extra_info["Delim Index"].get<std::string>();
    auto delim_index = std::atol(index.c_str());
    ctx.delimiter_expressions[delim_index] = join_condition;
    node = std::make_unique<Join>(join_type, Join::Strategy::HASH, join_condition);
  } else if (operator_name == "TOP_N") {
    // DuckDB uses a special sort + limit node when asking for Top N
    std::unique_ptr<Sort> sortNode;
    auto sort_keys = parseOrderColumns(node_json);
    if (!sort_keys.empty()) {
      sortNode = std::make_unique<Sort>(sort_keys);
    }

    auto limit_string = extra_info["Top"].get<std::string>();
    auto limit = std::atol(limit_string.c_str());
    node = std::make_unique<Limit>(limit);
    // For some reason, Duck does not estimate a cardinality for this type
    node->rows_estimated = limit;
    if (sortNode) {
      sortNode->rows_actual = Node::UNKNOWN;
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
      for (auto& column : extra_info["Aggregates"]) {
        aggregate_columns.push_back(Column(column.get<std::string>()));
      }
    }
    node = std::make_unique<GroupBy>(GroupBy::Strategy::SIMPLE, std::vector<Column>({}), aggregate_columns);
  } else if (operator_name == "HASH_GROUP_BY" || operator_name == "PERFECT_HASH_GROUP_BY") {
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
    if (node->rows_actual == Node::UNKNOWN) {
      node->rows_actual = actual_rows;
    }
    while (!ctx.propagate_estimate.empty()) {
      ctx.propagate_estimate.top()->rows_estimated = estimate;
      ctx.propagate_estimate.pop();
    }
  } else if (node->rows_estimated == 0) {
    // HACK: Likely a bug in DuckDB. Some nodes do not have estimates, even though they clearly care about it
    ctx.propagate_estimate.push(node.get());
  }

  if (node->rows_actual != Node::UNKNOWN) {
    while (!ctx.propagate_actual.empty()) {
      ctx.propagate_actual.top()->rows_actual = actual_rows;
      ctx.propagate_actual.pop();
    }
  }

  return node;
}


std::unique_ptr<Node> buildExplainNode(json& node_json, ExplainContext& ctx) {
  // Determine the node type from the "Node Type" field
  if (!node_json.contains("operator_name")) {
    throw std::runtime_error("Missing 'Node Type' in EXPLAIN node");
  }
  auto node = createNodeFromJson(node_json, ctx);
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

  /* High level stats about the query */
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

std::string Connection::version() {
  const auto version_string = fetchScalar("SELECT version()").get<SqlString>().get();
  const std::regex versionRegex(R"(v(\d+\.\d+.\d+))");
  std::smatch match;
  if (std::regex_search(version_string, match, versionRegex)) {
    return match[1];
  }
  return "Unknown";
}

void Connection::close() {
  impl_->close();
}
}