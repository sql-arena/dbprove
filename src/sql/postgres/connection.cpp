#include "connection.h"
#include "row.h"
#include <libpq-fe.h>
#include "result.h"
#include <dbprove/sql/sql.h>
#include <explain_nodes.h>
#include <nlohmann/json.hpp>

#include <cassert>
#include <fstream>
#include <unordered_set>


class sql::postgres::Connection::Pimpl {
public:
  Connection& connection;
  const CredentialPassword credential;
  PGconn* conn;

  explicit Pimpl(Connection& connection, const CredentialPassword& credential)
    : connection(connection)
    , credential(credential) {
    std::string conninfo =
        "dbname=" + credential.database +
        " user=" + credential.username +
        " port=" + std::to_string(credential.port);
    if (credential.password.has_value()) {
      conninfo += " password=" + credential.password.value();
    }
    conn = PQconnectdb(conninfo.c_str());

    if (PQstatus(conn) != CONNECTION_OK) {
      std::string error = PQerrorMessage(conn);
      PQfinish(conn);
      throw ConnectionException(credential, error);
    }
  }

  /// @brief Handles return values from calls into postgres
  /// @note: If we throw here, we will call `PQClear`. But it is the responsibility of the caller to clear on success
  void check_return(PGresult* result, std::string_view statement) const {
    // ReSharper disable once CppTooWideScope
    const auto status = PQresultStatus(result);
    switch (status) {
      case PGRES_TUPLES_OK:
      case PGRES_COMMAND_OK:
      case PGRES_EMPTY_QUERY:
      case PGRES_SINGLE_TUPLE:
      case PGRES_COPY_IN: // Ready to receive data on COPY
        /* Legit and harmless status code */
        return;

      default:
        std::string error_msg = PQerrorMessage(this->conn);
        assert(error_msg.size() > 0);
        const char* sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
        std::string state;
        if (sqlstate == nullptr) {
          state = "UNKNOWN";
        } else {
          state = sqlstate;
        }
        PQclear(result);

        if (state.starts_with("42P01")) {
          throw InvalidTableException(error_msg);
        }
        if (state.starts_with("42")) {
          throw sql::SyntaxException(error_msg, statement);
        }
        throw sql::ConnectionException(credential, error_msg);
    }
  }

  PGresult* execute(const std::string_view statement) const {
    const auto mapped_statement = connection.mapTypes(statement);
    PGresult* result = PQexecParams(conn,
                                    mapped_statement.c_str(),
                                    0,
                                    nullptr,
                                    nullptr,
                                    nullptr,
                                    nullptr,
                                    1);
    check_return(result, statement);
    return result;
  }

  auto executeRaw(const std::string_view statement) const {
    const auto mapped_statement = connection.mapTypes(statement);
    PGresult* result = PQexec(conn, mapped_statement.data());
    auto status = PQresultStatus(result);
    check_return(result, statement);
    PQclear(result);
    return status;
  }
};

sql::postgres::Connection::Connection(const CredentialPassword& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>(*this, credential)) {
}

const sql::ConnectionBase::TypeMap& sql::postgres::Connection::typeMap() const {
  static const TypeMap map = {{"DOUBLE", "FLOAT8"}, {"STRING", "VARCHAR"}};
  return map;
}

sql::postgres::Connection::~Connection() = default;

void sql::postgres::Connection::execute(const std::string_view statement) {
  impl_->executeRaw(statement);
}

std::unique_ptr<sql::ResultBase> sql::postgres::Connection::fetchAll(const std::string_view statement) {
  PGresult* result = impl_->execute(statement);
  return std::make_unique<Result>(result);
}


std::unique_ptr<sql::ResultBase> sql::postgres::Connection::fetchMany(const std::string_view statement) {
  PGresult* result = impl_->execute(statement);
  // TODO: Figure out how multi result protocol works, for now, grab first result
  return std::make_unique<Result>(result);
}

std::unique_ptr<sql::RowBase> sql::postgres::Connection::fetchRow(const std::string_view statement) {
  PGresult* result = impl_->execute(statement);
  const auto row_count = PQntuples(result);
  if (row_count == 0) {
    PQclear(result);
    throw EmptyResultException(statement);
  }
  if (row_count > 1) {
    PQclear(result);
    throw InvalidRowsException("Expected to find a single row in the data, but found: " + std::to_string(row_count),
                               statement);
  }
  return std::make_unique<Row>(result);
}

sql::SqlVariant sql::postgres::Connection::fetchScalar(const std::string_view statement) {
  const auto row = fetchRow(statement);
  if (row->columnCount() != 1) {
    throw InvalidColumnsException("Expected to find a single column in the data", statement);
  }
  return row->asVariant(0);
}

void check_bulk_return(int status, PGconn* conn) {
  if (status != 1) {
    throw std::runtime_error("Failed to send data to the database " + std::string(PQerrorMessage(conn)));
  }
}

void sql::postgres::Connection::bulkLoad(
    const std::string_view table,
    const std::vector<std::filesystem::path> source_paths) {
  /*
   * Copying data into Postgres:
   *
   * This interfaces is so obscenely braindead that you simply have to read the code.
   * Basically, stuff can fail at any time during copy and how you exactly get the error message
   * and handle it depends on what part of the flow you are in.
   *
   * The aim here is to turn the PG errors into subclasses of sql::Exception and give them some
   * decent error codes and error messages
   */

  validateSourcePaths(source_paths);

  const auto cn = impl_->conn;
  for (const auto& path : source_paths) {
    std::ifstream file(path.string(), std::ios::binary);
    if (!file.is_open()) {
      throw std::ios_base::failure("Failed to open source file: " + path.string());
    }

    // First, we need to tell PG that a copy stream is coming. This puts the server into a special mode
    std::string copyQuery = "COPY "
                            + std::string(table)
                            + " FROM STDIN"
                            + " WITH (FORMAT csv, DELIMITER '|', NULL '', HEADER)";
    const auto ready_status = impl_->executeRaw(copyQuery);
    assert(ready_status == PGRES_COPY_IN); // We better have handled this already

    // Chunk file content to the database (synchronously) with 1MB chunks so we can hide latency
    // There is an "async" interface too - but it requires polling sockets and manually backing off
    // based on return codes. One day, I will make this work - after I lose my will to live!
    constexpr size_t buffer_size = 1024 * 1024;
    auto buf = std::make_unique<char[]>(buffer_size);
    while (file) {
      file.read(buf.get(), buffer_size);
      const std::streamsize n = file.gcount();
      if (n > 0) {
        const auto progress_status = PQputCopyData(cn, buf.get(), static_cast<int>(n));
        check_bulk_return(progress_status, cn);
      }
    }
    const auto end_status = PQputCopyEnd(cn, nullptr);
    check_bulk_return(end_status, cn);

    // After our final row (which is marked by PQOutCopyEnd we now get a result back telling us if it worked
    const auto final_result = PQgetResult(cn);
    impl_->check_return(final_result, copyQuery);
    PQclear(PQgetResult(cn));

    // Drain the connection - the usual libpq pointless logic
    while (PGresult* leftover = PQgetResult(cn)) {
      PQclear(leftover);
    }
  }
}

using namespace sql::explain;
using namespace nlohmann;
/**
 * Create the appropriate node type based on PostgreSQL node type
 * Skip past nodes we currently don't care about.
 */
std::unique_ptr<sql::explain::Node> createNodeFromPgType(const json& node_json) {
  const auto pg_node_type = node_json["Node Type"].get<std::string>();
  std::unique_ptr<Node> node;

  if (pg_node_type == "Seq Scan" || pg_node_type == "Index Scan" || pg_node_type == "Index Only Scan") {
    std::string relation_name;
    if (node_json.contains("Relation Name")) {
      relation_name = node_json["Relation Name"].get<std::string>();
    }
    if (node_json.contains("Alias")) {
      relation_name = node_json["Alias"].get<std::string>();
    }

    node = std::make_unique<Scan>(relation_name);
  } else if (pg_node_type == "Hash Join" || pg_node_type == "Nested Loop" || pg_node_type == "Merge Join") {
    std::string join_condition;

    Join::Strategy join_strategy;
    if (pg_node_type == "Hash Join") {
      join_strategy = Join::Strategy::HASH;
    } else if (pg_node_type == "Nested Loop") {
      join_strategy = Join::Strategy::LOOP;
    } else if (pg_node_type == "Merge Join") {
      join_strategy = Join::Strategy::MERGE;
      join_condition = node_json["Merge Cond"].get<std::string>();
    }

    auto join_type = Join::Type::INNER;
    if (node_json.contains("Join Type")) {
      // TODO: Parse the strategy into the enum
      auto join_type = node_json["Join Type"].get<std::string>();
    }
    if (node_json.contains("Join Filter")) {
      join_condition = node_json["Join Filter"].get<std::string>();
    }
    if (node_json.contains("Hash Cond")) {
      join_condition = node_json["Hash Cond"].get<std::string>();
    }
    if (!join_condition.empty() &&
        join_condition.front() == '(' &&
        join_condition.back() == ')') {
      join_condition = join_condition.substr(1, join_condition.length() - 2);
    }

    node = std::make_unique<Join>(join_type, join_strategy, join_condition);
  } else if (pg_node_type == "Sort") {
    std::vector<Column> sort_keys;
    for (const auto& col : node_json["Sort Key"]) {
      const std::string desc_suffix = " DESC";
      auto sort_order = Column::Sorting::ASC;
      auto name = col.get<std::string>();
      if (name.size() >= desc_suffix.size() &&
          name.substr(name.size() - desc_suffix.size()) == desc_suffix) {
        // Sort keys end with DESC (yeah, a string) if they are descending
        name = name.substr(0, name.size() - desc_suffix.size());
        sort_order = Column::Sorting::DESC;
      }
      Column c(name, sort_order);
      sort_keys.push_back(c);
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

    auto group_strategy = GroupBy::Strategy::SIMPLE;
    if (node_json.contains("Strategy")) {
      const auto strategy = node_json["Strategy"].get<std::string>();
      if (strategy == "Hashed") {
        group_strategy = GroupBy::Strategy::HASH;
      } else if (strategy == "Sorted") {
        group_strategy = GroupBy::Strategy::SORT_MERGE;
      } else {
        throw std::runtime_error("Did not know GROUP BY strategy " + strategy);
      }
    } else if (node_json.contains("Group Key")) {
      // The strategy field is only present if the strategy is Hash or if there is an aggregate
      // value
      group_strategy = GroupBy::Strategy::SORT_MERGE;
    }

    if (node_json.contains("Group Key")) {
      for (const auto& key : node_json["Group Key"]) {
        group_keys.push_back(Column(key.get<std::string>()));
      }
    }
    std::vector<Column> aggregates;
    if (node_json.contains("Output")) {
      std::vector<Column> out;
      for (const auto& key : node_json["Output"]) {
        out.push_back(Column(key.get<std::string>()));
      }
      std::unordered_set<Column> group_keys_set(group_keys.begin(), group_keys.end());
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
    /* PG has a special node for distinct unions when the input is sorted */
    node = std::make_unique<Union>(Union::Type::DISTINCT);
  } else {
    return nullptr;
  }

  /* Cost and estimation information */
  if (node_json.contains("Total Cost")) {
    node->cost = node_json["Total Cost"].get<double>();
  }

  /* Row estimates and actuals */
  if (node_json.contains("Plan Rows")) {
    node->rows_estimated = node_json["Plan Rows"].get<double>();
  }
  if (node_json.contains("Actual Rows")) {
    node->rows_actual = node_json["Actual Rows"].get<double>();
  }

  // Filter conditions
  if (node_json.contains("Filter")) {
    node->filter_condition = node_json["Filter"].get<std::string>();
  }

  // Output columns
  if (node_json.contains("Output") && node_json["Output"].is_array()) {
    for (const auto& col : node_json["Output"]) {
      node->columns_output.push_back(col.get<std::string>());
    }
  }

  return node;
}


std::unique_ptr<sql::explain::Node> buildExplainNode(nlohmann::json& node_json) {
  // Determine the node type from "Node Type" field
  if (!node_json.contains("Node Type")) {
    throw std::runtime_error("Missing 'Node Type' in EXPLAIN node");
  }

  auto node = createNodeFromPgType(node_json);
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
    node = createNodeFromPgType(node_json);
  }

  if (node_json.contains("Plans") && node_json["Plans"].is_array()) {
    for (auto& child_json : node_json["Plans"]) {
      auto child_node = buildExplainNode(child_json);
      node->addChild(std::move(child_node));
    }
  }

  return node;
}


std::unique_ptr<sql::explain::Plan> buildExplainPlan(nlohmann::json& json) {
  if (!json.is_array() || json.empty() || !json[0].contains("Plan")) {
    throw std::runtime_error("Invalid EXPLAIN JSON format");
  }

  /* High lever stats about the query */
  double planning_time = 0.0;
  double execution_time = 0.0;

  if (json[0].contains("Planning Time")) {
    planning_time = json[0]["Planning Time"].get<double>();
  }
  if (json[0].contains("Execution Time")) {
    execution_time = json[0]["Execution Time"].get<double>();
  }

  auto& plan_json = json[0]["Plan"];

  /* Construct the tree*/
  auto root_node = buildExplainNode(plan_json);

  if (!root_node) {
    // TODO: Add the plan here
    throw std::runtime_error("Invalid EXPLAIN plan output, could not construct a plan from ");
  }

  // Create and return the plan object with timing information
  auto plan = std::make_unique<Plan>(std::move(root_node));
  plan->planning_time = planning_time;
  plan->execution_time = execution_time;
  return plan;
}


std::unique_ptr<Plan> sql::postgres::Connection::explain(const std::string_view statement) {
  const std::string explain_modded = "EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON)\n" + std::string(statement);

  const auto result = fetchScalar(explain_modded);
  assert(result.is<SqlString>());

  auto explain_string = result.get<SqlString>().get();
  auto explain_json = json::parse(explain_string);

  return buildExplainPlan(explain_json);
}