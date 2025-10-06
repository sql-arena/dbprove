#include "connection.h"
#include "row.h"
#include <libpq-fe.h>
#include "result.h"
#include <dbprove/sql/sql.h>
#include <explain_nodes.h>
#include <nlohmann/json.hpp>

#include <cassert>
#include <fstream>
#include <regex>
#include <unordered_set>

#include "explain/plan.h"


class sql::postgres::Connection::Pimpl {
public:
  Connection& connection;
  const CredentialPassword credential;
  PGconn* conn;

  explicit Pimpl(Connection& connection, const CredentialPassword& credential)
    : connection(connection)
    , credential(credential) {
    std::string connection_string =
        "host=" + credential.host +
        " dbname=" + credential.database +
        " user=" + credential.username +
        " port=" + std::to_string(credential.port);
    if (credential.password.has_value()) {
      connection_string += " password=" + credential.password.value();
    }
    conn = PQconnectdb(connection_string.c_str());

    check_connection();
  }

  void safeClose() {
    if (conn) {
      PQfinish(conn);
      conn = nullptr;
    }
  }

  void check_connection() {
    if (conn == nullptr) {
      throw ConnectionClosedException(credential);
    }

    if (PQstatus(conn) != CONNECTION_OK) {
      const std::string error = PQerrorMessage(conn);
      safeClose();
      throw ConnectionException(credential, error);
    }
  }

  /// @brief Handles return values from calls into postgres
  /// @note: If we throw here, we will call `PQClear`. But it is the responsibility of the caller to clear on success
  void check_return(PGresult* result, const std::string_view statement) const {
    // ReSharper disable once CppTooWideScope
    const auto status = PQresultStatus(result);
    switch (status) {
      case PGRES_TUPLES_OK:
      case PGRES_COMMAND_OK:
      case PGRES_EMPTY_QUERY:
      case PGRES_SINGLE_TUPLE:
      case PGRES_COPY_IN: // Ready to receive data on COPY
        return;
      /* Legit and harmless status code */

      default:
        const std::string error_msg = PQerrorMessage(this->conn);
        assert(!error_msg.empty());
        const char* sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
        std::string state;
        if (sqlstate == nullptr) {
          state = "UNKNOWN";
        } else {
          state = sqlstate;
        }
        PQclear(result);

        if (state.starts_with("42P01") // The Table  already exists
            || state.starts_with("42710") // The Foreign Key already exists
        ) {
          throw InvalidObjectException(error_msg);
        }
        if (state.starts_with("42")) {
          throw SyntaxException(error_msg, statement);
        }
        throw ConnectionException(credential, error_msg);
    }
  }

  PGresult* execute(const std::string_view statement) {
    check_connection();
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

  [[maybe_unused]] auto executeRaw(const std::string_view statement) {
    check_connection();
    const auto mapped_statement = connection.mapTypes(statement);
    PGresult* result = PQexec(conn, mapped_statement.data());
    const auto status = PQresultStatus(result);
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

sql::postgres::Connection::~Connection() {
  PQfinish(impl_->conn);
}

void sql::postgres::Connection::execute(const std::string_view statement) {
  [[maybe_unused]] auto s = impl_->executeRaw(statement);
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

void check_bulk_return(const int status, const PGconn* conn) {
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
    std::string copy_query = "COPY "
                             + std::string(table)
                             + " FROM STDIN"
                             + " WITH (FORMAT csv, DELIMITER '|', NULL '', HEADER)";
    const auto ready_status = impl_->executeRaw(copy_query);
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

    // After our final row (which is marked by PQOutCopyEnd) we now get a result back telling us if it worked
    const auto final_result = PQgetResult(cn);
    impl_->check_return(final_result, copy_query);
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
std::unique_ptr<Node> createNodeFromPgType(const json& node_json) {
  const auto pg_node_type = node_json["Node Type"].get<std::string>();
  std::unique_ptr<Node> node;
  double actual_override = -1;

  if (pg_node_type == "Seq Scan" || pg_node_type == "Index Scan" || pg_node_type == "Index Only Scan") {
    std::string relation_name;
    if (node_json.contains("Relation Name")) {
      relation_name = node_json["Relation Name"].get<std::string>();
    }
    if (node_json.contains("Alias")) {
      relation_name = node_json["Alias"].get<std::string>();
    }
    node = std::make_unique<Scan>(relation_name);
  } else if (pg_node_type == "Bitmap Heap Scan" || pg_node_type == "Bitmap Index Scan") {
    /* PG does a bitmap intersection of indexes using e special operators.
     * They can be chained together and be arbitrarily nested
     * We just care about the top one as that is the actual outcome of the scan.
     */
    std::string relation_name;
    if (node_json.contains("Relation Name")) {
      relation_name = node_json["Relation Name"].get<std::string>();
    }
    if (node_json.contains("Alias")) {
      relation_name = node_json["Alias"].get<std::string>();
    }
    if (relation_name.empty()) {
      // One of the lower bitmap operations, we already rendered the scan.
      return nullptr;
    }
    node = std::make_unique<Scan>(relation_name);
  } else if (pg_node_type == "Hash Join"
             || pg_node_type == "Nested Loop"
             || pg_node_type == "Merge Join") {
    std::string join_condition;

    Join::Strategy join_strategy;
    if (pg_node_type == "Hash Join") {
      // We treat
      join_strategy = Join::Strategy::HASH;
    } else if (pg_node_type == "Nested Loop") {
      join_strategy = Join::Strategy::LOOP;
      // Loops may have an index scan as a child. In that case, the join condition is on the scan.
      auto loop_child = node_json["Plans"][1];
      if (loop_child["Node Type"] == "Index Scan") {
        join_condition = loop_child["Index Cond"].get<std::string>();
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
      if (join_condition.empty()) {
        join_condition = condition;
      } else {
        join_condition.append(" AND ").append(condition);
      }
    }
    if (node_json.contains("Hash Cond")) {
      join_condition = node_json["Hash Cond"].get<std::string>();
    }
    if (!join_condition.empty() &&
        join_condition.front() == '(' &&
        join_condition.back() == ')') {
      join_condition = join_condition.substr(1, join_condition.length() - 2);
      join_condition = sql::cleanExpression(join_condition);
    }
    if (join_condition.empty()) {
      // PG does not use the notion of cross-join, it simply has loop join without conditions.
      join_type = Join::Type::CROSS;
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
        // Sort keys end with “DESC” (yeah, a string) if they’re descending.
        name = name.substr(0, name.size() - desc_suffix.size());
        sort_order = Column::Sorting::DESC;
      };
      sort_keys.emplace_back(Column(name, sort_order));
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
    /* PG has a special node for distinct unions when the input is sorted. */
    node = std::make_unique<Union>(Union::Type::DISTINCT);
  } else {
    return nullptr;
  }

  /* Cost and estimation information */
  if (node_json.contains("Total Cost")) {
    node->cost = node_json["Total Cost"].get<double>();
  }

  /* Row estimates and actual */
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
   * Because we run the sub-plan multiple times, the row counts we’re dealing with going through
   * the node is: (#loops * #rows)
   *
   * Since we don't want to unfairly skew estimate vs actual, we multiply both by the loop count.
   *
   * The `rows_actual` is the AVERAGE per loop.
   * Instead of using a double for an average value, like a sane human, Postgres uses an integer.
   * That then means that the value rounds DOWN to zero - misrepresenting the actual. For those cases
   * we then have to fall back to the loop count.
   */
  if (node_json.contains("Actual Loops")) {
    auto loop_count = node_json["Actual Loops"].get<double>();
    node->rows_actual *= loop_count;
    node->rows_actual = std::max(node->rows_actual, loop_count);
    node->rows_estimated *= loop_count;
  }

  // Filter conditions
  if (node_json.contains("Filter")) {
    node->setFilter(node_json["Filter"].get<std::string>());
  }

  // Output columns
  if (node_json.contains("Output") && node_json["Output"].is_array()) {
    for (const auto& col : node_json["Output"]) {
      node->columns_output.push_back(col.get<std::string>());
    }
  }

  return node;
}


std::unique_ptr<Node> buildExplainNode(json& node_json) {
  // Determine the node type from the “Node Type” field
  if (!node_json.contains("Node Type")) {
    throw std::runtime_error("Missing 'Node Type' in EXPLAIN node");
  }

  auto node = createNodeFromPgType(node_json);
  const auto node_type = node_json["Node Type"].get<std::string>();
  if (node == nullptr
      && (node_type == "BitmapAnd"
          || node_type == "Bitmap Index Scan"
          || node_type == "Bitmap Heap Scan"
      )
  ) {
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
    node = createNodeFromPgType(node_json);
  }

  if (node_json.contains("Plans") && node_json["Plans"].is_array()) {
    const auto plans = node_json["Plans"];
    for (size_t i = 0; i < plans.size(); ++i) {
      auto child_json = plans[i];
      auto child_node = buildExplainNode(child_json);
      if (child_node == nullptr) {
        continue;
      }
      if (child_node->type == NodeType::GROUP_BY && i != plans.size() - 1) {
        /* Postgres init plans are handled as aggregate of other plans!
         * Which means: Aggregate can have MORE than only child!
         * We don't care about these init plans, skip past them if we must and just take the last plan
         * under the aggregate.
         */
        continue;
      }
      node->addChild(std::move(child_node));
    }
  }

  return node;
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


std::unique_ptr<Plan> buildExplainPlan(json& json) {
  if (!json.is_array() || json.empty() || !json[0].contains("Plan")) {
    throw std::runtime_error("Invalid EXPLAIN JSON format");
  }

  /* High lever stats about the query*/
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
    throw std::runtime_error("Invalid EXPLAIN plan output, could not construct a plan from ");
  }

  flipJoins(*root_node.get());

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

std::string sql::postgres::Connection::version() {
  const auto versionString = fetchScalar("SELECT version()").get<SqlString>().get();
  const std::regex versionRegex(R"(PostgreSQL (\d+\.\d+))");
  std::smatch match;
  if (std::regex_search(versionString, match, versionRegex)) {
    return match[1];
  }
  return "Unknown";
}

void sql::postgres::Connection::close() {
  impl_->safeClose();
}