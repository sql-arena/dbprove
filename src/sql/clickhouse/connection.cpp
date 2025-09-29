#include "connection.h"
#include "result.h"
#include "result_base.h"
#include "sql_exceptions.h"
#include "credential.h"
#include "block_holder.h"
#include "group_by.h"
#include "join.h"
#include "projection.h"
#include "row.h"
#include "scan.h"
#include "selection.h"
#include "sort.h"
#include "explain/plan.h"

#include <string_view>
#include <fstream>
#include <regex>

#include "limit.h"
#include <clickhouse/client.h>
#include <nlohmann/json.hpp>
#include <plog/Log.h>

namespace ch = clickhouse;


namespace sql::clickhouse {
void handleClickHouseException(ch::Client& client, const ch::ServerException& e) {
  try {
    client.ResetConnection();
  } catch (const ch::Exception& e) {
    // NOOP: we want to make sure we still get the throw below
  }

  switch (e.GetCode()) {
    case 60:
      throw InvalidObjectException(e.what());
    case 62:
      throw SyntaxException(e.what());
    default:
      throw InvalidException("Unknown ClickHouse Error Code " + std::to_string(e.GetCode()) + " what " + e.what());
  }
}

class Connection::Pimpl {
public:
  const CredentialPassword& credential;

  explicit Pimpl(const CredentialPassword& credential)
    : credential(credential) {
    ch::ClientOptions options;
    options.SetHost(credential.host)
           .SetPort(credential.port)
           .SetUser(credential.username)
           .SetPassword(credential.password.value_or(""))
           .SetDefaultDatabase(credential.database);
    client = std::make_unique<ch::Client>(options);
  }

  std::unique_ptr<ch::Client> client;
};


std::vector<std::string> Connection::tableColumns(const std::string_view table) {
  std::vector<std::string> ret;
  auto result = fetchAll("DESCRIBE TABLE " + std::string(table));

  for (auto& row : result->rows()) {
    ret.push_back(row[0].asString());
  }
  return ret;
}

Connection::Connection(const CredentialPassword& credential, const Engine& engine)
  : ConnectionBase(credential, engine)
  , impl_(std::make_unique<Pimpl>(credential)) {
}

Connection::~Connection() {
}


void Connection::execute(std::string_view statement) {
  auto& client = *impl_->client;
  try {
    client.Execute(std::string(statement));
  } catch (const ch::ServerException& e) {
    handleClickHouseException(client, e);
  } catch (std::exception& e) {
    throw std::runtime_error(e.what());
  }
}

std::unique_ptr<ResultBase> Connection::fetchAll(const std::string_view statement) {
  /* Clickhouse contains the Block object and expects you to process it immediately
    in the lamdba. After that, the block goes out of scope and is deconstructed,
    But we want to hang on to it and pass to Result, so we have do do this dance
    of taking a shared pointer (which will create a deep copy) before passing it on
    to the constructor of Result. Ugly!
  */

  auto& client = *impl_->client;
  std::vector<std::shared_ptr<ch::Block>> blocks;
  try {
    client.Select(std::string(statement), [&](const ch::Block& b) {
      if (b.GetRowCount() == 0) {
        return;
      }
      blocks.push_back(std::make_shared<ch::Block>(b));
    });
  } catch (const ch::ServerException& e) {
    handleClickHouseException(client, e);
  } catch (std::exception& e) {
    throw std::runtime_error(e.what());
  }
  return std::make_unique<Result>(std::make_unique<BlockHolder>(blocks));
}

std::unique_ptr<ResultBase> Connection::fetchMany(const std::string_view statement) {
  return fetchAll(statement);
}

void Connection::bulkLoad(std::string_view table, std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);
  auto& client = *impl_->client;
  auto columns = tableColumns(table);
  std::vector<std::shared_ptr<ch::ColumnString>> column_data(columns.size());
  for (auto& col : column_data) {
    col = std::make_shared<ch::ColumnString>();
  }
  for (const auto& path : source_paths) {
    std::ifstream file(path);
    if (!file.is_open()) {
      throw std::runtime_error("Failed to open file: " + path.string());
    }
    size_t batch_size = 100000;
    size_t row_count = 0;
    std::string line;
    std::getline(file, line); // Skip header
    while (std::getline(file, line)) {
      std::istringstream ss(line);
      std::string value;
      size_t i = 0;
      while (std::getline(ss, value, '|')) {
        column_data[i]->Append(value);
        ++i;
      }
      ++row_count;
      if (row_count >= batch_size) {
        ch::Block block;
        for (size_t j = 0; j < columns.size(); ++j) {
          block.AppendColumn(columns[j], column_data[j]);
          column_data[j] = std::make_shared<ch::ColumnString>();
        }
        try {
          client.Insert(std::string(table), block);
        } catch (const ch::ServerException& e) {
          handleClickHouseException(client, e);
        }
        row_count = 0;
      }
    }
    if (row_count > 0) {
      ch::Block block;
      for (size_t j = 0; j < columns.size(); ++j) {
        block.AppendColumn(columns[j], column_data[j]);
      }
      try {
        client.Insert(std::string(table), block);
      } catch (const ch::ServerException& e) {
        handleClickHouseException(client, e);
      }
    }
  }
}

std::string Connection::version() {
  return fetchScalar("SELECT version() as v").asString();
}

void Connection::createSchema(const std::string_view schema_name) {
  // ClickHouse calls a schema a database
  execute("CREATE DATABASE " + std::string(schema_name));
}

std::string Connection::translateDialectDdl(const std::string_view ddl) const {
  auto result = std::string(ddl);
  if (ddl.contains("tpch.lineitem")) {
    result = std::regex_replace(result, std::regex(";"), "PRIMARY KEY (l_orderkey, l_linenumber);");
  }
  if (ddl.contains("tpch.partsupp")) {
    result = std::regex_replace(result, std::regex(";"), "PRIMARY KEY (ps_partkey, ps_suppkey);");
  }
  return result;
}

void Connection::analyse(std::string_view table_name) {
  // ClickHouse has no need for Analyse because it does not have a planner
}

using namespace sql::explain;
using namespace nlohmann;

std::string sanitiseColumn(std::string nonsense) {
  /* Clickhouse prefixes column names with __tableX which point at... Nothing...
   * There does not seem to be a way to get from __tableX to the actual table name.
   * Instead, strip it off
   */
  return std::regex_replace(nonsense, std::regex("__table\\d+\\.*"), "");
}


void parseProjections(std::vector<Column>& columns, const json& node_json) {
  if (!node_json.contains("Actions")) {
    return;
  }
  for (auto& action : node_json["Actions"]) {
    auto action_type = action["Node Type"].get<std::string>();
    if (action_type == "INPUT") {
      continue;
    }
    if (action_type == "COLUMN") {
      auto column = sanitiseColumn(action["Column"].get<std::string>());
      columns.emplace_back(column);
    } else if (action_type == "FUNCTION") {
      auto function = sanitiseColumn(action["Function Name"].get<std::string>());
      columns.emplace_back(function);
    } else {
      throw ExplainException("Unknown action type: " + action_type);
    }
  }
}

void parseSorting(std::vector<Column>& columns, const json& node_json) {
  if (!node_json.contains("Sort Description")) {
    return;
  }
  for (auto& column : node_json["Sort Description"]) {
    const auto name = column["Column"].get<std::string>();
    const auto sorting = column["Ascending"].get<bool>() ? Column::Sorting::ASC : Column::Sorting::DESC;
    columns.emplace_back(sanitiseColumn(name), sorting);
  }
}

void parseGroupKeys(std::vector<Column>& columns, const json& node_json) {
  if (!node_json.contains("Keys")) {
    return;
  }
  auto keys = node_json["Keys"];
  if (!keys.is_array()) {
    throw ExplainException("Expected keys in group by to be an array");
  }
  const auto key_count = node_json["Keys"].size();
  for (size_t i = 0; i < key_count; ++i) {
    columns.emplace_back(sanitiseColumn(keys[i].get<std::string>()));
  }
}

void parseAggregations(std::vector<Column>& columns, const json& node_json) {
  if (!node_json.contains("Aggregations")) {
    return;
  }
  auto aggregations = node_json["Aggregations"];
  if (!aggregations.is_array()) {
    throw ExplainException("Expected aggregations in group by to be an array");
  }
  const auto aggregation_count = node_json["Aggregations"].size();
  for (size_t i = 0; i < aggregation_count; ++i) {
    columns.emplace_back(sanitiseColumn(aggregations[i]["Name"].get<std::string>()));
  }
}

std::string parseClauses(const std::string& clause) {
  auto result = std::regex_replace(clause, std::regex("[\\[\\]]"), "");
  //  result = std::regex_replace(result, std::regex("\\,"), " AND");
  result = sanitiseColumn(result);
  return result;
}

std::unique_ptr<Node> createNodeFromJson(const json& node_json) {
  const auto node_type = node_json["Node Type"].get<std::string>();
  std::unique_ptr<Node> node = nullptr;
  if (node_type == "Expression") {
    std::vector<Column> columns_projected;
    parseProjections(columns_projected, node_json);
    if (columns_projected.empty()) {
      // Clickhouse will do these odd projections where it just reorganises column order
      return nullptr;
    }
    node = std::make_unique<Projection>(columns_projected);
  } else if (node_type == "ReadFromMergeTree") {
    auto table_name = node_json["Description"].get<std::string>();
    // TODO: Potentially add a filter node here based on what we filter from scan
    node = std::make_unique<Scan>(table_name);
  } else if (node_type == "Sorting") {
    std::vector<Column> columns_sorted;
    parseSorting(columns_sorted, node_json);
    node = std::make_unique<Sort>(columns_sorted);
  } else if (node_type == "Aggregating") {
    std::vector<Column> columns_aggregated;
    std::vector<Column> columns_grouped;
    parseGroupKeys(columns_grouped, node_json);
    parseAggregations(columns_aggregated, node_json);
    node = std::make_unique<GroupBy>(GroupBy::Strategy::HASH, columns_grouped, columns_aggregated);
  } else if (node_type == "Limit") {
    auto limit_value = node_json["Limit"].get<int64_t>();
    node = std::make_unique<Limit>(limit_value);
  } else if (node_type == "Filter") {
    const auto filter_condition = sanitiseColumn(node_json["Filter Column"].get<std::string>());
    node = std::make_unique<Selection>(filter_condition);
  } else if (node_type == "Join") {
    const auto join_strategy = node_json["Algorithm"].get<std::string>().contains("Hash")
                                 ? Join::Strategy::HASH
                                 : Join::Strategy::LOOP;
    std::string join_condition;
    const auto clauses = node_json["Clauses"];
    join_condition += parseClauses(clauses);

    const auto join_type = Join::typeFromString(node_json["Type"].get<std::string>());
    node = std::make_unique<Join>(join_type, join_strategy, join_condition);
  } else if (node_type == "CreatingSets") {
    // These are prep nodes that simply construct layouts
    return nullptr;
  }
  if (!node) {
    throw ExplainException("Could not parse ClickHouse node of type: " + node_type);
  }

  if (node_json.contains("Indexes")) {
    auto index_json = node_json["Indexes"];
    if (index_json.is_array() && index_json.size() > 0 && index_json[0].contains("Selected Granules")) {
      constexpr RowCount granule_size = 8192;
      node->rows_actual = index_json[0]["Selected Granules"].get<int>() * granule_size;
    }
  }

  return node;
}

std::unique_ptr<Node> buildExplainNode(json& node_json) {
  auto node = createNodeFromJson(node_json);
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
    node = createNodeFromJson(node_json);
  }

  if (node_json.contains("Plans")) {
    for (auto& child_json : node_json["Plans"]) {
      auto child_node = buildExplainNode(child_json);
      node->addChild(std::move(child_node));
    }
  }

  return node;
}

std::unique_ptr<Plan> buildExplainPlan(json& json) {
  if (!json.is_array()) {
    throw ExplainException("ClickHouse plans are supposed to be inside an array");
  }
  const auto top_plan = json[0];
  if (!top_plan.contains("Plan")) {
    throw ExplainException("Invalid EXPLAIN format. Expected to find a Plans node");
  }

  auto plan_json = top_plan["Plan"];

  /* Construct the tree*/
  auto root_node = buildExplainNode(plan_json);

  if (!root_node) {
    throw std::runtime_error("Invalid EXPLAIN plan output, could not construct a plan");
  }

  auto plan = std::make_unique<Plan>(std::move(root_node));
  plan->planning_time = 0;
  plan->execution_time = 0;

  return plan;
}

std::unique_ptr<Plan> Connection::explain(const std::string_view statement) {
  const std::string explain_stmt = "EXPLAIN PLAN json = 1, actions = 1, header = 1, indexes = 1\n"
                                   + std::string(statement)
                                   + "\nFORMAT TSVRaw";
  auto string_explain = fetchScalar(explain_stmt).asString();
  auto json_explain = json::parse(string_explain);
  auto plan = buildExplainPlan(json_explain);

  plan->flipJoins();
  return plan;
}
}