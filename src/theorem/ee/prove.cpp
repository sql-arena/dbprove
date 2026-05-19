#include "theorem.h"
#include "runner.h"
#include "init.h"
#include "query.h"

#include <dbprove/common/file_utility.h>
#include <dbprove/sql/engine.h>

#include <filesystem>
#include <array>
#include <sstream>
#include <string>
#include <vector>

namespace dbprove::theorem::ee {
namespace {
constexpr int kLineitemScale = 25;
constexpr int kTpchOrderKeyStride = 1500000;
constexpr int64_t kBaseJoinRowCount = 6001215;
constexpr int64_t kScaledJoinRowCount = kBaseJoinRowCount * kLineitemScale;
constexpr std::array<std::string_view, 2> kOrdersPayloadColumns{{
    "o_shippriority",
    "o_comment",
}};
constexpr std::array<std::string_view, 1> kLineitemPayloadColumns{{
    "l_linenumber",
}};
const std::vector<int> kOrdersScales = {
    1, 2, 3, 4, 5,
    6, 7, 8, 9, 10,
    11, 12, 13, 14, 15,
    16, 17, 18, 19, 20
};

std::string renderUnionAllSeries(const std::string_view name, const int count) {
  std::ostringstream sql;
  sql << name << "(replica_id) AS (\n";
  for (int i = 0; i < count; ++i) {
    if (i > 0) {
      sql << "UNION ALL\n";
    }
    sql << "  SELECT " << i << "\n";
  }
  sql << ")";
  return sql.str();
}

std::string joinScaleSql(const int orders_scale, const bool use_parquet_views) {
  const auto orders_table = use_parquet_views ? "tpch.orders_parquet" : "tpch.orders";
  const auto lineitem_table = use_parquet_views ? "tpch.lineitem_parquet" : "tpch.lineitem";
  std::ostringstream sql;
  sql << "WITH\n";
  sql << renderUnionAllSeries("lineitem_multiplier", kLineitemScale) << ",\n";
  sql << renderUnionAllSeries("orders_multiplier", orders_scale) << ",\n";
  sql << "scaled_lineitem AS (\n";
  sql << "  SELECT CAST(l.l_orderkey AS BIGINT) + CAST((l.l_suppkey % " << orders_scale << ") AS BIGINT) * " << kTpchOrderKeyStride << " AS l_orderkey";
  for (const auto& column : kLineitemPayloadColumns) {
    sql << ",\n         l." << column;
  }
  sql << "\n";
  sql << "  FROM " << lineitem_table << " l\n";
  sql << "  CROSS JOIN lineitem_multiplier lm\n";
  sql << "),\n";
  sql << "scaled_orders AS (\n";
  sql << "  SELECT CAST(o.o_orderkey AS BIGINT) + CAST(om.replica_id AS BIGINT) * " << kTpchOrderKeyStride << " AS o_orderkey";
  for (const auto& column : kOrdersPayloadColumns) {
    sql << ",\n         o." << column;
  }
  sql << "\n";
  sql << "  FROM " << orders_table << " o\n";
  sql << "  CROSS JOIN orders_multiplier om\n";
  sql << ")\n";
  sql << "SELECT COUNT(*) AS join_row_count,\n";
  sql << "       COUNT(l.l_orderkey) AS count_l_orderkey";
  for (const auto& column : kLineitemPayloadColumns) {
    sql << ",\n       COUNT(l." << column << ") AS count_" << column;
  }
  sql << ",\n       COUNT(o.o_orderkey) AS count_o_orderkey";
  for (const auto& column : kOrdersPayloadColumns) {
    sql << ",\n       COUNT(o." << column << ") AS count_" << column;
  }
  sql << "\n";
  sql << "FROM scaled_lineitem l\n";
  sql << "INNER JOIN scaled_orders o\n";
  sql << "  ON l.l_orderkey = o.o_orderkey";
  return sql.str();
}

std::vector<sql::SqlVariant> expectedJoinScaleRow() {
  return std::vector<sql::SqlVariant>(2 + kLineitemPayloadColumns.size() + 1 + kOrdersPayloadColumns.size(),
                                      sql::SqlVariant(kScaledJoinRowCount));
}

void ensureDuckDbParquetViews(Proof& proof) {
  if (proof.factory().engine().type() != sql::Engine::Type::DuckDB) {
    return;
  }

  const auto parquet_dir = proof.parquetDir().has_value()
                             ? std::filesystem::path(*proof.parquetDir())
                             : dbprove::common::get_project_root() / "docker" / "datafusion" / "tpch" / "sf1";
  const auto orders_parquet = (parquet_dir / "orders.parquet").string();
  const auto lineitem_parquet = (parquet_dir / "lineitem.parquet").string();

  auto conn = proof.factory().create();
  conn->execute("CREATE SCHEMA IF NOT EXISTS tpch");
  conn->execute("CREATE OR REPLACE VIEW tpch.orders_parquet AS SELECT * FROM read_parquet('" + orders_parquet + "')");
  conn->execute("CREATE OR REPLACE VIEW tpch.lineitem_parquet AS SELECT * FROM read_parquet('" + lineitem_parquet + "')");
  conn->close();
}

void runJoinScale(Proof& proof, const int orders_scale) {
  const auto engine_type = proof.factory().engine().type();
  if (engine_type != sql::Engine::Type::DataFusion
      && engine_type != sql::Engine::Type::DuckDB
      && engine_type != sql::Engine::Type::Trino) {
    proof.ensureDataset("tpch");
  }
  ensureDuckDbParquetViews(proof);
  const bool use_parquet_views = engine_type == sql::Engine::Type::DuckDB;
  Query query(joinScaleSql(orders_scale, use_parquet_views), proof.theorem.name.c_str(), 1);
  query.setExpectedRowValues(expectedJoinScaleRow());
  Runner runner(proof.factory());
  runner.serialMeasure(std::move(query), proof, proof.timingRuns());
}

void registerJoinScale(const int orders_scale) {
  const std::string padded_scale = orders_scale < 10
                                     ? "0" + std::to_string(orders_scale)
                                     : std::to_string(orders_scale);
  auto& theorem = addTheorem(
      "EE-JOIN-SCALE-" + padded_scale,
      "Join scale test with lineitem fixed at " + std::to_string(kLineitemScale) + "x and orders scaled to " + std::to_string(orders_scale) + "x",
      [orders_scale](Proof& proof) { runJoinScale(proof, orders_scale); },
      1);
  categoriseTheorem(theorem, Category::EE);
  tagTheorem(theorem, Tag("JOIN"));
}
}

void init() {
  static bool is_initialised = false;
  if (is_initialised) {
    return;
  }

  for (const int orders_scale : kOrdersScales) {
    registerJoinScale(orders_scale);
  }

  is_initialised = true;
}
}
