#include "theorem.h"
#include "runner.h"
#include "init.h"
#include "query.h"

#include <dbprove/common/file_utility.h>
#include <dbprove/sql/connection_factory.h>
#include <dbprove/sql/credential.h>
#include <dbprove/sql/engine.h>

#include <filesystem>
#include <array>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace dbprove::theorem::ee {
namespace {
struct PayloadProjection {
  std::string_view expr;
  std::string_view alias;
};

constexpr int kLineitemScale = 25;
constexpr int64_t kBaseJoinRowCount = 6001215;
constexpr int64_t kScaledJoinRowCount = kBaseJoinRowCount * kLineitemScale;
constexpr int64_t kBaseSumLineitemLinenumber = 18007100;
constexpr int64_t kBaseSumOrdersCustkeyJoined = 450367585226;
constexpr int64_t kLineitemReplicaWeightSum = (kLineitemScale * (kLineitemScale + 1)) / 2;
constexpr std::array<PayloadProjection, 8> kOrdersPayloadColumns{{
    {"o.o_custkey", "o_custkey"},
    {"o.o_orderstatus", "o_orderstatus"},
    {"o.o_totalprice", "o_totalprice"},
    {"o.o_orderdate", "o_orderdate"},
    {"o.o_orderpriority", "o_orderpriority"},
    {"o.o_clerk", "o_clerk"},
    {"o.o_shippriority", "o_shippriority"},
    {"o.o_comment", "o_comment"},
}};
constexpr std::array<PayloadProjection, 1> kLineitemPayloadColumns{{
    {"l.l_linenumber", "l_linenumber"},
}};
const std::vector<int> kOrdersScales = {
    1, 2, 3, 4, 5,
    6, 8, 10, 12, 14,
    16, 18, 20
};
constexpr std::string_view kJoinScaleMaterializationVersion = "ee-join-scale-v2";

std::filesystem::path defaultSourceParquetDir() {
  return dbprove::common::get_project_root() / "docker" / "datafusion" / "tpch" / "sf1";
}

std::filesystem::path defaultMaterializedParquetDir() {
  return dbprove::common::get_project_root() / "run" / "materialized" / "join_scale";
}

std::filesystem::path materializedParquetDirForProof(const std::optional<std::string>& parquet_dir) {
  if (parquet_dir.has_value()) {
    return std::filesystem::path(*parquet_dir);
  }
  return defaultMaterializedParquetDir();
}

std::string ordersScaleTableName(const int orders_scale) {
  const auto padded_scale = orders_scale < 10
                              ? "0" + std::to_string(orders_scale)
                              : std::to_string(orders_scale);
  return "orders_scale_" + padded_scale;
}

std::vector<std::filesystem::path> expectedMaterializedFiles(const std::filesystem::path& root) {
  std::vector<std::filesystem::path> files;
  files.push_back(root / "lineitem_25x" / "lineitem_25x.parquet");
  for (const int orders_scale : kOrdersScales) {
    const auto table_name = ordersScaleTableName(orders_scale);
    files.push_back(root / table_name / (table_name + ".parquet"));
  }
  return files;
}

bool materializedArtifactsAreReady(const std::filesystem::path& root) {
  const auto manifest_path = root / "manifest.txt";
  if (!std::filesystem::exists(manifest_path)) {
    return false;
  }

  std::ifstream manifest_stream(manifest_path);
  std::string manifest_version;
  std::getline(manifest_stream, manifest_version);
  if (manifest_version != kJoinScaleMaterializationVersion) {
    return false;
  }

  for (const auto& file : expectedMaterializedFiles(root)) {
    if (!std::filesystem::exists(file)) {
      return false;
    }
  }
  return true;
}

std::string readParquetSql(const std::filesystem::path& path) {
  return "read_parquet('" + path.string() + "')";
}

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

std::string lineitemMaterializationSql(const std::filesystem::path& source_dir, const std::filesystem::path& output_file) {
  std::ostringstream sql;
  sql << "COPY (\n";
  sql << "WITH\n";
  sql << renderUnionAllSeries("lineitem_multiplier", kLineitemScale) << "\n";
  sql << "SELECT CAST(l.l_orderkey AS BIGINT) AS l_orderkey,\n";
  sql << "       CAST(l.l_suppkey AS BIGINT) AS l_suppkey,\n";
  sql << "       CAST(l.l_linenumber AS BIGINT) AS l_linenumber,\n";
  sql << "       CAST(lm.replica_id AS BIGINT) AS lineitem_replica_id\n";
  sql << "FROM " << readParquetSql(source_dir / "lineitem.parquet") << " l\n";
  sql << "CROSS JOIN lineitem_multiplier lm\n";
  sql << ") TO '" << output_file.string() << "' (FORMAT PARQUET)";
  return sql.str();
}

std::string ordersMaterializationSql(const std::filesystem::path& source_dir,
                                     const std::filesystem::path& output_file,
                                     const int orders_scale) {
  std::ostringstream sql;
  sql << "COPY (\n";
  sql << "WITH\n";
  sql << renderUnionAllSeries("orders_multiplier", orders_scale) << "\n";
  sql << "SELECT CAST(CAST(o.o_orderkey AS BIGINT) * " << orders_scale
      << " + CAST(om.replica_id AS BIGINT) AS BIGINT) AS join_key,\n";
  sql << "       CAST(o.o_orderkey AS BIGINT) AS o_orderkey,\n";
  sql << "       CAST(om.replica_id AS BIGINT) AS orders_replica_id";
  for (const auto& column : kOrdersPayloadColumns) {
    sql << ",\n       " << column.expr << " AS " << column.alias;
  }
  sql << "\nFROM " << readParquetSql(source_dir / "orders.parquet") << " o\n";
  sql << "CROSS JOIN orders_multiplier om\n";
  sql << ") TO '" << output_file.string() << "' (FORMAT PARQUET)";
  return sql.str();
}

std::string joinScaleSql(const int orders_scale) {
  const auto orders_table = "tpch." + ordersScaleTableName(orders_scale);
  const auto lineitem_table = "tpch.lineitem_25x";
  std::ostringstream sql;
  sql << "SELECT COUNT(*) AS join_row_count,\n";
  sql << "       SUM((CAST(l.l_linenumber AS BIGINT) + CAST(o.o_custkey AS BIGINT)) * CAST(l.lineitem_replica_id + 1 AS BIGINT)\n";
  sql << "           + CAST(o.orders_replica_id AS BIGINT)) AS sum_join_payload,\n";
  sql << "       COUNT(l.l_orderkey) AS count_l_orderkey";
  for (const auto& column : kLineitemPayloadColumns) {
    sql << ",\n       COUNT(l." << column.alias << ") AS count_" << column.alias;
  }
  sql << ",\n       COUNT(o.o_orderkey) AS count_o_orderkey";
  for (const auto& column : kOrdersPayloadColumns) {
    sql << ",\n       COUNT(o." << column.alias << ") AS count_" << column.alias;
  }
  sql << "\n";
  sql << "FROM " << lineitem_table << " l\n";
  sql << "INNER JOIN " << orders_table << " o\n";
  sql << "  ON CAST(CAST(l.l_orderkey AS BIGINT) * " << orders_scale
      << " + CAST((l.l_suppkey % " << orders_scale << ") AS BIGINT) AS BIGINT) = o.join_key";
  return sql.str();
}

int64_t bucketSumForScale(const int orders_scale) {
  switch (orders_scale) {
    case 1: return 0;
    case 2: return 3001623;
    case 3: return 5996287;
    case 4: return 9003909;
    case 5: return 12005169;
    case 6: return 15001753;
    case 8: return 20996129;
    case 10: return 27018379;
    case 12: return 32999269;
    case 14: return 39016327;
    case 16: return 45003257;
    case 18: return 51005701;
    case 20: return 57022969;
    default:
      throw std::runtime_error("Unsupported join scale for expected bucket sum: " + std::to_string(orders_scale));
  }
}

std::vector<sql::SqlVariant> expectedJoinScaleRow(const int orders_scale) {
  std::vector<sql::SqlVariant> expected;
  expected.reserve(3 + kLineitemPayloadColumns.size() + 1 + kOrdersPayloadColumns.size());
  expected.emplace_back(kScaledJoinRowCount);
  expected.emplace_back(
      kLineitemReplicaWeightSum * (kBaseSumLineitemLinenumber + kBaseSumOrdersCustkeyJoined)
      + kLineitemScale * bucketSumForScale(orders_scale));
  expected.emplace_back(kScaledJoinRowCount);
  for ([[maybe_unused]] const auto& column : kLineitemPayloadColumns) {
    expected.emplace_back(kScaledJoinRowCount);
  }
  expected.emplace_back(kScaledJoinRowCount);
  for ([[maybe_unused]] const auto& column : kOrdersPayloadColumns) {
    expected.emplace_back(kScaledJoinRowCount);
  }
  return expected;
}

void ensureDuckDbMaterializedViews(Proof& proof) {
  if (proof.factory().engine().type() != sql::Engine::Type::DuckDB) {
    return;
  }

  const auto parquet_dir = materializedParquetDirForProof(proof.parquetDir());
  if (!materializedArtifactsAreReady(parquet_dir)) {
    throw std::runtime_error(
        "Join-scale parquet artifacts are missing under " + parquet_dir.string()
        + ". Run dbprove --prepare-ee-join-scale first.");
  }

  auto conn = proof.factory().create();
  conn->execute("CREATE SCHEMA IF NOT EXISTS tpch");
  conn->execute(
      "CREATE OR REPLACE VIEW tpch.lineitem_25x AS SELECT * FROM read_parquet('"
      + (parquet_dir / "lineitem_25x" / "lineitem_25x.parquet").string() + "')");
  for (const int orders_scale : kOrdersScales) {
    const auto table_name = ordersScaleTableName(orders_scale);
    conn->execute(
        "CREATE OR REPLACE VIEW tpch." + table_name + " AS SELECT * FROM read_parquet('"
        + (parquet_dir / table_name / (table_name + ".parquet")).string() + "')");
  }
  conn->close();
}

void runJoinScale(Proof& proof, const int orders_scale) {
  const auto engine_type = proof.factory().engine().type();
  if (engine_type != sql::Engine::Type::DataFusion
      && engine_type != sql::Engine::Type::DuckDB
      && engine_type != sql::Engine::Type::Trino) {
    proof.ensureDataset("tpch");
  }
  ensureDuckDbMaterializedViews(proof);
  Query query(joinScaleSql(orders_scale), proof.theorem.name.c_str(), 1);
  query.setExpectedRowValues(expectedJoinScaleRow(orders_scale));
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

void prepareJoinScaleArtifacts(std::ostream& console, const std::optional<std::string>& source_parquet_dir) {
  const auto source_dir = source_parquet_dir.has_value()
                            ? std::filesystem::path(*source_parquet_dir)
                            : defaultSourceParquetDir();
  const auto output_root = defaultMaterializedParquetDir();

  if (materializedArtifactsAreReady(output_root)) {
    console << "EE join-scale parquet artifacts already present at " << output_root << '\n';
    return;
  }

  if (!std::filesystem::exists(source_dir / "orders.parquet")
      || !std::filesystem::exists(source_dir / "lineitem.parquet")) {
    throw std::runtime_error("Expected source parquet files under " + source_dir.string());
  }

  std::filesystem::remove_all(output_root);
  std::filesystem::create_directories(output_root);
  std::filesystem::create_directories(output_root / "lineitem_25x");
  for (const int orders_scale : kOrdersScales) {
    std::filesystem::create_directories(output_root / ordersScaleTableName(orders_scale));
  }

  const auto duckdb_file = output_root / "materialize.duckdb";
  sql::ConnectionFactory factory(
      sql::Engine("duckdb"),
      sql::CredentialFile(duckdb_file.string()));
  auto conn = factory.create();

  console << "Materializing lineitem_25x into " << output_root << '\n';
  conn->execute(lineitemMaterializationSql(
      source_dir,
      output_root / "lineitem_25x" / "lineitem_25x.parquet"));

  for (const int orders_scale : kOrdersScales) {
    console << "Materializing " << ordersScaleTableName(orders_scale) << '\n';
    conn->execute(ordersMaterializationSql(
        source_dir,
        output_root / ordersScaleTableName(orders_scale) / (ordersScaleTableName(orders_scale) + ".parquet"),
        orders_scale));
  }
  conn->close();

  std::ofstream manifest_stream(output_root / "manifest.txt");
  manifest_stream << kJoinScaleMaterializationVersion << '\n';
  manifest_stream << "source=" << source_dir.string() << '\n';
  manifest_stream << "lineitem_scale=" << kLineitemScale << '\n';
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
