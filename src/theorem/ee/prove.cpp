#include "theorem.h"
#include "runner.h"
#include "init.h"
#include "query.h"

#include <sstream>
#include <string>
#include <vector>

namespace dbprove::theorem::ee {
namespace {
constexpr int kLineitemScale = 100;
const std::vector<int> kOrdersScales = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

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

std::string joinScaleSql(const int orders_scale) {
  std::ostringstream sql;
  sql << "WITH\n";
  sql << "order_key_space AS (\n";
  sql << "  SELECT MAX(o_orderkey) + 1 AS span\n";
  sql << "  FROM tpch.orders\n";
  sql << "),\n";
  sql << renderUnionAllSeries("lineitem_multiplier", kLineitemScale) << ",\n";
  sql << renderUnionAllSeries("orders_multiplier", orders_scale) << ",\n";
  sql << "scaled_lineitem AS (\n";
  sql << "  SELECT l.l_orderkey + (lm.replica_id % " << orders_scale << ") * ks.span AS l_orderkey,\n";
  sql << "         l.l_linenumber AS l_linenumber\n";
  sql << "  FROM tpch.lineitem l\n";
  sql << "  CROSS JOIN lineitem_multiplier lm\n";
  sql << "  CROSS JOIN order_key_space ks\n";
  sql << "),\n";
  sql << "scaled_orders AS (\n";
  sql << "  SELECT o.o_orderkey + om.replica_id * ks.span AS o_orderkey,\n";
  sql << "         o.o_orderstatus AS o_orderstatus\n";
  sql << "  FROM tpch.orders o\n";
  sql << "  CROSS JOIN orders_multiplier om\n";
  sql << "  CROSS JOIN order_key_space ks\n";
  sql << ")\n";
  sql << "SELECT COUNT(l.l_linenumber) AS lineitem_rows,\n";
  sql << "       COUNT(o.o_orderstatus) AS order_rows\n";
  sql << "FROM scaled_lineitem l\n";
  sql << "INNER JOIN scaled_orders o\n";
  sql << "  ON l.l_orderkey = o.o_orderkey";
  return sql.str();
}

void runJoinScale(Proof& proof, const int orders_scale) {
  proof.ensureDataset("tpch");
  Query query(joinScaleSql(orders_scale), proof.theorem.name.c_str(), 1);
  Runner runner(proof.factory());
  runner.serialExplain(std::move(query), proof);
}

void registerJoinScale(const int orders_scale) {
  const std::string padded_scale = orders_scale < 10
                                     ? "0" + std::to_string(orders_scale)
                                     : std::to_string(orders_scale);
  auto& theorem = addTheorem(
      "EE-JOIN-SCALE-" + padded_scale,
      "Join scale test with lineitem fixed at 100x and orders scaled to " + std::to_string(orders_scale) + "x",
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
