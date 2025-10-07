#pragma once
#include "generator_state.h"
#include "dbprove_generator/embedded_sql.h"
#include "tpch_nation.h"

constexpr size_t TPCH_SF = 1;

REGISTER_GENERATOR("tpch.supplier", resource::supplier_sql, supplier_gen, TPCH_SF * 10000);
REGISTER_GENERATOR("tpch.part", resource::part_sql, part_gen, TPCH_SF * 200000);
REGISTER_GENERATOR("tpch.partsupp", resource::partsupp_sql, partsupp_gen, TPCH_SF * 150000 * 4);
REGISTER_GENERATOR("tpch.customer", resource::customer_sql, customer_gen, TPCH_SF * 150000);
REGISTER_GENERATOR("tpch.orders", resource::orders_sql, orders_lineitem_gen, TPCH_SF * 150000 * 10);
REGISTER_GENERATOR("tpch.lineitem", resource::lineitem_sql, orders_lineitem_gen, TPCH_SF * 150000 * 10 * 4);
REGISTER_GENERATOR("tpch.nation", resource::nation_sql, nation_gen, std::size(tpch_nations));
REGISTER_GENERATOR("tpch.region", resource::region_sql, region_gen, std::size(tpch_regions));

REGISTER_FK("tpch.lineitem", ("l_orderkey"), "tpch.orders", ("o_orderkey"));
REGISTER_FK("tpch.lineitem", ("l_partkey"), "tpch.part", ("p_partkey"));
REGISTER_FK("tpch.lineitem", ("l_suppkey"), "tpch.supplier", ("s_suppkey"));
REGISTER_FK("tpch.lineitem", ("l_partkey", "l_suppkey"), "tpch.partsupp", ("ps_partkey", "ps_suppkey"));
REGISTER_FK("tpch.orders", ("o_custkey"), "tpch.customer", ("c_custkey"));
REGISTER_FK("tpch.partsupp", ("ps_partkey"), "tpch.part", ("p_partkey"));
REGISTER_FK("tpch.partsupp", ("ps_suppkey"), "tpch.supplier", ("s_suppkey"));
REGISTER_FK("tpch.supplier", ("s_nationkey"), "tpch.nation", ("n_nationkey"));
REGISTER_FK("tpch.customer", ("c_nationkey"), "tpch.nation", ("n_nationkey"));
REGISTER_FK("tpch.nation", ("n_regionkey"), "tpch.region", ("r_regionkey"));
