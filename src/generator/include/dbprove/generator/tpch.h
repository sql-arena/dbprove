#pragma once
#include "generator_state.h"
#include "dbprove_generator/embedded_sql.h"
#include "tpch_nation.h"

constexpr size_t TPCH_SF = 1;

REGISTER_GENERATOR("tpch.supplier", "tpch", resource::supplier_sql, supplier_download, TPCH_SF * 10000);
REGISTER_GENERATOR("tpch.part", "tpch", resource::part_sql, part_download, TPCH_SF * 200000);
REGISTER_GENERATOR("tpch.partsupp", "tpch", resource::partsupp_sql, partsupp_download, TPCH_SF * 200000 * 4);
REGISTER_GENERATOR("tpch.customer", "tpch", resource::customer_sql, customer_download, TPCH_SF * 150000);
REGISTER_GENERATOR("tpch.orders", "tpch", resource::orders_sql, orders_download, TPCH_SF * 150000 * 10);
REGISTER_GENERATOR("tpch.lineitem", "tpch", resource::lineitem_sql, lineitem_download, 6001215);
REGISTER_GENERATOR("tpch.nation", "tpch", resource::nation_sql, nation_download, std::size(tpch_nations));
REGISTER_GENERATOR("tpch.region", "tpch", resource::region_sql, region_download, std::size(tpch_regions));
