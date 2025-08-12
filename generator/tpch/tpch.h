#pragma once
#include "generator/generator_state.h"
#include "generator/embedded_sql.h"
#include "tpch_nation.h"

constexpr size_t TPCH_SF = 1;

REGISTER_GENERATOR("SUPPLIER", resource::customer_sql, supplier_gen, TPCH_SF * 10000);
REGISTER_GENERATOR("PART", resource::part_sql, part_gen, TPCH_SF * 200000);
REGISTER_GENERATOR("PARTSUPP", resource::partsupp_sql, partsupp_gen, TPCH_SF * 150000 * 4);
REGISTER_GENERATOR("CUSTOMER", resource::customer_sql, customer_gen, TPCH_SF * 150000);
REGISTER_GENERATOR("ORDERS", resource::orders_sql, orders_lineitem_gen, TPCH_SF * 150000 * 10);
REGISTER_GENERATOR("LINEITEM", resource::lineitem_sql, orders_lineitem_gen, 0);
REGISTER_GENERATOR("NATION", resource::nation_sql, nation_gen, std::size(tpch_nations));
REGISTER_GENERATOR("REGION", resource::region_sql, region_gen, std::size(tpch_regions));

