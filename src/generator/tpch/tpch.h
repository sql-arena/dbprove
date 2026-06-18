#pragma once

#include <dbprove/generator/generator_state.h>
#include <dbprove/generator/sql_resources.h>

constexpr size_t TPCH_SF = 1;
constexpr size_t TPCH_NATION_ROWS = 25;
constexpr size_t TPCH_REGION_ROWS = 5;

REGISTER_TABLE("supplier", "tpch_sf1", resource::supplier_sql, TPCH_SF * 10000, 1);
REGISTER_TABLE("part", "tpch_sf1", resource::part_sql, TPCH_SF * 200000, 1);
REGISTER_TABLE("partsupp", "tpch_sf1", resource::partsupp_sql, TPCH_SF * 200000 * 4, 1);
REGISTER_TABLE("customer", "tpch_sf1", resource::customer_sql, TPCH_SF * 150000, 1);
REGISTER_TABLE("orders", "tpch_sf1", resource::orders_sql, TPCH_SF * 150000 * 10, 1);
REGISTER_TABLE("lineitem", "tpch_sf1", resource::lineitem_sql, 6001215, 1);
REGISTER_TABLE("nation", "tpch_sf1", resource::nation_sql, TPCH_NATION_ROWS, 1);
REGISTER_TABLE("region", "tpch_sf1", resource::region_sql, TPCH_REGION_ROWS, 1);
