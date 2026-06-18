#pragma once

#include <dbprove/generator/generator_state.h>
#include <dbprove/generator/sql_resources.h>

constexpr size_t LINEITEM_25X_ROWS = 150030375;

REGISTER_TABLE("lineitem_25x", "scale", resource::lineitem_25x_sql, LINEITEM_25X_ROWS, 1);
REGISTER_TABLE("orders_scale_01", "scale", resource::orders_scale_01_sql, 1500000, 1);
REGISTER_TABLE("orders_scale_02", "scale", resource::orders_scale_02_sql, 3000000, 1);
REGISTER_TABLE("orders_scale_03", "scale", resource::orders_scale_03_sql, 4500000, 1);
REGISTER_TABLE("orders_scale_04", "scale", resource::orders_scale_04_sql, 6000000, 1);
REGISTER_TABLE("orders_scale_05", "scale", resource::orders_scale_05_sql, 7500000, 1);
REGISTER_TABLE("orders_scale_06", "scale", resource::orders_scale_06_sql, 9000000, 1);
REGISTER_TABLE("orders_scale_08", "scale", resource::orders_scale_08_sql, 12000000, 1);
REGISTER_TABLE("orders_scale_10", "scale", resource::orders_scale_10_sql, 15000000, 1);
REGISTER_TABLE("orders_scale_12", "scale", resource::orders_scale_12_sql, 18000000, 1);
REGISTER_TABLE("orders_scale_14", "scale", resource::orders_scale_14_sql, 21000000, 1);
REGISTER_TABLE("orders_scale_16", "scale", resource::orders_scale_16_sql, 24000000, 1);
REGISTER_TABLE("orders_scale_18", "scale", resource::orders_scale_18_sql, 27000000, 1);
REGISTER_TABLE("orders_scale_20", "scale", resource::orders_scale_20_sql, 30000000, 1);
