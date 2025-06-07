#pragma once
#include "generator_state.h"

REGISTER_GENERATOR("SUPPLIER", supplier_gen);
REGISTER_GENERATOR("PART", part_gen);
REGISTER_GENERATOR("PARTSUPP", partsupp_gen);
REGISTER_GENERATOR("CUSTOMER", customer_gen);
REGISTER_GENERATOR("ORDERS", orders_lineitem_gen);
REGISTER_GENERATOR("LINEITEM", orders_lineitem_gen);
REGISTER_GENERATOR("NATION", nation_gen);
REGISTER_GENERATOR("REGION", region_gen);

