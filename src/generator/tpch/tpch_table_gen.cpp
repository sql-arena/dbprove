#include <plog/Log.h>

#include <chrono>
#include <ostream>
#include <filesystem>
#include <fstream>

#include "tpch_text.h"
#include "generator/date_range.h"
#include "generator/double_range.h"
#include "generator/foreign_key.h"
#include "generator/formatter.h"
#include "generator/integer_range.h"
#include "generator/key.h"
#include "generator/set.h"
#include "tpch_container.h"
#include "tpch_nation.h"
#include "tpch_phone.h"
#include "tpch_types.h"
#include "generator/v_string.h"
#include "tpch.h"
#include "generator/generator_state.h"
#include <dbprove/common/string.h>
#include "generator/generated_table.h"

using namespace std::chrono;
using namespace generator;


template <typename T>
void reportProgress(const std::string_view table_name, T current, T target) {
  constexpr T num_reports = 10;
  const T report_interval = target / num_reports;
  if (current % report_interval == 0 && current > 0) {
    PLOGI << "Table: " << table_name << " input generated " << current << " rows so far";
  }
}

template <typename T>
void reportProgress(const std::string_view table_name, T current, T target,
                    const std::string_view secondary_table_name, T secondary_current) {
  constexpr T num_reports = 10;
  const T report_interval = target / num_reports;
  if (current % report_interval == 0 && current > 0) {
    PLOGI << "Table: " << table_name << " input generated " << current << " rows so far"
    << " and " + std::string(secondary_table_name) + " " + std::to_string(secondary_current) + " rows";
  }
}


constexpr size_t part_count = 200000;
// Named to match the TPC-H spec
constexpr auto STARTDATE = sys_days(1992y / January / 1);
constexpr auto CURRENTDATE = sys_days(1995y / June / 17);
constexpr auto ENDDATE = sys_days(1998y / December / 31);

uint64_t supplier_for_part(const uint64_t part_key, const uint64_t i) {
  constexpr size_t S = TPCH_SF * 10000;
  return (part_key + (i * ((S / 4) + (part_key - 1) / S))) % (S + 1);
}

double price_for_part(const uint64_t part_key) {
  return (90000.0 + (part_key / 10) % 20001 + 100.0 * (part_key % 1000)) /
         100.0;
}

template <typename T>
void c(std::ostream& out, T value, const bool is_last = false) {
  constexpr auto col_separator = GeneratorState::columnSeparator();
  constexpr auto row_separator = GeneratorState::rowSeparator();

  if (is_last) {
    out << value << row_separator;
  } else {
    out << value << col_separator;
  }
}

void nation_gen(GeneratorState& state) {
  auto const nation_count = state.table("tpch.nation").row_count;
  const auto file_name = state.csvPath("tpch.nation");
  std::ofstream nation(file_name);
  c(nation, "N_NATIONKEY");
  c(nation, "N_NAME");
  c(nation, "N_REGIONKEY", true);

  using namespace generator;
  for (size_t i = 0; i < nation_count; ++i) {
    c(nation, i);
    c(nation, tpch_nations[i].first);
    c(nation, tpch_nations[i].second, true);
  }
  state.registerGeneration("tpch.nation", file_name);
}

void region_gen(GeneratorState& state) {
  const size_t region_count = state.table("tpch.region").row_count;
  const auto file_name = state.csvPath("tpch.region");
  std::ofstream region(file_name);
  constexpr auto col_separator = state.columnSeparator();
  constexpr auto row_separator = state.rowSeparator();
  region << "R_REGIONKEY" << col_separator;
  region << "R_NAME" << col_separator;
  region << "R_COMMENT" << row_separator;

  TpchText r_comment(31, 115);

  for (size_t i = 0; i < region_count; ++i) {
    c(region, i);
    c(region, tpch_regions[i]);
    c(region, r_comment.next(), true);
  }
  state.registerGeneration("tpch.region", file_name);
}


void supplier_gen(GeneratorState& state) {
  const auto supplier_count = state.table("tpch.supplier").row_count;
  const auto file_name = state.csvPath("tpch.supplier");
  std::ofstream supplier(file_name);
  Key s_suppkey;
  Formatter s_name("Supplie{:09}r");
  VString s_address(10, 40);
  ForeignKey s_nationkey(0, 24);
  DoubleRange s_acctbal(-999.99, 9999.99);
  TpchText s_comment_base(25, 100);
  DoubleRange s_comment_chance(0.0, 1.0);

  // SF * 5 + SF * 5 rows must have special strings (see: TPC-H spec section 4.2.3)
  constexpr double fraction_customer_comment = 1 * 10000.0 / 1 * 5;

  // Header
  c(supplier, "S_SUPPKEY");
  c(supplier, "S_NAME");
  c(supplier, "S_ADDRESS");
  c(supplier, "S_NATIONKEY");
  c(supplier, "S_PHONE");
  c(supplier, "S_ACCTBAL");
  c(supplier, "S_COMMENT", true);

  for (size_t row = 0; row < supplier_count; ++row) {
    reportProgress("tpch.supplier", row, supplier_count);
    const auto key = s_suppkey.next();
    c(supplier, key);
    c(supplier, s_name.next(key));

    c(supplier, s_address.next());
    c(supplier, s_nationkey.next());
    c(supplier, s_acctbal.next());

    auto comment = s_comment_base.next();

    if (s_comment_chance.next() < fraction_customer_comment) {
      constexpr std::string_view s_comment_customer = "Customer";
      std::string_view second_string;
      if (s_comment_chance.next() < 0.5) {
        second_string = "Complaints";
      } else {
        second_string = "Recommends";
      }
      auto customer_offset = s_comment_chance.random(
          comment.size() - s_comment_customer.size() - second_string.size());
      comment.insert(customer_offset, s_comment_customer);
      auto second_string_offset = s_comment_chance.random(
          comment.size() - s_comment_customer.size() - customer_offset);
      comment.insert(customer_offset + second_string_offset, second_string);
    }
    c(supplier, comment, true);
  }
  state.registerGeneration("tpch.supplier", file_name);
}


void part_gen(GeneratorState& state) {
  const auto part_count = state.table("tpch.part").row_count;
  auto file_name = state.csvPath("tpch.part");
  std::ofstream part(file_name);
  using namespace generator;
  Key p_partkey;

  constexpr std::string_view p_name_strings[] = {
      "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black",
      "blanched", "blue",
      "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon",
      "chocolate", "coral",
      "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim",
      "dodger", "drab", "firebrick",
      "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green",
      "grey", "honeydew",
      "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon",
      "light", "lime", "linen",
      "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty",
      "moccasin", "navajo",
      "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru",
      "pink", "plum", "powder",
      "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon",
      "sandy", "seashell", "sienna",
      "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle",
      "tomato", "turquoise", "violet",
      "wheat", "white", "yellow"
  };
  Set<std::string_view> p_name(p_name_strings);
  Formatter p_mfgr("Manufacturer{}");
  IntegerRange p_mfgr_random(1, 5);
  Formatter p_brand("Brand{}{}");
  IntegerRange p_brand_random(1, 5);
  TpchTypes p_type;
  IntegerRange p_size(1, 50);
  TpchContainer p_container;
  TpchText p_comment_base(5, 22);

  for (size_t row = 0; row < part_count; ++row) {
    auto key = p_partkey.next();
    c(part, key);
    auto name_parts = p_name.n_of(5);
    std::string name = join(p_name.n_of(5), " ");
    c(part, name);
    auto M = p_mfgr_random.next();
    c(part, p_mfgr.next(M));
    auto N = p_brand_random.next();
    c(part, p_brand.next(M, N));
    c(part, p_type.next());
    c(part, p_size.next());
    c(part, p_container.next());
    c(part, p_comment_base.next());
  }
  state.registerGeneration("tpch.part", file_name);
}


void partsupp_gen(GeneratorState& state) {
  const size_t partsupp_count = state.table("tpch.partsupp").row_count;
  const auto file_name = state.csvPath("tpch.partsupp");
  std::ofstream partsupp(file_name);
  c(partsupp, "PS_PARTKEY");
  c(partsupp, "PS_SUPPKEY");
  c(partsupp, "PS_AVAILQTY");
  c(partsupp, "PS_SUPPLYCOST");
  c(partsupp, "PS_COMMENT");

  using namespace generator;
  Key p_partkey;
  IntegerRange ps_availqty(1, 9999);
  DoubleRange ps_supplycost(1, 1000.0);
  TpchText ps_comment(49, 198);

  for (size_t row = 0; row < partsupp_count / 4; ++row) {
    const auto ps_part_key = p_partkey.next();

    for (size_t i = 0; i < 4; ++i) {
      c(partsupp, ps_part_key);
      const uint64_t ps_supp_key = supplier_for_part(ps_part_key, i);
      c(partsupp, ps_supp_key);
      c(partsupp, ps_availqty.next());
      c(partsupp, ps_supplycost.next());
      c(partsupp, ps_comment.next(), true);
    }
  }
  state.registerGeneration("tpch.partsupp", file_name);
}


void customer_gen(GeneratorState& state) {
  const size_t customer_count = state.table("tpch.customer").row_count;
  auto file_name = state.csvPath("tpch.customer");
  std::ofstream customer(file_name);
  c(customer, "C_CUSTKEY");
  c(customer, "C_NAME");
  c(customer, "C_ADDRESS");
  c(customer, "C_NATIONKEY");
  c(customer, "C_PHONE");
  c(customer, "C_ACCTBAL");
  c(customer, "C_MKTSEGMENT");
  c(customer, "C_COMMENT", true);

  using namespace generator;
  Key c_custkey;
  Formatter c_name("Customer{:09}");
  VString c_address(10, 40);
  ForeignKey c_nationkey(0, 24);
  TpchPhone c_phone;
  DoubleRange c_acctbal(-999.99, 9999.99);
  constexpr std::string_view segments[] = {"AUTOMOBILE", "BUILDING",
                                           "FURNITURE", "HOUSEHOLD",
                                           "MACHINERY"};
  Set<std::string_view> c_mktsegment(segments);
  TpchText c_comment(29, 116);

  for (size_t row = 0; row < customer_count; ++row) {
    auto key = c_custkey.next();
    c(customer, key);
    c(customer, c_name.next(key));
    c(customer, c_address.next());
    c(customer, c_nationkey.next());
    c(customer, c_phone.next());
    c(customer, c_acctbal.next());
    c(customer, c_mktsegment.next());
    c(customer, c_comment.next(), true);
  }
  state.registerGeneration("tpch.customer", file_name);
}

void orders_lineitem_gen(GeneratorState& state) {
  const auto orders_count = state.table("tpch.orders").row_count;
  const auto orders_file_name = state.csvPath("tpch.orders");
  const auto lineitem_file_name = state.csvPath("tpch.lineitem");
  std::ofstream orders(orders_file_name);
  std::ofstream lineitem(lineitem_file_name);

  // LINEITEM header
  c(lineitem, "L_ORDERKEY");
  c(lineitem, "L_PARTKEY");
  c(lineitem, "L_SUPPKEY");
  c(lineitem, "L_LINENUMBER");
  c(lineitem, "L_QUANTITY");
  c(lineitem, "L_EXTENDEDPRICE");
  c(lineitem, "L_DISCOUNT");
  c(lineitem, "L_TAX");
  c(lineitem, "L_RETURNFLAG");
  c(lineitem, "L_LINESTATUS");
  c(lineitem, "L_SHIPDATE");
  c(lineitem, "L_COMMITDATE");
  c(lineitem, "L_RECEIPTDATE");
  c(lineitem, "L_SHIPINSTRUCT");
  c(lineitem, "L_SHIPMODE");
  c(lineitem, "L_COMMENT", true);

  // ORDER header
  c(orders, "O_ORDERKEY");
  c(orders, "O_CUSTKEY");
  c(orders, "O_ORDERSTATUS");
  c(orders, "O_TOTALPRICE");
  c(orders, "O_ORDERDATE");
  c(orders, "O_ORDERPRIORITY");
  c(orders, "O_CLERK");
  c(orders, "O_SHIPPRIORITY");
  c(orders, "O_COMMENT", true);

  using namespace generator;

  Key o_orderkey;
  IntegerRange<> o_custkey(1, TPCH_SF * 150000);
  IntegerRange<> lineitems_per_order(1, 7);
  IntegerRange<> l_partkey(1, TPCH_SF * part_count);
  IntegerRange<> l_suppkey_index(0, 3);
  DoubleRange l_quantity(1, 50);
  DoubleRange l_discount(0.00, 0.10);
  DoubleRange l_tax(0.00, 0.08);
  DateRange o_orderdate(STARTDATE, ENDDATE);
  IntegerRange<> l_shipdate_offset(1, 121);
  IntegerRange<> l_commitdate_offset(30, 90);
  IntegerRange<> l_receiptdate_offset(1, 30);
  constexpr std::string_view instructions[] = {"DELIVER IN PERSON",
                                               "COLLECT COD", "NONE",
                                               "TAKE BACK RETURN"};
  Set<std::string_view> l_shipinstruct(instructions);
  constexpr std::string_view returnflags[] = {"R", "A"};
  Set<std::string_view> l_returnflag(returnflags);
  constexpr std::string_view shipmodes[] = {"REG AIR", "AIR", "RAIL", "SHIP",
                                            "TRUCK", "MAIL", "FOB"};
  Set<std::string_view> l_shipmode(shipmodes);
  TpchText l_comment(10, 43);
  TpchText o_comment(19, 78);
  IntegerRange<> o_clerk_random(1, TPCH_SF * 1000);
  Formatter o_clerk("Clerk{:09}");
  constexpr std::string_view priorities[] = {"1-URGENT", "2-HIGH", "3-MEDIUM",
                                             "4-NOT SPECIFIED", "5-LOW"};
  Set<std::string_view> o_orderpriority(priorities);

  size_t lineitem_count = 0;
  size_t key_jump = 0;
  for (size_t row = 0; row < orders_count; ++row) {
    // TPC-h requires only every third customer has an order and enforces with a simple modulo
    uint64_t custkey = 0;
    for (; custkey % 3 != 0; custkey = o_custkey.next()) {
    }

    if (++key_jump == 8) {
      // See Comment on section 4.2.3 of TPC-H spec. This is to make the key range sparse
      o_orderkey.skip(custkey);
    }
    auto orderkey = o_orderkey.next();
    auto orderdate = o_orderdate.next();
    size_t linestatus_o_count = 0;
    size_t linestatus_f_count = 0;
    double totalprice = 0.0;

    const auto num_lines = lineitems_per_order.next();
    for (size_t li = 0; li < num_lines; ++li) {
      ++lineitem_count;

      c(lineitem, orderkey);
      const auto partkey = l_partkey.next();
      c(lineitem, partkey);
      const auto suppkey = supplier_for_part(partkey, l_suppkey_index.next());
      c(lineitem, suppkey);
      c(lineitem, li);
      const auto quantity = l_quantity.next();
      c(lineitem, quantity);
      const auto extended_price = quantity * price_for_part(partkey);
      c(lineitem, extended_price);
      const auto discount = l_discount.next();
      c(lineitem, discount);
      const auto tax = l_tax.next();
      c(lineitem, tax);

      totalprice += extended_price * (1 + tax) * (1 - discount);

      // dates must be calculated first, but the ordering of output column is not in this dependency order
      const auto shipdate = orderdate + days(l_shipdate_offset.next());
      const auto commitdate = orderdate + days(l_commitdate_offset.next());
      const auto receiptdate = shipdate + days(l_receiptdate_offset.next());

      // L_RETURNFLAG
      if (receiptdate < CURRENTDATE) {
        c(lineitem, l_returnflag.next());
      } else {
        c(lineitem, "N");
      }
      // L_LINESTATUS
      if (shipdate > CURRENTDATE) {
        ++linestatus_f_count;
        c(lineitem, "F");
      } else {
        ++linestatus_o_count;
        c(lineitem, "O");
      }
      c(lineitem, to_date_string(shipdate));
      c(lineitem, to_date_string(commitdate));
      c(lineitem, to_date_string(receiptdate));
      c(lineitem, l_shipinstruct.next());
      c(lineitem, l_shipmode.next());
      c(lineitem, l_comment.next(), true);
    }

    // O_ORDERSTATUS
    if (linestatus_o_count == 0) {
      c(orders, "F");
    } else if (linestatus_f_count == 0) {
      c(orders, "O");
    } else {
      c(orders, "P");
    }

    c(orders, orderkey);
    c(orders, custkey);
    c(orders, totalprice);
    c(orders, to_date_string(orderdate));
    c(orders, o_orderpriority.next());
    c(orders, o_clerk.next(o_clerk_random.next()));
    c(orders, "0"); // O_SHIPPRIORITY is indeed a const
    c(orders, o_comment.next(), true);

    reportProgress("orders", row, orders_count, "lineitem", lineitem_count);
  }
  state.registerGeneration("tpch.lineitem", lineitem_file_name);
  state.registerGeneration("tpch.orders", orders_file_name);
  state.table("tpch.lineitem").row_count = lineitem_count;
}