#include <dbprove/generator/generator_state.h>

using generator::GeneratorState;


void region_download(GeneratorState& state, sql::ConnectionBase*) {
  state.downloadFromCloud("tpch", "region", "sf1/");
}

void nation_download(GeneratorState& state, sql::ConnectionBase*) {
  state.downloadFromCloud("tpch", "nation", "sf1/");
}

void orders_download(GeneratorState& state, sql::ConnectionBase*) {
  state.downloadFromCloud("tpch", "orders", "sf1/");
}

void lineitem_download(GeneratorState& state, sql::ConnectionBase*) {
  state.downloadFromCloud("tpch", "lineitem", "sf1/");
}

void partsupp_download(GeneratorState& state, sql::ConnectionBase*) {
  state.downloadFromCloud("tpch", "partsupp", "sf1/");
}

void part_download(GeneratorState& state, sql::ConnectionBase*) {
  state.downloadFromCloud("tpch", "part", "sf1/");
}

void supplier_download(GeneratorState& state, sql::ConnectionBase*) {
  state.downloadFromCloud("tpch", "supplier", "sf1/");
}

void customer_download(GeneratorState& state, sql::ConnectionBase*) {
  state.downloadFromCloud("tpch", "customer", "sf1/");
}
