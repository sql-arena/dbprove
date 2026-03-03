#include <dbprove/generator/generator_state.h>

using generator::GeneratorState;


void region_download(GeneratorState& state, sql::ConnectionBase*) {
  state.prepareFileInput("tpch", "region", "sf1/");
}

void nation_download(GeneratorState& state, sql::ConnectionBase*) {
  state.prepareFileInput("tpch", "nation", "sf1/");
}

void orders_download(GeneratorState& state, sql::ConnectionBase*) {
  state.prepareFileInput("tpch", "orders", "sf1/");
}

void lineitem_download(GeneratorState& state, sql::ConnectionBase*) {
  state.prepareFileInput("tpch", "lineitem", "sf1/");
}

void partsupp_download(GeneratorState& state, sql::ConnectionBase*) {
  state.prepareFileInput("tpch", "partsupp", "sf1/");
}

void part_download(GeneratorState& state, sql::ConnectionBase*) {
  state.prepareFileInput("tpch", "part", "sf1/");
}

void supplier_download(GeneratorState& state, sql::ConnectionBase*) {
  state.prepareFileInput("tpch", "supplier", "sf1/");
}

void customer_download(GeneratorState& state, sql::ConnectionBase*) {
  state.prepareFileInput("tpch", "customer", "sf1/");
}
