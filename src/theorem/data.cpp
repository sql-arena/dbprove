#include "theorem.h"
#include <dbprove/ux/ux.h>
#include "query.h"
#include <string>

namespace dbprove::theorem {
void DataExplain::render(std::ostream& out) {
  using namespace sql::explain;
  ux::Header(out, "Query Plan", 10);
  plan->render(out, ux::Terminal::width());

  std::vector<ux::RowStats> stats;
  stats.push_back({"Join", plan->rowsJoined()});
  stats.push_back({"Aggregate", plan->rowsAggregated()});
  stats.push_back({"Sort", plan->rowsSorted()});
  stats.push_back({"Scan", plan->rowsScanned()});
  RowStatTable(out, stats);

  ux::EstimationStatTable(out, plan->misEstimations());
}


void DataQuery::render(std::ostream& out) {
  ux::Header(out, "Query", 10);
  out << query.text() << std::endl;
}


// TODO: provide CSV rendering
}