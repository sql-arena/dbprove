#include "theorem.h"
#include <dbprove/ux/ux.h>
#include "query.h"
#include <string>

namespace dbprove::theorem {
void DataExplain::render(std::ostream& out) {
  ux::Header(out, "Query Plan", 10);
  plan->render(out, ux::Terminal::width());

  std::vector<ux::RowStats> stats;
  stats.push_back({"Joined", plan->rowsJoined()});
  stats.push_back({"Aggregated", plan->rowsAggregated()});
  stats.push_back({"Sorted", plan->rowsSorted()});
  stats.push_back({"Scanned", plan->rowsScanned()});
  RowStatTable(out, stats);
}


void DataQuery::render(std::ostream& out) {
  ux::Header(out, "Query", 10);
  out << query.text() << std::endl;
}
}