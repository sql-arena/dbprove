#include "theorem.h"
#include <dbprove/ux/ux.h>
#include "query.h"
#include <string>
#include <sstream>

#include <internal/basic_csv_parser.hpp>

namespace dbprove::theorem {
void DataExplain::render(Proof& proof) {
  using namespace sql::explain;
  auto& out = proof.console();
  ux::Header(out, "Query Plan", 10);
  if (!plan) {
    throw std::runtime_error(
        "No plan available for theorem: " + proof.theorem.name +
        " this indicates an error in the client library for Engine: " + proof.factory().engine().name());
  }
  plan->render(out, ux::Terminal::width());

  const auto joined = plan->rowsJoined();
  const auto aggregated = plan->rowsAggregated();
  const auto sorted = plan->rowsSorted();
  const auto scanned = plan->rowsScanned();
  const auto hash_build = plan->rowsHashBuild();
  std::vector<ux::RowStats> stats;
  stats.push_back({"Join Probe", joined});
  stats.push_back({"Join Build", hash_build});
  stats.push_back({"Aggregate", aggregated});
  stats.push_back({"Sort", sorted});
  stats.push_back({"Scan", scanned});
  RowStatTable(out, stats);

  proof.writeCsv("Join", std::to_string(joined), Unit::Rows);
  proof.writeCsv("Aggregate", std::to_string(aggregated), Unit::Rows);
  proof.writeCsv("Sort", std::to_string(sorted), Unit::Rows);
  proof.writeCsv("Scan", std::to_string(scanned), Unit::Rows);
  auto mis_estimates = plan->misEstimations();
  ux::EstimationStatTable(out, mis_estimates);
  // For CSV dump, it looks better to collect all operations together.
  std::ranges::sort(mis_estimates, [](const auto& lhs, const auto& rhs) {
    if (lhs.operation != rhs.operation) {
      return lhs.operation < rhs.operation;
    }
    return lhs.magnitude < rhs.magnitude;
  });

  for (const auto& [operation, magnitude, count] : mis_estimates) {
    const auto name = "Mis-estimate " + std::string(to_string(operation)) + " " + magnitude.to_string();
    proof.writeCsv(name, std::to_string(count), Unit::Magnitude);
  }
  const std::string csv_plan;
  std::ostringstream plan_stream(csv_plan);
  plan->render(plan_stream, 500);
  proof.writeCsv("Plan", csv_plan, Unit::Plan);
}

void DataQuery::render(Proof& proof) {
  auto& out = proof.console();
  ux::Header(out, "Query", 10);
  out << query.text() << std::endl;
}
}