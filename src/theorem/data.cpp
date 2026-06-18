#include "theorem.h"
#include <dbprove/ux/ux.h>
#include "query.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <numeric>
#include <string>
#include <sstream>

#include <internal/basic_csv_parser.hpp>

namespace dbprove::theorem {
namespace {
std::string lowerCamelFromLabel(std::string label) {
  if (label.empty()) {
    return label;
  }

  label[0] = static_cast<char>(std::tolower(static_cast<unsigned char>(label[0])));
  return label;
}

std::string formatWallClockTimestamp(
    const std::chrono::time_point<std::chrono::system_clock>& timestamp) {
  const auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(timestamp);
  const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(timestamp - seconds).count();
  const auto time_t = std::chrono::system_clock::to_time_t(seconds);

  std::tm utc_time {};
#if defined(_WIN32)
  gmtime_s(&utc_time, &time_t);
#else
  gmtime_r(&time_t, &utc_time);
#endif

  std::ostringstream out;
  out << std::put_time(&utc_time, "%Y-%m-%dT%H:%M:%S")
      << '.' << std::setw(6) << std::setfill('0') << micros << 'Z';
  return out.str();
}
}  // namespace

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
  const auto seeked = plan->rowsSeeked();
  const auto scanned = plan->rowsScanned();
  const auto distributed = plan->rowsDistributed();
  const auto hash_build = plan->rowsHashBuild();
  std::vector<ux::RowStats> stats;
  stats.push_back({"Join", joined});
  stats.push_back({"Hash", hash_build});
  stats.push_back({"Aggregate", aggregated});
  stats.push_back({"Sort", sorted});
  stats.push_back({"Distribute", distributed});
  stats.push_back({"Seek", seeked});
  stats.push_back({"Scan", scanned});
  RowStatTable(out, stats);

  proof.setCurrentQueryOperatorRows("join", static_cast<int64_t>(joined));
  proof.setCurrentQueryOperatorRows("aggregate", static_cast<int64_t>(aggregated));
  proof.setCurrentQueryOperatorRows("sort", static_cast<int64_t>(sorted));
  proof.setCurrentQueryOperatorRows("distribute", static_cast<int64_t>(distributed));
  proof.setCurrentQueryOperatorRows("seek", static_cast<int64_t>(seeked));
  proof.setCurrentQueryOperatorRows("scan", static_cast<int64_t>(scanned));
  proof.setCurrentQueryOperatorRows("hash", static_cast<int64_t>(hash_build));
  auto mis_estimates = plan->misEstimations();
  ux::EstimationStatTable(out, mis_estimates);
  std::ranges::sort(mis_estimates, [](const auto& lhs, const auto& rhs) {
    if (lhs.operation != rhs.operation) {
      return lhs.operation < rhs.operation;
    }
    return lhs.magnitude < rhs.magnitude;
  });

  for (const auto& [operation, magnitude, count] : mis_estimates) {
    proof.setCurrentQueryMisEstimate(lowerCamelFromLabel(std::string(to_string(operation))),
                                     magnitude.to_string(),
                                     static_cast<int64_t>(count));
  }
  const std::string csv_plan;
  std::ostringstream plan_stream(csv_plan);
  plan->render(plan_stream, 500);
  proof.setCurrentQueryPlan(plan_stream.str());
}

void DataQuery::render(Proof& proof) {
  auto& out = proof.console();
  ux::Header(out, "Query", 10);
  out << query.text() << std::endl;
  proof.beginQuery(query.text());

  const auto& stats = query.stats();
  if (stats.empty()) {
    return;
  }

  proof.setCurrentQueryStartTime(formatWallClockTimestamp(stats.front().start_wall_time));

  const auto [min_it, max_it] = std::minmax_element(stats.begin(), stats.end(), [](const QueryStats& lhs, const QueryStats& rhs) {
    return lhs.duration < rhs.duration;
  });

  const auto total_duration = std::accumulate(stats.begin(), stats.end(), std::chrono::microseconds::zero(),
                                              [](const std::chrono::microseconds total, const QueryStats& stat) {
                                                return total + stat.duration;
                                              });
  const auto avg_duration = total_duration / static_cast<int64_t>(stats.size());
  const auto min_duration = min_it->duration;

  const double mean_us = static_cast<double>(avg_duration.count());
  const double variance_us = std::accumulate(stats.begin(), stats.end(), 0.0,
                                             [mean_us](const double total, const QueryStats& stat) {
                                               const double delta = static_cast<double>(stat.duration.count()) - mean_us;
                                               return total + (delta * delta);
                                             }) / static_cast<double>(stats.size());
  const double stddev_us = std::sqrt(variance_us);

  out << "Runs: " << stats.size()
      << ", best: " << min_duration.count() << " us"
      << ", avg: " << avg_duration.count() << " us"
      << ", stddev: " << std::llround(stddev_us) << " us"
      << ", min: " << min_duration.count() << " us"
      << ", max: " << max_it->duration.count() << " us" << std::endl;

  proof.setCurrentQueryBestRuntimeMicroseconds(min_duration.count());
  proof.setRuntimeSummaryMicroseconds(min_duration.count(),
                                      avg_duration.count(),
                                      min_it->duration.count(),
                                      max_it->duration.count(),
                                      stddev_us);
}
}
