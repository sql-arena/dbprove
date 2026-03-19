#include "runner.h"
#include "theorem.h"
#include "query.h"
#include <dbprove/sql/sql_exceptions.h>
#include <plog/Log.h>
#include <functional>
#include <thread>
#include <vector>

namespace dbprove::theorem {
namespace {
std::optional<sql::RowCount> expectedRowCountFor(const Query& query, const Proof& proof, const size_t query_count) {
  if (query.expectedRowCount().has_value()) {
    return query.expectedRowCount();
  }
  if (proof.theorem.expectedRowCount().has_value() && query_count != 1) {
    throw std::logic_error("Theorem '" + proof.theorem.name
                           + "' has a theorem-level expected row count but runs multiple queries. "
                             "Set the expected row count on each Query instead.");
  }
  if (query_count == 1) {
    return proof.theorem.expectedRowCount();
  }
  return std::nullopt;
}

void validateExpectedRowCount(sql::ConnectionBase& connection, const Query& query, const Proof& proof,
                              const std::optional<sql::RowCount> expected_row_count) {
  if (!expected_row_count.has_value()) {
    return;
  }
  if (proof.artifactMode()) {
    PLOGI << "Artifact mode: skipping row count validation for theorem '" << proof.theorem.name << "'";
    return;
  }

  auto result = connection.fetchAll(query.textTagged());
  result->drain();
  const auto actual_row_count = result->rowCount();
  if (actual_row_count == *expected_row_count) {
    return;
  }

  PLOGE << "Theorem '" << proof.theorem.name << "' expected " << *expected_row_count
        << " rows, but query returned " << actual_row_count;
  throw sql::UnexpectedRowCountException(*expected_row_count, actual_row_count, query.text());
}
}

void do_threads(const size_t threadCount, std::function<void()> thread_work) {
  std::vector<std::thread> threads;
  for (size_t i = 0; i < threadCount; ++i) {
    threads.emplace_back(thread_work);
  }
  for (auto& thread : threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

void Runner::serial(const std::span<Query>& queries, const size_t iterations) const {
  const auto connection = factory_.create();
  for (size_t i = 0; i < iterations; ++i) {
    for (auto& query : queries) {
      auto qs = query.start();
      connection->execute(query.textTagged());
      query.stop(qs);
      query.summariseThread();
    }
  }
  connection->close();
}

void Runner::serial(Query& query, size_t iterations) const {
  return serial(std::span(&query, 1), iterations);
}

void Runner::parallelApart(const size_t threadCount, std::span<Query>& queries) const {
  auto thread_work = [this, &queries]() {
    const auto connection = factory_.create();

    for (auto& query : queries) {
      auto qs = query.start();
      connection->execute(query.textTagged());
      query.stop(qs);
      query.summariseThread();
    }
    connection->close();
  };
  do_threads(threadCount, thread_work);
}

void Runner::parallelTogether(size_t threadCount, std::span<Query>& queries) const {
  std::atomic<size_t> query_index{0};
  auto thread_work = [this, &queries, &query_index]() {
    auto connection = factory_.create();
    size_t index = query_index++;
    if (index > queries.size()) {
      return;
    }
    auto& query = queries[index];

    auto qs = query.start();
    connection->execute(query.textTagged());
    query.stop(qs);
    query.summariseThread();
    connection->close();
  };

  do_threads(threadCount, thread_work);
}

void Runner::serialExplain(std::span<Query>& queries, Proof& proof) const {
  const auto connection = factory_.create();
  for (auto& query : queries) {
    DataQuery(query).render(proof);
    auto qs = query.start();
    validateExpectedRowCount(*connection, query, proof, expectedRowCountFor(query, proof, queries.size()));
    auto explain = connection->explain(query.textTagged(), proof.theorem.name);
    query.stop(qs);
    proof.data.push_back(std::make_unique<DataExplain>(std::move(explain)));
  }
  connection->close();
  proof.render();
}

void Runner::serialExplain(Query&& query, Proof& state) const
{
  if (!query.expectedRowCount().has_value() && state.theorem.expectedRowCount().has_value()) {
    query.setExpectedRowCount(state.theorem.expectedRowCount());
  }
  std::vector<Query> queries;
  queries.push_back(std::move(query));
  auto span = std::span(queries);
  serialExplain(span, state);
}
}
