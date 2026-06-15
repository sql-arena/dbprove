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

void validateExpectedRowValues(const Query& query, const sql::RowBase& row) {
  if (!query.expectedRowValues().has_value()) {
    return;
  }
  const auto& expected_values = *query.expectedRowValues();
  if (row.columnCount() != expected_values.size()) {
    throw std::runtime_error("Expected " + std::to_string(expected_values.size())
                             + " columns in theorem result row but got "
                             + std::to_string(row.columnCount()) + " for query: " + query.text());
  }

  for (size_t column_index = 0; column_index < expected_values.size(); ++column_index) {
    const auto actual = row[column_index].asString();
    const auto expected = expected_values[column_index].asString();
    if (actual != expected) {
      throw std::runtime_error("Unexpected scalar result at column " + std::to_string(column_index)
                               + " for query: " + query.text()
                               + ". Expected '" + expected + "' but got '" + actual + "'");
    }
  }
}

void validateExpectedRowCount(const Query& query, const Proof& proof,
                              const std::optional<sql::RowCount> expected_row_count,
                              const sql::RowCount actual_row_count) {
  if (!expected_row_count.has_value()) {
    return;
  }
  if (proof.artifactMode()) {
    PLOGI << "Artifact mode: skipping row count validation for theorem '" << proof.theorem.name << "'";
    return;
  }
  if (actual_row_count == *expected_row_count) {
    return;
  }

  PLOGE << "Theorem '" << proof.theorem.name << "' expected " << *expected_row_count
        << " rows, but query returned " << actual_row_count;
  throw sql::UnexpectedRowCountException(*expected_row_count, actual_row_count, query.text());
}

sql::RowCount executeMeasuredQuery(sql::ConnectionBase& connection, Query& query) {
  auto& qs = query.start();
  if (query.expectedRowValues().has_value()) {
    auto row = connection.fetchRow(query.textTagged());
    query.stop(qs);
    query.summariseThread();
    validateExpectedRowValues(query, *row);
    return 1;
  }

  auto result = connection.fetchAll(query.textTagged());
  result->drain();
  query.stop(qs);
  query.summariseThread();
  return result->rowCount();
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
      auto& qs = query.start();
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
      auto& qs = query.start();
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

    auto& qs = query.start();
    connection->execute(query.textTagged());
    query.stop(qs);
    query.summariseThread();
    connection->close();
  };

  do_threads(threadCount, thread_work);
}

void Runner::serialExplain(std::span<Query>& queries, Proof& proof) const {
  const auto connection = factory_.create();
  connection->setQueryTimeout(proof.queryTimeoutSeconds());
  for (auto& query : queries) {
    proof.data.push_back(std::make_unique<DataQuery>(query));
    if (!proof.artifactMode()) {
      auto& qs = query.start();
      auto result = connection->fetchAll(query.textTagged());
      result->drain();
      query.stop(qs);
      query.summariseThread();
      validateExpectedRowCount(query, proof, expectedRowCountFor(query, proof, queries.size()), result->rowCount());
    }
    auto explain = connection->explain(query.textTagged(), proof.theorem.name);
    proof.data.push_back(std::make_unique<DataExplain>(std::move(explain)));
  }
  connection->close();
  proof.render();
}

void Runner::serialMeasure(std::span<Query>& queries, Proof& proof, const size_t iterations) const {
  if (proof.artifactMode()) {
    throw std::runtime_error(
        "Artifact replay mode only supports explain-based theorem runs backed by generated artifacts");
  }
  const auto connection = factory_.create();
  connection->setQueryTimeout(proof.queryTimeoutSeconds());
  for (auto& query : queries) {
    proof.data.push_back(std::make_unique<DataQuery>(query));
    for (size_t iteration = 0; iteration < iterations; ++iteration) {
      const auto row_count = executeMeasuredQuery(*connection, query);
      validateExpectedRowCount(query, proof, expectedRowCountFor(query, proof, queries.size()), row_count);
    }
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

void Runner::serialMeasure(Query&& query, Proof& state, const size_t iterations) const
{
  if (!query.expectedRowCount().has_value() && state.theorem.expectedRowCount().has_value()) {
    query.setExpectedRowCount(state.theorem.expectedRowCount());
  }
  std::vector<Query> queries;
  queries.push_back(std::move(query));
  auto span = std::span(queries);
  serialMeasure(span, state, iterations);
}
}
