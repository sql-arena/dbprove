#include "runner.h"
#include "theorem.h"
#include "query.h"
#include <functional>
#include <thread>
#include <vector>

namespace dbprove::theorem {
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
    proof.data.push_back(std::make_unique<DataQuery>(query));
    auto qs = query.start();
    auto explain = connection->explain(query.textTagged());
    query.stop(qs);
    proof.data.push_back(std::make_unique<DataExplain>(std::move(explain)));
  }
  connection->close();
  proof.render();
}

void Runner::serialExplain(Query&& query, Proof& state) const
{
  std::vector<Query> queries;
  queries.push_back(std::move(query));
  auto span = std::span(queries);
  serialExplain(span, state);
}
}
