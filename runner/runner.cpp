#include "runner.h"

#include <functional>
#include <thread>
#include <vector>

#include "Spinner.h"

void do_threads(size_t threadCount, std::function<void()> thread_work) {
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


void Runner::Serial(const std::span<Query>& queries, size_t iterations) const {
  const auto connection = factory_.create();
  for (size_t i = 0; i < iterations; ++i) {
    for (auto& query : queries) {
      auto qs = query.start();
      connection->execute(query.textTagged());
      query.stop(qs);
      ux::Spin();
      query.summariseThread();
    }
  }
  connection->close();
}

void Runner::Serial(Query& query, size_t iterations) const {
  return Serial(std::span(&query, 1), iterations);
}

void Runner::ParallelApart(size_t threadCount, std::span<Query>& queries) const {
  auto thread_work = [this, &queries]() {
    const auto connection = factory_.create();

    for (auto& query : queries) {
      auto qs = query.start();
      connection->execute(query.textTagged());
      query.stop(qs);
      ux::Spin();
      query.summariseThread();
    }
    connection->close();
  };
  do_threads(threadCount, thread_work);
}

void Runner::ParallelTogether(size_t threadCount, std::span<Query>& queries) const {
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
    ux::Spin();
    query.summariseThread();
    connection->close();
  };

  do_threads(threadCount, thread_work);
}