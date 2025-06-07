#pragma once
#include <string>
#include <vector>
#include <chrono>
#include <mutex>


struct QueryStats {
  std::chrono::time_point<std::chrono::steady_clock> start_time;
  std::chrono::microseconds duration;
  bool success = true;
  size_t rows_affected = 0;
};

class Query {
  const std::string text_;
  const std::string text_tagged_;
  std::mutex stats_mutex_;
  std::vector<QueryStats> stats_;
  static thread_local std::vector<QueryStats> thread_stats_;

  static std::string tagSQL(const std::string& sql, const char* prefix) {
    if (!prefix) {
      return sql;
    }
    return "/*" + std::string(prefix) + "*/" + sql;
  }

public:
  explicit Query(std::string&& text, const char* theorem = nullptr)
    : text_(std::move(text))
    , text_tagged_(tagSQL(text_, theorem)) {
  };
  const std::string& text() const { return text_; }
  const std::string& textTagged() const { return text_tagged_; }

  QueryStats& start() {
    thread_stats_.push_back({});
    QueryStats& stat = thread_stats_.back();
    stat.start_time = std::chrono::steady_clock::now();
    return thread_stats_.back();
  }

  void stop(QueryStats& stat) {
    auto end_time = std::chrono::steady_clock::now();
    stat.duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - stat.start_time);
  }

  void summariseThread() {
    std::lock_guard lock(stats_mutex_);
    stats_.reserve(stats_.size() + thread_stats_.size());
    for (auto& s : thread_stats_) {
      stats_.emplace_back(std::move(s));
    }
  }
};