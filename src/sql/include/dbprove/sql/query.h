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
  std::string text_;
  std::string text_tagged_;
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
  explicit Query(std::string text, const char* theorem = nullptr)
    : text_(std::move(text))
    , text_tagged_(tagSQL(text_, theorem)) {
  };
  explicit Query(std::string_view text, const char* theorem = nullptr)
    : text_(std::string(text))
    , text_tagged_(tagSQL(text_, theorem)) {
  };
  explicit Query(const char* text, const char* theorem = nullptr)
    : text_(text)
    , text_tagged_(tagSQL(text_, theorem)) {
  }

  Query(Query&& other) noexcept {
    text_ = std::move(other.text_);
    text_tagged_ = std::move(other.text_tagged_);
    stats_ = std::move(other.stats_);
    thread_stats_ = std::move(other.thread_stats_);
  };
  Query& operator=(Query&& other) noexcept {
    if (this != &other) {
      text_ = std::move(other.text_);
      text_tagged_ = std::move(other.text_tagged_);
      stats_ = std::move(other.stats_);
      // No need to move the mutex
    }
    return *this;
  }

  const std::string& text() const { return text_; }
  const std::string& textTagged() const { return text_tagged_; }

  QueryStats& start() {
    thread_stats_.push_back({});
    QueryStats& stat = thread_stats_.back();
    stat.start_time = std::chrono::steady_clock::now();
    return thread_stats_.back();
  }

  void stop(QueryStats& stat) {
    const auto end_time = std::chrono::steady_clock::now();
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