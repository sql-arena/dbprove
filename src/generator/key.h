#pragma once
#include <atomic>
#include <cstdint>

namespace generator {
template <typename T = uint64_t>
class Key {
  std::atomic<T> current_;

public:
  Key()
    : current_(1) {
  }

  /// @brief Skip past keys
  T skip(T keys_to_skip) {
    current_ += keys_to_skip;
    return current_;
  }

  T next() {
    return current_++;
  }
};
}