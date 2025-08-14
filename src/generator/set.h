#pragma once
#include <set>
#include <vector>
#include <map>
#include <span>
#include <random>
#include <iterator>

#include "generator_object.h"

namespace generator {
template <typename T>
class Set : GeneratorObject {
  std::map<size_t, T> map_;
  std::uniform_int_distribution<size_t> distribution_;

public:
  explicit Set(const std::span<const T> set) {
    size_t i = 0;
    for (auto& item : set) {
      map_[i++] = item;
    }
    if (i == 0)
      throw std::runtime_error("Cannot generate from an empty set");
    distribution_ = std::uniform_int_distribution<size_t>(0, i - 1);
  }

  T next() {
    size_t entry = distribution_(gen_);
    return map_.at(entry);
  }

  std::vector<T> n_of(size_t n) {
    if (n > map_.size())
      throw std::runtime_error("Request exceeds set size");
    std::vector<T> result;
    std::set<size_t> used;
    for (size_t s = 0; s < n; s = result.size()) {
      size_t idx = distribution_(gen_);
      if (used.insert(idx).second) {
        result.push_back(map_.at(idx));
      }
    }
    return result;
  }
};
}