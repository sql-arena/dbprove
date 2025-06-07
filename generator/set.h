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
  std::map<size_t, const T*> map_;
  std::uniform_int_distribution<size_t> distribution_;

public:
  explicit Set(const std::span<const T> set) {
    unsigned i = 0;
    for (auto& item : set) {
      map_[i++] = &item;
    }
    if (i == 0)
      throw std::runtime_error("Cannot generate from an empty set");
    distribution_ = std::uniform_int_distribution<size_t>(0, i - 1);
  }

  const T* next() {
    size_t entry = distribution_(gen_);
    return map_.at(entry);
  }

  std::vector<const T*> n_of(unsigned n) {
    if (n > map_.size())
      throw std::runtime_error("Request exceeds set size");
    std::vector<const T*> result;
    std::set<size_t> used;
    for (auto s = 0; s < n; s = result.size()) {
      size_t idx = distribution_(gen_);
      if (used.insert(idx).second) {
        result.push_back(map_.at(idx));
      }
    }
    return result;
  }
};
}