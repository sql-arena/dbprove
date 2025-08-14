#pragma once
#include <cstdint>
#include <random>

#include "generator_object.h"

namespace generator {
template <typename T = uint64_t>
class ForeignKey : GeneratorObject {
  std::uniform_int_distribution<T> distribution_;

public:
  explicit ForeignKey(T minValue, T maxValue)
    : distribution_(
        std::uniform_int_distribution<T>(minValue, maxValue)) {
  }

  T next() {
    return distribution_(gen_);
  }
};
}