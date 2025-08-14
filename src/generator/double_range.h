#pragma once
#include <random>

#include "generator_object.h"

namespace generator {
class DoubleRange : GeneratorObject {
  const double minValue_;
  const double maxValue_;
  std::uniform_real_distribution<> distribution_;

public:
  explicit DoubleRange(const double minValue, const double maxValue)
    : minValue_(minValue)
    , maxValue_(maxValue)
    , distribution_(std::uniform_real_distribution(minValue, maxValue)) {
  }

  double next() {
    return distribution_(gen_);
  }

  size_t random(const size_t n) {
    const auto r = distribution_(gen_);
    return static_cast<size_t>(r * static_cast<double>(n));
  }
};
}