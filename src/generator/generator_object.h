#pragma once
#include <random>

namespace generator {
class GeneratorObject {
protected:
  std::ranlux24_base gen_;

  explicit GeneratorObject(const uint32_t seed = std::random_device{}())
    : gen_(std::ranlux24_base(seed)) {
  };
};
}