#pragma once

namespace generator {
class GeneratorObject {
protected:
  std::mt19937 gen_;

  explicit GeneratorObject(const uint32_t seed = std::random_device{}())
    : gen_(std::mt19937(seed)) {
  };
};
}