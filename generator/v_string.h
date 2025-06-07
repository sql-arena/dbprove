#pragma once
#include <cstdint>
#include <random>
#include <stdexcept>

#include "generator_object.h"

namespace generator {
/**
 * @brief A V-string as defined in the TPC-H spec
 *
 * This is a random string between min and max length made of characters sampled from at least
 * 64 different letters
 */
class VString : GeneratorObject {
  /// Ripped from the TPC-H dbgen tool
  static constexpr char alpha_num[65] = "0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";

  const uint32_t minLength_;
  const uint32_t maxLength_;
  std::uniform_int_distribution<uint32_t> lengthDistribution_;
  std::uniform_int_distribution<char> charDistribution_;
  std::mt19937 genChar_;

public:
  explicit VString(const uint32_t min_length, const uint32_t max_length)
    : minLength_(min_length)
    , maxLength_(max_length)
    , genChar_(std::mt19937(std::random_device{}())) {
    if (minLength_ > maxLength_) {
      throw std::invalid_argument("minLength must be <= maxLength");
    }
    lengthDistribution_ = std::uniform_int_distribution(minLength_, maxLength_);
    charDistribution_ = std::uniform_int_distribution<char>(0, sizeof(alpha_num) - 1);
  }

  std::string next() {
    const auto len = lengthDistribution_(gen_);
    std::string result;
    result.resize(len);
    for (uint32_t i = 0; i < len; ++i) {
      result[i] = alpha_num[charDistribution_(genChar_)];
    }
    return result;
  }
};
}