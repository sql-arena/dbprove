#include "include/dbprove/common/pretty.h"

namespace dbprove::common {
std::string pad(std::string result) {
  constexpr size_t TARGET_SIZE = 8;
  if (result.size() < TARGET_SIZE) {
    result.insert(result.begin(), TARGET_SIZE - result.size(), ' '); // Pad with spaces
  }
  return result;
}

std::string PrettyHumanCount(const size_t count) {
  std::string result;
  if (count < 100'000'000) {
    result = std::to_string(count);
  } else if (count < 10'000'000'000) {
    result = std::to_string(count / 1'000'000) + "M";
  } else if (count < 1000'000'000'000'000) {
    result = std::to_string(count / 1000'000'000'000) + "B";
  } else if (count < 1000'000'000'000'000'000) {
    result = std::to_string(count / 1000'000'000'000'000) + "T";
  } else {
    // INF is a multi byte char, so can't just pad
    return "       ∞";
  }

  return pad(result);
}

std::string PrettyUnknown() {
  return pad("-");
}
}