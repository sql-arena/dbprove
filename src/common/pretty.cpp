#include "include/dbprove/common/pretty.h"

namespace dbprove::common {
std::string PrettyHumanCount(const size_t count) {
  constexpr size_t TARGET_SIZE = 8;
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
    return "       ∞";
  }

  if (result.size() < TARGET_SIZE) {
    result.insert(result.begin(), TARGET_SIZE - result.size(), ' '); // Pad with spaces
  }
  return result;
}
}