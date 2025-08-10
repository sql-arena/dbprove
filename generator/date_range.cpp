#include "date_range.h"
using namespace std::chrono;

generator::DateRange::DateRange(const sys_days minValue, const sys_days maxValue)
  : minValue_(minValue)
  , maxValue_(maxValue)
  , distribution_(std::uniform_int_distribution(static_cast<int64_t>((maxValue - minValue).count()))) {
}

sys_days generator::DateRange::next() {
  return minValue_ + days(distribution_(gen_));
}