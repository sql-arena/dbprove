#include "date_range.h"

#include <plog/Log.h>

#include <cassert>
#include <iostream>
using namespace std::chrono;

generator::DateRange::DateRange(const sys_days minValue, const sys_days maxValue)
  : minValue_(minValue)
  , maxValue_(maxValue)
  , distribution_(std::uniform_int_distribution(static_cast<int64_t>(0),
                                                static_cast<int64_t>((maxValue - minValue).count())))

 {
  if (maxValue_ < minValue_) {
    throw std::invalid_argument("maxValue must be greater than minValue when constructing a DateRange");
  }

}

sys_days generator::DateRange::next() {
  return minValue_ + days(distribution_(gen_));
}