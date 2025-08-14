#include "tpch_phone.h"
#include "tpch_nation.h"

generator::TpchPhone::TpchPhone()
  : local_number1_(100, 999)
  , local_number2_(100, 999)
  , local_number3_(1000, 9999)
  , num_nations_(std::size(tpch_nations))
  , country_code_(0, num_nations_) {
}

std::string generator::TpchPhone::next() {
  std::string r;
  r.reserve(15);
  r += std::to_string(country_code_.next() + 10);
  r += "-";
  r += std::to_string(local_number1_.next());
  r += "-";
  r += std::to_string(local_number2_.next());
  r += "-";
  r += std::to_string(local_number3_.next());
  return r;
}