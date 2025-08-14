#include "tpch_types.h"

constexpr std::string_view syllable1[] = {"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"};
constexpr std::string_view syllable2[] = {"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"};
constexpr std::string_view syllable3[] = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};


std::string generator::TpchTypes::next() {
  std::string result;
  result += syllable1[random_source_.next(6)];
  result += " ";
  result += syllable2[random_source_.next(5)];
  result += " ";
  result += syllable3[random_source_.next(5)];
  return result;
}