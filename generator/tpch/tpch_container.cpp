#include "tpch_container.h"
constexpr std::string_view syllable1[] = {"SM", "LG", "MED", "JUMBO", "WRAP"};
constexpr std::string_view syllable2[] = {"CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"};

std::string generator::TpchContainer::next() {
  std::string result;
  result += syllable1[random_source_.next(5)];
  result += " ";
  result += syllable2[random_source_.next(8)];
  return result;
}