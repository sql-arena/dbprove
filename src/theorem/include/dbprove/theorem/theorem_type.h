#pragma once
#include <string_view>
#include <array>
#include <utility>

enum class TheoremType {
  CLI,
  EE,
  SE,
  PLAN,
  WLM
};


constexpr std::pair<std::string_view, TheoremType> theorem_types[] = {
    {"CLI", TheoremType::CLI},
    {"EE", TheoremType::EE},
    {"SE", TheoremType::SE},
    {"PLAN", TheoremType::PLAN},
    {"WLM", TheoremType::WLM}
};

constexpr std::array<std::string_view, 5> theorem_names = {"CLI", "EE", "SE", "PLAN", "WLM"};