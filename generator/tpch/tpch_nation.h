#pragma once
#include <map>
#include <string_view>

constexpr std::pair<std::string_view, size_t> tpch_nations[] = {
    {"ALGERIA", 0},
    {"ARGENTINA", 1},
    {"BRAZIL", 1},
    {"CANADA", 1},
    {"EGYPT", 4},
    {"ETHIOPIA", 0},
    {"FRANCE", 3},
    {"GERMANY", 3},
    {"INDIA", 2},
    {"INDONESIA", 2},
    {"IRAN", 4},
    {"IRAQ", 4},
    {"JAPAN", 2},
    {"JORDAN", 4},
    {"KENYA", 0},
    {"MOROCCO", 0},
    {"MOZAMBIQUE", 0},
    {"PERU", 1},
    {"CHINA", 2},
    {"ROMANIA", 3},
    {"SAUDI ARABIA", 4},
    {"VIETNAM", 2},
    {"RUSSIA", 3},
    {"UNITED KINGDOM", 3},
    {"UNITED STATES", 1}
};

constexpr std::string_view tpch_regions[] = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};