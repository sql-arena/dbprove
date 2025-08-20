#pragma once
#include <string>
#include <map>

inline int operation_sort(const std::string& operation) {
  const static std::map<std::string, int> operation_priority = {
      {"Join", 1},
      {"Aggregate", 2},
      {"Sort", 3},
      {"Scan", 4}
  };
  return operation_priority.at(operation);
}

inline std::vector<std::string> operation_order() {
  return {"Join", "Aggregate", "Sort", "Scan"};
}