#include "theorem.h"
#include <ranges>

namespace dbprove::theorem {
static std::map<Type, std::string_view> typeMap_ = {
    {Type::CLI, "CLI"},
    {Type::PLAN, "PLAN"}
};

std::string_view typeName(const Type type) {
  if (!typeMap_.contains(type)) {
    throw std::runtime_error("Unknown theorem type");
  }
  return typeMap_.at(type);
}

Type typeEnum(const std::string& type_name) {
  for (const auto& [type, name] : typeMap_) {
    if (name == type_name) {
      return type;
    }
  }
  throw std::runtime_error("Unknown theorem type " + std::string(type_name));
}

std::set<std::string_view> allTypeNames() {
  std::set<std::string_view> typeNames;
  for (const auto& type : typeMap_ | std::views::keys) {
    typeNames.insert(to_string(type));
  }
  return typeNames;
}
}