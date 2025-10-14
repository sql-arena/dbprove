#include "theorem.h"
#include <ranges>

namespace dbprove::theorem {
static std::map<Category, std::string_view> typeMap_ = {{Category::CLI, "CLI"}, {Category::PLAN, "PLAN"}};

std::string_view typeName(const Category type) {
  if (!typeMap_.contains(type)) {
    throw std::runtime_error("Unknown theorem type");
  }
  return typeMap_.at(type);
}

Tag::Tag(std::string name)
  : name(std::move(name)) {
  if (this->name.empty()) {
    throw std::runtime_error("Tag name cannot be empty");
  }
  if (this->name.find_first_of("|,\n") != std::string::npos) {
    throw std::runtime_error("Tag name cannot contain '|', newlines or ',' as that is needed for CSV rendering");
  }
}

Category typeEnum(const std::string& type_name) {
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