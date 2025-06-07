#include "generator_state.h"


std::map<std::string_view, generator::GeneratorFunc>&
generator::GeneratorState::generators() {
  static std::map<std::string_view, GeneratorFunc> registry;
  return registry;
}

size_t generator::GeneratorState::generate(const std::string_view name) {
  if (tables_.contains(name)) {
    return tables_.at(name)->rowCount();
  }
  if (!generators().contains(name)) {
    throw std::runtime_error(
        "Generator not found for table: " + std::string(name));
  }
  generators().at(name)(*this);
  if (!tables_.contains(name)) {
    throw std::runtime_error("After generation the table " + std::string(name)
                             + " was not in the map. This likely means the generator forgot to call registerGeneration");
  }
  return tables_.at(name)->rowCount();
}

void generator::GeneratorState::registerGeneration(
    std::string_view name, const size_t rowCount,
    const std::filesystem::path& filePath) {
  auto table = std::make_unique<GeneratedTable>(name, rowCount, filePath);

  tables_.emplace(table->name(), std::move(table));
}

const generator::GeneratedTable& generator::GeneratorState::table(
    const std::string_view name) const {
  if (!tables_.contains(name)) {
    throw std::runtime_error(
        "Table not found: " + std::string(name) +
        ". Did you forget to generate it?");
  }
  return *tables_.at(name);
}

generator::Registrar::Registrar(std::string_view name, const GeneratorFunc& f) {
  GeneratorState::generators()[name] = f;
}