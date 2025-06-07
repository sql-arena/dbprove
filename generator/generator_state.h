#pragma once
#include <assert.h>
#include <filesystem>
#include <map>
#include <functional>
#include "generated_table.h"

namespace generator {
class GeneratorState;
using GeneratorFunc = std::function<void(GeneratorState&)>;


class GeneratorState {
  friend struct Registrar;
  const std::filesystem::path basePath_;
  std::map<std::string_view, std::unique_ptr<GeneratedTable>> tables_{};
  static constexpr std::string_view colSeparator_ = "|";
  static constexpr std::string_view rowSeparator_ = "\n";
  static std::map<std::string_view, GeneratorFunc>& generators();

public:
  explicit GeneratorState(const std::filesystem::path& basePath)
    : basePath_(basePath) {
  }

  /// @brief Generate a table (if not already made) and return the row count
  size_t generate(std::string_view name);
  void registerGeneration(std::string_view name, size_t rowCount, const std::filesystem::path& filePath);
  const GeneratedTable& table(std::string_view name) const;
  [[nodiscard]] const std::filesystem::path& basePath() const { return basePath_; }
  static constexpr std::string_view columnSeparator() { return colSeparator_; }
  static constexpr std::string_view rowSeparator() { return rowSeparator_; }
};

struct Registrar {
  Registrar(std::string_view name, const GeneratorFunc& f);
};
}

/**
 * Macros to register generator functions.
 *
 *  Usage:
 *      REGISTER_GENERATOR("<name>", <funcPtr>);
 *
 */

#define CONCATENATE_DETAIL(x, y) x##y
#define CONCATENATE(x, y) CONCATENATE_DETAIL(x, y)

#define REGISTER_GENERATOR(NAME, FUNC) \
    extern void FUNC(generator::GeneratorState&); \
    static inline generator::Registrar CONCATENATE(_registrar_, __COUNTER__)(NAME, FUNC);
