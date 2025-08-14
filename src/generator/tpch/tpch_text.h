#pragma once
#include <cstdint>
#include <mutex>
#include <optional>
#include <random>
#include <string>

#include "variable_integer.h"
#include "weighted_select.h"

namespace generator {
/// @brief Random text following a grammar as defined by clause 4.2.2.10-14 of the TPC-H spec
class TpchText : GeneratorObject {
  const size_t minLength_;
  const size_t maxLength_;
  static std::string generatedText_;

  static void addWord(std::string_view word);

  static void generateNounPhrase();

  static void generateVerbPhrase();

  static void generatePrepositionalPhrase();

  /// @brief Generate the text buffer everything else is picked from
  static void generate();

  std::uniform_int_distribution<size_t> lengthDistribution_;
  VariableInteger offsetDistribution_;

public:
  explicit TpchText(const size_t min_length, const size_t max_length)
    : GeneratorObject(42)
    , minLength_(min_length)
    , maxLength_(max_length)
    , lengthDistribution_(
        std::uniform_int_distribution(
            min_length, max_length)) {
    generate();
  };

  static constexpr size_t max_generated_text_length = 300 * 1024 * 1024;

  std::string next() {
    const auto len = lengthDistribution_(gen_);

    const size_t offset = offsetDistribution_.next(max_generated_text_length - len);
    return generatedText_.substr(offset, len);
  }
};
}