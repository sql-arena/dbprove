#pragma once
#include <cstdlib>
#include <string>
#include <string_view>
#include <optional>

/**
 * @brief Get the first available environment variable from a list of possible names
 *
 * @tparam Args Variable number of std::string_view arguments
 * @param names One or more environment variable names to check
 * @return std::string The value of the first available environment variable or nullopt if not found
 */
#define _CRT_SECURE_NO_WARNINGS
template<typename... Args>
std::optional<std::string> getEnvVar(Args... names) {
  auto checkEnv = [](std::string_view name) -> std::string {
#ifdef _WIN32
    char* value = nullptr;
    size_t len = 0;
    if (_dupenv_s(&value, &len, name.data()) == 0 && value != nullptr) {
      std::string result(value);
      free(value);
      return result;
    }
    return std::string();
#else
    const char* value = std::getenv(name.data());
    return value ? std::string(value) : std::string();
#endif

  };

  for (const auto& name : {names...}) {
    std::string result = checkEnv(name);
    if (!result.empty()) {
      return result;
    }
  }
  return std::nullopt;

}
#undef _CRT_SECURE_NO_WARNINGS
