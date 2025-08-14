#include "messages.h"
#include <rang.hpp>

void Error(std::string_view message) {
  std::cout << rang::fg::red << message << rang::fg::reset << std::endl;
}