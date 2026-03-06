#include <catch2/catch_test_macros.hpp>
#include <dbprove/ux/ux.h>
#include <algorithm>
#include <cstdlib>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/ioctl.h>
#include <unistd.h>
#endif

TEST_CASE("Terminal width is never smaller than 80") {
  dbprove::ux::Terminal::configure();
  REQUIRE(dbprove::ux::Terminal::width() >= 80);
}

namespace {
unsigned int measuredStdoutWidthOrDefault() {
  constexpr unsigned int kDefaultWidth = 120;
#ifdef _WIN32
  CONSOLE_SCREEN_BUFFER_INFO csbi;
  const HANDLE hStdOut = GetStdHandle(STD_OUTPUT_HANDLE);
  if (GetConsoleScreenBufferInfo(hStdOut, &csbi)) {
    const auto width = static_cast<unsigned int>(csbi.srWindow.Right - csbi.srWindow.Left + 1);
    if (width > 0) {
      return width;
    }
  }
  return kDefaultWidth;
#else
  auto columnsFromEnv = []() -> unsigned int {
    if (const char* columns = std::getenv("COLUMNS")) {
      char* end = nullptr;
      const auto parsed = std::strtoul(columns, &end, 10);
      if (end != columns && *end == '\0' && parsed > 0) {
        return static_cast<unsigned int>(parsed);
      }
    }
    return 0;
  };
  struct winsize w {};
  if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == 0 && w.ws_col > 0) {
    return static_cast<unsigned int>(w.ws_col);
  }
  const auto env_width = columnsFromEnv();
  if (env_width > 0) {
    return env_width;
  }
  return kDefaultWidth;
#endif
}

unsigned int normalizedUxWidth(const unsigned int raw_width) {
  constexpr unsigned int kMinimumWidth = 80;
  return std::max(raw_width, kMinimumWidth);
}
}

TEST_CASE("Terminal width matches measured stdout width after UX normalization") {
  const auto raw_width = measuredStdoutWidthOrDefault();
  const auto expected = normalizedUxWidth(raw_width);

  dbprove::ux::Terminal::configure();

  REQUIRE(dbprove::ux::Terminal::width() == expected);
}
