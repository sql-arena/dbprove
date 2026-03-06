#include "include/dbprove/ux/ux.h"
#include <plog/Log.h>
#include <algorithm>
#include <cstdlib>
#include <limits>
#ifdef _WIN32
#include <windows.h>
#else
#include <sys/ioctl.h>
#include <unistd.h>
#endif

namespace dbprove::ux {
static constexpr Distance minimum_screen_width = 80;
static constexpr Distance default_screen_width = 120;
static Distance screen_width = default_screen_width;

Distance normalizeWidth(const unsigned int width) {
  const auto clamped = std::max<unsigned int>(width, minimum_screen_width);
  return static_cast<Distance>(std::min<unsigned int>(clamped, std::numeric_limits<Distance>::max()));
}

unsigned int columnsFromEnv() {
  if (const char* columns = std::getenv("COLUMNS")) {
    char* end = nullptr;
    const auto parsed = std::strtoul(columns, &end, 10);
    if (end != columns && *end == '\0' && parsed > 0) {
      return static_cast<unsigned int>(parsed);
    }
  }
  return 0;
}

void resolveTerminalWidth() {
  unsigned int measured_width = default_screen_width;
#ifdef _WIN32
  // Windows-specific method to get terminal width
  CONSOLE_SCREEN_BUFFER_INFO csbi;
  HANDLE hStdOut = GetStdHandle(STD_OUTPUT_HANDLE);
  if (GetConsoleScreenBufferInfo(hStdOut, &csbi)) {
    const auto width = static_cast<unsigned int>(csbi.srWindow.Right - csbi.srWindow.Left + 1);
    if (width > 0) {
      measured_width = width;
    }
  }
#else
  // Unix-based platform method to get terminal width
  struct winsize w;
  if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == 0 && w.ws_col > 0) {
    measured_width = static_cast<unsigned int>(w.ws_col);
  } else {
    const auto env_width = columnsFromEnv();
    if (env_width > 0) {
      measured_width = env_width;
    }
  }
#endif
  screen_width = normalizeWidth(measured_width);
  PLOGI << "Detected terminal width " << measured_width;
}

void Terminal::configure() {
#ifdef _WIN32
  /* Configure UTF8 output and ANSI terminal */
  SetConsoleOutputCP(CP_UTF8);
  HANDLE hOut = GetStdHandle(STD_OUTPUT_HANDLE);
  DWORD dwMode = 0;
  GetConsoleMode(hOut, &dwMode);
  dwMode |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;
  SetConsoleMode(hOut, dwMode);
#endif

  resolveTerminalWidth();
}


Distance Terminal::width() { return screen_width; };
}
