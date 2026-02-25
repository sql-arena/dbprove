#include "include/dbprove/ux/ux.h"
#ifdef _WIN32
#include <windows.h>
#else
#include <sys/ioctl.h>
#include <unistd.h>
#endif

namespace dbprove::ux {
static constexpr Distance default_screen_width = 120;
static Distance screen_width = default_screen_width;

void resolveTerminalWidth() {
#ifdef _WIN32
  // Windows-specific method to get terminal width
  CONSOLE_SCREEN_BUFFER_INFO csbi;
  HANDLE hStdOut = GetStdHandle(STD_OUTPUT_HANDLE);
  if (GetConsoleScreenBufferInfo(hStdOut, &csbi)) {
    screen_width = csbi.srWindow.Right - csbi.srWindow.Left + 1;
  } else {
    screen_width = default_screen_width;
  }
#else
  // Unix-based platform method to get terminal width
  struct winsize w;
  if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == 0) {
    screen_width = w.ws_col;  // Return terminal width
  } else {
    screen_width = default_screen_width;  // Fallback to default width
  }
#endif
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