#include "terminal.h"

namespace dbprove::ux {
#ifdef _WIN32
#include <windows.h>
#endif
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
}
}