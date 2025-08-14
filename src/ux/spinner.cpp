#include "spinner.h"

namespace dbprove::ux {


void Spin() {
  constexpr const char* frames[] = { "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏" };
  static constexpr size_t frame_count = sizeof(frames) / sizeof(frames[0]);
  static size_t index = 0;
  static std::atomic<bool> spinning{false};

  bool expected = false;
  if (!spinning.compare_exchange_strong(expected, true)) {
    return;
  }


  std::cout << "\b" << frames[index] << std::flush;
  index = (index + 1) % frame_count;

  spinning.store(false);

}

}