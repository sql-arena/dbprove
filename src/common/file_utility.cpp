#include "include/dbprove/common/file_utility.h"

namespace dbprove::common {
using namespace std::filesystem;

path make_directory(const std::string& directory) {
  const auto currentWorkingDir = current_path();
  const auto directoryToMake = currentWorkingDir / directory;

  if (!exists(directoryToMake)) {
    try {
      create_directory(directoryToMake);
    } catch (const std::filesystem::filesystem_error& e) {
      throw std::runtime_error(
          "Failed to create 'table_data' directory: " + std::string(e.what()));
    }
  } else {
    if (!is_directory(directoryToMake)) {
      throw std::runtime_error("'table_data' exists but is not a directory.");
    }
  }
  // We need write permission to put stuff the directory. Instead of relying on platform specific checks for
  // that, just try to write
  const path testFile = directoryToMake / ".test_write_permission";
  try {
    std::ofstream testStream(testFile);
    if (!testStream) {
      throw std::runtime_error("'table_data' exists, but no write permission.");
    }
    testStream.close();
    remove(testFile);
  } catch (...) {
    throw std::runtime_error("'table_data' exists, but no write permission.");
  }
  return directoryToMake;
}
}