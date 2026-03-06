#include "include/dbprove/common/file_utility.h"

namespace dbprove::common {
using namespace std::filesystem;

path make_directory(const std::string& directory) {
  const auto currentWorkingDir = current_path();
  const auto directoryToMake = currentWorkingDir / directory;

  if (!exists(directoryToMake)) {
    try {
      create_directories(directoryToMake);
    } catch (const std::filesystem::filesystem_error& e) {
      throw std::runtime_error(
          "Failed to create directory: " + directoryToMake.string() + " Error: " + std::string(e.what()));
    }
  } else {
    if (!is_directory(directoryToMake)) {
      throw std::runtime_error("Path exists but is not a directory: " + directoryToMake.string());
    }
  }
  // We need write permission to put stuff the directory. Instead of relying on platform specific checks for
  // that, just try to write
  const path testFile = directoryToMake / ".test_write_permission";
  try {
    std::ofstream testStream(testFile);
    if (!testStream) {
      throw std::runtime_error("Directory exists, but no write permission: " + directoryToMake.string());
    }
    testStream.close();
    remove(testFile);
  } catch (...) {
    throw std::runtime_error("Directory exists, but no write permission: " + directoryToMake.string());
  }
  return directoryToMake;
}
std::filesystem::path get_project_root() {
  const std::filesystem::path current_file{__FILE__};
  return current_file.parent_path().parent_path().parent_path();
}
}