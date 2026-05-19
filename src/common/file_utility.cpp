#include "include/dbprove/common/file_utility.h"

#include <optional>

namespace dbprove::common {
using namespace std::filesystem;

namespace {
bool looks_like_project_root(const path& candidate) {
  return exists(candidate / "docker" / "docker-compose.yml")
         && exists(candidate / "src" / "dbprove" / "main.cpp");
}

std::optional<path> discover_project_root_from(path start) {
  auto current = std::move(start);
  while (!current.empty()) {
    if (looks_like_project_root(current)) {
      return current;
    }
    const auto parent = current.parent_path();
    if (parent == current) {
      break;
    }
    current = parent;
  }
  return std::nullopt;
}
}

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
  if (const auto from_cwd = discover_project_root_from(current_path()); from_cwd.has_value()) {
    return *from_cwd;
  }

  const std::filesystem::path current_file{__FILE__};
  if (const auto from_source_path = discover_project_root_from(current_file.parent_path().parent_path().parent_path());
      from_source_path.has_value()) {
    return *from_source_path;
  }
  return current_file.parent_path().parent_path().parent_path();
}
}
