#pragma once
#include <filesystem>
#include <fstream>


namespace dbprove::common {
std::filesystem::path make_directory(const std::string& directory);
std::filesystem::path get_project_root();
}