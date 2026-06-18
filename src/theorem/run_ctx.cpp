#include "theorem.h"

#include <dbprove/common/file_utility.h>

#include <fstream>
#include <stdexcept>

namespace dbprove::theorem {
namespace {
std::string sanitiseProofFilename(std::string_view proof_name) {
  std::string filename;
  filename.reserve(proof_name.size() + 4);
  for (const char c : proof_name) {
    const bool safe = (c >= 'A' && c <= 'Z')
                   || (c >= 'a' && c <= 'z')
                   || (c >= '0' && c <= '9')
                   || c == '-'
                   || c == '_'
                   || c == '.';
    filename.push_back(safe ? c : '_');
  }
  filename += ".json";
  return filename;
}
}

void RunCtx::writeProofJson(const std::string_view proof_name, const std::string_view content) const {
  if (proof_directory_path_.empty()) {
    return;
  }
  std::filesystem::create_directories(proof_directory_path_);
  const auto path = proof_directory_path_ / sanitiseProofFilename(proof_name);
  std::ofstream out(path, std::ios::out | std::ios::trunc);
  if (!out.is_open()) {
    throw std::runtime_error("Failed to open proof file for writing: " + path.string());
  }
  out.write(content.data(), static_cast<std::streamsize>(content.size()));
  out.flush();
  if (!out.good()) {
    throw std::runtime_error("Failed to write proof JSON file: " + path.string());
  }
}

RunCtx::RunCtx(const sql::Engine& engine, const sql::Credential& credentials, generator::GeneratorState& generator,
               std::ostream& console, std::string engine_version, std::optional<std::string> connection_artifacts_path,
               const dbprove::StorageVariant storage_variant,
               std::optional<uint32_t> query_timeout_seconds, const size_t timing_runs,
               std::optional<std::string> parquet_dir,
               const std::optional<std::filesystem::path> proof_directory,
               const bool artifact_mode)
  : proof_directory_path_(proof_directory.value_or(std::filesystem::path{}))
  , engine(engine)
  , engine_version(std::move(engine_version))
  , storage_variant(storage_variant)
  , credentials(credentials)
  , generator(generator)
  , factory(engine, credentials, connection_artifacts_path)
  , console(console)
  , artifact_mode(artifact_mode)
  , query_timeout_seconds(query_timeout_seconds)
  , timing_runs(timing_runs)
  , parquet_dir(std::move(parquet_dir)) {
}

RunCtx::~RunCtx() = default;
}
