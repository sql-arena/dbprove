#include "theorem.h"

#include <dbprove/common/file_utility.h>

#include <fstream>
#include <sstream>
#include <stdexcept>
#include <unordered_map>

namespace dbprove::theorem {
class RunCtx::CsvWriter {
public:
  std::unordered_map<std::string, std::unique_ptr<std::ofstream>> outputs;

  CsvWriter() = default;
};

namespace {
std::string renderCsvLine(const std::vector<std::string_view>& values);

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
  filename += ".csv";
  return filename;
}

const std::vector<std::string_view>& csvHeader() {
  static const std::vector<std::string_view> kHeader{
      "ENGINE",
      "ID",
      "CATEGORIES",
      "TAGS",
      "THEOREM",
      "THEOREM_DESCRIPTION",
      "PROOF_NAME",
      "PROOF_VALUE",
      "PROOF_UNIT",
  };
  return kHeader;
}

std::ofstream& csvStreamForProof(RunCtx::CsvWriter& writer,
                                 const std::filesystem::path& proof_directory,
                                 const bool write_csv_header,
                                 std::string_view proof_name) {
  const auto key = std::string(proof_name);
  if (const auto it = writer.outputs.find(key); it != writer.outputs.end()) {
    return *it->second;
  }

  std::filesystem::create_directories(proof_directory);
  auto file = std::make_unique<std::ofstream>(
      proof_directory / sanitiseProofFilename(proof_name),
      std::ios::out | std::ios::trunc);
  if (!file->is_open()) {
    throw std::runtime_error("Failed to open proof file for theorem: " + key);
  }
  if (write_csv_header) {
    const auto header = renderCsvLine(csvHeader());
    file->write(header.data(), static_cast<std::streamsize>(header.size()));
    file->flush();
    if (!file->good()) {
      throw std::runtime_error("Failed to write proof CSV header for theorem: " + key);
    }
  }
  auto& ref = *file;
  writer.outputs.emplace(key, std::move(file));
  return ref;
}
};

namespace {
void writeQuotedField(std::string& out, const std::string_view value) {
  out.push_back('"');
  for (const char c : value) {
    if (c == '"') {
      out.push_back('"');
    }
    out.push_back(c);
  }
  out.push_back('"');
}

std::string renderCsvLine(const std::vector<std::string_view>& values) {
  std::string line;
  for (const auto& value : values) {
    line.reserve(line.size() + value.size() + 8);
  }

  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      line.push_back('|');
    }
    writeQuotedField(line, values[i]);
  }
  line.push_back('\n');
  return line;
}

std::string describeCsvRow(const std::vector<std::string_view>& values) {
  if (values.size() >= 7) {
    return "theorem '" + std::string(values[4]) + "', proof field '" + std::string(values[6]) + "'";
  }
  if (!values.empty()) {
    return "first field '" + std::string(values.front()) + "'";
  }
  return "empty csv row";
}
}

void RunCtx::writeCsv(const std::string_view proof_name, const std::vector<std::string_view>& values) const {
  if (proof_directory_path_.empty()) {
    return;
  }
  const auto line = renderCsvLine(values);

  auto& out = csvStreamForProof(*legacy_writer, proof_directory_path_, write_csv_header_, proof_name);
  out.write(line.data(), static_cast<std::streamsize>(line.size()));
  out.flush();

  if (!out.good()) {
    throw std::runtime_error("Failed to write proof CSV row for " + describeCsvRow(values));
  }
}

RunCtx::RunCtx(const sql::Engine& engine, const sql::Credential& credentials, generator::GeneratorState& generator,
               std::ostream& console, std::ostream& csv, std::optional<std::string> artifacts_path,
               std::optional<uint32_t> query_timeout_seconds, const size_t timing_runs,
               std::optional<std::string> parquet_dir,
               const bool write_csv_header,
               const std::optional<std::filesystem::path> proof_directory)
  : legacy_writer(std::make_unique<CsvWriter>())
  , proof_directory_path_(proof_directory.value_or(std::filesystem::path{}))
  , write_csv_header_(write_csv_header)
  , engine(engine)
  , credentials(credentials)
  , generator(generator)
  , factory(engine, credentials, artifacts_path)
  , console(console)
  , csv(csv)
  , artifact_mode(artifacts_path.has_value())
  , query_timeout_seconds(query_timeout_seconds)
  , timing_runs(timing_runs)
  , parquet_dir(std::move(parquet_dir)) {
}

RunCtx::~RunCtx() = default;
}
