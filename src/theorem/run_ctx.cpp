#include "theorem.h"

#include <sstream>
#include <stdexcept>

namespace dbprove::theorem {
class RunCtx::CsvWriter {
public:
  std::ostream& out;

  explicit CsvWriter(std::ostream& out)
    : out(out) {
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

void RunCtx::writeCsv(const std::vector<std::string_view>& values) const {
  const auto line = renderCsvLine(values);

  writer->out.write(line.data(), static_cast<std::streamsize>(line.size()));
  writer->out.flush();

  if (!writer->out.good()) {
    throw std::runtime_error("Failed to write proof CSV row for " + describeCsvRow(values));
  }
}

RunCtx::RunCtx(const sql::Engine& engine, const sql::Credential& credentials, generator::GeneratorState& generator,
               std::ostream& console, std::ostream& csv, std::optional<std::string> artifacts_path,
               std::optional<uint32_t> query_timeout_seconds, const size_t timing_runs,
               std::optional<std::string> parquet_dir)
  : writer(std::make_unique<CsvWriter>(csv))
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
  writeCsv(std::vector<std::string_view>{"ENGINE",
                                         "ID",
                                         "CATEGORIES",
                                         "TAGS",
                                         "THEOREM",
                                         "THEOREM_DESCRIPTION",
                                         "PROOF_NAME",
                                         "PROOF_VALUE",
                                         "PROOF_UNIT"});
}

RunCtx::~RunCtx() = default;
}
