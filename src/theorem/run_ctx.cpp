#include "theorem.h"

namespace dbprove::theorem {
class RunCtx::CsvWriter {
public:
  std::ostream& out;

  explicit CsvWriter(std::ostream& out)
    : out(out) {
  }
};

namespace {
void writeQuotedField(std::ostream& out, const std::string_view value) {
  out.put('"');
  for (const char c : value) {
    if (c == '"') {
      out.put('"');
    }
    out.put(c);
  }
  out.put('"');
}
}

void RunCtx::writeCsv(const std::vector<std::string_view>& values) const {
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      writer->out.put('|');
    }
    writeQuotedField(writer->out, values[i]);
  }
  writer->out.put('\n');
}

RunCtx::RunCtx(const sql::Engine& engine, const sql::Credential& credentials, generator::GeneratorState& generator,
               std::ostream& console, std::ostream& csv, std::optional<std::string> artifacts_path)
  : writer(std::make_unique<CsvWriter>(csv))
  , engine(engine)
  , credentials(credentials)
  , generator(generator)
  , factory(engine, credentials, artifacts_path)
  , console(console)
  , csv(csv)
  , artifact_mode(artifacts_path.has_value()) {
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
