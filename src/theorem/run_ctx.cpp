#include "theorem.h"
#include <csv.hpp>

namespace dbprove::theorem {
using namespace csv;

class RunCtx::CsvWriter {
public:
  using PipeWriter = csv::DelimWriter<std::ostream, '|', '"', false>;
  PipeWriter writer;

  explicit CsvWriter(std::ostream& out)
    : writer(out) {
  }
};

void RunCtx::writeCsv(const std::vector<std::string_view>& values) const {
  writer->writer << values;
}

RunCtx::RunCtx(const sql::Engine& engine, const sql::Credential& credentials, generator::GeneratorState& generator,
               std::ostream& console, std::ostream& csv)
  : writer(std::make_unique<CsvWriter>(csv))
  , engine(engine)
  , credentials(credentials)
  , generator(generator)
  , console(console)
  , csv(csv) {
  writer->writer << std::vector<std::string_view>{"ENGINE",
                                                  "ID",
                                                  "CATEGORIES",
                                                  "TAGS",
                                                  "THEOREM",
                                                  "THEOREM_DESCRIPTION",
                                                  "PROOF_NAME",
                                                  "PROOF_VALUE",
                                                  "PROOF_UNIT"};
}

RunCtx::~RunCtx() = default;
}