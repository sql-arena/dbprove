#include "theorem.h"
#include <string>
#include <vector>
#include <ranges>
#include <nlohmann/json.hpp>
#include <dbprove/ux/ux.h>

#include "init.h"
#include <plog/Log.h>

namespace dbprove::theorem {
namespace {
std::string_view classifyRunStatus(const std::string_view message) {
  if (message.find("Query timed out after") != std::string_view::npos) {
    return "TIMEOUT";
  }
  return "ERROR";
}

std::string renderFailureMessage(const std::exception& error, const std::optional<std::string>& render_error) {
  if (!render_error.has_value()) {
    return error.what();
  }
  return std::string(error.what()) + " | render failure: " + *render_error;
}

std::string renderFailureMessage(const char* error_message, const std::optional<std::string>& render_error) {
  if (!render_error.has_value()) {
    return error_message;
  }
  return std::string(error_message) + " | render failure: " + *render_error;
}

std::optional<std::string> tryRenderProof(Proof& proof) {
  try {
    proof.render();
    return std::nullopt;
  } catch (const std::exception& e) {
    PLOGW << "Failed to render proof for theorem '" << proof.theorem.name << "': " << e.what();
    return e.what();
  } catch (...) {
    PLOGW << "Failed to render proof for theorem '" << proof.theorem.name << "' with unknown non-std exception";
    return "unknown non-std exception";
  }
}

void persistProofOutput(Proof& proof, RunCtx& state) {
  try {
    state.writeProofJson(proof.theorem.name, proof.toJson());
  } catch (const std::exception& e) {
    PLOGE << "Failed to write proof JSON for theorem '" << proof.theorem.name << "': " << e.what();
  } catch (...) {
    PLOGE << "Failed to write proof JSON for theorem '" << proof.theorem.name << "' with unknown non-std exception";
  }
}

std::string configVersionJson(const RunCtx& input_state, const std::string_view version) {
  nlohmann::ordered_json document;
  document["theorem"] = {
      {"name", "CONFIG-VERSION"},
      {"displayName", "CONFIG-VERSION"},
      {"description", "Version of Engine"},
      {"categories", nlohmann::json::array()},
      {"tags", nlohmann::json::array({"CONFIG"})},
  };
  document["engine"] = input_state.engine.name();
  document["version"] = version;
  document["storageVariant"] = to_string(input_state.storage_variant);
  return document.dump(2);
}
}

void run_theorem(const Theorem& theorem, RunCtx& state) {
  auto proof = std::make_unique<Proof>(theorem, state);
  try {
    theorem.func(*proof);
    proof->setRunStatus("OK");
    persistProofOutput(*proof, state);
    state.proofs.push_back(std::move(proof));
  } catch (const std::exception& e) {
    const auto render_error = tryRenderProof(*proof);
    proof->setRunStatus(std::string(classifyRunStatus(e.what())));
    proof->setErrorMessage(renderFailureMessage(e, render_error));
    persistProofOutput(*proof, state);
    state.proofs.push_back(std::move(proof));
    throw;
  } catch (...) {
    const auto render_error = tryRenderProof(*proof);
    proof->setRunStatus("ERROR");
    proof->setErrorMessage(renderFailureMessage("Unknown non-std exception", render_error));
    persistProofOutput(*proof, state);
    state.proofs.push_back(std::move(proof));
    throw;
  }
}

void writeVersion(RunCtx& input_state) {
  if (input_state.artifact_mode) {
    PLOGI << "Artifact mode: skipping engine version lookup";
    return;
  }
  PLOGI << "Reading Version...";
  std::string version = "unknown";
  try {
    version = input_state.factory.create()->version();
  } catch (const std::exception& e) {
    PLOGW << "Failed to read engine version for proof output: " << e.what();
  } catch (...) {
    PLOGW << "Failed to read engine version for proof output with unknown non-std exception";
  }

  input_state.writeProofJson("CONFIG-VERSION", configVersionJson(input_state, version));
  PLOGI << "The Version of the engine is: " << version;
}

bool prove(const std::vector<const Theorem*>& theorems, RunCtx& input_state) {
  writeVersion(input_state);
  auto all_succeeded = true;

  for (const auto& theorem : theorems) {
    ux::PreAmpleTheorem(input_state.console, theorem->name);
    try {
      run_theorem(*theorem, input_state);
    } catch (const DatasetBootstrapException& e) {
      all_succeeded = false;
      PLOGE << "Theorem '" << theorem->name << "' failed: " << e.what();
    } catch (const std::exception& e) {
      all_succeeded = false;
      PLOGE << "Theorem '" << theorem->name << "' failed: " << e.what();
    } catch (...) {
      all_succeeded = false;
      PLOGE << "Theorem '" << theorem->name << "' failed with unknown non-std exception";
    }
  }
  return all_succeeded;
}

std::vector<const Theorem*> parse(const std::vector<std::string>& theorems) {
  // We only want to run each theorem once. Remove duplicates first.
  std::set<const Theorem*> parsed_theorems;
  if (theorems.size() == 0) {
    // If the user didn't supply any theorems, default to all
    for (auto& t : std::views::values(allTheorems())) {
      parsed_theorems.insert(t.get());
    }
  } else {
    for (const auto& t : theorems) {
      if (allTypeNames().contains(t)) {
        /* User passed a category: pick everything.*/
        auto all_theorems_in_type = allTheoremsInCategory(typeEnum(t));
        parsed_theorems.insert(all_theorems_in_type.begin(), all_theorems_in_type.end());
        continue;
      }
      
      bool found_by_tag = false;
      try {
        const Tag tag(t);
        for (const auto& theorem_ptr : std::views::values(allTheorems())) {
          if (theorem_ptr->hasTag(tag)) {
            parsed_theorems.insert(theorem_ptr.get());
            found_by_tag = true;
          }
        }
      } catch (...) {
        // Tag construction might fail if it's not uppercase or has invalid chars,
        // which is fine, we'll try it as a theorem name next.
      }

      if (found_by_tag) {
        continue;
      }

      if (!allTheorems().contains(t)) {
        /* Specific theorem*/
        throw std::runtime_error("Unknown theorem: " + t);
      }
      parsed_theorems.insert(allTheorems().at(t).get());
    }
  }
  // Run theorems in name order.
  std::vector sorted_theorems(parsed_theorems.begin(), parsed_theorems.end());
  std::ranges::sort(sorted_theorems, [](const Theorem* a, const Theorem* b) { return *a < *b; });
  return sorted_theorems;
}
}
