#pragma once

#include "file_utility.h"

#include <filesystem>
#include <chrono>
#include <string>
#include <string_view>
#include <vector>

namespace dbprove::common {
inline constexpr std::string_view kDbproveManagedComposeProjectName = "dbprove-managed";
inline constexpr std::string_view kDbproveLegacyComposeProjectName = "dbprove-scale";

struct DockerCommandResult {
  int exit_code = 0;
  std::string output;

  [[nodiscard]] bool succeeded() const {
    return exit_code == 0;
  }
};

class DockerRunner {
public:
  DockerRunner(std::filesystem::path project_root = get_project_root(),
               std::string compose_project_name = std::string(kDbproveManagedComposeProjectName));

  [[nodiscard]] const std::filesystem::path& projectRoot() const;
  [[nodiscard]] const std::filesystem::path& composeFile() const;
  [[nodiscard]] const std::string& composeProjectName() const;

  void ensureDaemonRunning() const;
  [[nodiscard]] DockerCommandResult runDocker(const std::vector<std::string>& args) const;
  [[nodiscard]] DockerCommandResult runCompose(const std::vector<std::string>& args) const;
  [[nodiscard]] DockerCommandResult buildComposeService(std::string_view service) const;
  [[nodiscard]] DockerCommandResult upComposeService(std::string_view service, bool detached = true) const;
  [[nodiscard]] DockerCommandResult downComposeProject(bool remove_orphans = true) const;
  [[nodiscard]] DockerCommandResult removeContainersByNamePrefix(std::string_view prefix) const;
  void waitForHttpOk(std::string_view url, std::chrono::seconds timeout) const;
  [[nodiscard]] DockerCommandResult buildImage(const std::filesystem::path& dockerfile,
                                               std::string_view tag,
                                               const std::filesystem::path& context,
                                               const std::vector<std::string>& extra_args = {}) const;

private:
  std::filesystem::path project_root_;
  std::filesystem::path compose_file_;
  std::string compose_project_name_;

  [[nodiscard]] static DockerCommandResult runCommand(const std::string& command);
  [[nodiscard]] static std::string joinArgs(const std::vector<std::string>& args);
};

class DockerComposeSession {
public:
  DockerComposeSession() = default;
  explicit DockerComposeSession(DockerRunner runner);
  ~DockerComposeSession();

  void start(std::string_view service);
  void stop() noexcept;

private:
  void ensureMountDirectory(std::string_view service);
  [[nodiscard]] static std::string tailOutput(const std::string& output, size_t max_chars = 4000);
  [[nodiscard]] static std::string commandErrorMessage(std::string_view action,
                                                       std::string_view service,
                                                       const DockerCommandResult& result);

  DockerRunner runner_;
  bool active_ = false;
};
}
