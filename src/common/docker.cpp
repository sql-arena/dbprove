#include "include/dbprove/common/docker.h"

#include "include/dbprove/common/file_utility.h"
#include "include/dbprove/common/string.h"

#include <array>
#include <chrono>
#include <cstdio>
#include <sstream>
#include <thread>

#ifdef _WIN32
#include <process.h>
#else
#include <sys/wait.h>
#endif

namespace dbprove::common {
namespace {
#ifdef _WIN32
constexpr auto dbprove_popen = _popen;
constexpr auto dbprove_pclose = _pclose;
#else
constexpr auto dbprove_popen = popen;
constexpr auto dbprove_pclose = pclose;
#endif
}

DockerRunner::DockerRunner(std::filesystem::path project_root, std::string compose_project_name)
  : project_root_(std::move(project_root))
  , compose_file_(project_root_ / "docker" / "docker-compose.yml")
  , compose_project_name_(std::move(compose_project_name)) {
}

const std::filesystem::path& DockerRunner::projectRoot() const {
  return project_root_;
}

const std::filesystem::path& DockerRunner::composeFile() const {
  return compose_file_;
}

const std::string& DockerRunner::composeProjectName() const {
  return compose_project_name_;
}

void DockerRunner::ensureDaemonRunning() const {
  if (runDocker({"info"}).succeeded()) {
    return;
  }

#if defined(__APPLE__)
  const auto start_result = runCommand(joinArgs(std::vector<std::string>{"open", "--background", "-a", "Docker"}));
  if (!start_result.succeeded()) {
    throw std::runtime_error("Docker daemon is not running and Docker Desktop failed to start.\n" + start_result.output);
  }
#elif defined(__linux__)
  const auto start_result = runCommand(joinArgs(std::vector<std::string>{"systemctl", "start", "docker"}));
  if (!start_result.succeeded()) {
    throw std::runtime_error("Docker daemon is not running and 'systemctl start docker' failed.\n"
                             + start_result.output + "\nPlease start Docker manually.");
  }
#else
  throw std::runtime_error("Docker daemon is not running. Please start Docker manually on this platform.");
#endif

  constexpr auto retry_delay = std::chrono::seconds(2);
  constexpr int max_retries = 30;
  for (int attempt = 0; attempt < max_retries; ++attempt) {
    if (runDocker({"info"}).succeeded()) {
      return;
    }
    std::this_thread::sleep_for(retry_delay);
  }

  throw std::runtime_error("Docker failed to start after waiting for 60 seconds.");
}

DockerCommandResult DockerRunner::runDocker(const std::vector<std::string>& args) const {
  std::vector<std::string> command = {"docker"};
  command.insert(command.end(), args.begin(), args.end());
  return runCommand(joinArgs(command));
}

DockerCommandResult DockerRunner::runCompose(const std::vector<std::string>& args) const {
  std::vector<std::string> command = {
      "docker",
      "compose",
      "-p",
      compose_project_name_,
      "-f",
      compose_file_.string(),
  };
  command.insert(command.end(), args.begin(), args.end());
  return runCommand(joinArgs(command));
}

DockerCommandResult DockerRunner::runComposeFile(const std::vector<std::string>& args) const {
  std::vector<std::string> command = {
      "docker",
      "compose",
      "-f",
      compose_file_.string(),
  };
  command.insert(command.end(), args.begin(), args.end());
  return runCommand(joinArgs(command));
}

DockerCommandResult DockerRunner::buildComposeService(const std::string_view service) const {
  return runCompose({"build", std::string(service)});
}

DockerCommandResult DockerRunner::upComposeService(const std::string_view service, const bool detached) const {
  std::vector<std::string> args = {"up"};
  if (detached) {
    args.emplace_back("-d");
  }
  args.emplace_back(std::string(service));
  return runCompose(args);
}

DockerCommandResult DockerRunner::downComposeProject(const bool remove_orphans) const {
  std::vector<std::string> args = {"down"};
  if (remove_orphans) {
    args.emplace_back("--remove-orphans");
  }
  return runCompose(args);
}

DockerCommandResult DockerRunner::downComposeFile(const bool remove_orphans) const {
  std::vector<std::string> args = {"down"};
  if (remove_orphans) {
    args.emplace_back("--remove-orphans");
  }
  return runComposeFile(args);
}

DockerCommandResult DockerRunner::buildImage(const std::filesystem::path& dockerfile,
                                             const std::string_view tag,
                                             const std::filesystem::path& context,
                                             const std::vector<std::string>& extra_args) const {
  std::vector<std::string> args = {
      "build",
      "-f",
      dockerfile.string(),
      "-t",
      std::string(tag),
  };
  args.insert(args.end(), extra_args.begin(), extra_args.end());
  args.emplace_back(context.string());
  return runDocker(args);
}

DockerCommandResult DockerRunner::runCommand(const std::string& command) {
  DockerCommandResult result;
  std::array<char, 4096> buffer{};

  FILE* pipe = dbprove_popen((command + " 2>&1").c_str(), "r");
  if (pipe == nullptr) {
    result.exit_code = -1;
    result.output = "Failed to start command: " + command;
    return result;
  }

  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    result.output.append(buffer.data());
  }

  const int close_status = dbprove_pclose(pipe);
#ifdef _WIN32
  result.exit_code = close_status;
#else
  if (WIFEXITED(close_status)) {
    result.exit_code = WEXITSTATUS(close_status);
  } else {
    result.exit_code = close_status;
  }
#endif
  return result;
}

std::string DockerRunner::joinArgs(const std::vector<std::string>& args) {
  std::ostringstream out;
  bool first = true;
  for (const auto& arg : args) {
    if (!first) {
      out << ' ';
    }
    first = false;
    out << shell_quote(arg);
  }
  return out.str();
}

DockerComposeSession::DockerComposeSession(DockerRunner runner)
  : runner_(std::move(runner)) {
}

DockerComposeSession::~DockerComposeSession() {
  stop();
}

void DockerComposeSession::start(const std::string_view service) {
  runner_.ensureDaemonRunning();
  cleanupPreviousRuns();
  ensureMountDirectory(service);

  const auto build_result = runner_.buildComposeService(service);
  if (!build_result.succeeded()) {
    throw std::runtime_error(commandErrorMessage("build", service, build_result));
  }

  const auto up_result = runner_.upComposeService(service);
  if (!up_result.succeeded()) {
    throw std::runtime_error(commandErrorMessage("start", service, up_result));
  }

  active_ = true;
}

void DockerComposeSession::stop() noexcept {
  if (!active_) {
    return;
  }
  static_cast<void>(runner_.downComposeProject());
  active_ = false;
}

void DockerComposeSession::cleanupPreviousRuns() {
  static_cast<void>(runner_.downComposeFile());
  static_cast<void>(runner_.downComposeProject());

  DockerRunner legacy_runner(runner_.projectRoot(), std::string(kDbproveLegacyComposeProjectName));
  static_cast<void>(legacy_runner.downComposeProject());
}

void DockerComposeSession::ensureMountDirectory(const std::string_view service) {
  std::string mount_name(service);
  const auto suffix = mount_name.find('-');
  if (suffix != std::string::npos) {
    mount_name.resize(suffix);
  }

  if (mount_name == "postgresql"
      || mount_name == "clickhouse"
      || mount_name == "mssql"
      || mount_name == "trino"
      || mount_name == "datafusion") {
    static_cast<void>(make_directory((runner_.projectRoot() / "run" / "mount" / mount_name).string()));
  }
}

std::string DockerComposeSession::tailOutput(const std::string& output, const size_t max_chars) {
  if (output.size() <= max_chars) {
    return output;
  }
  return output.substr(output.size() - max_chars);
}

std::string DockerComposeSession::commandErrorMessage(const std::string_view action,
                                                      const std::string_view service,
                                                      const DockerCommandResult& result) {
  std::string message = "Failed to " + std::string(action) + " docker service '"
                      + std::string(service) + "' (exit code " + std::to_string(result.exit_code) + ")";
  if (!result.output.empty()) {
    message += "\n" + tailOutput(result.output);
  }
  return message;
}
}
