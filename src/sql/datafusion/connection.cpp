#include "connection.h"

#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <regex>
#include <sstream>
#include <thread>
#include <string_view>

#include "result.h"
#include "sql_exceptions.h"
#include <dbprove/common/file_utility.h>
#include <dbprove/common/string.h>
#include <nlohmann/json.hpp>
#ifdef _WIN32
#include <process.h>
#else
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace sql::datafusion {
using ordered_json = nlohmann::ordered_json;
using json = nlohmann::json;

namespace {
#ifdef _WIN32
constexpr auto dbprove_popen = _popen;
constexpr auto dbprove_pclose = _pclose;
#else
constexpr auto dbprove_popen = popen;
constexpr auto dbprove_pclose = pclose;
#endif

struct CommandResult {
  int exit_code = 0;
  std::string output;
};

CommandResult runCommand(std::string_view command);

std::filesystem::path datafusionWorkspaceDir() {
  return dbprove::common::get_project_root() / "run" / "mount" / "datafusion";
}

std::filesystem::path dockerComposeFile() {
  return dbprove::common::get_project_root() / "docker" / "docker-compose.yml";
}

constexpr std::string_view kComposeProjectName = "dbprove-scale";

std::string composeExecPrefix() {
  return "docker compose -p " + shell_quote(kComposeProjectName)
         + " -f " + shell_quote(dockerComposeFile().string());
}

void waitForDataFusionBootstrap() {
  using namespace std::chrono_literals;
  const auto deadline = std::chrono::steady_clock::now() + 60s;
  const auto ready_command = composeExecPrefix()
                             + " exec -T datafusion sh -lc "
                             + shell_quote("test -f /tmp/datafusion-bootstrap-ready");
  while (std::chrono::steady_clock::now() < deadline) {
    if (runCommand(ready_command).exit_code == 0) {
      return;
    }
    std::this_thread::sleep_for(500ms);
  }
  throw std::runtime_error("Timed out waiting for DataFusion bootstrap readiness marker in the datafusion container");
}

std::string readTextFile(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    throw std::runtime_error("Failed to open file: " + path.string());
  }
  std::ostringstream buffer;
  buffer << in.rdbuf();
  return buffer.str();
}

std::string normalizeCliStatement(std::string_view sql);
std::string describeExitCode(int exit_code);

#ifndef _WIN32
class PersistentCliSession {
  pid_t pid_ = -1;
  FILE* input_ = nullptr;
  FILE* output_ = nullptr;

  static std::string sessionCommand() {
    return composeExecPrefix() +
           " exec -T datafusion sh -lc " +
           shell_quote("exec /opt/datafusion-cli/bin/datafusion-cli --format json --quiet "
                       "-r /workspace/datafusion-bootstrap.sql");
  }

  static std::string trimLine(std::string line) {
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
      line.pop_back();
    }
    return line;
  }

  static std::string describeWaitStatus(const int status) {
    if (WIFSIGNALED(status)) {
      return "Persistent DataFusion session terminated by signal "
             + std::to_string(WTERMSIG(status)) + " before producing JSON output";
    }
    if (WIFEXITED(status)) {
      return describeExitCode(WEXITSTATUS(status));
    }
    return "Persistent DataFusion session ended before producing JSON output";
  }

public:
  PersistentCliSession() {
    int stdin_pipe[2];
    int stdout_pipe[2];
    if (pipe(stdin_pipe) != 0 || pipe(stdout_pipe) != 0) {
      throw std::runtime_error("Failed to create pipes for persistent DataFusion session");
    }

    pid_ = fork();
    if (pid_ < 0) {
      ::close(stdin_pipe[0]);
      ::close(stdin_pipe[1]);
      ::close(stdout_pipe[0]);
      ::close(stdout_pipe[1]);
      throw std::runtime_error("Failed to fork persistent DataFusion session");
    }

    if (pid_ == 0) {
      dup2(stdin_pipe[0], STDIN_FILENO);
      dup2(stdout_pipe[1], STDOUT_FILENO);
      dup2(stdout_pipe[1], STDERR_FILENO);
      ::close(stdin_pipe[0]);
      ::close(stdin_pipe[1]);
      ::close(stdout_pipe[0]);
      ::close(stdout_pipe[1]);
      execl("/bin/sh", "sh", "-lc", sessionCommand().c_str(), static_cast<char*>(nullptr));
      _exit(127);
    }

    ::close(stdin_pipe[0]);
    ::close(stdout_pipe[1]);
    input_ = fdopen(stdin_pipe[1], "w");
    output_ = fdopen(stdout_pipe[0], "r");
    if (input_ == nullptr || output_ == nullptr) {
      close();
      throw std::runtime_error("Failed to open stdio streams for persistent DataFusion session");
    }
  }

  ~PersistentCliSession() {
    close();
  }

  void close() {
    if (input_ != nullptr) {
      fputs("\\q\n", input_);
      fflush(input_);
      fclose(input_);
      input_ = nullptr;
    }
    if (output_ != nullptr) {
      fclose(output_);
      output_ = nullptr;
    }
    if (pid_ > 0) {
      int status = 0;
      if (waitpid(pid_, &status, WNOHANG) == 0) {
        kill(pid_, SIGTERM);
        waitpid(pid_, &status, 0);
      }
      pid_ = -1;
    }
  }

  std::string runJsonQuery(std::string_view sql) {
    if (input_ == nullptr || output_ == nullptr) {
      throw std::runtime_error("Persistent DataFusion session is not open");
    }

    const std::string statement = normalizeCliStatement(sql);
    if (fwrite(statement.data(), 1, statement.size(), input_) != statement.size()) {
      throw std::runtime_error("Failed to write query to persistent DataFusion session");
    }
    fflush(input_);

    std::array<char, 65536> buffer{};
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), output_) != nullptr) {
      std::string line = trimLine(buffer.data());
      if (line.empty()) {
        continue;
      }
      if (line == trimLine(std::string(statement))) {
        continue;
      }
      const auto trimmed = trim_string(line);
      if (!trimmed.empty() && (trimmed.front() == '[' || trimmed.front() == '{')) {
        return line;
      }
      if (trimmed.starts_with("Error") || trimmed.starts_with("error")) {
        throw std::runtime_error(line);
      }
    }

    if (pid_ > 0) {
      int status = 0;
      const auto waited = waitpid(pid_, &status, WNOHANG);
      if (waited == pid_) {
        pid_ = -1;
        throw std::runtime_error(describeWaitStatus(status));
      }
    }

    throw std::runtime_error("Persistent DataFusion session ended before producing JSON output");
  }
};
#endif

CommandResult runCommand(std::string_view command) {
  FILE* pipe = dbprove_popen((std::string(command) + " 2>&1").c_str(), "r");
  if (pipe == nullptr) {
    throw std::runtime_error("Failed to launch process: " + std::string(command));
  }

  std::array<char, 4096> buffer{};
  std::string output;
  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    output += buffer.data();
  }

  const int raw_status = dbprove_pclose(pipe);
#ifdef _WIN32
  const int exit_code = raw_status;
#else
  const int exit_code = raw_status == -1 ? raw_status : WEXITSTATUS(raw_status);
#endif
  return CommandResult{exit_code, output};
}

#ifndef _WIN32
CommandResult runCommandWithInput(std::string_view command, std::string_view input) {
  int stdin_pipe[2];
  int stdout_pipe[2];
  if (pipe(stdin_pipe) != 0 || pipe(stdout_pipe) != 0) {
    throw std::runtime_error("Failed to create pipes for process input");
  }

  const pid_t pid = fork();
  if (pid < 0) {
    close(stdin_pipe[0]);
    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    close(stdout_pipe[1]);
    throw std::runtime_error("Failed to fork process for command input");
  }

  if (pid == 0) {
    dup2(stdin_pipe[0], STDIN_FILENO);
    dup2(stdout_pipe[1], STDOUT_FILENO);
    dup2(stdout_pipe[1], STDERR_FILENO);
    close(stdin_pipe[0]);
    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    close(stdout_pipe[1]);
    execl("/bin/sh", "sh", "-lc", std::string(command).c_str(), static_cast<char*>(nullptr));
    _exit(127);
  }

  close(stdin_pipe[0]);
  close(stdout_pipe[1]);
  if (!input.empty()) {
    const auto* data = input.data();
    size_t remaining = input.size();
    while (remaining > 0) {
      const auto written = write(stdin_pipe[1], data, remaining);
      if (written < 0) {
        close(stdin_pipe[1]);
        close(stdout_pipe[0]);
        kill(pid, SIGTERM);
        waitpid(pid, nullptr, 0);
        throw std::runtime_error("Failed to write stdin to child process");
      }
      data += written;
      remaining -= static_cast<size_t>(written);
    }
  }
  close(stdin_pipe[1]);

  std::array<char, 4096> buffer{};
  std::string output;
  ssize_t bytes_read = 0;
  while ((bytes_read = read(stdout_pipe[0], buffer.data(), buffer.size())) > 0) {
    output.append(buffer.data(), static_cast<size_t>(bytes_read));
  }
  close(stdout_pipe[0]);

  int status = 0;
  waitpid(pid, &status, 0);
  const int exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : status;
  return CommandResult{exit_code, output};
}
#endif

std::string trimOutput(std::string output) {
  while (!output.empty() && std::isspace(static_cast<unsigned char>(output.back())) != 0) {
    output.pop_back();
  }
  return output;
}

std::string normalizeCliStatement(std::string_view sql) {
  std::string normalized(sql);
  while (!normalized.empty() && std::isspace(static_cast<unsigned char>(normalized.back())) != 0) {
    normalized.pop_back();
  }
  if (normalized.empty() || normalized.back() != ';') {
    normalized.push_back(';');
  }
  normalized.push_back('\n');
  return normalized;
}

std::string describeExitCode(const int exit_code) {
  switch (exit_code) {
    case 126:
      return "DataFusion CLI exited with code 126 (command invoked cannot execute)";
    case 127:
      return "DataFusion CLI exited with code 127 (command not found or failed to launch)";
    case 130:
      return "DataFusion CLI exited with code 130 (interrupted by SIGINT)";
    case 137:
      return "Out of memory (DataFusion CLI exited with code 137, likely cgroup kill during query execution)";
    case 143:
      return "DataFusion CLI exited with code 143 (terminated by SIGTERM)";
    default:
      if (exit_code >= 128) {
        return "DataFusion CLI exited with code " + std::to_string(exit_code)
               + " (likely terminated by signal " + std::to_string(exit_code - 128) + ")";
      }
      return "DataFusion CLI exited with code " + std::to_string(exit_code);
  }
}

SqlTypeKind inferType(const SqlVariant& value) {
  if (value.is<SqlSmallInt>()) {
    return SqlTypeKind::SMALLINT;
  }
  if (value.is<SqlInt>()) {
    return SqlTypeKind::INT;
  }
  if (value.is<SqlBigInt>()) {
    return SqlTypeKind::BIGINT;
  }
  if (value.is<SqlFloat>()) {
    return SqlTypeKind::REAL;
  }
  if (value.is<SqlDouble>()) {
    return SqlTypeKind::DOUBLE;
  }
  if (value.is<SqlDecimal>()) {
    return SqlTypeKind::DECIMAL;
  }
  if (value.is<SqlString>()) {
    return SqlTypeKind::STRING;
  }
  return SqlTypeKind::SQL_NULL;
}

SqlVariant jsonValueToVariant(const ordered_json& value) {
  if (value.is_null()) {
    return SqlVariant();
  }
  if (value.is_number_integer()) {
    const auto numeric = value.get<int64_t>();
    if (numeric >= std::numeric_limits<int16_t>::min() && numeric <= std::numeric_limits<int16_t>::max()) {
      return SqlVariant(static_cast<int16_t>(numeric));
    }
    if (numeric >= std::numeric_limits<int32_t>::min() && numeric <= std::numeric_limits<int32_t>::max()) {
      return SqlVariant(static_cast<int32_t>(numeric));
    }
    return SqlVariant(numeric);
  }
  if (value.is_number_unsigned()) {
    return SqlVariant(static_cast<int64_t>(value.get<uint64_t>()));
  }
  if (value.is_number_float()) {
    return SqlVariant(value.get<double>());
  }
  if (value.is_boolean()) {
    return SqlVariant(value.get<bool>() ? "true" : "false");
  }
  if (value.is_string()) {
    const auto text = value.get<std::string>();
    static const std::regex decimal_pattern(R"(^-?\d+\.\d+$)");
    if (std::regex_match(text, decimal_pattern)) {
      return SqlVariant(SqlDecimal(text));
    }
    return SqlVariant(text);
  }
  return SqlVariant(value.dump());
}

[[noreturn]] void throwForCommandError(const Credential& credential, std::string_view statement, int exit_code,
                                       const std::string& output) {
  if (exit_code != 0 && output.empty()) {
    throw std::runtime_error(describeExitCode(exit_code));
  }
  if (exit_code == 137) {
    throw std::runtime_error(describeExitCode(exit_code));
  }
  const auto lowered = to_lower(output);
  if (lowered.contains("error during planning")) {
    if (lowered.contains("unsupported") || lowered.contains("not implemented")) {
      throw NotImplementedException(output);
    }
    if (lowered.contains("table") && lowered.contains("not found")) {
      throw InvalidObjectException(output);
    }
    throw SyntaxException(output, statement);
  }
  if (lowered.contains("schema error") || lowered.contains("table") && lowered.contains("not found")) {
    throw InvalidObjectException(output);
  }
  if (lowered.contains("not implemented")) {
    throw NotImplementedException(output);
  }
  throw ConnectionException(credential, output);
}
}  // namespace

class Connection::Pimpl {
  CredentialNone credential_;
  bool closed_ = false;
#ifndef _WIN32
  std::unique_ptr<PersistentCliSession> session_;
#endif

  static std::string bootstrapSqlPath() { return "/workspace/datafusion-bootstrap.sql"; }

 public:
  explicit Pimpl(CredentialNone credential) : credential_(std::move(credential)) {
    waitForDataFusionBootstrap();
  }

  void ensureOpen() const {
    if (closed_) {
      throw ConnectionClosedException(credential_);
    }
  }

  void close() {
    closed_ = true;
#ifndef _WIN32
    if (session_) {
      session_->close();
      session_.reset();
    }
#endif
  }

#ifndef _WIN32
  PersistentCliSession& session() {
    if (!session_) {
      session_ = std::make_unique<PersistentCliSession>();
    }
    return *session_;
  }
#endif

  CommandResult runOneShotCli(std::string_view sql, bool quiet) const {
    const std::string quiet_flag = quiet ? " --quiet" : "";
    const std::string command = composeExecPrefix() +
                                " exec -T datafusion sh -lc " +
                                shell_quote("exec /opt/datafusion-cli/bin/datafusion-cli --data-path /workspace "
                                            "--mem-pool-type fair -m 2g -d 4g -r " +
                                            shell_quote(bootstrapSqlPath()) + quiet_flag + " --format json -b 1000000");
#ifdef _WIN32
    return runCommand(command);
#else
    return runCommandWithInput(command, normalizeCliStatement(sql));
#endif
  }

  CommandResult runCli(std::string_view sql, bool quiet = true) const {
    ensureOpen();
    return runOneShotCli(sql, quiet);
  }

  CommandResult runPhysicalPlanHelper(std::string_view sql) const {
    ensureOpen();
    const auto bootstrap_path = datafusionWorkspaceDir() / "datafusion-bootstrap.sql";
    const std::string command = composeExecPrefix() +
                                " exec -T datafusion sh -lc " +
                                shell_quote(
                                    "export TMPDIR=/workspace/datafusion-spill; "
                                    "exec /opt/datafusion-plan-json/bin/datafusion-plan-json --bootstrap-file " +
                                    shell_quote("/workspace/" + bootstrap_path.filename().string()) + " --sql-stdin");
#ifdef _WIN32
    return runCommand(command);
#else
    return runCommandWithInput(command, normalizeCliStatement(sql));
#endif
  }

  std::string runPersistentJsonQuery(std::string_view sql) {
    ensureOpen();
#ifdef _WIN32
    const auto result = runCli(sql);
    if (result.exit_code != 0) {
      throwForCommandError(credential_, sql, result.exit_code, result.output);
    }
    return trimOutput(result.output);
#else
    return session().runJsonQuery(sql);
#endif
  }
};

Connection::Connection(const CredentialNone& credential, const Engine& engine,
                       std::optional<std::string> artifacts_path)
    : ConnectionBase(credential, engine, std::move(artifacts_path)), impl_(std::make_unique<Pimpl>(credential)) {}

Connection::~Connection() = default;

const ConnectionBase::TypeMap& Connection::typeMap() const {
  static const TypeMap map = {};
  return map;
}

void Connection::execute(std::string_view statement) {
  const auto result = impl_->runCli(statement, true);
  if (result.exit_code != 0) {
    throwForCommandError(credential, statement, result.exit_code, result.output);
  }
}

std::unique_ptr<ResultBase> Connection::fetchJsonQuery(std::string_view statement) {
  const auto raw_output = trimOutput(impl_->runPersistentJsonQuery(statement));
  ordered_json payload;
  try {
    payload = ordered_json::parse(raw_output);
  } catch (const std::exception& e) {
    throw ProtocolException("Failed to parse DataFusion JSON output: " + std::string(e.what()));
  }

  if (!payload.is_array()) {
    throw ProtocolException("Expected DataFusion CLI to return a JSON array");
  }

  std::vector<std::vector<SqlVariant>> rows;
  std::vector<std::string> column_names;
  std::vector<SqlTypeKind> column_types;
  for (const auto& row_json : payload) {
    if (!row_json.is_object()) {
      throw ProtocolException("Expected DataFusion row payload to be an object");
    }

    if (column_names.empty()) {
      for (auto it = row_json.begin(); it != row_json.end(); ++it) {
        column_names.push_back(it.key());
        column_types.push_back(SqlTypeKind::SQL_NULL);
      }
    }

    std::vector<SqlVariant> row;
    row.reserve(column_names.size());
    for (size_t index = 0; index < column_names.size(); ++index) {
      const auto& value = row_json.at(column_names[index]);
      auto variant = jsonValueToVariant(value);
      if (column_types[index] == SqlTypeKind::SQL_NULL && !variant.is<SqlNull>()) {
        column_types[index] = inferType(variant);
      }
      row.push_back(std::move(variant));
    }
    rows.push_back(std::move(row));
  }

  return std::make_unique<Result>(std::move(rows), std::move(column_types));
}

std::unique_ptr<ResultBase> Connection::fetchAll(std::string_view statement) { return fetchJsonQuery(statement); }

void Connection::bulkLoad(const std::string_view table, const std::vector<std::filesystem::path> source_paths) {
  validateSourcePaths(source_paths);
  throw NotImplementedException(
      "The DataFusion driver uses a pre-mounted dataset container and does not implement bulk load");
}

void Connection::analyse(std::string_view table_name) {
  const auto statement = "SELECT COUNT(*) FROM " + std::string(table_name);
  execute(statement);
}

bool Connection::shouldSkipDatasetTuning(std::string_view dataset) { return dataset == "tpch"; }

std::string Connection::version() { return fetchScalar("SELECT version()").asString(); }

void Connection::close() { impl_->close(); }

json Connection::fetchPhysicalPlanJson(std::string_view statement) const {
  const auto result = impl_->runPhysicalPlanHelper(statement);
  if (result.exit_code != 0) {
    throwForCommandError(credential, statement, result.exit_code, result.output);
  }

  try {
    return json::parse(trimOutput(result.output));
  } catch (const std::exception& e) {
    throw ProtocolException("Failed to parse DataFusion physical plan JSON: " + std::string(e.what()) + "\n" +
                            result.output);
  }
}

}  // namespace sql::datafusion
