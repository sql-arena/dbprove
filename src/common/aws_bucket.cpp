#include "include/dbprove/common/aws_bucket.h"

#include "include/dbprove/common/string.h"

#include <array>
#include <cstdlib>
#include <cstdio>
#include <filesystem>
#include <stdexcept>
#include <string>

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

struct CommandResult {
  int exit_code = -1;
  std::string output;

  [[nodiscard]] bool succeeded() const {
    return exit_code == 0;
  }
};

std::string trimOutput(std::string output) {
  while (!output.empty() && (output.back() == '\n' || output.back() == '\r')) {
    output.pop_back();
  }
  return output;
}

CommandResult runCommand(const std::string& command) {
  CommandResult result;
  std::array<char, 4096> buffer{};

  FILE* pipe = dbprove_popen((command + " 2>&1").c_str(), "r");
  if (pipe == nullptr) {
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

std::string activeAwsProfileName() {
  if (const char* profile = std::getenv("AWS_PROFILE"); profile != nullptr && *profile != '\0') {
    return profile;
  }
  return "default";
}

void ensureAwsCliPresent() {
  const auto result = runCommand("command -v aws");
  if (result.succeeded()) {
    return;
  }
  throw std::runtime_error(
      "AWS CLI was not found on PATH, but AWSBucket requires it for downloads.\n"
      "Please install the aws cli and retry.");
}

std::string formatAwsAccessError(const CommandResult& result) {
  const auto profile = activeAwsProfileName();
  const auto output = trimOutput(result.output);

  if (output.find("The SSO session associated with this profile has expired") != std::string::npos
      || output.find("Token has expired and refresh failed") != std::string::npos) {
    return "AWS profile '" + profile + "' is not currently authenticated.\n"
           "Run 'aws sso login --profile " + profile + "' and retry.\n"
           "aws output:\n" + output;
  }

  if (output.find("Unable to locate credentials") != std::string::npos
      || output.find("Unable to load credentials") != std::string::npos
      || output.find("could not be found") != std::string::npos) {
    return "AWS credentials were not available for profile '" + profile + "'.\n"
           "Set AWS_PROFILE to a valid profile or configure AWS credentials, then retry.\n"
           "aws output:\n" + output;
  }

  if (output.find("The config profile") != std::string::npos && output.find("could not be found") != std::string::npos) {
    return "AWS profile '" + profile + "' does not exist in your AWS config.\n"
           "Choose a valid AWS profile and retry.\n"
           "aws output:\n" + output;
  }

  return "AWS access check failed for profile '" + profile + "'.\n"
         "aws output:\n" + output;
}

bool isAwsAuthFailure(const CommandResult& result) {
  const auto output = trimOutput(result.output);
  return output.find("The SSO session associated with this profile has expired") != std::string::npos
      || output.find("Token has expired and refresh failed") != std::string::npos
      || output.find("Unable to locate credentials") != std::string::npos
      || output.find("Unable to load credentials") != std::string::npos
      || output.find("The config profile") != std::string::npos
      || output.find("could not be found") != std::string::npos
      || output.find("InvalidAccessKeyId") != std::string::npos
      || output.find("ExpiredToken") != std::string::npos
      || output.find("AccessDenied") != std::string::npos;
}
}

AWSBucket::AWSBucket(std::string bucket_uri)
  : bucket_uri_(std::move(bucket_uri)) {
}

const std::string& AWSBucket::bucketUri() const {
  return bucket_uri_;
}

void AWSBucket::downloadFile(std::string_view object_path, const std::filesystem::path& destination_path) const {
  ensureAwsCliPresent();

  const std::string source = bucket_uri_ + "/" + std::string(object_path);
  const std::string destination = shell_quote(destination_path.string());
  const std::string signed_command = "aws s3 cp " + shell_quote(source) + " " + destination;
  const auto signed_result = runCommand(signed_command);
  if (signed_result.succeeded()) {
    return;
  }

  if (isAwsAuthFailure(signed_result)) {
    const std::string anonymous_command = "aws s3 cp --no-sign-request " + shell_quote(source) + " " + destination;
    const auto anonymous_result = runCommand(anonymous_command);
    if (anonymous_result.succeeded()) {
      return;
    }

    std::filesystem::remove(destination_path);
    throw std::runtime_error("AWS bucket download failed for " + source
                             + ". Signed request failed with an authentication error and anonymous retry also failed.\n"
                             + "signed aws output:\n" + trimOutput(signed_result.output)
                             + "\nanonymous aws output:\n" + trimOutput(anonymous_result.output));
  }

  std::filesystem::remove(destination_path);
  throw std::runtime_error("AWS bucket download failed for " + source + ".\naws output:\n" + trimOutput(signed_result.output));
}
}
