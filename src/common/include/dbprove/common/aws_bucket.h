#pragma once

#include <filesystem>
#include <string>
#include <string_view>

namespace dbprove::common {
class AWSBucket {
  std::string bucket_uri_;

public:
  explicit AWSBucket(std::string bucket_uri);

  [[nodiscard]] const std::string& bucketUri() const;
  void downloadFile(std::string_view object_path, const std::filesystem::path& destination_path) const;
};
}
