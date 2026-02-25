#include <iostream>

#include "connection.h"
#include <dbprove/sql/explain/plan.h>
#include "select.h"

#include <curl/curl.h>
#include <nlohmann/json.hpp>

namespace sql::databricks {
using namespace sql::explain;
using nlohmann::json;

namespace {
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* s) {
  size_t newLength = size * nmemb;
  try {
    s->append(static_cast<char*>(contents), newLength);
    return newLength;
  } catch (std::bad_alloc&) {
    return 0;
  }
}
}

std::unique_ptr<Plan> Connection::explain(const std::string_view statement) {
  // For initial validation: call the Statements API directly, print raw JSON, and return an empty plan.
  const std::string explain_stmt = std::string("EXPLAIN EXTENDED ") + std::string(statement);

  try {
    CURL* curl = curl_easy_init();
    if (!curl) {
      throw std::runtime_error("Failed to initialize curl in Databricks::explain");
    }

    std::string endpoint = token_.endpoint_url;
    if (endpoint.rfind("http", 0) != 0) {
      endpoint = "https://" + endpoint;
    }

    // Build payload
    json payload = {
        {"statement", explain_stmt},
        {"warehouse_id", token_.database},
        {"wait_timeout", "30s"},
        {"disposition", "INLINE"}
    };
    const std::string jsonPayload = payload.dump();

    std::string readBuffer;
    curl_easy_setopt(curl, CURLOPT_URL, endpoint.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, jsonPayload.c_str());

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Accept: application/json");
    const std::string authHeader = std::string("Authorization: Bearer ") + token_.token;
    headers = curl_slist_append(headers, authHeader.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    const CURLcode res = curl_easy_perform(curl);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
      std::cerr << "curl_easy_perform() failed in explain: " << curl_easy_strerror(res) << std::endl;
    } else {
      // Print raw JSON
      try {
        auto resp = json::parse(readBuffer);
        std::cout << "Databricks EXPLAIN EXTENDED raw JSON:" << std::endl;
        std::cout << resp.dump(2) << std::endl;
        // Convenience: print first cell if present
        if (resp.contains("result") && resp["result"].contains("data_array") &&
            resp["result"]["data_array"].is_array() && !resp["result"]["data_array"].empty() &&
            resp["result"]["data_array"][0].is_array() && !resp["result"]["data_array"][0].empty()) {
          const auto plan_text = resp["result"]["data_array"][0][0].get<std::string>();
          std::cout << "\nDatabricks EXPLAIN EXTENDED text (first cell):\n" << plan_text << std::endl;
        }
      } catch (const std::exception& ex) {
        std::cout << "Databricks EXPLAIN EXTENDED returned non-JSON or unexpected payload:\n" << readBuffer << std::endl;
      }
    }
  } catch (const std::exception& ex) {
    std::cout << "Failed to retrieve EXPLAIN EXTENDED from Databricks: " << ex.what() << std::endl;
  }

  // Return an empty minimal plan for now so we can wire up the flow end-to-end
  auto root = std::make_unique<Select>();
  return std::make_unique<Plan>(std::move(root));
}

}
